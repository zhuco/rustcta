use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use base64::{engine::general_purpose, Engine as _};
use rustcta_exchange_api::{
    AmendOrderRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, OpenOrdersRequest,
    OrderListConditionalLeg, OrderListLegType, OrderListRequest, PlaceOrderRequest,
    PublicStreamKind, PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest,
    RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{
    btcturk_symbol, parse_btcturk_open_orders, parse_btcturk_order_book, parse_btcturk_order_state,
    parse_btcturk_recent_fills, parse_btcturk_symbol_rules,
};
use super::private::{
    open_orders_request_spec_fixture, place_order_request_spec_fixture,
    query_order_request_spec_fixture, recent_fills_request_spec_fixture,
};
use super::signing::btcturk_signature;
use super::streams::{btcturk_subscribe_payload, btcturk_ws_auth_payload};
use super::{BtcTurkGatewayAdapter, BtcTurkGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("btcturk").expect("exchange")
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("account").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn symbol() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "TRY").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTCTRY")
            .expect("symbol"),
    }
}

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    query: HashMap<String, String>,
    body: Option<Value>,
    headers: HashMap<String, String>,
}

impl SeenRequest {
    fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .get(&key.to_ascii_lowercase())
            .map(String::as_str)
    }
}

async fn spawn_rest_server(responses: Vec<Value>) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_requests = Arc::clone(&seen);
    let responses = Arc::new(Mutex::new(responses.into_iter()));

    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut buffer = vec![0_u8; 8192];
            let bytes_read = stream.read(&mut buffer).await.unwrap();
            let request_text = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
            seen_requests
                .lock()
                .unwrap()
                .push(parse_seen_request(&request_text));
            let body = responses
                .lock()
                .unwrap()
                .next()
                .unwrap_or_else(|| serde_json::json!({}));
            let body_text = body.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body_text.len(),
                body_text
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    (format!("http://{address}"), seen)
}

fn parse_seen_request(request_text: &str) -> SeenRequest {
    let request_line = request_text.lines().next().unwrap_or_default();
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default().to_string();
    let target = request_parts.next().unwrap_or_default();
    let (path, query_text) = target.split_once('?').unwrap_or((target, ""));
    let query = query_text
        .split('&')
        .filter(|part| !part.is_empty())
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            Some((key.to_string(), value.to_string()))
        })
        .collect();
    let headers = request_text
        .lines()
        .skip(1)
        .take_while(|line| !line.trim().is_empty())
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    let body = request_text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.trim())
        .filter(|body| !body.is_empty())
        .and_then(|body| serde_json::from_str(body).ok());
    SeenRequest {
        method,
        path: path.to_string(),
        query,
        body,
        headers,
    }
}

#[test]
fn parser_should_normalize_try_markets_and_orderbook() {
    let exchange_info: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcturk/exchange_info.json"
    ))
    .expect("fixture");
    let rules = parse_btcturk_symbol_rules(exchange_id(), &[], &exchange_info).expect("rules");
    assert!(rules
        .iter()
        .any(|rule| rule.base_asset == "BTC" && rule.quote_asset == "TRY"));
    assert!(rules
        .iter()
        .any(|rule| rule.base_asset == "ETH" && rule.quote_asset == "TRY"));

    let orderbook: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcturk/orderbook.json"
    ))
    .expect("fixture");
    let snapshot = parse_btcturk_order_book(&symbol(), &orderbook).expect("orderbook");
    assert_eq!(snapshot.bids[0].price, 2_499_000.0);
    assert_eq!(btcturk_symbol(&symbol()), "BTCTRY");
}

#[test]
fn private_readback_parsers_should_cover_orders_and_fills() {
    let order_value: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcturk/order_status_success.json"
    ))
    .expect("order fixture");
    let order = parse_btcturk_order_state(&exchange_id(), Some(&symbol()), &order_value)
        .expect("order state");
    assert_eq!(order.exchange_order_id.as_deref(), Some("123456789"));
    assert_eq!(order.client_order_id.as_deref(), Some("offline-fixture"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.side, OrderSide::Buy);
    assert_eq!(order.filled_quantity, "0.00400000");

    let open_value: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcturk/open_orders_success.json"
    ))
    .expect("open orders fixture");
    let open =
        parse_btcturk_open_orders(&exchange_id(), Some(&symbol()), &open_value).expect("open");
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].status, OrderStatus::Open);

    let fills_value: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcturk/fills.json"
    ))
    .expect("fills fixture");
    let fills = parse_btcturk_recent_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &symbol(),
        &fills_value,
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("987654321"));
    assert_eq!(fills[0].order_id.as_deref(), Some("123456789"));
    assert_eq!(fills[0].fee_asset.as_deref(), Some("TRY"));
}

#[test]
fn request_spec_and_signing_should_match_hmac_boundary() {
    let spec = place_order_request_spec_fixture();
    assert_eq!(spec["path"], "/api/v1/order");
    assert_eq!(spec["auth"], "btcturk_hmac_sha256_base64");
    assert_eq!(spec["body"]["pairSymbol"], "BTCTRY");

    let query_spec = query_order_request_spec_fixture();
    assert_eq!(query_spec["method"], "GET");
    assert_eq!(query_spec["path"], "/api/v1/order/123456789");
    assert_eq!(query_spec["headers"]["Accept"], "application/json");

    let open_spec = open_orders_request_spec_fixture();
    assert_eq!(open_spec["path"], "/api/v1/openOrders");
    assert_eq!(open_spec["query"]["pairSymbol"], "BTCTRY");

    let fills_spec = recent_fills_request_spec_fixture();
    assert_eq!(fills_spec["path"], "/api/v1/users/transactions/trade");
    assert_eq!(fills_spec["query"]["orderId"], "123456789");

    let secret = general_purpose::STANDARD.encode(b"test-secret");
    let signature = btcturk_signature("public-key", &secret, 1_700_000_000_000).expect("signature");
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcturk/signing_vectors/rest_hmac_sha256.json"
    ))
    .expect("fixture");
    assert_eq!(fixture["signature"], signature);
}

#[tokio::test]
async fn private_write_should_be_offline_request_spec_only() {
    let adapter = BtcTurkGatewayAdapter::new(BtcTurkGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_query_order);
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("2500000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "btcturk.place_order_offline_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn private_readbacks_should_fail_closed_without_credentials() {
    let adapter = BtcTurkGatewayAdapter::new(BtcTurkGatewayConfig {
        enabled_private_rest: true,
        api_key: None,
        api_secret: None,
        ..Default::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_query_order);

    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("123456789".to_string()),
        })
        .await
        .expect_err("private readback disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "btcturk.query_order"
        }
    ));
}

#[tokio::test]
async fn private_readbacks_should_sign_get_requests_when_enabled() {
    let query_response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcturk/order_status_success.json"
    ))
    .expect("query order response");
    let open_response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcturk/open_orders_success.json"
    ))
    .expect("open orders response");
    let fills_response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcturk/fills.json"
    ))
    .expect("recent fills response");
    let (base_url, seen) =
        spawn_rest_server(vec![query_response, open_response, fills_response]).await;
    let adapter = BtcTurkGatewayAdapter::new(BtcTurkGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("btcturk-key".to_string()),
        api_secret: Some(general_purpose::STANDARD.encode(b"btcturk-secret")),
        enabled_private_rest: true,
        ..Default::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("123456789".to_string()),
        })
        .await
        .expect("query order")
        .order
        .expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("123456789"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol()),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);
    assert_eq!(open.orders[0].status, OrderStatus::Open);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol()),
            client_order_id: None,
            exchange_order_id: Some("123456789".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect("recent fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("987654321"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/api/v1/order/123456789");
    assert!(requests[0].query.is_empty());
    assert_eq!(requests[1].method, "GET");
    assert_eq!(requests[1].path, "/api/v1/openOrders");
    assert_eq!(
        requests[1].query.get("pairSymbol").map(String::as_str),
        Some("BTCTRY")
    );
    assert_eq!(requests[2].method, "GET");
    assert_eq!(requests[2].path, "/api/v1/users/transactions/trade");
    assert_eq!(
        requests[2].query.get("pairSymbol").map(String::as_str),
        Some("BTCTRY")
    );
    assert_eq!(
        requests[2].query.get("orderId").map(String::as_str),
        Some("123456789")
    );
    assert_eq!(
        requests[2].query.get("limit").map(String::as_str),
        Some("100")
    );
    for request in &requests {
        assert_eq!(request.header("x-pck"), Some("btcturk-key"));
        assert!(request.header("x-stamp").is_some());
        assert!(request.header("x-signature").is_some());
        assert_eq!(request.header("accept"), Some("application/json"));
        assert!(request.body.is_none());
    }
}

#[tokio::test]
async fn advanced_order_surfaces_should_remain_explicitly_unsupported() {
    let adapter = BtcTurkGatewayAdapter::new(BtcTurkGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_cancel_all_orders);

    let symbol = symbol();
    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-item"),
        symbol: symbol.clone(),
        client_order_id: Some("client-batch-1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("2500000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol.clone(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("123456789".to_string()),
            new_client_order_id: None,
            new_quantity: "0.02".to_string(),
        })
        .await
        .expect_err("amend unsupported");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: "btcturk.amend_order_unsupported"
        }
    ));

    let list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oco"),
            symbol: symbol.clone(),
            list_client_order_id: Some("oco-1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some("2600000".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("2400000".to_string()),
                stop_price: Some("2450000".to_string()),
                time_in_force: None,
                client_order_id: Some("oco-below".to_string()),
            },
        })
        .await
        .expect_err("order-list unsupported");
    assert!(matches!(
        list_error,
        ExchangeApiError::Unsupported {
            operation: "btcturk.order_list_unsupported"
        }
    ));

    let batch_place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![order],
        })
        .await
        .expect_err("batch place unsupported");
    assert!(matches!(
        batch_place_error,
        ExchangeApiError::Unsupported {
            operation: "btcturk.batch_place_orders_unsupported"
        }
    ));

    let cancel = CancelOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("cancel-item"),
        symbol: symbol.clone(),
        client_order_id: None,
        exchange_order_id: Some("123456789".to_string()),
    };
    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![cancel],
        })
        .await
        .expect_err("batch cancel unsupported");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: "btcturk.batch_cancel_orders_unsupported"
        }
    ));

    let cancel_all_error = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol),
        })
        .await
        .expect_err("cancel-all unsupported");
    assert!(matches!(
        cancel_all_error,
        ExchangeApiError::Unsupported {
            operation: "btcturk.cancel_all_orders_unsupported"
        }
    ));

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcturk/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["advanced_orders"]["runtime_enabled"], false);
    assert_eq!(boundary["advanced_orders"]["native_batch"], false);
}

#[test]
fn websocket_helpers_should_cover_public_and_private_payload_shapes() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload =
        btcturk_subscribe_payload("orderbook", &subscription.symbol.exchange_symbol.symbol);
    assert_eq!(payload[0], 151);
    assert_eq!(payload[1]["event"], "BTCTRY");

    let auth = btcturk_ws_auth_payload("public-key", 1_700_000_000_000, 3000, "sig");
    assert_eq!(auth[0], 114);
    assert_eq!(auth[1]["publicKey"], "public-key");
}
