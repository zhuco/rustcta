use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    AmendOrderRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelOrderRequest,
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, OrderListConditionalLeg, OrderListLegType,
    OrderListRequest, PlaceOrderRequest, PublicStreamKind, PublicStreamSubscription,
    QueryOrderRequest, RecentFillsRequest, RequestContext, SymbolScope, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};
use serde_json::json;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{
    bitso_book, bitso_canonical_pair, parse_order_ack_id, parse_order_book_shape,
    parse_symbol_rules,
};
use super::private::{
    bitso_cancel_order_path, bitso_place_order_body, create_order_request_spec_fixture,
};
use super::private_parser::{parse_balance_assets, parse_fill_ids, parse_open_order_ids};
use super::signing::{bitso_authorization_header, bitso_hmac_signature, bitso_signature_payload};
use super::streams::{
    bitso_diff_orders_sequence, bitso_diff_orders_sequence_is_contiguous,
    bitso_public_order_book_ws_policy, bitso_public_subscribe_payload, bitso_reconnect_policy_ms,
};
use super::{BitsoGatewayAdapter, BitsoGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bitso").expect("exchange")
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

fn symbol(symbol: &str) -> SymbolScope {
    let (base, quote) = bitso_canonical_pair(symbol).expect("canonical pair");
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, symbol)
            .expect("symbol"),
    }
}

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    query: HashMap<String, String>,
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
                .unwrap_or_else(|| json!({"success": true, "payload": []}));
            let body_text = body.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body_text.len(),
                body_text
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    (format!("http://{address}/api/v3"), seen)
}

fn parse_seen_request(request_text: &str) -> SeenRequest {
    let request_line = request_text.lines().next().unwrap_or_default();
    let method = request_line
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string();
    let target = request_line.split_whitespace().nth(1).unwrap_or_default();
    let (path, query_text) = target.split_once('?').unwrap_or((target, ""));
    let query = query_text
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (key.to_string(), value.to_string())
        })
        .collect();
    let headers = request_text
        .lines()
        .skip(1)
        .take_while(|line| !line.trim().is_empty())
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    SeenRequest {
        method,
        path: path.to_string(),
        query,
        headers,
    }
}

fn private_config(rest_base_url: String) -> BitsoGatewayConfig {
    BitsoGatewayConfig {
        rest_base_url,
        api_key: Some("bitso-key".to_string()),
        api_secret: Some("bitso-secret".to_string()),
        enabled_private_rest: true,
        ..BitsoGatewayConfig::default()
    }
}

#[test]
fn symbol_normalization_should_cover_latin_america_fiat_quotes() {
    assert_eq!(bitso_book("BTC/MXN"), "btc_mxn");
    assert_eq!(bitso_book("btc-brl"), "btc_brl");
    assert_eq!(
        bitso_canonical_pair("ETHARS").expect("ars"),
        ("ETH".to_string(), "ARS".to_string())
    );
    assert_eq!(
        bitso_canonical_pair("BTC/BRL").expect("brl"),
        ("BTC".to_string(), "BRL".to_string())
    );
}

#[test]
fn request_spec_and_signing_should_stay_secret_free() {
    let spec = create_order_request_spec_fixture();
    assert_eq!(spec["path"], "/api/v3/orders");
    assert_eq!(spec["body"]["book"], "btc_mxn");
    assert_eq!(
        spec["headers"]["Authorization"],
        "Bitso <key>:<nonce>:<signature>"
    );

    let payload = bitso_signature_payload(
        1_700_000_000_000,
        "post",
        "/api/v3/orders",
        r#"{"book":"btc_mxn","side":"buy"}"#,
    );
    let signature = bitso_hmac_signature("test-secret", &payload).expect("signature");
    assert_eq!(signature.len(), 64);
    assert_eq!(
        bitso_authorization_header("key", 1_700_000_000_000, &signature),
        format!("Bitso key:1700000000000:{signature}")
    );

    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/signing_vectors/rest_place_order.json"
    ))
    .expect("fixture");
    assert_eq!(fixture["payload"], payload);
}

#[test]
fn place_order_body_should_use_major_minor_and_fiat_book_names() {
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTC/MXN"),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("650000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let body = bitso_place_order_body(&request).expect("body");
    assert_eq!(body["book"], "btc_mxn");
    assert_eq!(body["major"], "0.01");
    assert_eq!(body["price"], "650000");
    assert_eq!(body["time_in_force"], "goodtillcancelled");

    let mut market = request;
    market.order_type = OrderType::Market;
    market.price = None;
    market.quote_quantity = Some("1000".to_string());
    let body = bitso_place_order_body(&market).expect("market");
    assert_eq!(body["minor"], "1000");
    assert!(body.get("price").is_none());
}

#[test]
fn fixtures_should_parse_orderbook_order_ack_and_boundary() {
    let order_book: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/orderbook_btc_mxn.json"
    ))
    .expect("orderbook");
    let (bids, asks, sequence) = parse_order_book_shape(&order_book).expect("shape");
    assert_eq!((bids, asks), (1, 1));
    assert_eq!(sequence.as_deref(), Some("27214"));

    let order_ack: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/order_ack.json"
    ))
    .expect("ack");
    assert_eq!(
        parse_order_ack_id(&order_ack).expect("oid"),
        "qlbga6b600n3xta7"
    );

    let boundary: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["funding_operations"], "unsupported");
    assert_eq!(boundary["private_write_mode"], "offline_request_spec_only");

    let books: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/available_books_success.json"
    ))
    .expect("books");
    let rules = parse_symbol_rules(&exchange_id(), &[], &books).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].quote_asset, "MXN");
}

#[test]
fn private_read_fixtures_should_cover_balances_open_orders_and_fills() {
    let balances: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/balances.json"
    ))
    .expect("balances");
    assert_eq!(
        parse_balance_assets(&balances).expect("assets"),
        vec!["BTC", "MXN"]
    );

    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/open_orders.json"
    ))
    .expect("open orders");
    assert_eq!(
        parse_open_order_ids(&open_orders).expect("orders"),
        vec!["offline-order-1"]
    );

    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/fills.json"
    ))
    .expect("fills");
    assert_eq!(parse_fill_ids(&fills).expect("fills"), vec!["1000001"]);
    assert_eq!(
        bitso_cancel_order_path("offline-order-1"),
        "/orders/offline-order-1"
    );
}

#[test]
fn websocket_helpers_should_map_bitso_channels() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol("BTC/MXN"),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let payload = bitso_public_subscribe_payload(&subscription);
    assert_eq!(payload["action"], "subscribe");
    assert_eq!(payload["book"], "btc_mxn");
    assert_eq!(payload["type"], "orders");
    assert_eq!(bitso_reconnect_policy_ms(), (30_000, 45_000, 60_000));

    let policy = bitso_public_order_book_ws_policy();
    assert_eq!(policy.url, "wss://ws.bitso.com");
    assert_eq!(policy.protocol, "json_websocket");
    assert_eq!(policy.snapshot_channel, "orders");
    assert_eq!(policy.delta_channel, "diff-orders");
    assert_eq!(policy.snapshot_depth_per_side, 20);
    assert_eq!(policy.fixed_update_interval_ms, None);
    assert_eq!(policy.delta_sequence_field, "sequence");
    assert_eq!(policy.checksum, None);
    assert!(policy.resync.contains("REST order_book snapshot"));
}

#[test]
fn websocket_orderbook_fixtures_should_cover_orders_and_diff_orders_sequence() {
    let orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/ws_public_orders.json"
    ))
    .expect("orders fixture");
    assert_eq!(orders["type"], "orders");
    assert_eq!(orders["book"], "btc_mxn");
    assert_eq!(orders["payload"]["bids"].as_array().expect("bids").len(), 1);
    assert_eq!(orders["payload"]["asks"].as_array().expect("asks").len(), 1);

    let diff_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/ws_public_diff_orders.json"
    ))
    .expect("diff-orders fixture");
    assert_eq!(diff_orders["type"], "diff-orders");
    assert_eq!(diff_orders["book"], "btc_mxn");
    assert_eq!(
        bitso_diff_orders_sequence(&diff_orders).expect("sequence"),
        27215
    );
    assert!(bitso_diff_orders_sequence_is_contiguous(Some(27214), 27215));
    assert!(!bitso_diff_orders_sequence_is_contiguous(
        Some(27214),
        27216
    ));
}

#[tokio::test]
async fn live_trading_surfaces_should_remain_explicitly_unsupported() {
    let adapter = BitsoGatewayAdapter::new(BitsoGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_query_order);
    assert!(!capabilities.supports_open_orders);
    assert!(!capabilities.supports_recent_fills);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTC/MXN"),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("650000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bitso.place_order_offline_request_spec_only"
        }
    ));

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol("BTC/MXN"),
            client_order_id: Some("offline-fixture".to_string()),
            exchange_order_id: Some("qlbga6b600n3xta7".to_string()),
            new_client_order_id: None,
            new_quantity: "0.02".to_string(),
        })
        .await
        .expect_err("amend unsupported");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: "bitso.replace_order_not_promoted"
        }
    ));

    let order_list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oco"),
            symbol: symbol("BTC/MXN"),
            list_client_order_id: Some("oco-1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some("700000".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("600000".to_string()),
                stop_price: Some("610000".to_string()),
                time_in_force: None,
                client_order_id: Some("oco-below".to_string()),
            },
        })
        .await
        .expect_err("order-list unsupported");
    assert!(matches!(
        order_list_error,
        ExchangeApiError::Unsupported {
            operation: "bitso.order_list_unsupported"
        }
    ));

    let batch_place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![PlaceOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("batch-place-order"),
                symbol: symbol("BTC/MXN"),
                client_order_id: Some("batch-place-1".to_string()),
                side: OrderSide::Buy,
                position_side: None,
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: "0.01".to_string(),
                price: Some("650000".to_string()),
                quote_quantity: None,
                reduce_only: false,
                post_only: false,
            }],
        })
        .await
        .expect_err("batch place unsupported");
    assert!(matches!(
        batch_place_error,
        ExchangeApiError::Unsupported {
            operation: "bitso.batch_place_orders_unsupported"
        }
    ));

    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel"),
                symbol: symbol("BTC/MXN"),
                client_order_id: Some("offline-fixture".to_string()),
                exchange_order_id: Some("qlbga6b600n3xta7".to_string()),
            }],
        })
        .await
        .expect_err("batch cancel unsupported");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: "bitso.batch_cancel_orders_unsupported"
        }
    ));
}

#[tokio::test]
async fn private_readback_should_fail_closed_without_env_guard() {
    let adapter = BitsoGatewayAdapter::new(BitsoGatewayConfig {
        api_key: Some("bitso-key".to_string()),
        api_secret: Some("bitso-secret".to_string()),
        enabled_private_rest: false,
        ..BitsoGatewayConfig::default()
    })
    .expect("adapter");
    assert!(!adapter.capabilities().supports_private_rest);

    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol("BTC/MXN"),
            client_order_id: None,
            exchange_order_id: Some("offline-order-1".to_string()),
        })
        .await
        .expect_err("disabled private rest");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bitso.query_order"
        }
    ));
}

#[tokio::test]
async fn private_readbacks_should_use_signed_read_only_rest() {
    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/open_orders_success.json"
    ))
    .expect("open orders fixture");
    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitso/fills_success.json"
    ))
    .expect("fills fixture");
    let query_order = json!({
        "success": true,
        "payload": {
            "oid": "offline-order-1",
            "book": "btc_mxn",
            "side": "buy",
            "type": "limit",
            "status": "open",
            "original_amount": "0.01000000",
            "unfilled_amount": "0.00500000",
            "price": "650000.00",
            "origin_id": "offline-fixture",
            "created_at": "2026-06-08T00:00:00.000+00:00"
        }
    });
    let (base_url, seen) = spawn_rest_server(vec![query_order, open_orders, fills]).await;
    let adapter = BitsoGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);

    let queried = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-order"),
            symbol: symbol("BTC/MXN"),
            client_order_id: None,
            exchange_order_id: Some("offline-order-1".to_string()),
        })
        .await
        .expect("query order");
    let order = queried.order.expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("offline-order-1"));
    assert_eq!(order.client_order_id.as_deref(), Some("offline-fixture"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.filled_quantity, "0.005");

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol("BTC/MXN")),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);
    assert_eq!(
        open.orders[0].exchange_order_id.as_deref(),
        Some("offline-open-order-1")
    );

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol("BTC/MXN")),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(10),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("1000001"));
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("MXN"));

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/api/v3/orders/offline-order-1");
    assert_eq!(seen[1].path, "/api/v3/open_orders");
    assert_eq!(
        seen[1].query.get("book").map(String::as_str),
        Some("btc_mxn")
    );
    assert_eq!(seen[2].path, "/api/v3/user_trades");
    assert_eq!(
        seen[2].query.get("book").map(String::as_str),
        Some("btc_mxn")
    );
    assert_eq!(seen[2].query.get("limit").map(String::as_str), Some("10"));
    for request in seen.iter() {
        let authorization = request
            .header("Authorization")
            .expect("authorization header");
        assert!(authorization.starts_with("Bitso bitso-key:"));
        assert!(!authorization.contains("bitso-secret"));
    }
}
