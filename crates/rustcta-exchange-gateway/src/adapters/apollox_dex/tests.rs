use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    BatchAtomicity, BatchExecutionMode, BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient,
    OpenOrdersRequest, PlaceOrderRequest, PublicStreamKind, PublicStreamSubscription,
    QueryOrderRequest, RecentFillsRequest, RequestContext, SymbolRulesRequest, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{
    parse_batch_place_orders_ack, parse_open_orders, parse_order_state, parse_orderbook_snapshot,
    parse_recent_fills, parse_symbol_rules,
};
use super::streams::{apollox_parse_public_depth_event, apollox_public_unsubscribe_payload};
use super::streams::{apollox_public_subscribe_payload, apollox_ws_policy_ms};
use super::{private, signing, ApolloxDexGatewayAdapter, ApolloxDexGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("apollox_dex").expect("exchange")
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
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTCUSDT")
            .expect("symbol"),
    }
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "exchange_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/exchange_info.json"
        ),
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/apollox_dex/orderbook.json")
        }
        "unsupported_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/unsupported_boundary.json"
        ),
        "signing_vector" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/signing_vectors/place_order_hmac.json"
        ),
        "batch_signing_vector" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/signing_vectors/batch_place_orders_hmac.json"
        ),
        "batch_place_ack" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/parser/batch_place_orders_ack.json"
        ),
        "order_success" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/order_success.json"
        ),
        "open_orders_success" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/open_orders_success.json"
        ),
        "recent_fills_success" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/recent_fills_success.json"
        ),
        "ws_subscribe" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/ws/public_depth_subscribe.json"
        ),
        "ws_unsubscribe" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/ws/public_depth_unsubscribe.json"
        ),
        "ws_depth_event" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/ws/public_depth_event.json"
        ),
        _ => panic!("unknown fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "exchange_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/request_specs/exchange_info.json"
        ),
        "depth" => {
            include_str!(
                "../../../../../tests/fixtures/exchanges/apollox_dex/request_specs/depth.json"
            )
        }
        "place_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/request_specs/place_order.json"
        ),
        "query_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/request_specs/query_order.json"
        ),
        "open_orders" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/request_specs/open_orders.json"
        ),
        "get_recent_fills" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/request_specs/get_recent_fills.json"
        ),
        "get_fees" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/request_specs/get_fees.json"
        ),
        "get_positions" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/request_specs/get_positions.json"
        ),
        "batch_place_orders" => include_str!(
            "../../../../../tests/fixtures/exchanges/apollox_dex/request_specs/batch_place_orders.json"
        ),
        _ => panic!("unknown request spec {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn limit_order(request_id: &str, client_order_id: &str) -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(request_id),
        symbol: symbol(),
        client_order_id: Some(client_order_id.to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("65000.5".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    query: BTreeMap<String, String>,
    headers: BTreeMap<String, String>,
}

impl SeenRequest {
    fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .get(&key.to_ascii_lowercase())
            .map(String::as_str)
    }
}

async fn spawn_rest_server(response: Value) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_requests = Arc::clone(&seen);
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buffer = vec![0_u8; 8192];
            let bytes_read = stream.read(&mut buffer).await.unwrap();
            let request_text = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
            seen_requests
                .lock()
                .unwrap()
                .push(parse_seen_request(&request_text));
            let body = response.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    (format!("http://{address}"), seen)
}

fn parse_seen_request(request_text: &str) -> SeenRequest {
    let request_line = request_text.lines().next().unwrap_or_default();
    let target = request_line.split_whitespace().nth(1).unwrap_or_default();
    let (path, query_text) = target.split_once('?').unwrap_or((target, ""));
    let query = query_text
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (
                key.to_string(),
                urlencoding::decode(value).unwrap_or_default().into_owned(),
            )
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
        method: request_line
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string(),
        path: path.to_string(),
        query,
        headers,
    }
}

#[test]
fn parser_should_parse_apollox_perpetual_exchange_info() {
    let rules = parse_symbol_rules(&exchange_id(), &fixture("exchange_info")).expect("rules");

    assert_eq!(rules.len(), 2);
    let btc = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "BTCUSDT")
        .expect("btc rule");
    assert_eq!(btc.symbol.market_type, MarketType::Perpetual);
    assert_eq!(btc.base_asset, "BTC");
    assert_eq!(btc.quote_asset, "USDT");
    assert_eq!(btc.price_increment.as_deref(), Some("0.10"));
    assert_eq!(btc.quantity_increment.as_deref(), Some("0.001"));
    assert_eq!(btc.min_quantity.as_deref(), Some("0.001"));
    assert_eq!(btc.min_notional.as_deref(), Some("5"));
    assert_eq!(btc.price_precision, Some(1));
    assert_eq!(btc.quantity_precision, Some(3));
    assert!(btc.supports_post_only);
    assert!(btc.supports_reduce_only);
}

#[test]
fn parser_should_parse_apollox_orderbook_snapshot() {
    let snapshot =
        parse_orderbook_snapshot(&exchange_id(), symbol(), &fixture("orderbook")).expect("book");

    assert_eq!(snapshot.best_bid().expect("bid").price, 65000.1);
    assert_eq!(snapshot.best_bid().expect("bid").quantity, 1.25);
    assert_eq!(snapshot.best_ask().expect("ask").price, 65001.2);
    assert_eq!(snapshot.sequence, Some(987654321));
    assert!(snapshot.exchange_timestamp.is_some());
}

#[test]
fn capabilities_should_enable_credential_gated_batch_place_runtime() {
    let mut config = ApolloxDexGatewayConfig::default();
    config.api_key = Some("test-key".to_string());
    config.api_secret = Some("test-secret".to_string());
    config.enabled_private_rest = true;
    config.enabled_public_streams = true;
    let adapter = ApolloxDexGatewayAdapter::new(config).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_place_order);
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(capabilities.capabilities_v2.private_rest.is_supported());
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.max_items,
        Some(5)
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_place_orders
            .supports_partial_failure
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_symbol_rules"
            && endpoint.path.as_deref() == Some("/fapi/v1/exchangeInfo")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_place_orders"
            && endpoint.method.as_deref() == Some("POST")
            && endpoint.path.as_deref() == Some("/fapi/v1/batchOrders")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_cancel_orders"
            && endpoint.path.as_deref() == Some("/unsupported/apollox_dex/batch_cancel_orders")));

    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["v1_orderbook_perp"], true);
    assert_eq!(boundary["v2_onchain_trading_enabled"], false);
    assert_eq!(boundary["private_write_runtime_enabled"], false);
    assert_eq!(boundary["batch_place_orders_native_spec_only"], true);
    assert_eq!(boundary["batch_place_orders_parser_fixture"], true);
    assert!(boundary["advanced_order_unsupported"]
        .as_array()
        .expect("advanced unsupported")
        .iter()
        .any(|operation| operation == "batch_cancel_orders"));
}

#[tokio::test]
async fn private_order_methods_should_return_spec_only_boundary() {
    let adapter =
        ApolloxDexGatewayAdapter::new(ApolloxDexGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: Some("cli-apollox-place".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("65000.5".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = adapter
        .place_order(request)
        .await
        .expect_err("place order unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: private::PLACE_ORDER_UNSUPPORTED
        }
    ));

    let batch_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![limit_order("batch-place-1", "cli-apollox-batch-1")],
        })
        .await
        .expect_err("batch place runtime stays offline");
    assert!(matches!(
        batch_error,
        ExchangeApiError::Unsupported {
            operation: "apollox_dex.batch_place_orders_private_rest_not_enabled"
        }
    ));
}

#[tokio::test]
async fn batch_place_runtime_should_send_signed_request_and_parse_partial_report() {
    let (base_url, seen) = spawn_rest_server(fixture("batch_place_ack")).await;
    let adapter = ApolloxDexGatewayAdapter::new(ApolloxDexGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..ApolloxDexGatewayConfig::default()
    })
    .expect("adapter");
    let response = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place-runtime"),
            exchange: exchange_id(),
            orders: vec![
                limit_order("batch-place-1", "cli-apollox-batch-1"),
                limit_order("batch-place-2", "cli-apollox-batch-2"),
            ],
        })
        .await
        .expect("batch place runtime");

    assert_eq!(response.orders.len(), 1);
    let report = response.report.expect("report");
    assert_eq!(report.succeeded_count(), 1);
    assert_eq!(report.failed_count(), 1);
    assert!(report.requires_reconciliation());

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/fapi/v1/batchOrders");
    assert_eq!(requests[0].header("x-mbx-apikey"), Some("test-key"));
    assert_eq!(requests[0].query["recvWindow"], "5000");
    assert!(requests[0].query["signature"].len() >= 64);
    assert!(requests[0].query["batchOrders"].contains("cli-apollox-batch-1"));
}

#[tokio::test]
async fn private_readbacks_should_fail_closed_without_guard() {
    let adapter =
        ApolloxDexGatewayAdapter::new(ApolloxDexGatewayConfig::default()).expect("adapter");

    let query_error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("987654321".to_string()),
        })
        .await
        .expect_err("query guard");
    assert!(matches!(
        query_error,
        ExchangeApiError::Unsupported {
            operation: private::QUERY_ORDER_UNSUPPORTED
        }
    ));

    let open_error = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-disabled"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol()),
            page: None,
        })
        .await
        .expect_err("open guard");
    assert!(matches!(
        open_error,
        ExchangeApiError::Unsupported {
            operation: private::OPEN_ORDERS_UNSUPPORTED
        }
    ));

    let fills_error = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills-disabled"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol()),
            client_order_id: None,
            exchange_order_id: Some("987654321".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect_err("fills guard");
    assert!(matches!(
        fills_error,
        ExchangeApiError::Unsupported {
            operation: private::RECENT_FILLS_UNSUPPORTED
        }
    ));
}

#[tokio::test]
async fn private_readbacks_should_send_signed_get_requests_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(fixture("order_success")).await;
    let adapter = ApolloxDexGatewayAdapter::new(ApolloxDexGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..ApolloxDexGatewayConfig::default()
    })
    .expect("adapter");
    let response = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-runtime"),
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("987654321".to_string()),
        })
        .await
        .expect("query runtime");
    let order = response.order.expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("987654321"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/fapi/v1/order");
    assert_eq!(requests[0].header("x-mbx-apikey"), Some("test-key"));
    assert_eq!(requests[0].query["symbol"], "BTCUSDT");
    assert_eq!(requests[0].query["orderId"], "987654321");
    assert_eq!(requests[0].query["recvWindow"], "5000");
    assert!(requests[0].query["signature"].len() >= 64);

    let (base_url, seen) = spawn_rest_server(fixture("open_orders_success")).await;
    let adapter = ApolloxDexGatewayAdapter::new(ApolloxDexGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..ApolloxDexGatewayConfig::default()
    })
    .expect("adapter");
    let response = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-runtime"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol()),
            page: None,
        })
        .await
        .expect("open orders runtime");
    assert_eq!(response.orders.len(), 2);
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/fapi/v1/openOrders");
    assert_eq!(requests[0].query["symbol"], "BTCUSDT");
    assert_eq!(requests[0].header("x-mbx-apikey"), Some("test-key"));

    let (base_url, seen) = spawn_rest_server(fixture("recent_fills_success")).await;
    let adapter = ApolloxDexGatewayAdapter::new(ApolloxDexGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..ApolloxDexGatewayConfig::default()
    })
    .expect("adapter");
    let response = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills-runtime"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol()),
            client_order_id: None,
            exchange_order_id: Some("987654321".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect("fills runtime");
    assert_eq!(response.fills.len(), 2);
    assert_eq!(response.fills[0].fill_id.as_deref(), Some("12345"));
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/fapi/v1/userTrades");
    assert_eq!(requests[0].query["symbol"], "BTCUSDT");
    assert_eq!(requests[0].query["orderId"], "987654321");
    assert_eq!(requests[0].query["limit"], "100");
    assert_eq!(requests[0].header("x-mbx-apikey"), Some("test-key"));
}

#[test]
fn request_specs_and_signing_vector_should_match_offline_hmac_contract() {
    request_spec("exchange_info")
        .assert_matches(&ActualHttpRequest::new("GET", "/fapi/v1/exchangeInfo"))
        .expect("exchangeInfo spec");
    request_spec("depth")
        .assert_matches(
            &ActualHttpRequest::new("GET", "/fapi/v1/depth").with_query([
                ("symbol".to_string(), "BTCUSDT".to_string()),
                ("limit".to_string(), "100".to_string()),
            ]),
        )
        .expect("depth spec");

    let params = [
        ("symbol".to_string(), "BTCUSDT".to_string()),
        ("side".to_string(), "BUY".to_string()),
        ("type".to_string(), "LIMIT".to_string()),
        ("timeInForce".to_string(), "GTC".to_string()),
        ("quantity".to_string(), "0.01".to_string()),
        ("price".to_string(), "65000.5".to_string()),
        ("reduceOnly".to_string(), "false".to_string()),
        (
            "newClientOrderId".to_string(),
            "cli-apollox-place".to_string(),
        ),
        ("recvWindow".to_string(), "5000".to_string()),
        ("timestamp".to_string(), "1700000000000".to_string()),
    ];
    let (query, signature) =
        signing::apollox_signed_query("test-secret", params.clone()).expect("signature");
    let signing_vector = fixture("signing_vector");
    assert_eq!(
        query,
        signing_vector["canonical_query"].as_str().expect("query")
    );
    assert_eq!(
        signature,
        signing_vector["signature"].as_str().expect("signature")
    );

    let mut actual_query = params.into_iter().collect::<BTreeMap<_, _>>();
    actual_query.insert("signature".to_string(), signature);
    request_spec("place_order")
        .assert_matches(
            &ActualHttpRequest::new("POST", "/fapi/v1/order")
                .with_query(actual_query)
                .with_headers([
                    ("X-MBX-APIKEY".to_string(), "test-key".to_string()),
                    (
                        "Content-Type".to_string(),
                        "application/x-www-form-urlencoded".to_string(),
                    ),
                ]),
        )
        .expect("place order spec");

    request_spec("query_order")
        .assert_matches(
            &ActualHttpRequest::new("GET", "/fapi/v1/order")
                .with_query([
                    ("symbol".to_string(), "BTCUSDT".to_string()),
                    ("orderId".to_string(), "987654321".to_string()),
                    ("recvWindow".to_string(), "5000".to_string()),
                    ("timestamp".to_string(), "1700000000000".to_string()),
                    ("signature".to_string(), "fixture-signature".to_string()),
                ])
                .with_headers([("X-MBX-APIKEY".to_string(), "test-key".to_string())]),
        )
        .expect("query order spec");

    request_spec("open_orders")
        .assert_matches(
            &ActualHttpRequest::new("GET", "/fapi/v1/openOrders")
                .with_query([
                    ("symbol".to_string(), "BTCUSDT".to_string()),
                    ("recvWindow".to_string(), "5000".to_string()),
                    ("timestamp".to_string(), "1700000000000".to_string()),
                    ("signature".to_string(), "fixture-signature".to_string()),
                ])
                .with_headers([("X-MBX-APIKEY".to_string(), "test-key".to_string())]),
        )
        .expect("open orders spec");

    request_spec("get_recent_fills")
        .assert_matches(
            &ActualHttpRequest::new("GET", "/fapi/v1/userTrades")
                .with_query([
                    ("symbol".to_string(), "BTCUSDT".to_string()),
                    ("orderId".to_string(), "987654321".to_string()),
                    ("limit".to_string(), "100".to_string()),
                    ("recvWindow".to_string(), "5000".to_string()),
                    ("timestamp".to_string(), "1700000000000".to_string()),
                    ("signature".to_string(), "fixture-signature".to_string()),
                ])
                .with_headers([("X-MBX-APIKEY".to_string(), "test-key".to_string())]),
        )
        .expect("get recent fills spec");

    request_spec("get_fees")
        .assert_matches(
            &ActualHttpRequest::new("GET", "/fapi/v1/commissionRate")
                .with_query([
                    ("symbol".to_string(), "BTCUSDT".to_string()),
                    ("recvWindow".to_string(), "5000".to_string()),
                    ("timestamp".to_string(), "1700000000000".to_string()),
                    ("signature".to_string(), "fixture-signature".to_string()),
                ])
                .with_headers([("X-MBX-APIKEY".to_string(), "test-key".to_string())]),
        )
        .expect("get fees spec");

    request_spec("get_positions")
        .assert_matches(
            &ActualHttpRequest::new("GET", "/fapi/v2/positionRisk")
                .with_query([
                    ("symbol".to_string(), "BTCUSDT".to_string()),
                    ("recvWindow".to_string(), "5000".to_string()),
                    ("timestamp".to_string(), "1700000000000".to_string()),
                    ("signature".to_string(), "fixture-signature".to_string()),
                ])
                .with_headers([("X-MBX-APIKEY".to_string(), "test-key".to_string())]),
        )
        .expect("get positions spec");

    let batch_orders = "[{\"symbol\":\"BTCUSDT\",\"side\":\"BUY\",\"type\":\"LIMIT\",\"timeInForce\":\"GTC\",\"quantity\":\"0.01\",\"price\":\"65000.5\",\"reduceOnly\":\"false\",\"newClientOrderId\":\"cli-apollox-batch-1\"}]";
    let batch_params = [
        ("batchOrders".to_string(), batch_orders.to_string()),
        ("recvWindow".to_string(), "5000".to_string()),
        ("timestamp".to_string(), "1700000000000".to_string()),
    ];
    let (batch_query, batch_signature) =
        signing::apollox_signed_query("test-secret", batch_params.clone())
            .expect("batch signature");
    let batch_vector = fixture("batch_signing_vector");
    assert_eq!(
        batch_query,
        batch_vector["canonical_query"].as_str().expect("query")
    );
    assert_eq!(
        batch_signature,
        batch_vector["signature"].as_str().expect("signature")
    );

    let mut actual_batch_query = batch_params.into_iter().collect::<BTreeMap<_, _>>();
    actual_batch_query.insert("signature".to_string(), batch_signature);
    request_spec("batch_place_orders")
        .assert_matches(
            &ActualHttpRequest::new("POST", "/fapi/v1/batchOrders")
                .with_query(actual_batch_query)
                .with_headers([
                    ("X-MBX-APIKEY".to_string(), "test-key".to_string()),
                    (
                        "Content-Type".to_string(),
                        "application/x-www-form-urlencoded".to_string(),
                    ),
                ]),
        )
        .expect("batch place spec");
}

#[test]
fn parser_should_parse_batch_place_ack_with_partial_failure_report() {
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-parser"),
        exchange: exchange_id(),
        orders: vec![
            limit_order("batch-place-1", "cli-apollox-batch-1"),
            limit_order("batch-place-2", "cli-apollox-batch-2"),
        ],
    };
    let (orders, report) =
        parse_batch_place_orders_ack(&exchange_id(), &request, &fixture("batch_place_ack"))
            .expect("batch ack");

    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].exchange_order_id.as_deref(), Some("987654321"));
    assert_eq!(
        orders[0].client_order_id.as_deref(),
        Some("cli-apollox-batch-1")
    );
    assert_eq!(report.total_items, 2);
    assert_eq!(report.succeeded_count(), 1);
    assert_eq!(report.failed_count(), 1);
    assert!(report.requires_reconciliation());
    assert_eq!(
        report.results[1]
            .error
            .as_ref()
            .expect("failed item")
            .code
            .as_deref(),
        Some("-2022")
    );
}

#[test]
fn private_readback_parsers_should_parse_order_open_orders_and_fills() {
    let order = parse_order_state(&exchange_id(), Some(&symbol()), &fixture("order_success"))
        .expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("987654321"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.filled_quantity, "0.010");

    let orders = parse_open_orders(
        &exchange_id(),
        Some(&symbol()),
        MarketType::Perpetual,
        &fixture("open_orders_success"),
    )
    .expect("open orders");
    assert_eq!(orders.len(), 2);
    assert_eq!(orders[1].status, OrderStatus::PartiallyFilled);
    assert!(orders[1].reduce_only);

    let fills = parse_recent_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &symbol(),
        &fixture("recent_fills_success"),
    )
    .expect("fills");
    assert_eq!(fills.len(), 2);
    assert_eq!(fills[0].fill_id.as_deref(), Some("12345"));
    assert_eq!(fills[0].order_id.as_deref(), Some("987654321"));
    assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
    assert_eq!(fills[1].liquidity_role, rustcta_types::LiquidityRole::Maker);
}

#[test]
fn websocket_helpers_should_build_binance_style_subscriptions() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    assert_eq!(
        apollox_public_subscribe_payload(&subscription),
        fixture("ws_subscribe")
    );
    assert_eq!(
        apollox_public_unsubscribe_payload(&subscription),
        fixture("ws_unsubscribe")
    );
    let snapshot =
        apollox_parse_public_depth_event(&exchange_id(), symbol(), &fixture("ws_depth_event"))
            .expect("ws depth");
    assert_eq!(snapshot.sequence, Some(987654322));
    assert_eq!(snapshot.best_bid().expect("bid").price, 65010.0);
    assert_eq!(apollox_ws_policy_ms(), (180_000, 600_000, 86_400_000));
}

#[test]
fn public_requests_validate_exchange_and_market_type() {
    let adapter =
        ApolloxDexGatewayAdapter::new(ApolloxDexGatewayConfig::default()).expect("adapter");
    let request = SymbolRulesRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("symbols"),
        symbols: vec![SymbolScope {
            market_type: MarketType::Spot,
            ..symbol()
        }],
    };
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let error = runtime
        .block_on(adapter.get_symbol_rules(request))
        .expect_err("spot unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "apollox_dex.unsupported_market_type"
        }
    ));

    assert_eq!(
        json!({
            "exchange": "apollox_dex",
            "scope": "v1_orderbook_perp"
        })["exchange"],
        "apollox_dex"
    );
}
