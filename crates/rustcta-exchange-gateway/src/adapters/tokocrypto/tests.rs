use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest, RequestContext,
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
    market_data_symbol, parse_open_orders, parse_order_state, parse_orderbook_snapshot,
    parse_recent_fills, parse_symbol_rules,
};
use super::streams::{private_subscribe_payload, public_subscription_spec};
use super::{TokocryptoGatewayAdapter, TokocryptoGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn exchange_id() -> ExchangeId {
    ExchangeId::new("tokocrypto").expect("exchange")
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

fn symbol_scope() -> rustcta_exchange_api::SymbolScope {
    rustcta_exchange_api::SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTC_USDT")
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
                .unwrap_or_else(|| json!({}));
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

#[test]
fn tokocrypto_parser_fixtures_should_cover_symbols_and_depth() {
    let symbols: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/tokocrypto/symbols_success.json"
    ))
    .expect("symbols");
    let rules = parse_symbol_rules(&exchange_id(), &symbols).expect("rules");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTC_USDT");
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "USDT");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.01000000"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.00001000"));
    assert_eq!(rules[0].min_notional.as_deref(), Some("10.00000000"));

    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/tokocrypto/depth_success.json"
    ))
    .expect("depth");
    let snapshot = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), &depth).expect("book");
    assert_eq!(snapshot.sequence, Some(1027024));
    assert_eq!(snapshot.best_bid().unwrap().price, 4.0);
    assert_eq!(snapshot.best_ask().unwrap().price, 4.000002);
    assert_eq!(market_data_symbol("BTC_USDT").unwrap(), "BTCUSDT");
}

#[tokio::test]
async fn tokocrypto_adapter_capabilities_should_be_spot_public_with_offline_private_boundary() {
    let adapter =
        TokocryptoGatewayAdapter::new(TokocryptoGatewayConfig::default()).expect("adapter");
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

    let err = adapter
        .get_positions(rustcta_exchange_api::PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbols: Vec::new(),
        })
        .await
        .expect_err("positions unsupported");
    assert!(format!("{err:?}").contains("tokocrypto.positions"));
}

#[test]
fn tokocrypto_private_readback_fixtures_should_parse() {
    let query_order: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/tokocrypto/order_success.json"
    ))
    .expect("query order");
    let order =
        parse_order_state(&exchange_id(), Some(&symbol_scope()), &query_order).expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("12345"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.side, OrderSide::Buy);

    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/tokocrypto/open_orders_success.json"
    ))
    .expect("open orders");
    let orders =
        parse_open_orders(&exchange_id(), Some(&symbol_scope()), &open_orders).expect("orders");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].exchange_order_id.as_deref(), Some("12345"));

    let recent_fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/tokocrypto/recent_fills_success.json"
    ))
    .expect("fills");
    let fills = parse_recent_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &symbol_scope(),
        &recent_fills,
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("9001"));
    assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
}

#[tokio::test]
async fn tokocrypto_private_readbacks_should_fail_closed_when_disabled() {
    let adapter =
        TokocryptoGatewayAdapter::new(TokocryptoGatewayConfig::default()).expect("adapter");
    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("12345".to_string()),
        })
        .await
        .expect_err("disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "tokocrypto.query_order"
        }
    ));
}

#[tokio::test]
async fn tokocrypto_writes_should_remain_offline_request_spec_only() {
    let adapter =
        TokocryptoGatewayAdapter::new(TokocryptoGatewayConfig::default()).expect("adapter");
    let error = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol_scope(),
            client_order_id: Some("client-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "0.01".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect_err("place unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "tokocrypto.place_order.offline_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn tokocrypto_private_readbacks_should_use_signed_get_requests() {
    let query_order: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/tokocrypto/order_success.json"
    ))
    .expect("query order");
    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/tokocrypto/open_orders_success.json"
    ))
    .expect("open orders");
    let recent_fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/tokocrypto/recent_fills_success.json"
    ))
    .expect("fills");
    let (base_url, seen) = spawn_rest_server(vec![query_order, open_orders, recent_fills]).await;
    let adapter = TokocryptoGatewayAdapter::new(TokocryptoGatewayConfig {
        rest_base_url: base_url,
        enabled_private_rest: true,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        ..TokocryptoGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("12345".to_string()),
        })
        .await
        .expect("query")
        .order
        .expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("12345"));

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open");
    assert_eq!(open.orders.len(), 1);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("12345".to_string()),
            from_trade_id: Some("9000".to_string()),
            start_time: None,
            end_time: None,
            limit: Some(50),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/open/v1/orders/detail");
    assert_eq!(
        seen[0].query.get("symbol").map(String::as_str),
        Some("BTC_USDT")
    );
    assert_eq!(
        seen[0].query.get("orderId").map(String::as_str),
        Some("12345")
    );
    assert_eq!(
        seen[0].query.get("recvWindow").map(String::as_str),
        Some("5000")
    );
    assert!(seen[0].query.contains_key("timestamp"));
    assert!(seen[0].query.contains_key("signature"));
    assert_eq!(seen[0].header("x-mbx-apikey"), Some("test-key"));
    assert_eq!(seen[1].path, "/open/v1/orders");
    assert_eq!(seen[2].path, "/open/v1/orders/trades");
    assert_eq!(
        seen[2].query.get("fromId").map(String::as_str),
        Some("9000")
    );
    assert_eq!(seen[2].query.get("limit").map(String::as_str), Some("50"));
    for request in seen {
        assert!(!request
            .query
            .values()
            .any(|value| value.contains("test-secret")));
        assert_eq!(
            request.header("content-type"),
            Some("application/x-www-form-urlencoded")
        );
    }
}

#[test]
fn tokocrypto_public_ws_subscription_should_use_payload_fixture() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let spec = public_subscription_spec(&subscription, "wss://stream-cloud.tokocrypto.site/stream")
        .expect("subscription");
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/tokocrypto/ws/public_depth_subscribe.json"
    ))
    .expect("ws fixture");
    assert_eq!(fixture["stream"].as_str(), Some(spec.stream.as_str()));
    assert_eq!(spec.subscribe_payload, fixture["subscribe_payload"]);
    assert_eq!(spec.unsubscribe_payload, fixture["unsubscribe_payload"]);
    assert_eq!(
        private_subscribe_payload("fixture-listen-token")["params"][0],
        "fixture-listen-token"
    );
}

#[test]
fn tokocrypto_named_registration_should_accept_aliases() {
    let gateway = AdapterBackedGateway::new("tokocrypto-test");
    gateway
        .register_named_adapter("toko_crypto")
        .expect("register alias");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}
