use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    AccountId, ExchangeClient, OpenOrdersRequest, PublicStreamKind, PublicStreamSubscription,
    RecentFillsRequest, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{
    parse_orderbook_snapshot, parse_symbol_rules, parse_zaif_open_orders, parse_zaif_recent_fills,
};
use super::streams::public_subscription_spec;
use super::{ZaifGatewayAdapter, ZaifGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("zaif").expect("exchange")
}

fn context(request_id: &str) -> rustcta_exchange_api::RequestContext {
    rustcta_exchange_api::RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: None,
        account_id: None,
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn context_with_account(request_id: &str) -> rustcta_exchange_api::RequestContext {
    rustcta_exchange_api::RequestContext {
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
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "JPY").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "btc_jpy")
            .expect("symbol"),
    }
}

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    body: String,
    headers: HashMap<String, String>,
}

impl SeenRequest {
    fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .get(&key.to_ascii_lowercase())
            .map(String::as_str)
    }
}

async fn spawn_tapi_server(responses: Vec<Value>) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
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
                .unwrap_or_else(|| serde_json::json!({ "success": 1, "return": {} }));
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
        .map(|(_, body)| body.trim().to_string())
        .unwrap_or_default();
    SeenRequest {
        method,
        path: target.to_string(),
        body,
        headers,
    }
}

#[test]
fn zaif_parser_fixtures_should_cover_pairs_and_depth() {
    let pairs: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/zaif/currency_pairs_success.json"
    ))
    .expect("pairs");
    let rules = parse_symbol_rules(&exchange_id(), &pairs).expect("rules");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "btc_jpy");
    assert_eq!(rules[0].price_increment.as_deref(), Some("5"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.0001"));
    assert_eq!(rules[0].price_precision, Some(0));

    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/zaif/depth_btc_jpy.json"
    ))
    .expect("depth");
    let snapshot = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), &depth).expect("book");
    assert_eq!(snapshot.best_bid().unwrap().price, 6500000.0);
    assert_eq!(snapshot.best_ask().unwrap().price, 6500500.0);

    let open_orders: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/zaif/active_orders_success.json"
    ))
    .expect("active orders");
    let orders = parse_zaif_open_orders(&exchange_id(), Some(&symbol_scope()), &open_orders)
        .expect("orders");
    assert_eq!(orders[0].exchange_order_id.as_deref(), Some("123456789"));
    assert_eq!(orders[0].status, OrderStatus::Open);

    let trade_history: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/zaif/trade_history_success.json"
    ))
    .expect("trade history");
    let fills = parse_zaif_recent_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &symbol_scope(),
        &trade_history,
    )
    .expect("fills");
    assert_eq!(fills[0].fill_id.as_deref(), Some("987654321"));
    assert_eq!(fills[0].side, OrderSide::Sell);
}

#[tokio::test]
async fn zaif_adapter_capabilities_should_be_spot_only_with_write_boundary() {
    let adapter = ZaifGatewayAdapter::new(ZaifGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_private_streams);

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
    assert!(format!("{err:?}").contains("zaif.positions"));
}

#[tokio::test]
async fn zaif_readback_runtime_should_send_signed_tapi_requests_when_enabled() {
    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/zaif/active_orders_success.json"
    ))
    .expect("open orders response");
    let trade_history: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/zaif/trade_history_success.json"
    ))
    .expect("trade history response");
    let (base_url, seen) = spawn_tapi_server(vec![open_orders, trade_history]).await;
    let adapter = ZaifGatewayAdapter::new(ZaifGatewayConfig {
        public_rest_base_url: base_url.clone(),
        private_rest_base_url: format!("{base_url}/tapi"),
        api_key: Some("zaif-key".to_string()),
        api_secret: Some("zaif-secret".to_string()),
        enabled_private_rest: true,
        ..Default::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);
    assert!(!capabilities.supports_place_order);

    let orders = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(
        orders.orders[0].exchange_order_id.as_deref(),
        Some("123456789")
    );

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context_with_account("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
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
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("123456789"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/tapi");
    assert_eq!(requests[0].header("key"), Some("zaif-key"));
    assert!(requests[0]
        .header("sign")
        .is_some_and(|value| !value.is_empty()));
    assert!(requests[0].body.contains("method=active_orders"));
    assert!(requests[0].body.contains("currency_pair=btc_jpy"));
    assert_eq!(requests[1].method, "POST");
    assert!(requests[1].body.contains("method=trade_history"));
    assert!(requests[1].body.contains("count=10"));
}

#[test]
fn zaif_public_ws_subscription_should_use_pair_query() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let spec =
        public_subscription_spec(&subscription, "wss://ws.zaif.jp/stream").expect("subscription");
    assert_eq!(spec.url, "wss://ws.zaif.jp/stream?currency_pair=btc_jpy");
    assert_eq!(spec.subscribe_payload["currency_pair"], "btc_jpy");
}
