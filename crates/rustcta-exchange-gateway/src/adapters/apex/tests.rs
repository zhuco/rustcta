use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, PageRequest, PlaceOrderRequest,
    PublicStreamKind, PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest,
    RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::signing::{apex_hmac_signature, apex_signing_payload, apex_sorted_form_body};
use super::streams::{
    apex_ping_payload, apex_private_login_payload, apex_public_subscribe_payload,
    apex_reconnect_policy_ms,
};
use super::{ApexGatewayAdapter, ApexGatewayConfig};
use crate::adapters::apex::parser::{
    parse_open_orders, parse_order, parse_orderbook_snapshot, parse_recent_fills,
    parse_symbol_rules,
};
use crate::adapters::apex::private::{
    open_orders_request_spec_fixture, query_order_params, query_order_request_spec_fixture,
    recent_fills_request_spec_fixture, symbol_page_params,
};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("apex").expect("exchange")
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

fn symbol(market_type: MarketType) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, "BTCUSDT")
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
fn capabilities_should_scope_apex_to_perpetual_and_keep_private_writes_disabled() {
    let adapter = ApexGatewayAdapter::new(ApexGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_batch_place_order);

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/apex/unsupported_boundary.json"
    ))
    .expect("fixture");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_blocker"], "zklink_l2_signature");
}

#[test]
fn parser_fixtures_should_decode_symbols_and_order_book() {
    let symbols: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/apex/symbols_perpetual.json"
    ))
    .expect("symbols fixture");
    let rules = parse_symbol_rules(&exchange_id(), &[], &symbols).expect("symbol rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "USDT");
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.001"));

    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/apex/depth_btcusdt.json"
    ))
    .expect("depth fixture");
    let book = parse_orderbook_snapshot(&exchange_id(), symbol(MarketType::Perpetual), &depth)
        .expect("order book");
    assert_eq!(book.bids[0].price, 64999.5);
    assert_eq!(book.asks[0].price, 65000.0);
    assert_eq!(book.sequence, Some(123456789));
}

#[test]
fn signing_fixture_should_cover_private_read_hmac_shape() {
    let body = apex_sorted_form_body(&[
        ("limit", Some("100")),
        ("symbol", Some("BTC-USDT")),
        ("page", Some("1")),
    ]);
    assert_eq!(body, "limit=100&page=1&symbol=BTC-USDT");
    let payload = apex_signing_payload(
        "2026-06-08T00:00:00.000Z",
        "GET",
        "/api/v3/open-orders?limit=100&page=1&symbol=BTC-USDT",
        "",
    );
    let signature = apex_hmac_signature("test-secret", &payload).expect("signature");
    assert!(!signature.is_empty());

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/apex/signing_vectors/private_get_open_orders.json"
    ))
    .expect("fixture");
    assert_eq!(fixture["payload"], payload);
    assert_eq!(fixture["expected_signature"], signature);
}

#[test]
fn private_readback_fixtures_should_parse() {
    let order: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/apex/order_success.json"
    ))
    .expect("order fixture");
    let order =
        parse_order(&exchange_id(), &symbol(MarketType::Perpetual), &order).expect("parsed order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("offline-order-id"));
    assert_eq!(order.client_order_id.as_deref(), Some("offline-client-id"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.filled_quantity, "0.004");
    assert_eq!(order.average_fill_price.as_deref(), Some("64990"));

    let open: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/apex/open_orders_success.json"
    ))
    .expect("open fixture");
    let orders = parse_open_orders(&exchange_id(), &symbol(MarketType::Perpetual), &open)
        .expect("open orders");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].status, OrderStatus::Open);
    assert_eq!(
        orders[0].exchange_order_id.as_deref(),
        Some("open-order-id")
    );

    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/apex/recent_fills_success.json"
    ))
    .expect("fills fixture");
    let fills = parse_recent_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &symbol(MarketType::Perpetual),
        &fills,
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("offline-fill-id"));
    assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
}

#[test]
fn private_readback_request_specs_should_match_fixture_shape() {
    let symbol = symbol(MarketType::Perpetual);
    let query_params = query_order_params(&symbol, "offline-order-id");
    assert_eq!(
        query_params.get("symbol").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_eq!(
        query_order_request_spec_fixture()["path"].as_str(),
        Some("/api/v3/order?id=offline-order-id&symbol=BTC-USDT")
    );

    let page_params = symbol_page_params(&symbol, Some(250));
    assert_eq!(page_params.get("limit").map(String::as_str), Some("100"));
    assert_eq!(
        open_orders_request_spec_fixture()["path"].as_str(),
        Some("/api/v3/open-orders?limit=100&page=1&symbol=BTC-USDT")
    );
    assert_eq!(
        recent_fills_request_spec_fixture()["path"].as_str(),
        Some("/api/v3/fills?limit=100&page=1&symbol=BTC-USDT")
    );
}

#[tokio::test]
async fn place_order_should_require_zklink_l2_signature() {
    let adapter = ApexGatewayAdapter::new(ApexGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(MarketType::Perpetual),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "apex.place_order_requires_zklink_l2_signature"
        }
    ));
}

#[tokio::test]
async fn private_readback_should_fail_closed_without_explicit_guard() {
    let adapter = ApexGatewayAdapter::new(ApexGatewayConfig::default()).expect("adapter");
    let request = QueryOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("query-disabled"),
        symbol: symbol(MarketType::Perpetual),
        client_order_id: None,
        exchange_order_id: Some("offline-order-id".to_string()),
    };
    let error = adapter.query_order(request).await.expect_err("disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "apex.private_rest_disabled"
        }
    ));
}

#[tokio::test]
async fn private_readback_should_send_signed_get_requests_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/apex/order_success.json"
        ))
        .expect("order"),
        serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/apex/open_orders_success.json"
        ))
        .expect("open"),
        serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/apex/recent_fills_success.json"
        ))
        .expect("fills"),
    ])
    .await;
    let adapter = ApexGatewayAdapter::new(ApexGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..ApexGatewayConfig::default()
    })
    .expect("adapter");
    let symbol = symbol(MarketType::Perpetual);

    let query = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol.clone(),
            client_order_id: None,
            exchange_order_id: Some("offline-order-id".to_string()),
        })
        .await
        .expect("query");
    assert_eq!(
        query.order.unwrap().exchange_order_id.as_deref(),
        Some("offline-order-id")
    );

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol.clone()),
            page: Some(PageRequest::first_page(100)),
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/api/v3/order");
    assert_eq!(
        seen[0].query.get("id").map(String::as_str),
        Some("offline-order-id")
    );
    assert_eq!(seen[1].path, "/api/v3/open-orders");
    assert_eq!(seen[2].path, "/api/v3/fills");
    for request in &seen {
        assert_eq!(request.header("apex-api-key"), Some("key"));
        assert_eq!(request.header("apex-passphrase"), Some("passphrase"));
        assert!(request.header("apex-signature").is_some());
        assert!(request.header("apex-timestamp").is_some());
    }
}

#[test]
fn websocket_helpers_should_build_public_and_private_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol(MarketType::Perpetual),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let topic = format!(
        "orderBook200.H.{}",
        subscription.symbol.exchange_symbol.symbol
    );
    assert_eq!(
        apex_public_subscribe_payload(&topic)["args"][0],
        "orderBook200.H.BTCUSDT"
    );
    assert_eq!(apex_ping_payload(1_700_000_000_000)["op"], "ping");
    let login = apex_private_login_payload("key", "pass", "ts", "sig");
    assert_eq!(login["op"], "login");
    assert!(login["args"][0]
        .as_str()
        .expect("login string")
        .contains("ws_zk_accounts_v3"));
    assert_eq!(apex_reconnect_policy_ms(), (15_000, 15_000, 45_000));
}
