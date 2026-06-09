use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest, RequestContext, SymbolScope,
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
    luno_symbol, parse_luno_open_orders, parse_luno_order_book, parse_luno_order_state,
    parse_luno_recent_fills, parse_luno_symbol_rules,
};
use super::private::{
    open_orders_request_spec_fixture, place_order_request_spec_fixture,
    query_order_request_spec_fixture, recent_fills_request_spec_fixture,
};
use super::signing::luno_basic_auth_header;
use super::streams::{
    luno_public_order_book_ws_policy, luno_stream_url, luno_ws_auth_payload, luno_ws_sequence,
    luno_ws_sequence_is_contiguous,
};
use super::{LunoGatewayAdapter, LunoGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("luno").expect("exchange")
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
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "ZAR").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "XBTZAR")
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
fn parser_should_cover_multi_region_fiat_markets_and_orderbook() {
    let tickers: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/tickers.json"
    ))
    .expect("fixture");
    let rules = parse_luno_symbol_rules(exchange_id(), &[], &tickers).expect("rules");
    assert!(rules
        .iter()
        .any(|rule| rule.base_asset == "BTC" && rule.quote_asset == "ZAR"));
    assert!(rules
        .iter()
        .any(|rule| rule.base_asset == "BTC" && rule.quote_asset == "MYR"));
    assert!(rules
        .iter()
        .any(|rule| rule.base_asset == "BTC" && rule.quote_asset == "NGN"));
    assert!(rules
        .iter()
        .any(|rule| rule.base_asset == "BTC" && rule.quote_asset == "IDR"));

    let orderbook: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/orderbook_top.json"
    ))
    .expect("fixture");
    let snapshot = parse_luno_order_book(&symbol(), &orderbook).expect("orderbook");
    assert_eq!(snapshot.asks[0].price, 1_201_000.0);
    assert_eq!(luno_symbol(&symbol()), "XBTZAR");
}

#[test]
fn request_spec_and_signing_should_match_basic_auth_boundary() {
    let spec = place_order_request_spec_fixture();
    assert_eq!(spec["path"], "/api/1/postorder");
    assert_eq!(spec["auth"], "http_basic_api_key");
    assert_eq!(spec["form"]["pair"], "XBTZAR");
    assert_eq!(
        query_order_request_spec_fixture()["path"],
        "/api/1/orders/BXMC2CJ7HNB88U4"
    );
    assert_eq!(
        open_orders_request_spec_fixture()["query"]["pair"],
        "XBTZAR"
    );
    assert_eq!(recent_fills_request_spec_fixture()["query"]["limit"], "100");

    let header = luno_basic_auth_header("key-id", "key-secret").expect("header");
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/signing_vectors/basic_auth.json"
    ))
    .expect("fixture");
    assert_eq!(fixture["authorization"], header);
}

#[test]
fn parser_should_cover_private_readbacks() {
    let query_order: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/order_success.json"
    ))
    .expect("fixture");
    let order =
        parse_luno_order_state(&exchange_id(), Some(&symbol()), &query_order).expect("query order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("BXMC2CJ7HNB88U4"));
    assert_eq!(order.status, OrderStatus::Open);
    assert_eq!(order.price.as_deref(), Some("1200000.00"));

    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/open_orders.json"
    ))
    .expect("fixture");
    let orders =
        parse_luno_open_orders(&exchange_id(), Some(&symbol()), &open_orders).expect("orders");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].side, OrderSide::Buy);

    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/fills.json"
    ))
    .expect("fixture");
    let fills = parse_luno_recent_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &symbol(),
        &fills,
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("1"));
    assert_eq!(fills[0].fee_amount, Some(12.0));
}

#[tokio::test]
async fn private_write_should_be_offline_request_spec_only() {
    let adapter = LunoGatewayAdapter::new(LunoGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: None,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("1200000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "luno.place_order_offline_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn private_readbacks_should_fail_closed_when_private_rest_disabled() {
    let adapter = LunoGatewayAdapter::new(LunoGatewayConfig::default()).expect("adapter");
    let request = QueryOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("query-disabled"),
        symbol: symbol(),
        client_order_id: None,
        exchange_order_id: Some("BXMC2CJ7HNB88U4".to_string()),
    };
    let error = adapter.query_order(request).await.expect_err("disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "luno.query_order"
        }
    ));
}

#[tokio::test]
async fn private_readbacks_should_use_basic_auth_get_requests() {
    let query_order: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/order_success.json"
    ))
    .expect("query fixture");
    let open_orders: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/open_orders.json"
    ))
    .expect("open fixture");
    let fills: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/fills.json"
    ))
    .expect("fills fixture");
    let (base_url, seen) = spawn_rest_server(vec![query_order, open_orders, fills]).await;
    let adapter = LunoGatewayAdapter::new(LunoGatewayConfig {
        rest_base_url: base_url,
        enabled_private_rest: true,
        api_key_id: Some("key-id".to_string()),
        api_key_secret: Some("key-secret".to_string()),
        ..LunoGatewayConfig::default()
    })
    .expect("adapter");

    let query = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("BXMC2CJ7HNB88U4".to_string()),
        })
        .await
        .expect("query");
    assert_eq!(
        query.order.and_then(|order| order.exchange_order_id),
        Some("BXMC2CJ7HNB88U4".to_string())
    );

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
        .expect("open");
    assert_eq!(open.orders.len(), 1);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol()),
            client_order_id: None,
            exchange_order_id: Some("BXMC2CJ7HNB88U4".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(25),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/api/1/orders/BXMC2CJ7HNB88U4");
    assert_eq!(
        seen[0].header("authorization"),
        Some("Basic a2V5LWlkOmtleS1zZWNyZXQ=")
    );
    assert_eq!(seen[1].path, "/api/1/listorders");
    assert_eq!(
        seen[1].query.get("pair").map(String::as_str),
        Some("XBTZAR")
    );
    assert_eq!(seen[2].path, "/api/1/listtrades");
    assert_eq!(
        seen[2].query.get("pair").map(String::as_str),
        Some("XBTZAR")
    );
    assert_eq!(
        seen[2].query.get("order_id").map(String::as_str),
        Some("BXMC2CJ7HNB88U4")
    );
    assert_eq!(seen[2].query.get("limit").map(String::as_str), Some("25"));
}

#[test]
fn websocket_helpers_should_cover_pair_scoped_streams() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    assert_eq!(
        luno_stream_url(
            "wss://ws.luno.com/api/1/stream",
            &subscription.symbol.exchange_symbol.symbol
        ),
        "wss://ws.luno.com/api/1/stream/XBTZAR"
    );
    let auth = luno_ws_auth_payload("key-id", "key-secret");
    assert_eq!(auth["api_key_id"], "key-id");

    let policy = luno_public_order_book_ws_policy();
    assert_eq!(policy.url_template, "wss://ws.luno.com/api/1/stream/{pair}");
    assert!(policy.auth_required);
    assert_eq!(policy.auth_payload_fields, ["api_key_id", "api_key_secret"]);
    assert_eq!(policy.initial_message, "full_order_book_state");
    assert_eq!(policy.update_message, "atomic_order_book_updates");
    assert_eq!(policy.fixed_update_interval_ms, None);
    assert_eq!(policy.sequence_field, "sequence");
    assert_eq!(policy.checksum, None);
    assert!(policy.sequence_continuity.contains("n-1"));
    assert!(policy.resync.contains("reconnect"));
    assert!(policy.rest_snapshot_endpoint.contains("/api/1/orderbook"));
}

#[test]
fn websocket_orderbook_fixtures_should_cover_initial_update_and_gap_policy() {
    let initial: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/ws/market_stream_initial_orderbook.json"
    ))
    .expect("initial fixture");
    assert_eq!(initial["status"], "ACTIVE");
    assert_eq!(initial["asks"].as_array().expect("asks").len(), 1);
    assert_eq!(initial["bids"].as_array().expect("bids").len(), 1);
    assert!(initial["fixture_notes"]["auth_required"]
        .as_bool()
        .expect("auth_required"));

    let update: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/luno/ws/market_stream_update.json"
    ))
    .expect("update fixture");
    let initial_sequence = luno_ws_sequence(&initial).expect("initial sequence");
    let update_sequence = luno_ws_sequence(&update).expect("update sequence");
    assert_eq!(initial_sequence, 24352);
    assert_eq!(update_sequence, 24353);
    assert!(luno_ws_sequence_is_contiguous(
        Some(initial_sequence),
        update_sequence
    ));
    assert!(!luno_ws_sequence_is_contiguous(
        Some(initial_sequence),
        update_sequence + 1
    ));
    assert!(update["fixture_notes"]["resync"]
        .as_str()
        .expect("resync")
        .contains("reconnect"));
}
