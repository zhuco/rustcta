use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, PlaceOrderRequest, QueryOrderRequest,
    RecentFillsRequest, RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::private::{
    balances_request_spec_fixture, cancel_all_orders_request_spec_fixture,
    cancel_order_by_client_id_request_spec_fixture, cancel_order_request_spec_fixture,
    fees_request_spec_fixture, open_orders_request_spec_fixture, place_order_request_spec_fixture,
    query_order_request_spec_fixture, recent_fills_request_spec_fixture,
};
use super::private::{open_orders_query, query_order_endpoint, recent_fills_query};
use super::private_parser::{
    parse_onetrading_open_orders, parse_onetrading_order, parse_onetrading_recent_fills,
};
use super::signing::onetrading_authorization;
use super::streams::onetrading_private_auth_payload;
use super::{OneTradingGatewayAdapter, OneTradingGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("onetrading").expect("exchange")
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
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "EUR").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTC_EUR")
            .expect("symbol"),
    }
}

fn fixture(name: &str) -> Value {
    let raw = match name {
        "order_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/onetrading/order_success.json")
        }
        "open_orders_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/onetrading/open_orders_success.json"
        ),
        "recent_fills_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/onetrading/recent_fills_success.json"
        ),
        other => panic!("unknown fixture {other}"),
    };
    serde_json::from_str(raw).expect("fixture json")
}

#[test]
fn request_specs_and_bearer_boundary_should_match_docs() {
    assert_eq!(
        onetrading_authorization("<redacted:token>").expect("auth"),
        "Bearer <redacted:token>"
    );
    assert_eq!(balances_request_spec_fixture()["path"], "/account/balances");
    assert_eq!(fees_request_spec_fixture()["path"], "/account/fees");
    assert_eq!(
        open_orders_request_spec_fixture()["path"],
        "/account/orders"
    );
    assert_eq!(
        query_order_request_spec_fixture()["path"],
        "/account/orders/<redacted:order_id>"
    );
    assert_eq!(
        recent_fills_request_spec_fixture()["path"],
        "/account/trades"
    );
    assert_eq!(place_order_request_spec_fixture()["method"], "POST");
    assert_eq!(place_order_request_spec_fixture()["body"]["type"], "LIMIT");
    assert_eq!(
        place_order_request_spec_fixture()["body"]["time_in_force"],
        "GOOD_TILL_CANCELLED"
    );
    assert_eq!(
        cancel_order_request_spec_fixture()["path"],
        "/account/orders/<redacted:order_id>"
    );
    assert_eq!(
        cancel_order_by_client_id_request_spec_fixture()["path"],
        "/account/orders/client/<redacted:order_id>"
    );
    assert_eq!(cancel_all_orders_request_spec_fixture()["method"], "DELETE");
    assert_eq!(
        query_order_endpoint("ot/order 1").expect("encoded"),
        "/account/orders/ot%2Forder%201"
    );
    let open_query = open_orders_query(&symbol(), None);
    assert_eq!(
        open_query.get("instrument_code").map(String::as_str),
        Some("BTC_EUR")
    );
    assert_eq!(
        open_query.get("max_page_size").map(String::as_str),
        Some("50")
    );
    let fills_query = recent_fills_query(&symbol(), Some(250));
    assert_eq!(
        fills_query.get("max_page_size").map(String::as_str),
        Some("100")
    );
}

#[tokio::test]
async fn private_write_should_be_offline_request_spec_only() {
    let adapter =
        OneTradingGatewayAdapter::new(OneTradingGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: Some("<redacted:order_id>".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("64000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "onetrading.place_order_offline_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn private_readbacks_should_fail_closed_without_guard() {
    let adapter =
        OneTradingGatewayAdapter::new(OneTradingGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_query_order);
    assert!(!capabilities.supports_open_orders);
    assert!(!capabilities.supports_recent_fills);

    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("ot-order-1".to_string()),
        })
        .await
        .expect_err("private REST disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "onetrading.query_order"
        }
    ));
}

#[test]
fn private_parsers_should_normalize_order_and_fill_fixtures() {
    let order = parse_onetrading_order(
        &exchange_id(),
        Some(&symbol()),
        &fixture("order_success.json"),
    )
    .expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("ot-order-1"));
    assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.filled_quantity, "0.00400000");

    let open = parse_onetrading_open_orders(
        &exchange_id(),
        &symbol(),
        &fixture("open_orders_success.json"),
    )
    .expect("open orders");
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].status, OrderStatus::Open);
    assert!(open[0].post_only);

    let request_context = context("fills");
    let fills = parse_onetrading_recent_fills(
        &exchange_id(),
        request_context.tenant_id.expect("tenant"),
        request_context.account_id.expect("account"),
        &symbol(),
        &fixture("recent_fills_success.json"),
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].order_id.as_deref(), Some("ot-order-1"));
    assert_eq!(fills[0].fill_id.as_deref(), Some("ot-trade-1"));
    assert_eq!(fills[0].price, 63950.0);
    assert_eq!(fills[0].quantity, 0.004);
}

#[tokio::test]
async fn private_readbacks_should_use_bearer_get_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("order_success.json"),
        fixture("open_orders_success.json"),
        fixture("recent_fills_success.json"),
    ])
    .await;
    let adapter = OneTradingGatewayAdapter::new(OneTradingGatewayConfig {
        rest_base_url: base_url,
        enabled_private_rest: true,
        api_token: Some("fixture-token".to_string()),
        ..OneTradingGatewayConfig::default()
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
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("ot-order-1".to_string()),
        })
        .await
        .expect("query");
    assert_eq!(
        order
            .order
            .as_ref()
            .and_then(|order| order.exchange_order_id.as_deref()),
        Some("ot-order-1")
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
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(25),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/account/orders/ot-order-1");
    assert_eq!(
        requests[0].headers.get("authorization").map(String::as_str),
        Some("Bearer fixture-token")
    );

    assert_eq!(requests[1].method, "GET");
    assert_eq!(requests[1].path, "/account/orders");
    assert_eq!(
        requests[1].query.get("instrument_code").map(String::as_str),
        Some("BTC_EUR")
    );
    assert_eq!(
        requests[1]
            .query
            .get("with_just_orders")
            .map(String::as_str),
        Some("true")
    );

    assert_eq!(requests[2].method, "GET");
    assert_eq!(requests[2].path, "/account/trades");
    assert_eq!(
        requests[2].query.get("max_page_size").map(String::as_str),
        Some("25")
    );
    assert_eq!(
        requests[2].headers.get("authorization").map(String::as_str),
        Some("Bearer fixture-token")
    );
}

#[test]
fn private_websocket_boundary_should_stay_disabled_until_verified() {
    let auth = onetrading_private_auth_payload("<redacted:token>");
    assert_eq!(auth["type"], "AUTHENTICATE");
    assert_eq!(auth["api_token"], "<redacted:token>");

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/onetrading/ws_private_auth.json"
    ))
    .expect("private auth fixture");
    assert_eq!(fixture, auth);

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/onetrading/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["futures_live_enabled"], false);
    assert_eq!(boundary["private_ws_live_enabled"], false);
    assert_eq!(boundary["withdrawals_enabled"], false);
}

#[test]
fn product_line_boundary_should_keep_futures_perpetual_project_unimplemented() {
    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/onetrading/request_specs/product_line_source_boundary.json"
    ))
    .expect("product line boundary");

    assert_eq!(boundary["exchange"], "onetrading");
    assert_eq!(boundary["status"], "project_unimplemented");
    assert_eq!(boundary["current_adapter_scope"][0], "spot");
    assert_eq!(boundary["official_product_lines"][0], "futures");
    assert_eq!(boundary["official_product_lines"][1], "perpetual");
    assert!(boundary["required_endpoints"]["private_account"]
        .as_array()
        .expect("private account gaps")
        .iter()
        .any(|gap| gap == "positions"));
    assert!(boundary["runtime_gaps"]
        .as_array()
        .expect("runtime gaps")
        .iter()
        .any(|gap| gap == "dry-run or sandbox guard before enabling live writes"));

    let adapter =
        OneTradingGatewayAdapter::new(OneTradingGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(!capabilities.supports_positions);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_private_rest);
}

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
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
    let (head, _) = request_text
        .split_once("\r\n\r\n")
        .unwrap_or((request_text, ""));
    let request_line = request_text.lines().next().unwrap_or_default();
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default().to_string();
    let target = request_parts.next().unwrap_or_default();
    let (path, query_text) = target.split_once('?').unwrap_or((target, ""));
    let query = query_text
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (key.to_string(), value.to_string())
        })
        .collect();
    let headers = head
        .lines()
        .skip(1)
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
