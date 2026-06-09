use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, PageRequest, PlaceOrderRequest,
    QueryOrderRequest, RecentFillsRequest, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::private::{
    balances_request_spec_fixture, cancel_all_orders_request_spec_fixture,
    cancel_order_request_spec_fixture, open_orders_query, open_orders_request_spec_fixture,
    place_order_request_spec_fixture, positions_request_spec_fixture, query_order_endpoint,
    query_order_request_spec_fixture, recent_fills_query, recent_fills_request_spec_fixture,
};
use super::private_parser::{
    parse_arkham_open_orders, parse_arkham_order, parse_arkham_recent_fills,
};
use super::signing::arkham_signature;
use super::streams::arkham_private_auth_payload;
use super::{ArkhamGatewayAdapter, ArkhamGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("arkham").expect("exchange")
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

fn symbol(market_type: MarketType, raw: &str, base: &str, quote: &str) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, raw).expect("symbol"),
    }
}

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "order_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/arkham/order_success.json")
        }
        "open_orders_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/arkham/open_orders_success.json")
        }
        "recent_fills_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/arkham/recent_fills_success.json")
        }
        "request_specs/query_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/arkham/request_specs/query_order.json"
        ),
        "request_specs/get_open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/arkham/request_specs/get_open_orders.json"
        ),
        "request_specs/get_recent_fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/arkham/request_specs/get_recent_fills.json"
        ),
        other => panic!("unknown fixture {other}"),
    };
    serde_json::from_str(text).expect(name)
}

#[test]
fn request_specs_and_signing_should_match_hmac_boundary() {
    let place = place_order_request_spec_fixture();
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], "/orders/new");
    assert_eq!(place["auth"], "arkham_hmac_sha256_base64");
    assert_eq!(place["body"]["symbol"], "BTC_USDT");
    assert_eq!(place["body"]["clientOrderId"], "<redacted:order_id>");
    assert_eq!(balances_request_spec_fixture()["path"], "/account/balances");
    assert_eq!(
        positions_request_spec_fixture()["path"],
        "/account/positions"
    );
    assert_eq!(
        cancel_order_request_spec_fixture()["path"],
        "/orders/cancel"
    );
    assert_eq!(
        cancel_all_orders_request_spec_fixture()["path"],
        "/orders/cancel/all"
    );
    assert_eq!(
        query_order_request_spec_fixture()["path"],
        "/orders/<redacted:order_id>"
    );
    assert_eq!(open_orders_request_spec_fixture()["path"], "/orders");
    assert_eq!(
        open_orders_request_spec_fixture()["query"]["status"],
        "open"
    );
    assert_eq!(recent_fills_request_spec_fixture()["path"], "/trades");

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/arkham/signing_vectors/rest_hmac_sha256.json"
    ))
    .expect("fixture");
    let signature = arkham_signature(
        fixture["principal"].as_str().expect("principal"),
        fixture["credential_material_base64"]
            .as_str()
            .expect("credential material"),
        fixture["expires_us"].as_i64().expect("expires"),
        fixture["method"].as_str().expect("method"),
        fixture["path"].as_str().expect("path"),
        fixture["body"].as_str().expect("body"),
    )
    .expect("signature");
    assert_eq!(fixture["expected_hmac_sha256_base64"], signature);
}

#[test]
fn readback_request_spec_fixtures_should_match_native_guarded_runtime() {
    let query_order = fixture("request_specs/query_order.json");
    assert_eq!(query_order["method"], "GET");
    assert_eq!(query_order["path"], "/orders/<redacted:order_id>");
    assert_eq!(query_order["auth"], "arkham_hmac_sha256_base64");
    assert_eq!(query_order["runtime"], "guarded_private_rest");
    assert_eq!(query_order["request_spec_only"], false);

    let open_orders = fixture("request_specs/get_open_orders.json");
    assert_eq!(open_orders["method"], "GET");
    assert_eq!(open_orders["path"], "/orders");
    assert_eq!(open_orders["query"]["status"], "open");
    assert_eq!(open_orders["runtime"], "guarded_private_rest");

    let recent_fills = fixture("request_specs/get_recent_fills.json");
    assert_eq!(recent_fills["method"], "GET");
    assert_eq!(recent_fills["path"], "/trades");
    assert_eq!(recent_fills["query"]["limit"], "100");
    assert_eq!(recent_fills["runtime"], "guarded_private_rest");

    let spot = symbol(MarketType::Spot, "BTC_USDT", "BTC", "USDT");
    assert_eq!(
        query_order_endpoint("ark-order-1").expect("endpoint"),
        "/orders/ark-order-1"
    );
    assert_eq!(open_orders_query(&spot, Some(250))["limit"], "100");
    assert_eq!(recent_fills_query(&spot, Some(0))["limit"], "1");
}

#[test]
fn readback_parsers_should_normalize_order_and_fill_fixtures() {
    let spot = symbol(MarketType::Spot, "BTC_USDT", "BTC", "USDT");
    let order =
        parse_arkham_order(&exchange_id(), &spot, &fixture("order_success.json")).expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("ark-order-1"));
    assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.filled_quantity, "0.00400000");

    let open =
        parse_arkham_open_orders(&exchange_id(), &spot, &fixture("open_orders_success.json"))
            .expect("open orders");
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].exchange_order_id.as_deref(), Some("ark-open-1"));
    assert_eq!(open[0].status, OrderStatus::Open);

    let fills = parse_arkham_recent_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &spot,
        &fixture("recent_fills_success.json"),
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("ark-fill-1"));
    assert_eq!(fills[0].order_id.as_deref(), Some("ark-order-1"));
    assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
}

#[tokio::test]
async fn private_readbacks_should_fail_closed_without_private_flag() {
    let adapter = ArkhamGatewayAdapter::new(ArkhamGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b"secret",
        )),
        enabled_private_rest: false,
        ..ArkhamGatewayConfig::default()
    })
    .expect("adapter");
    let request = QueryOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("query"),
        symbol: symbol(MarketType::Spot, "BTC_USDT", "BTC", "USDT"),
        client_order_id: None,
        exchange_order_id: Some("ark-order-1".to_string()),
    };

    let error = adapter.query_order(request).await.expect_err("disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "arkham.private_rest_disabled"
        }
    ));

    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_query_order);
    assert!(!capabilities.supports_open_orders);
    assert!(!capabilities.supports_recent_fills);
}

#[tokio::test]
async fn private_readbacks_should_use_signed_rest_paths_and_headers() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("order_success.json"),
        fixture("open_orders_success.json"),
        fixture("recent_fills_success.json"),
    ])
    .await;
    let adapter = ArkhamGatewayAdapter::new(ArkhamGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("fixture-key".to_string()),
        api_secret: Some(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b"fixture-secret",
        )),
        enabled_private_rest: true,
        ..ArkhamGatewayConfig::default()
    })
    .expect("adapter");
    let spot = symbol(MarketType::Spot, "BTC_USDT", "BTC", "USDT");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_balances);

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: spot.clone(),
            client_order_id: None,
            exchange_order_id: Some("ark-order-1".to_string()),
        })
        .await
        .expect("query order");
    assert_eq!(
        order.order.and_then(|order| order.exchange_order_id),
        Some("ark-order-1".to_string())
    );

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot.clone()),
            page: Some(PageRequest::first_page(25)),
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
            symbol: Some(spot),
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

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/orders/ark-order-1");
    assert_eq!(
        requests[0]
            .headers
            .get("arkham-api-key")
            .map(String::as_str),
        Some("fixture-key")
    );
    assert!(requests[0].headers.contains_key("arkham-signature"));
    assert_eq!(
        requests[0]
            .headers
            .get("arkham-broker-id")
            .map(String::as_str),
        Some("1001")
    );

    assert_eq!(requests[1].method, "GET");
    assert_eq!(requests[1].path, "/orders");
    assert_eq!(
        requests[1].query.get("symbol").map(String::as_str),
        Some("BTC_USDT")
    );
    assert_eq!(
        requests[1].query.get("status").map(String::as_str),
        Some("open")
    );
    assert_eq!(
        requests[1].query.get("limit").map(String::as_str),
        Some("25")
    );

    assert_eq!(requests[2].method, "GET");
    assert_eq!(requests[2].path, "/trades");
    assert_eq!(
        requests[2].query.get("symbol").map(String::as_str),
        Some("BTC_USDT")
    );
    assert_eq!(
        requests[2].query.get("limit").map(String::as_str),
        Some("10")
    );
}

#[tokio::test]
async fn private_write_should_be_offline_request_spec_only() {
    let adapter = ArkhamGatewayAdapter::new(ArkhamGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(MarketType::Spot, "BTC_USDT", "BTC", "USDT"),
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
            operation: "arkham.place_order_offline_request_spec_only"
        }
    ));
}

#[test]
fn private_websocket_boundary_should_stay_disabled_until_verified() {
    let auth = arkham_private_auth_payload(
        "<redacted:api_key>",
        1_700_000_300_000_000,
        "<redacted:signature>",
    );
    assert_eq!(auth["op"], "auth");
    assert_eq!(auth["headers"]["Arkham-Broker-Id"], "1001");

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/arkham/ws_private_auth.json"
    ))
    .expect("private auth fixture");
    assert_eq!(fixture, auth);

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/arkham/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["intel_api_is_trading_adapter"], false);
    assert_eq!(boundary["private_ws_live_enabled"], false);
}

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
}

async fn spawn_rest_server(
    responses: Vec<serde_json::Value>,
) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
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
