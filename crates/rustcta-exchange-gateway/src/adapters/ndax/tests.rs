use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, PlaceOrderRequest, PositionsRequest,
    PublicStreamKind, PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest,
    RequestContext, SymbolScope, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{ndax_canonical_pair, ndax_symbol, parse_order_book_shape, parse_symbol_rules};
use super::private::{
    cancel_order_body, cancel_order_request_spec, fills_request_spec, ndax_limit_order_body,
    open_orders_request_spec, place_order_request_spec,
};
use super::public::{instruments_request_spec, l2_snapshot_request_spec};
use super::signing::{
    ndax_hmac_sha256_base64, ndax_redacted_signed_headers, ndax_signature_payload,
    ndax_signed_headers,
};
use super::streams::{
    ndax_private_auth_payload, ndax_public_subscribe_payload, ndax_reconnect_policy_ms,
    ndax_unsubscribe_payload, parse_ws_function_name, parse_ws_level2_shape,
};
use super::transport::decode_gateway_payload;
use super::{NdaxGatewayAdapter, NdaxGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("ndax").expect("exchange")
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
    let (base, quote) = ndax_canonical_pair(symbol).expect("canonical pair");
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
fn symbol_normalization_should_cover_cad_markets() {
    assert_eq!(ndax_symbol("btc/cad"), "BTCCAD");
    assert_eq!(ndax_symbol("ETH-CAD"), "ETHCAD");
    assert_eq!(
        ndax_canonical_pair("BTCCAD").expect("cad"),
        ("BTC".to_string(), "CAD".to_string())
    );
    assert_eq!(
        ndax_canonical_pair("ETH_CAD").expect("cad separator"),
        ("ETH".to_string(), "CAD".to_string())
    );
}

#[test]
fn public_gateway_specs_and_fixtures_should_parse() {
    let instruments_spec = instruments_request_spec(1);
    assert_eq!(instruments_spec["body"]["n"], "GetInstruments");
    assert_eq!(instruments_spec["body"]["o"], "{\"OMSId\":1}");

    let book_spec = l2_snapshot_request_spec(1, "btc/cad", 50);
    assert_eq!(book_spec["body"]["n"], "GetL2Snapshot");
    assert!(book_spec["body"]["o"]
        .as_str()
        .expect("payload")
        .contains("BTCCAD"));

    let instruments: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/instruments_success.json"
    ))
    .expect("instruments");
    let rules = parse_symbol_rules(&exchange_id(), &[], &instruments).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "CAD");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.00000001"));

    let order_book: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/orderbook_btc_cad.json"
    ))
    .expect("order book");
    assert_eq!(parse_order_book_shape(&order_book).expect("shape"), (1, 1));

    let envelope: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/gateway_instruments_envelope.json"
    ))
    .expect("envelope");
    let decoded = decode_gateway_payload(&envelope).expect("decoded");
    assert!(decoded.as_array().expect("array").len() >= 2);

    let boundary: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["private_write_mode"], "offline_request_spec_only");
    assert_eq!(
        boundary["private_read_runtime"],
        "credential_gated_order_and_fill_readback"
    );
    assert_eq!(boundary["fiat_ledger_runtime"], "unsupported");
}

#[test]
fn request_spec_and_signing_should_stay_secret_free() {
    let body = "{\"accountId\":1001,\"instrumentId\":1,\"limitPrice\":\"65000\",\"orderType\":2,\"quantity\":\"0.01\",\"side\":0,\"timeInForce\":1}";
    let payload = ndax_signature_payload("1700000000", "POST", "/orders", body);
    assert_eq!(payload, format!("1700000000POST/orders{body}"));
    let signature = ndax_hmac_sha256_base64(b"fixture-secret", &payload).expect("signature");
    assert_eq!(signature, "Gzj+heNRZlaUpsntUhFPdFjxfTEJiUlDUSuGp6hhhJU=");

    let headers = ndax_signed_headers(
        "fixture-key",
        "Zml4dHVyZS1zZWNyZXQ=",
        Some("fixture-passphrase"),
        "1700000000",
        "POST",
        "/orders",
        body,
    )
    .expect("headers");
    assert_eq!(
        headers.get("X-NDAX-SIGNATURE").map(String::as_str),
        Some(signature.as_str())
    );
    assert_eq!(
        ndax_redacted_signed_headers()["X-NDAX-SIGNATURE"],
        "<redacted:signature>"
    );

    let vector: crate::signing_spec::SigningVector = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/signing_vectors/rest_order_signature.json"
    ))
    .expect("signing vector");
    vector.verify().expect("standard signing vector verifies");

    for raw in [
        include_str!(
            "../../../../../tests/fixtures/exchanges/ndax/request_specs/get_balances.json"
        ),
        include_str!("../../../../../tests/fixtures/exchanges/ndax/request_specs/query_order.json"),
        include_str!(
            "../../../../../tests/fixtures/exchanges/ndax/request_specs/get_open_orders.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/ndax/request_specs/get_recent_fills.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/ndax/request_specs/place_order_limit.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/ndax/request_specs/cancel_order.json"
        ),
    ] {
        let spec: crate::request_spec::RequestSpec =
            serde_json::from_str(raw).expect("request spec");
        assert_eq!(spec.exchange, "ndax");
        assert!(spec
            .forbid_header_values_containing
            .contains(&"fixture-secret".to_string()));
    }
}

#[test]
fn product_line_boundary_should_pin_margin_gap_and_contract_unsupported() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/request_specs/product_line_source_boundary.json"
    ))
    .expect("product line boundary");
    assert_eq!(fixture["exchange"], "ndax");
    assert_eq!(fixture["boundary"], "product_line_source");
    assert_eq!(fixture["status"], "project_unimplemented");
    assert_eq!(fixture["runtime_enabled"], false);
    assert_eq!(fixture["private_rest_enabled"], false);
    assert_eq!(
        fixture["official_product_lines"],
        serde_json::json!(["margin_account_semantics"])
    );
    assert_eq!(
        fixture["unsupported_standard_product_lines"],
        serde_json::json!(["futures", "perpetual", "options"])
    );
    for required in [
        "margin account eligibility and account-mode semantics",
        "margin balance, position, profit/loss, leverage, and risk parser fixtures",
        "product-scoped private order lifecycle and permission guard",
        "post-write readback/reconciliation and live dry-run gate",
    ] {
        let gaps = fixture["required_audit_gaps"]
            .as_array()
            .expect("required audit gaps");
        assert!(
            gaps.iter().any(|gap| gap.as_str() == Some(required)),
            "missing audit gap {required}"
        );
    }

    let mapping = include_str!("endpoint_mapping.yaml");
    for required in [
        "operation: margin_product",
        "status: project_unimplemented",
        "official_gap: margin_account_semantics",
        "boundary: project_unimplemented_product_line",
        "source_boundary_fixture: tests/fixtures/exchanges/ndax/request_specs/product_line_source_boundary.json",
        "operation: futures_product",
        "operation: perpetual_product",
        "operation: options_product",
        "status: unsupported",
    ] {
        assert!(
            mapping.contains(required),
            "endpoint_mapping missing {required}"
        );
    }
}

#[test]
fn private_request_builders_should_map_offline_order_fields() {
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTCCAD"),
        client_order_id: Some("100200300".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let body = ndax_limit_order_body(&request, 1001, 1).expect("body");
    assert_eq!(body["AccountId"], 1001);
    assert_eq!(body["InstrumentId"], 1);
    assert_eq!(body["Side"], 0);
    assert_eq!(body["OrderType"], 2);
    assert_eq!(body["LimitPrice"], "65000");

    let place = place_order_request_spec(body);
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], "/orders");
    assert_eq!(place["auth"], "ndax_hmac_sha256_base64");

    let cancel_body = cancel_order_body(1001, "offline-order-1");
    assert_eq!(cancel_body["OrderId"], "offline-order-1");
    let cancel = cancel_order_request_spec("offline-order-1");
    assert_eq!(cancel["method"], "DELETE");
    assert_eq!(cancel["path"], "/orders/offline-order-1");

    let open = open_orders_request_spec(1001, Some(1));
    assert_eq!(open["method"], "GET");
    assert_eq!(open["path"], "/orders");
    assert_eq!(open["query"]["accountId"], 1001);
    assert_eq!(open["query"]["instrumentId"], 1);

    let fills = fills_request_spec(1001, Some(1), Some(100));
    assert_eq!(fills["method"], "GET");
    assert_eq!(fills["path"], "/fills");
    assert_eq!(fills["query"]["accountId"], 1001);
    assert_eq!(fills["query"]["instrumentId"], 1);
    assert_eq!(fills["query"]["limit"], 100);
}

#[test]
fn websocket_helpers_should_emit_payloads_and_parse_l2_fixtures() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol("BTCCAD"),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let payload = ndax_public_subscribe_payload(2, 1, &subscription, 50);
    assert_eq!(payload["n"], "SubscribeLevel2");
    assert!(payload["o"].as_str().expect("payload").contains("BTCCAD"));
    assert_eq!(
        ndax_unsubscribe_payload(3, "UnsubscribeLevel2", 1, "BTCCAD")["n"],
        "UnsubscribeLevel2"
    );
    let auth = ndax_private_auth_payload(
        4,
        "<redacted:api_key>",
        "<redacted:user_id>",
        "<nonce>",
        "<redacted:signature>",
    );
    assert_eq!(auth["n"], "AuthenticateUser");
    assert!(auth["o"]
        .as_str()
        .expect("auth")
        .contains("<redacted:signature>"));
    assert_eq!(ndax_reconnect_policy_ms(), (30_000, 45_000, 60_000));

    let ws_book: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/ws/level2_snapshot.json"
    ))
    .expect("ws book");
    assert_eq!(
        parse_ws_function_name(&ws_book).as_deref(),
        Some("Level2UpdateEvent")
    );
    assert_eq!(parse_ws_level2_shape(&ws_book).expect("shape"), (1, 1));
}

#[tokio::test]
async fn live_trading_surfaces_should_remain_explicitly_unsupported() {
    let adapter = NdaxGatewayAdapter::new(NdaxGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_query_order);
    assert!(!capabilities.supports_open_orders);
    assert!(!capabilities.supports_recent_fills);
    assert!(!capabilities.supports_positions);

    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTCCAD"),
        client_order_id: Some("100200300".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
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
            operation: "ndax.place_order_offline_request_spec_only"
        }
    ));

    let positions_error = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: Vec::new(),
        })
        .await
        .expect_err("positions unsupported");
    assert!(matches!(
        positions_error,
        ExchangeApiError::Unsupported {
            operation: "ndax.positions_unsupported_spot_only"
        }
    ));

    let query_error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol("BTCCAD"),
            client_order_id: None,
            exchange_order_id: Some("offline-order-1".to_string()),
        })
        .await
        .expect_err("private readback disabled");
    assert!(matches!(
        query_error,
        ExchangeApiError::Unsupported {
            operation: "ndax.query_order_private_rest_disabled"
        }
    ));
}

#[tokio::test]
async fn private_readbacks_should_sign_get_requests_when_enabled() {
    let query_response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/order_status_success.json"
    ))
    .expect("query order response fixture");
    let open_response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/open_orders_success.json"
    ))
    .expect("open orders response fixture");
    let fills_response: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/recent_fills_success.json"
    ))
    .expect("recent fills response fixture");
    let (base_url, seen) =
        spawn_rest_server(vec![query_response, open_response, fills_response]).await;
    let adapter = NdaxGatewayAdapter::new(NdaxGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("ndax-key".to_string()),
        api_secret: Some("Zml4dHVyZS1zZWNyZXQ=".to_string()),
        passphrase: Some("ndax-passphrase".to_string()),
        user_id: Some("ndax-user".to_string()),
        account_id: Some(1001),
        enabled_private_rest: true,
        ..Default::default()
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
            symbol: symbol("BTCCAD"),
            client_order_id: None,
            exchange_order_id: Some("offline-order-1".to_string()),
        })
        .await
        .expect("query order")
        .order
        .expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("offline-order-1"));
    assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
    assert_eq!(order.status, OrderStatus::Open);
    assert_eq!(order.filled_quantity, "0.02000000");

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol("BTCCAD")),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);
    assert_eq!(
        open.orders[0].exchange_order_id.as_deref(),
        Some("offline-order-1")
    );

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol("BTCCAD")),
            client_order_id: None,
            exchange_order_id: Some("offline-order-1".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect("recent fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("fill-1"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("offline-order-1"));
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("CAD"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/orders/offline-order-1");
    assert_eq!(
        requests[0].query.get("accountId").map(String::as_str),
        Some("1001")
    );
    assert_eq!(requests[1].method, "GET");
    assert_eq!(requests[1].path, "/orders");
    assert_eq!(
        requests[1].query.get("accountId").map(String::as_str),
        Some("1001")
    );
    assert_eq!(
        requests[1].query.get("instrumentId").map(String::as_str),
        Some("1")
    );
    assert_eq!(requests[2].method, "GET");
    assert_eq!(requests[2].path, "/fills");
    assert_eq!(
        requests[2].query.get("accountId").map(String::as_str),
        Some("1001")
    );
    assert_eq!(
        requests[2].query.get("instrumentId").map(String::as_str),
        Some("1")
    );
    assert_eq!(
        requests[2].query.get("limit").map(String::as_str),
        Some("100")
    );
    for request in &requests {
        assert_eq!(request.header("x-ndax-apikey"), Some("ndax-key"));
        assert!(request.header("x-ndax-signature").is_some());
        assert!(request.header("x-ndax-timestamp").is_some());
        assert_eq!(request.header("x-ndax-passphrase"), Some("ndax-passphrase"));
        assert!(request.body.is_none());
    }
}
