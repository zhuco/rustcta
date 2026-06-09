use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, FeesRequest, PositionsRequest,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::adapters::AdapterBackedGateway;
use crate::signing_spec::SigningVector;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::{
    myokx_heartbeat_policy, myokx_pong_payload, myokx_private_login_payload,
    myokx_private_subscribe_payload, myokx_public_subscribe_payload,
    myokx_public_unsubscribe_payload, myokx_public_ws_session,
};
use super::{new_adapter, MyOkxGatewayConfig};

#[test]
fn myokx_config_should_map_to_okx_eea_profile() {
    let config = MyOkxGatewayConfig::default();
    assert_eq!(config.rest_base_url, "https://eea.okx.com");
    assert_eq!(
        config.public_ws_url,
        "wss://wseea.okx.com:8443/ws/v5/public"
    );

    let okx_profile = config.into_okx_config();
    assert_eq!(okx_profile.exchange_id, "myokx");
    assert_eq!(okx_profile.rest_base_url, "https://eea.okx.com");
    assert!(!okx_profile.enabled_private_rest);
    assert!(okx_profile.api_key.is_none());
    assert_eq!(
        okx_profile.unsupported_market_type_operation,
        "myokx.non_spot_market_type"
    );
}

#[test]
fn myokx_adapter_should_register_by_name() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["myokx"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[tokio::test]
async fn myokx_capabilities_should_be_spot_public_only() {
    let adapter = new_adapter(MyOkxGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.exchange, exchange_id());
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("private rest should remain disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "myokx.get_balances"
        }
    ));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_balances"
            && endpoint.path.as_deref() == Some("/unsupported/myokx/get_balances")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .all(|endpoint| { endpoint.market_types == vec![MarketType::Spot] }));

    let positions_error = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![ExchangeSymbol::new(
                exchange_id(),
                MarketType::Perpetual,
                "BTC-USDT-SWAP",
            )
            .unwrap()],
        })
        .await
        .expect_err("non-spot product line should stay disabled");
    assert!(matches!(
        positions_error,
        ExchangeApiError::Unsupported {
            operation: "myokx.non_spot_market_type"
        }
    ));
}

#[tokio::test]
async fn myokx_get_balances_should_reuse_okx_v5_signed_runtime_when_enabled() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{
            "totalEq": "1000.00",
            "details": [
                {
                    "ccy": "USDT",
                    "cashBal": "1000.00",
                    "availBal": "950.00",
                    "frozenBal": "50.00"
                },
                {
                    "ccy": "BTC",
                    "cashBal": "0.25",
                    "availBal": "0.20",
                    "frozenBal": "0.05"
                }
            ]
        }]
    })])
    .await;
    let adapter = new_adapter(MyOkxGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..MyOkxGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances-enabled"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("balances");

    assert_eq!(response.balances.len(), 1);
    assert_eq!(response.balances[0].exchange_id, exchange_id());
    assert_eq!(response.balances[0].balances.len(), 1);
    assert_eq!(response.balances[0].balances[0].asset, "USDT");
    assert_eq!(response.balances[0].balances[0].total, 1000.0);
    assert_eq!(response.balances[0].balances[0].available, 950.0);
    assert_eq!(response.balances[0].balances[0].locked, 50.0);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/api/v5/account/balance");
    assert_eq!(
        requests[0].headers.get("ok-access-key").map(String::as_str),
        Some("key")
    );
    assert!(requests[0]
        .headers
        .get("ok-access-sign")
        .is_some_and(|value| !value.is_empty()));
    assert_eq!(
        requests[0]
            .headers
            .get("ok-access-passphrase")
            .map(String::as_str),
        Some("passphrase")
    );
}

#[tokio::test]
async fn myokx_get_fees_should_reuse_okx_v5_signed_runtime_when_enabled() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{
            "instType": "SPOT",
            "instId": "BTC-USDT",
            "maker": "-0.0008",
            "taker": "-0.0010"
        }]
    })])
    .await;
    let adapter = new_adapter(MyOkxGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..MyOkxGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees-enabled"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");

    assert_eq!(response.fees.len(), 1);
    assert_eq!(response.fees[0].maker_rate, "-0.0008");
    assert_eq!(response.fees[0].taker_rate, "-0.0010");
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/api/v5/account/trade-fee");
    assert_eq!(
        requests[0].query.get("instType").map(String::as_str),
        Some("SPOT")
    );
    assert_eq!(
        requests[0].query.get("instId").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_eq!(
        requests[0].headers.get("ok-access-key").map(String::as_str),
        Some("key")
    );
    assert!(requests[0]
        .headers
        .get("ok-access-sign")
        .is_some_and(|value| !value.is_empty()));
    assert!(!requests[0]
        .headers
        .values()
        .any(|value| value.contains("secret")));
}

#[test]
fn myokx_product_line_boundary_should_remain_project_unimplemented() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/request_specs/product_line_source_boundary.json"
    ))
    .expect("product-line boundary fixture");
    assert_eq!(fixture["exchange"], "myokx");
    assert_eq!(fixture["profile_of"], "okx");
    assert_eq!(fixture["boundary"], "product_line_source");
    assert_eq!(fixture["status"], "project_unimplemented");
    assert_eq!(fixture["runtime_enabled"], false);
    assert_eq!(fixture["private_rest_enabled"], false);
    assert_eq!(
        fixture["official_product_lines"],
        serde_json::json!([
            "margin",
            "swap_perpetual",
            "futures",
            "options",
            "event_contracts"
        ])
    );

    for required in [
        "credential scope and API permission audit",
        "regional product and account eligibility guard",
        "private order lifecycle request specs and parsers",
        "positions, fills, settlement, and account reconciliation",
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
        "operation: contract_product",
        "operation: market_type_perpetual",
        "operation: market_type_futures",
        "operation: market_type_options",
        "operation: event_contract_product",
        "operation: margin_product",
        "boundary: project_unimplemented_product_line",
        "source_boundary_fixture: tests/fixtures/exchanges/myokx/request_specs/product_line_source_boundary.json",
        "required_audit_gaps:",
    ] {
        assert!(
            mapping.contains(required),
            "endpoint_mapping missing {required}"
        );
    }
}

#[test]
fn myokx_parser_fixtures_should_reuse_okx_spot_shape_with_profile_id() {
    let instruments: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/instruments.json"
    ))
    .expect("instruments fixture");
    let rules = parse_symbol_rules(&exchange_id(), &instruments).expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].symbol.exchange, exchange_id());
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTC-USDT");

    let orderbook: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/orderbook.json"
    ))
    .expect("orderbook fixture");
    let book = parse_orderbook_snapshot(&exchange_id(), symbol_scope(), &orderbook).expect("book");
    assert_eq!(book.exchange_id, exchange_id());
    assert_eq!(book.bids[0].price, 65000.0);
}

#[test]
fn myokx_ws_payloads_should_use_eea_hosts_and_okx_shapes() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let subscribe = myokx_public_subscribe_payload(&subscription).expect("subscribe");
    assert_eq!(subscribe["op"], "subscribe");
    assert_eq!(subscribe["args"][0]["channel"], "books");
    assert_eq!(subscribe["args"][0]["instId"], "BTC-USDT");

    let unsubscribe = myokx_public_unsubscribe_payload(&subscription).expect("unsubscribe");
    assert_eq!(unsubscribe["op"], "unsubscribe");

    let session =
        myokx_public_ws_session(&MyOkxGatewayConfig::default(), &subscription).expect("session");
    assert_eq!(session["url"], "wss://wseea.okx.com:8443/ws/v5/public");
    assert_eq!(myokx_heartbeat_policy()["client_pong"], "pong");
    assert_eq!(
        myokx_pong_payload(),
        serde_json::Value::String("pong".to_string())
    );
}

#[test]
fn myokx_private_ws_auth_shape_should_match_sanitized_signing_vector() {
    let vector: SigningVector = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/signing_vectors/ws_login.json"
    ))
    .expect("signing vector");
    assert_eq!(vector.expected_signature, "<redacted:signature>");
    assert_eq!(
        vector.canonical_payload().as_deref(),
        Some(vector.payload.as_str())
    );

    let login = myokx_private_login_payload(
        "sanitized-api-key",
        &vector.secret,
        "sanitized-passphrase",
        vector.timestamp.as_deref().expect("timestamp"),
    );
    assert_eq!(login["op"], "login");
    assert_eq!(login["args"][0]["sign"], vector.compute_signature());

    let private_subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").unwrap(),
        kind: PrivateStreamKind::Orders,
    };
    let subscribe = myokx_private_subscribe_payload(&private_subscription);
    assert_eq!(subscribe["args"][0]["channel"], "orders");
}

#[test]
fn myokx_boundary_fixtures_should_be_sanitized() {
    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    assert_eq!(boundary["exchange"], "myokx");
    assert_eq!(boundary["profile_of"], "okx");
    assert_eq!(boundary["private_rest"]["trade_enabled"], false);
    assert!(boundary["credential_values"]
        .as_object()
        .unwrap()
        .is_empty());

    let request_spec: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/request_specs/private_rest_unsupported.json"
    ))
    .expect("request spec fixture");
    assert_eq!(request_spec["support"], "unsupported");
    assert!(request_spec["credential_values"]
        .as_object()
        .unwrap()
        .is_empty());

    let ws_public: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/ws/public_books_snapshot.json"
    ))
    .expect("public ws fixture");
    assert_eq!(ws_public["arg"]["channel"], "books");

    let ws_private: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/myokx/ws/private_orders_unsupported.json"
    ))
    .expect("private ws fixture");
    assert!(ws_private["credential_values"]
        .as_object()
        .unwrap()
        .is_empty());
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("myokx").unwrap()
}

fn symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTC-USDT").unwrap(),
    }
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").unwrap()),
        account_id: Some(AccountId::new("account").unwrap()),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

#[derive(Clone)]
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
                .unwrap_or_else(|| json!({"code": "0", "data": []}));
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
            Some((key.to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    SeenRequest {
        method,
        path: path.to_string(),
        query,
        headers,
    }
}
