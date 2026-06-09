use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    BalancesRequest, ExchangeApiError, ExchangeClient, FeesRequest, OrderBookRequest,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::{
    okxus_heartbeat_policy, okxus_private_login_payload, okxus_private_subscribe_payload,
    okxus_public_subscribe_payload, okxus_public_unsubscribe_payload,
    parse_okxus_orderbook_ws_message,
};
use super::{new_adapter, OkxusGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn okxus_config_should_map_to_okx_us_profile() {
    let config = OkxusGatewayConfig::default();
    assert_eq!(config.rest_base_url, "https://us.okx.com");
    assert_eq!(config.public_ws_url, "wss://wsus.okx.com:8443/ws/v5/public");
    assert_eq!(
        config.private_ws_url,
        "wss://wsus.okx.com:8443/ws/v5/private"
    );

    let okx_profile = config.into_okx_config();
    assert_eq!(okx_profile.exchange_id, "okxus");
    assert_eq!(okx_profile.rest_base_url, "https://us.okx.com");
    assert!(!okx_profile.enabled_private_rest);
    assert!(okx_profile.api_key.is_none());
    assert!(okx_profile.api_secret.is_none());
    assert!(okx_profile.passphrase.is_none());
    assert_eq!(
        okx_profile.unsupported_market_type_operation,
        "okxus.non_spot_market_type"
    );
}

#[test]
fn okxus_adapter_should_register_by_name() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["okxus"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn okxus_capabilities_should_keep_private_trading_disabled() {
    let adapter = new_adapter(OkxusGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.exchange, exchange_id());
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
}

#[tokio::test]
async fn okxus_private_rest_should_remain_unsupported_without_us_scope_audit() {
    let adapter = new_adapter(OkxusGatewayConfig::default()).expect("adapter");
    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["USD".to_string()],
        })
        .await
        .expect_err("private rest must stay disabled");

    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "okxus.get_balances"
        }
    ));
}

#[tokio::test]
async fn okxus_get_balances_should_reuse_okx_v5_signed_runtime_when_enabled() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{
            "totalEq": "1000.00",
            "details": [
                {
                    "ccy": "USD",
                    "cashBal": "1000.00",
                    "availBal": "975.00",
                    "frozenBal": "25.00"
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
    let adapter = new_adapter(OkxusGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..OkxusGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances-enabled"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["USD".to_string()],
        })
        .await
        .expect("balances");

    assert_eq!(response.balances.len(), 1);
    assert_eq!(response.balances[0].exchange_id, exchange_id());
    assert_eq!(response.balances[0].balances.len(), 1);
    assert_eq!(response.balances[0].balances[0].asset, "USD");
    assert_eq!(response.balances[0].balances[0].total, 1000.0);
    assert_eq!(response.balances[0].balances[0].available, 975.0);
    assert_eq!(response.balances[0].balances[0].locked, 25.0);

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
async fn okxus_get_fees_should_reuse_okx_v5_signed_runtime_when_enabled() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{
            "instType": "SPOT",
            "instId": "BTC-USDT",
            "maker": "-0.0007",
            "taker": "-0.0009"
        }]
    })])
    .await;
    let adapter = new_adapter(OkxusGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..OkxusGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees-enabled"),
            symbols: vec![symbol_scope(MarketType::Spot, "BTC-USDT")],
        })
        .await
        .expect("fees");

    assert_eq!(response.fees.len(), 1);
    assert_eq!(response.fees[0].maker_rate, "-0.0007");
    assert_eq!(response.fees[0].taker_rate, "-0.0009");
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

#[tokio::test]
async fn okxus_non_spot_market_type_should_be_unsupported() {
    let adapter = new_adapter(OkxusGatewayConfig::default()).expect("adapter");
    let error = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(MarketType::Perpetual, "BTC-USDT-SWAP"),
            depth: Some(5),
        })
        .await
        .expect_err("non spot must be unsupported");

    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "okxus.non_spot_market_type"
        }
    ));
}

#[test]
fn okxus_public_rest_fixtures_should_parse_with_us_exchange_id() {
    let instruments: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/instruments.json"
    ))
    .expect("instruments fixture");
    let rules = parse_symbol_rules(&exchange_id(), &instruments).expect("symbol rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].symbol.exchange, exchange_id());
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTC-USD");
    assert_eq!(rules[0].quote_asset, "USD");

    let book_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/orderbook.json"
    ))
    .expect("orderbook fixture");
    let book = parse_orderbook_snapshot(
        &exchange_id(),
        symbol_scope(MarketType::Spot, "BTC-USD"),
        &book_fixture,
    )
    .expect("order book");
    assert_eq!(book.exchange_id, exchange_id());
    assert_eq!(book.exchange_symbol.unwrap().symbol, "BTC-USD");
    assert_eq!(book.bids[0].quantity, 0.75);
    assert!(book.exchange_timestamp.is_some());
}

#[test]
fn okxus_boundary_and_ws_fixtures_should_be_sanitized() {
    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    assert_eq!(boundary["exchange"], "okxus");
    assert_eq!(boundary["private_rest"]["trade_enabled"], false);
    assert_eq!(boundary["rest_base_url"], "https://us.okx.com");

    let request_spec: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/request_specs/private_rest_unsupported.json"
    ))
    .expect("request spec fixture");
    assert_eq!(request_spec["support"], "unsupported");
    assert!(request_spec["credential_values"]
        .as_object()
        .unwrap()
        .is_empty());

    let signing_vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/signing_vectors/rest_hmac_shape.json"
    ))
    .expect("signing fixture");
    assert_eq!(signing_vector["credential_values"], serde_json::json!({}));

    let public_ws: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/ws_public_orderbook.json"
    ))
    .expect("public ws fixture");
    assert_eq!(public_ws["payload"]["args"][0]["channel"], "books5");
    assert_eq!(public_ws["payload"]["args"][0]["instId"], "BTC-USD");

    let private_ws: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/ws_private_order_unsupported.json"
    ))
    .expect("private ws fixture");
    assert_eq!(private_ws["support"], "unsupported_pending_us_scope_audit");
}

#[test]
fn okxus_product_line_boundary_should_remain_project_unimplemented() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/request_specs/product_line_source_boundary.json"
    ))
    .expect("product-line boundary fixture");
    assert_eq!(fixture["exchange"], "okxus");
    assert_eq!(fixture["boundary"], "product_line_source");
    assert_eq!(fixture["status"], "project_unimplemented");
    assert_eq!(fixture["runtime_enabled"], false);
    assert_eq!(fixture["private_rest_enabled"], false);
    assert_eq!(
        fixture["official_product_lines"],
        serde_json::json!(["margin", "perpetual", "futures", "event_contracts"])
    );
    for required in [
        "credential scope and API permission audit",
        "state and account eligibility guard",
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
        "operation: market_type_non_spot",
        "operation: margin_product",
        "operation: futures_product",
        "operation: perpetual_product",
        "operation: event_contract_product",
        "boundary: project_unimplemented_product_line",
        "source_boundary_fixture: tests/fixtures/exchanges/okxus/request_specs/product_line_source_boundary.json",
        "required_audit_gaps:",
    ] {
        assert!(
            mapping.contains(required),
            "endpoint_mapping missing {required}"
        );
    }
}

#[test]
fn okxus_ws_payloads_and_parser_should_use_us_hosts_and_okx_shapes() {
    let public_subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol_scope(MarketType::Spot, "BTC-USD"),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let subscribe = okxus_public_subscribe_payload(&public_subscription).expect("subscribe");
    assert_eq!(subscribe["op"], "subscribe");
    assert_eq!(subscribe["args"][0]["channel"], "books5");
    assert_eq!(subscribe["args"][0]["instId"], "BTC-USD");

    let unsubscribe = okxus_public_unsubscribe_payload(&public_subscription).expect("unsubscribe");
    assert_eq!(unsubscribe["op"], "unsubscribe");

    let login = okxus_private_login_payload(
        "<redacted-key>",
        "<redacted-passphrase>",
        "1710000000",
        "<redacted-signature>",
    );
    assert_eq!(login["op"], "login");
    assert_eq!(login["args"][0]["apiKey"], "<redacted-key>");

    let private_subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").unwrap(),
        kind: PrivateStreamKind::Orders,
    };
    let private_subscribe =
        okxus_private_subscribe_payload(&private_subscription).expect("private subscribe");
    assert_eq!(private_subscribe["args"][0]["channel"], "orders");
    assert_eq!(private_subscribe["args"][0]["instType"], "SPOT");
    assert_eq!(okxus_heartbeat_policy()["client_pong"], "pong");

    let ws_orderbook: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/okxus/ws_public_orderbook_snapshot.json"
    ))
    .expect("ws orderbook fixture");
    let book = parse_okxus_orderbook_ws_message(
        &exchange_id(),
        symbol_scope(MarketType::Spot, "BTC-USD"),
        &ws_orderbook,
    )
    .expect("ws book");
    assert_eq!(book.exchange_id, exchange_id());
    assert_eq!(book.bids[0].quantity, 0.75);
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("okxus").unwrap()
}

fn symbol_scope(market_type: MarketType, symbol: &str) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, symbol).unwrap(),
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
