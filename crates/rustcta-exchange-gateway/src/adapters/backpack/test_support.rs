use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use base64::Engine;
use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, RequestContext, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
pub(super) struct SeenRequest {
    pub(super) method: String,
    pub(super) path: String,
    pub(super) query: HashMap<String, String>,
    pub(super) body: String,
    headers: HashMap<String, String>,
}

impl SeenRequest {
    pub(super) fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .get(&key.to_ascii_lowercase())
            .map(String::as_str)
    }

    pub(super) fn json_body(&self) -> Value {
        if self.body.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&self.body).expect("json request body")
        }
    }

    pub(super) fn raw_contains_secret(&self) -> bool {
        self.headers
            .values()
            .any(|value| value.contains(&test_secret()))
            || self.body.contains(&test_secret())
            || self.path.contains(&test_secret())
            || self
                .query
                .values()
                .any(|value| value.contains(&test_secret()))
    }
}

pub(super) async fn spawn_rest_server(
    responses: Vec<Value>,
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
            let mut buffer = vec![0_u8; 32768];
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
            let status = body.get("__status").and_then(Value::as_u64).unwrap_or(200);
            let body = if let Some(mut object) = body.as_object().cloned() {
                object.remove("__status");
                Value::Object(object)
            } else {
                body
            };
            let body_text = body.to_string();
            let response = format!(
                "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
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
    let method = request_line
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string();
    let target = request_line.split_whitespace().nth(1).unwrap_or_default();
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
            Some((key.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    let body = request_text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    SeenRequest {
        method,
        path: path.to_string(),
        query,
        body,
        headers,
    }
}

pub(super) fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("account").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: Utc::now(),
    }
}

pub(super) fn exchange_id() -> ExchangeId {
    ExchangeId::new("backpack").expect("exchange")
}

pub(super) fn spot_symbol_scope() -> SymbolScope {
    symbol_scope(MarketType::Spot, "SOL_USDC")
}

pub(super) fn perp_symbol_scope() -> SymbolScope {
    symbol_scope(MarketType::Perpetual, "SOL_USDC_PERP")
}

pub(super) fn symbol_scope(market_type: MarketType, symbol: &str) -> SymbolScope {
    let (base, quote) = symbol.split_once('_').unwrap_or((symbol, "USDC"));
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, symbol).expect("symbol"),
    }
}

pub(super) fn private_config(base_url: String) -> super::BackpackGatewayConfig {
    super::BackpackGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some(test_secret()),
        enabled_private_rest: true,
        recv_window_ms: 5_000,
        ..super::BackpackGatewayConfig::default()
    }
}

pub(super) fn test_secret() -> String {
    base64::engine::general_purpose::STANDARD.encode([7_u8; 32])
}

pub(super) fn fixture(name: &str) -> Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/backpack/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("fixture");
    serde_json::from_str(&text).expect("fixture json")
}

pub(super) fn request_spec(name: &str) -> Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/backpack/request_specs/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("request spec fixture");
    serde_json::from_str(&text).expect("request spec json")
}

pub(super) fn signing_vector(name: &str) -> Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/backpack/signing_vectors/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("signing vector fixture");
    serde_json::from_str(&text).expect("signing vector json")
}

pub(super) fn assert_signed_request(request: &SeenRequest, method: &str, path: &str) {
    assert_eq!(request.method, method);
    assert_eq!(request.path, path);
    assert_eq!(request.header("x-api-key"), Some("test-key"));
    assert!(request
        .header("x-signature")
        .is_some_and(|value| value.len() > 40));
    assert!(request
        .header("x-timestamp")
        .is_some_and(|value| value.parse::<i64>().is_ok()));
    assert_eq!(request.header("x-window"), Some("5000"));
    assert!(!request.raw_contains_secret());
}

pub(super) fn assert_request_matches_spec(request: &SeenRequest, spec: &Value) {
    assert_eq!(
        request.method,
        spec.get("method").and_then(Value::as_str).expect("method")
    );
    assert_eq!(
        request.path,
        spec.get("path").and_then(Value::as_str).expect("path")
    );
    let expected_query = spec
        .get("query")
        .and_then(Value::as_object)
        .expect("query object");
    for (key, expected) in expected_query {
        assert_eq!(
            request.query.get(key).map(String::as_str),
            expected.as_str(),
            "query parameter {key}"
        );
    }
    if spec.get("body").is_some_and(|value| !value.is_null()) {
        assert_eq!(request.json_body(), spec["body"]);
    } else {
        assert!(request.body.trim().is_empty());
    }
    if spec.get("signed").and_then(Value::as_bool) == Some(true) {
        assert_signed_request(
            request,
            spec.get("method").and_then(Value::as_str).expect("method"),
            spec.get("path").and_then(Value::as_str).expect("path"),
        );
    }
}

pub(super) fn fixed_time() -> (DateTime<Utc>, DateTime<Utc>) {
    let start = DateTime::from_timestamp_millis(1_704_000_000_000).expect("start");
    let end = DateTime::from_timestamp_millis(1_704_003_600_000).expect("end");
    (start, end)
}
