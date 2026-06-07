use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
pub(super) struct SeenRequest {
    pub(super) method: String,
    pub(super) path: String,
    pub(super) query: HashMap<String, String>,
    pub(super) form: HashMap<String, String>,
    pub(super) json: Value,
    pub(super) headers: HashMap<String, String>,
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
    let query = parse_pairs(query_text);
    let headers = request_text
        .lines()
        .skip(1)
        .take_while(|line| !line.trim().is_empty())
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    let body = request_text.split("\r\n\r\n").nth(1).unwrap_or_default();
    let form = parse_pairs(body);
    let json = serde_json::from_str(body).unwrap_or(Value::Null);
    SeenRequest {
        method,
        path: path.to_string(),
        query,
        form,
        json,
        headers,
    }
}

fn parse_pairs(text: &str) -> HashMap<String, String> {
    text.split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (key.to_string(), value.to_string())
        })
        .collect()
}

pub(super) fn exchange_id() -> ExchangeId {
    ExchangeId::new("lbank").expect("lbank exchange")
}

pub(super) fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("account").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

pub(super) fn spot_symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "btc_usdt")
            .expect("symbol"),
    }
}

pub(super) fn perp_symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTCUSDT")
            .expect("symbol"),
    }
}

pub(super) fn contract_symbol_scope() -> SymbolScope {
    perp_symbol_scope()
}

pub(super) fn private_config(base_url: String) -> super::LBankGatewayConfig {
    super::LBankGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        contract_rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..super::LBankGatewayConfig::default()
    }
}

pub(super) fn assert_signed_form_request(request: &SeenRequest, path: &str) {
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, path);
    assert_eq!(
        request.headers.get("signature_method").map(String::as_str),
        Some("HmacSHA256")
    );
    assert!(request
        .headers
        .get("timestamp")
        .is_some_and(|value| !value.is_empty()));
    assert!(request
        .headers
        .get("echostr")
        .is_some_and(|value| !value.is_empty()));
    assert_eq!(request.form.get("api_key").map(String::as_str), Some("key"));
    assert!(!request.form.contains_key("signature_method"));
    assert!(!request.form.contains_key("timestamp"));
    assert!(!request.form.contains_key("echostr"));
    assert!(request
        .form
        .get("sign")
        .is_some_and(|value| !value.is_empty()));
}

pub(super) fn assert_signed_json_request(request: &SeenRequest, path: &str) {
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, path);
    assert_eq!(
        request.headers.get("signature_method").map(String::as_str),
        Some("HmacSHA256")
    );
    assert!(request
        .headers
        .get("timestamp")
        .is_some_and(|value| !value.is_empty()));
    assert!(request
        .headers
        .get("echostr")
        .is_some_and(|value| !value.is_empty()));
    assert_eq!(request.json["api_key"], "key");
    assert!(request
        .json
        .get("sign")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty()));
    assert!(request.json.get("signature_method").is_none());
    assert!(request.json.get("timestamp").is_none());
    assert!(request.json.get("echostr").is_none());
}
