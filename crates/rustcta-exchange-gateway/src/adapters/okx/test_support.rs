use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::request_spec::ActualHttpRequest;

use super::OkxGatewayConfig;

#[derive(Debug, Clone)]
pub(super) struct SeenRequest {
    pub(super) method: String,
    pub(super) path: String,
    pub(super) query: HashMap<String, String>,
    pub(super) headers: HashMap<String, String>,
    pub(super) body: Option<Value>,
}

impl SeenRequest {
    pub(super) fn actual_http_request(&self) -> ActualHttpRequest {
        ActualHttpRequest::new(self.method.clone(), self.path.clone())
            .with_query(self.query.clone())
            .with_headers(self.headers.clone())
            .with_body(self.body.clone())
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
    let body = request_text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.trim())
        .filter(|body| !body.is_empty())
        .and_then(|body| serde_json::from_str(body).ok());
    SeenRequest {
        method,
        path: path.to_string(),
        query,
        headers,
        body,
    }
}

pub(super) fn exchange_id() -> ExchangeId {
    ExchangeId::new("okx").expect("okx exchange")
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

pub(super) fn symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTC-USDT")
            .expect("symbol"),
    }
}

pub(super) fn perpetual_symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTC-USDT-SWAP")
            .expect("symbol"),
    }
}

pub(super) fn futures_symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Futures,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Futures, "BTC-USD-240329")
            .expect("symbol"),
    }
}

pub(super) fn option_symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Option,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id(),
            MarketType::Option,
            "BTC-USD-240329-65000-C",
        )
        .expect("symbol"),
    }
}

pub(super) fn private_config(base_url: String) -> OkxGatewayConfig {
    OkxGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..OkxGatewayConfig::default()
    }
}

pub(super) fn assert_signed_okx_request(request: &SeenRequest, path: &str) {
    assert_signed_okx_request_method(request, "GET", path);
}

pub(super) fn assert_signed_okx_request_method(request: &SeenRequest, method: &str, path: &str) {
    assert_eq!(request.method, method);
    assert_eq!(request.path, path);
    assert_eq!(
        request.headers.get("ok-access-key").map(String::as_str),
        Some("key")
    );
    assert!(request
        .headers
        .get("ok-access-sign")
        .is_some_and(|value| !value.is_empty()));
    assert!(request
        .headers
        .get("ok-access-timestamp")
        .is_some_and(|value| !value.is_empty()));
    assert_eq!(
        request
            .headers
            .get("ok-access-passphrase")
            .map(String::as_str),
        Some("passphrase")
    );
}
