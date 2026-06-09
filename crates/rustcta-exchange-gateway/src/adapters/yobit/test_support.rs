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
    pub(super) headers: HashMap<String, String>,
    pub(super) body: Option<Value>,
    pub(super) body_text: String,
    pub(super) form: HashMap<String, String>,
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
            if bytes_read == 0 {
                continue;
            }
            let mut request_bytes = buffer[..bytes_read].to_vec();
            if let Some(content_length) = content_length(&request_bytes) {
                while body_len(&request_bytes) < content_length {
                    let bytes_read = stream.read(&mut buffer).await.unwrap();
                    if bytes_read == 0 {
                        break;
                    }
                    request_bytes.extend_from_slice(&buffer[..bytes_read]);
                }
            }
            let request_text = String::from_utf8_lossy(&request_bytes).to_string();
            seen_requests
                .lock()
                .unwrap()
                .push(parse_seen_request(&request_text));
            let body = responses
                .lock()
                .unwrap()
                .next()
                .unwrap_or_else(|| json!({ "ok": true }));
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

fn content_length(request: &[u8]) -> Option<usize> {
    let text = String::from_utf8_lossy(request);
    text.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse().ok())
            .flatten()
    })
}

fn body_len(request: &[u8]) -> usize {
    request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| request.len().saturating_sub(index + 4))
        .unwrap_or(0)
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
            Some((key.to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    let body = request_text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.trim())
        .unwrap_or_default()
        .to_string();
    let form = body
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (
                key.to_string(),
                urlencoding::decode(value)
                    .map(|value| value.into_owned())
                    .unwrap_or_else(|_| value.to_string()),
            )
        })
        .collect();
    let json_body = (!body.is_empty())
        .then_some(body.as_str())
        .filter(|body| !body.is_empty())
        .and_then(|body| serde_json::from_str(body).ok());
    SeenRequest {
        method,
        path: path.to_string(),
        query,
        headers,
        body: json_body,
        body_text: body,
        form,
    }
}

pub(super) fn exchange_id() -> ExchangeId {
    ExchangeId::new("yobit").expect("yobit exchange")
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
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "btc_usdt")
            .expect("symbol"),
    }
}
