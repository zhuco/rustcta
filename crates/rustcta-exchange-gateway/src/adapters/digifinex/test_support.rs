use std::io::Write;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct SeenRequest {
    pub method: String,
    pub path: String,
    pub query: String,
    pub body: String,
    pub headers: Vec<(String, String)>,
}

pub async fn spawn_rest_server(responses: Vec<Value>) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().expect("local addr");
    listener.set_nonblocking(true).expect("nonblocking");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_thread = seen.clone();
    std::thread::spawn(move || {
        let mut responses = responses.into_iter();
        loop {
            let Ok((mut stream, _)) = listener.accept() else {
                std::thread::sleep(std::time::Duration::from_millis(5));
                continue;
            };
            let mut buffer = [0_u8; 8192];
            let Ok(read) = std::io::Read::read(&mut stream, &mut buffer) else {
                continue;
            };
            let request = String::from_utf8_lossy(&buffer[..read]).to_string();
            let mut lines = request.lines();
            let first = lines.next().unwrap_or_default();
            let mut first_parts = first.split_whitespace();
            let method = first_parts.next().unwrap_or_default().to_string();
            let target = first_parts.next().unwrap_or_default();
            let (path, query) = target
                .split_once('?')
                .map(|(path, query)| (path.to_string(), query.to_string()))
                .unwrap_or_else(|| (target.to_string(), String::new()));
            let body = request
                .split("\r\n\r\n")
                .nth(1)
                .unwrap_or_default()
                .to_string();
            let headers = request
                .lines()
                .skip(1)
                .take_while(|line| !line.trim().is_empty())
                .filter_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    Some((key.trim().to_string(), value.trim().to_string()))
                })
                .collect();
            seen_thread.lock().unwrap().push(SeenRequest {
                method,
                path,
                query,
                body,
                headers,
            });
            let response = responses
                .next()
                .unwrap_or_else(|| serde_json::json!({"code": 0}));
            let body = response.to_string();
            let http = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(http.as_bytes());
        }
    });
    (format!("http://{address}"), seen)
}

pub fn exchange_id() -> ExchangeId {
    ExchangeId::new("digifinex").unwrap()
}

pub fn context(id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        request_id: Some(id.to_string()),
        tenant_id: Some(rustcta_types::TenantId::new("tenant").unwrap()),
        account_id: Some(rustcta_types::AccountId::new("account").unwrap()),
        run_id: None,
        requested_at: chrono::Utc::now(),
    }
}

pub fn spot_symbol_scope() -> SymbolScope {
    symbol_scope(MarketType::Spot, "BTC", "USDT", "btc_usdt")
}

pub fn swap_symbol_scope() -> SymbolScope {
    symbol_scope(MarketType::Perpetual, "BTC", "USDT", "btc_usdt")
}

fn symbol_scope(market_type: MarketType, base: &str, quote: &str, symbol: &str) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, symbol).unwrap(),
    }
}

pub fn assert_signed_request(request: &SeenRequest, method: &str, path: &str) {
    assert_eq!(request.method, method);
    assert_eq!(request.path, path);
    assert!(request
        .headers
        .iter()
        .any(|(key, _)| key.eq_ignore_ascii_case("ACCESS-KEY")));
    assert!(request
        .headers
        .iter()
        .any(|(key, _)| key.eq_ignore_ascii_case("ACCESS-SIGN")));
    assert!(!request.query.contains("secret"));
    assert!(!request.body.contains("secret"));
}
