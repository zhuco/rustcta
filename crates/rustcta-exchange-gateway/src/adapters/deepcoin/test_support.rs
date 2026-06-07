use std::collections::{BTreeMap, HashMap};
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
    pub(super) body: String,
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
            let mut buffer = vec![0_u8; 16384];
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
                .unwrap_or_else(|| json!({ "code": "0", "data": {} }));
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
        .take_while(|line| !line.is_empty())
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
        headers,
        body,
    }
}

pub(super) fn exchange_id() -> ExchangeId {
    ExchangeId::new("deepcoin").expect("deepcoin exchange")
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
    symbol_scope(MarketType::Spot, "BTC-USDT")
}

pub(super) fn perp_symbol_scope() -> SymbolScope {
    symbol_scope(MarketType::Perpetual, "BTC-USDT-SWAP")
}

fn symbol_scope(market_type: MarketType, symbol: &str) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, symbol).expect("symbol"),
    }
}

pub(super) fn assert_signed_deepcoin_request(request: &SeenRequest, method: &str, path: &str) {
    assert_eq!(request.method, method);
    assert_eq!(request.path, path);
    assert_eq!(
        request.headers.get("dc-access-key").map(String::as_str),
        Some("key")
    );
    assert!(request
        .headers
        .get("dc-access-sign")
        .is_some_and(|signature| !signature.is_empty()));
    assert!(request
        .headers
        .get("dc-access-timestamp")
        .is_some_and(|timestamp| !timestamp.is_empty()));
    assert_eq!(
        request
            .headers
            .get("dc-access-passphrase")
            .map(String::as_str),
        Some("pass")
    );
}

pub(super) fn actual_request(request: SeenRequest) -> crate::request_spec::ActualHttpRequest {
    let body = serde_json::from_str(&request.body).ok();
    crate::request_spec::ActualHttpRequest::new(request.method, request.path)
        .with_query(to_btree(request.query))
        .with_headers(to_btree(request.headers))
        .with_body(body)
}

pub(super) fn request_spec(name: &str) -> crate::request_spec::RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/deepcoin/request_specs/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_str(&std::fs::read_to_string(path).expect("deepcoin request spec fixture"))
        .expect("deepcoin request spec json")
}

pub(super) fn signing_vector(name: &str) -> crate::signing_spec::SigningVector {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/deepcoin/signing_vectors/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_str(&std::fs::read_to_string(path).expect("deepcoin signing vector fixture"))
        .expect("deepcoin signing vector json")
}

fn to_btree(map: HashMap<String, String>) -> BTreeMap<String, String> {
    map.into_iter().collect()
}

pub(super) fn fixture(name: &str) -> Value {
    let text = match name {
        "balances_spot" => {
            include_str!("../../../../../tests/fixtures/exchanges/deepcoin/balances_spot.json")
        }
        "empty_array" => {
            include_str!("../../../../../tests/fixtures/exchanges/deepcoin/empty_array.json")
        }
        "empty_order" => {
            include_str!("../../../../../tests/fixtures/exchanges/deepcoin/empty_order.json")
        }
        "error_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/deepcoin/error_response.json")
        }
        "fee_perp" => {
            include_str!("../../../../../tests/fixtures/exchanges/deepcoin/fee_perp.json")
        }
        "fills_spot" => {
            include_str!("../../../../../tests/fixtures/exchanges/deepcoin/fills_spot.json")
        }
        "missing_inst_id_order" => {
            include_str!(
                "../../../../../tests/fixtures/exchanges/deepcoin/missing_inst_id_order.json"
            )
        }
        "open_orders" => {
            include_str!("../../../../../tests/fixtures/exchanges/deepcoin/open_orders.json")
        }
        "order_partially_filled" => {
            include_str!(
                "../../../../../tests/fixtures/exchanges/deepcoin/order_partially_filled.json"
            )
        }
        "orderbook_perp" => {
            include_str!("../../../../../tests/fixtures/exchanges/deepcoin/orderbook_perp.json")
        }
        "positions_perp" => {
            include_str!("../../../../../tests/fixtures/exchanges/deepcoin/positions_perp.json")
        }
        "private_ws_push_short" => {
            include_str!(
                "../../../../../tests/fixtures/exchanges/deepcoin/private_ws_push_short.json"
            )
        }
        "public_ws_book" => {
            include_str!("../../../../../tests/fixtures/exchanges/deepcoin/public_ws_book.json")
        }
        "spot_symbol_rules" => {
            include_str!("../../../../../tests/fixtures/exchanges/deepcoin/spot_symbol_rules.json")
        }
        "symbol_rules_missing_inst_id" => {
            include_str!(
                "../../../../../tests/fixtures/exchanges/deepcoin/symbol_rules_missing_inst_id.json"
            )
        }
        _ => unreachable!("unknown deepcoin fixture"),
    };
    serde_json::from_str(text).expect("deepcoin fixture json")
}
