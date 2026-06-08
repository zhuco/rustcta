use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::ExchangeSymbol;
use serde_json::Value;

use super::parser::{normalize_coinmetro_symbol, value_as_u64};
use super::signing::websocket_token_query;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoinmetroWsMessage {
    BookUpdate { pair: String, sequence: Option<u64> },
    Tick { pair: String, sequence: Option<u64> },
    OrderStatus { order_id: Option<String> },
    WalletUpdate { currency: Option<String> },
    Unknown,
}

pub fn public_stream_url(ws_url: &str, symbols: &[ExchangeSymbol]) -> ExchangeApiResult<String> {
    stream_url(ws_url, None, symbols)
}

pub fn private_stream_url(
    ws_url: &str,
    device_id: Option<&str>,
    token: &str,
    symbols: &[ExchangeSymbol],
) -> ExchangeApiResult<String> {
    stream_url(
        ws_url,
        websocket_token_query(device_id, token).as_deref(),
        symbols,
    )
}

pub fn parse_ws_message(value: &Value) -> ExchangeApiResult<CoinmetroWsMessage> {
    if let Some(book) = value.get("bookUpdate") {
        return Ok(CoinmetroWsMessage::BookUpdate {
            pair: required_pair(book)?,
            sequence: book.get("seqNumber").and_then(value_as_u64),
        });
    }
    if let Some(tick) = value.get("tick") {
        return Ok(CoinmetroWsMessage::Tick {
            pair: required_pair(tick)?,
            sequence: tick.get("seqNum").and_then(value_as_u64),
        });
    }
    if let Some(order) = value.get("orderStatus") {
        return Ok(CoinmetroWsMessage::OrderStatus {
            order_id: order
                .get("orderID")
                .and_then(Value::as_str)
                .map(str::to_string),
        });
    }
    if let Some(wallet) = value.get("walletUpdate") {
        return Ok(CoinmetroWsMessage::WalletUpdate {
            currency: wallet
                .get("currency")
                .and_then(Value::as_str)
                .map(str::to_string),
        });
    }
    Ok(CoinmetroWsMessage::Unknown)
}

pub fn coinmetro_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: false,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
        ],
        supports_client_order_id: false,
        supports_exchange_order_id: true,
    }
}

fn stream_url(
    ws_url: &str,
    token_query: Option<&str>,
    symbols: &[ExchangeSymbol],
) -> ExchangeApiResult<String> {
    let mut query = Vec::new();
    let pairs = symbols
        .iter()
        .map(|symbol| normalize_coinmetro_symbol(&symbol.symbol))
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    if !pairs.is_empty() {
        query.push(format!("pairs={}", pairs.join(",")));
    }
    if let Some(token) = token_query.map(str::trim).filter(|value| !value.is_empty()) {
        query.push(format!("token={token}"));
    }
    if query.is_empty() {
        Ok(ws_url.trim_end_matches('?').to_string())
    } else {
        Ok(format!(
            "{}{}{}",
            ws_url.trim_end_matches('?'),
            if ws_url.contains('?') { "&" } else { "?" },
            query.join("&")
        ))
    }
}

fn required_pair(value: &Value) -> ExchangeApiResult<String> {
    value
        .get("pair")
        .and_then(Value::as_str)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmetro websocket message missing pair".to_string(),
        })
        .and_then(normalize_coinmetro_symbol)
}
