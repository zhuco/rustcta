use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::ExchangeSymbol;
use serde_json::{json, Value};

use super::parser::{normalize_coinmetro_symbol, number_from_value, value_as_i64, value_as_u64};
use super::signing::websocket_token_query;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoinmetroSequenceDecision {
    Accept,
    Resync,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoinmetroWsMessage {
    BookUpdate { pair: String, sequence: Option<u64> },
    Tick { pair: String, sequence: Option<u64> },
    OrderStatus { order_id: Option<String> },
    WalletUpdate { currency: Option<String> },
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinmetroPublicOrderBookWsPolicy {
    pub url: &'static str,
    pub subscription: &'static str,
    pub book_update_event: &'static str,
    pub tick_event: &'static str,
    pub fixed_update_interval_ms: Option<u64>,
    pub depth: Option<u32>,
    pub sequence_field: &'static str,
    pub checksum_algorithm: &'static str,
    pub checksum_source: &'static str,
    pub rest_snapshot_endpoint: &'static str,
    pub resync_strategy: &'static str,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoinmetroBookLevelDelta {
    pub price: String,
    pub quantity_delta: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoinmetroBookUpdate {
    pub pair: String,
    pub sequence: u64,
    pub asks: Vec<CoinmetroBookLevelDelta>,
    pub bids: Vec<CoinmetroBookLevelDelta>,
    pub checksum: u32,
    pub checksum_matches_payload: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoinmetroBboTick {
    pub pair: String,
    pub sequence: Option<u64>,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub timestamp_ms: Option<i64>,
}

pub fn coinmetro_public_order_book_ws_policy() -> CoinmetroPublicOrderBookWsPolicy {
    CoinmetroPublicOrderBookWsPolicy {
        url: "wss://api.coinmetro.com/ws",
        subscription: "query string pairs=BTCEUR,LTCEUR",
        book_update_event: "bookUpdate",
        tick_event: "tick",
        fixed_update_interval_ms: None,
        depth: None,
        sequence_field: "bookUpdate.seqNumber",
        checksum_algorithm: "crc32_unsigned",
        checksum_source: "lexicographically sorted non-zero ask price/quantity pairs followed by non-zero bid price/quantity pairs, concatenated without separators",
        rest_snapshot_endpoint: "GET /exchange/book/{pair}",
        resync_strategy: "build the local book from REST /exchange/book/{pair}; apply bookUpdate deltas in monotonically increasing seqNumber order; compare checksum against the maintained book after every update; rebuild after reconnect, stale stream, sequence regression, parse error, or checksum mismatch",
    }
}

impl CoinmetroPublicOrderBookWsPolicy {
    pub fn as_json(&self) -> Value {
        json!({
            "url": self.url,
            "subscription": self.subscription,
            "events": {
                "book_update": self.book_update_event,
                "bbo": self.tick_event,
            },
            "fixed_update_interval_ms": self.fixed_update_interval_ms,
            "depth": self.depth,
            "sequence": {
                "field": self.sequence_field,
                "continuity": "monotonic freshness; resync on regression or duplicate after a local book has been initialized",
            },
            "checksum": {
                "algorithm": self.checksum_algorithm,
                "source": self.checksum_source,
                "resync_on_mismatch": true,
            },
            "resync": {
                "rest_snapshot_endpoint": self.rest_snapshot_endpoint,
                "strategy": self.resync_strategy,
            },
        })
    }
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

pub fn parse_book_update(value: &Value) -> ExchangeApiResult<CoinmetroBookUpdate> {
    let book = value.get("bookUpdate").unwrap_or(value);
    let pair = required_pair(book)?;
    let sequence = book
        .get("seqNumber")
        .and_then(value_as_u64)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmetro bookUpdate missing seqNumber".to_string(),
        })?;
    let checksum = book.get("checksum").and_then(value_as_u64).ok_or_else(|| {
        ExchangeApiError::InvalidRequest {
            message: "coinmetro bookUpdate missing checksum".to_string(),
        }
    })? as u32;
    Ok(CoinmetroBookUpdate {
        pair,
        sequence,
        asks: parse_book_side_deltas(book.get("ask"), false)?,
        bids: parse_book_side_deltas(book.get("bid"), true)?,
        checksum,
        checksum_matches_payload: coinmetro_book_checksum_matches(book)?,
    })
}

pub fn parse_tick_bbo(value: &Value) -> ExchangeApiResult<CoinmetroBboTick> {
    let tick = value.get("tick").unwrap_or(value);
    Ok(CoinmetroBboTick {
        pair: required_pair(tick)?,
        sequence: tick.get("seqNum").and_then(value_as_u64),
        bid: tick.get("bid").and_then(number_from_value),
        ask: tick.get("ask").and_then(number_from_value),
        timestamp_ms: tick.get("timestamp").and_then(value_as_i64),
    })
}

pub fn coinmetro_sequence_decision(
    previous_sequence: Option<u64>,
    next_sequence: u64,
) -> CoinmetroSequenceDecision {
    match previous_sequence {
        Some(previous) if next_sequence <= previous => CoinmetroSequenceDecision::Resync,
        _ => CoinmetroSequenceDecision::Accept,
    }
}

pub fn coinmetro_book_checksum_matches(book: &Value) -> ExchangeApiResult<bool> {
    let expected = book.get("checksum").and_then(value_as_u64).ok_or_else(|| {
        ExchangeApiError::InvalidRequest {
            message: "coinmetro book missing checksum".to_string(),
        }
    })? as u32;
    Ok(coinmetro_crc32_unsigned(coinmetro_book_checksum_string(book)?.as_bytes()) == expected)
}

pub fn coinmetro_book_checksum_string(book: &Value) -> ExchangeApiResult<String> {
    let mut parts = Vec::new();
    append_checksum_side(&mut parts, book.get("ask"))?;
    append_checksum_side(&mut parts, book.get("bid"))?;
    Ok(parts.join(""))
}

pub fn coinmetro_crc32_unsigned(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
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

fn parse_book_side_deltas(
    side: Option<&Value>,
    descending: bool,
) -> ExchangeApiResult<Vec<CoinmetroBookLevelDelta>> {
    let Some(side) = side else {
        return Ok(Vec::new());
    };
    let levels = side
        .as_object()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmetro book side must be an object".to_string(),
        })?;
    let mut parsed = Vec::new();
    for (price, quantity) in levels {
        parsed.push(CoinmetroBookLevelDelta {
            price: normalize_price_key(price)?,
            quantity_delta: number_from_value(quantity).ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: format!("coinmetro book side has invalid quantity {quantity}"),
                }
            })?,
        });
    }
    parsed.sort_by(|left, right| {
        let left_price = left.price.parse::<f64>().unwrap_or_default();
        let right_price = right.price.parse::<f64>().unwrap_or_default();
        if descending {
            right_price.total_cmp(&left_price)
        } else {
            left_price.total_cmp(&right_price)
        }
    });
    Ok(parsed)
}

fn append_checksum_side(parts: &mut Vec<String>, side: Option<&Value>) -> ExchangeApiResult<()> {
    let Some(side) = side else {
        return Ok(());
    };
    let levels = side
        .as_object()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmetro checksum side must be an object".to_string(),
        })?;
    let mut rows = levels
        .iter()
        .filter_map(|(price, quantity)| {
            let quantity = checksum_quantity_string(quantity)?;
            if quantity_is_zero(&quantity) {
                return None;
            }
            Some((price.as_str(), quantity))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(right.0));
    for (price, quantity) in rows {
        parts.push(normalize_price_key(price)?);
        parts.push(quantity);
    }
    Ok(())
}

fn normalize_price_key(price: &str) -> ExchangeApiResult<String> {
    if price.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinmetro book price must not be empty".to_string(),
        });
    }
    Ok(price.to_string())
}

fn checksum_quantity_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn quantity_is_zero(text: &str) -> bool {
    text.parse::<f64>().is_ok_and(|quantity| quantity == 0.0)
}
