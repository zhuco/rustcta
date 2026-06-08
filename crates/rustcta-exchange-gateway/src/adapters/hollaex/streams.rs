use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiResult, HeartbeatDirection, HeartbeatPolicy, OrderBookResponse, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderBookLevel};
use serde_json::{json, Value};

use super::parser::{
    number_from_value, parse_datetime_value, split_symbol_guess, validation_error,
};
use super::signing::{sign_hmac_sha256_hex, ws_connect_hmac_sha256_payload};

pub fn public_orderbook_subscribe_payload(symbols: &[String]) -> Value {
    json!({
        "op": "subscribe",
        "args": symbols
            .iter()
            .map(|symbol| format!("orderbook:{symbol}"))
            .collect::<Vec<_>>()
    })
}

pub fn public_orderbook_unsubscribe_payload(symbols: &[String]) -> Value {
    json!({
        "op": "unsubscribe",
        "args": symbols
            .iter()
            .map(|symbol| format!("orderbook:{symbol}"))
            .collect::<Vec<_>>()
    })
}

pub fn client_ping_payload() -> Value {
    json!({"op": "ping"})
}

pub fn private_ws_url_with_hmac(
    base_url: &str,
    api_key: &str,
    api_secret: &str,
    expires_s: u64,
) -> ExchangeApiResult<String> {
    let payload = ws_connect_hmac_sha256_payload(expires_s);
    let signature = sign_hmac_sha256_hex(api_secret, &payload)?;
    Ok(format!(
        "{}?api-key={}&api-expires={}&api-signature={}",
        base_url.trim_end_matches('/'),
        urlencoding::encode(api_key),
        expires_s,
        urlencoding::encode(&signature)
    ))
}

pub fn heartbeat_policy() -> HeartbeatPolicy {
    HeartbeatPolicy {
        direction: HeartbeatDirection::ClientPing,
        ping_interval_ms: 30_000,
        pong_timeout_ms: 10_000,
        stale_message_ms: 60_000,
        requires_pong_payload_echo: false,
    }
}

pub fn parse_public_orderbook_notification(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderBookResponse>> {
    let data = value.get("data").unwrap_or(value);
    let symbol = value
        .get("symbol")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("topic")
                .or_else(|| value.get("channel"))
                .and_then(Value::as_str)
                .and_then(|topic| topic.split_once(':').map(|(_, symbol)| symbol))
        })
        .ok_or_else(|| rustcta_exchange_api::ExchangeApiError::InvalidRequest {
            message: "HollaEx WS order book notification missing symbol".to_string(),
        })?;
    parse_ws_book(exchange_id, symbol, data).map(|response| vec![response])
}

fn parse_ws_book(
    exchange_id: &ExchangeId,
    exchange_symbol: &str,
    value: &Value,
) -> ExchangeApiResult<OrderBookResponse> {
    let (base, quote) = split_symbol_guess(exchange_symbol).ok_or_else(|| {
        rustcta_exchange_api::ExchangeApiError::InvalidRequest {
            message: format!("cannot infer HollaEx WS symbol {exchange_symbol}"),
        }
    })?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            exchange_symbol.to_string(),
        )
        .map_err(validation_error)?,
    };
    let asks = parse_ws_levels(value.get("asks"))?;
    let bids = parse_ws_levels(value.get("bids"))?;
    let mut snapshot = rustcta_types::OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        symbol
            .canonical_symbol
            .clone()
            .expect("canonical symbol just constructed"),
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.exchange_timestamp = value.get("timestamp").and_then(parse_datetime_value);
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: rustcta_exchange_api::ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        order_book: snapshot,
    })
}

fn parse_ws_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value.and_then(Value::as_array).ok_or_else(|| {
        rustcta_exchange_api::ExchangeApiError::InvalidRequest {
            message: "HollaEx WS order book missing levels".to_string(),
        }
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                rustcta_exchange_api::ExchangeApiError::InvalidRequest {
                    message: "HollaEx WS order book level is not an array".to_string(),
                }
            })?;
            let price = array.first().and_then(number_from_value).ok_or_else(|| {
                rustcta_exchange_api::ExchangeApiError::InvalidRequest {
                    message: "HollaEx WS order book level has invalid price".to_string(),
                }
            })?;
            let quantity = array.get(1).and_then(number_from_value).ok_or_else(|| {
                rustcta_exchange_api::ExchangeApiError::InvalidRequest {
                    message: "HollaEx WS order book level has invalid quantity".to_string(),
                }
            })?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}
