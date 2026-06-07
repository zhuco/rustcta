#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, PrivateOrderStreamEventKind,
    PrivateStreamCapabilities, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType, OrderSide};
use serde_json::{json, Value};

use super::parser::{normalize_backpack_symbol, parse_error, parse_orderbook_snapshot};
use super::private_parser::{parse_fills, parse_order, parse_positions};
use super::signing::{backpack_signature, canonical_signing_payload};
use super::BackpackGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Clone, PartialEq)]
pub enum BackpackPublicStreamMessage {
    OrderBook(rustcta_types::OrderBookSnapshot),
    Trade(BackpackPublicTrade),
    Ticker(BackpackTicker),
    Candle(BackpackCandle),
    SubscriptionAck,
    Heartbeat,
    Ignored,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackpackPublicTrade {
    pub symbol: rustcta_exchange_api::SymbolScope,
    pub side: OrderSide,
    pub price: String,
    pub quantity: String,
    pub trade_id: Option<String>,
    pub traded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackpackTicker {
    pub symbol: rustcta_exchange_api::SymbolScope,
    pub last_price: Option<String>,
    pub mark_price: Option<String>,
    pub index_price: Option<String>,
    pub high_price: Option<String>,
    pub low_price: Option<String>,
    pub volume: Option<String>,
    pub quote_volume: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackpackCandle {
    pub symbol: rustcta_exchange_api::SymbolScope,
    pub interval: Option<String>,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
}

impl BackpackGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market_type(subscription.symbol.market_type)?;
        Ok(format!(
            "backpack:{}:{}",
            self.config.public_ws_url,
            backpack_public_stream(&subscription)
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_rest("backpack.subscribe_private_stream")?;
        Ok(format!(
            "backpack:{}:{}:{}",
            self.config.private_ws_url,
            backpack_private_stream(&subscription)?,
            subscription.account_id
        ))
    }

    pub fn private_subscribe_payload(
        &self,
        subscription: &PrivateStreamSubscription,
        timestamp_ms: i64,
    ) -> ExchangeApiResult<Value> {
        let key = self
            .config
            .api_key
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or(ExchangeApiError::Unsupported {
                operation: "backpack.private_ws_credentials",
            })?;
        let secret = self
            .config
            .api_secret
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or(ExchangeApiError::Unsupported {
                operation: "backpack.private_ws_credentials",
            })?;
        let payload = canonical_signing_payload(
            "subscribe",
            &std::collections::HashMap::new(),
            timestamp_ms,
            self.config.recv_window_ms,
        );
        Ok(json!({
            "method": "SUBSCRIBE",
            "params": [backpack_private_stream(subscription)?],
            "signature": [
                key,
                backpack_signature(secret, &payload)?,
                timestamp_ms.to_string(),
                self.config.recv_window_ms.to_string(),
            ],
        }))
    }
}

pub fn backpack_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: false,
        supports_balances: false,
        supports_positions: true,
        supports_account: false,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::Ack,
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
            PrivateOrderStreamEventKind::Expired,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn backpack_public_subscribe_payload(subscription: &PublicStreamSubscription) -> Value {
    json!({
        "method": "SUBSCRIBE",
        "params": [backpack_public_stream(subscription)],
    })
}

pub fn backpack_unsubscribe_payload(stream: &str) -> Value {
    json!({
        "method": "UNSUBSCRIBE",
        "params": [stream],
    })
}

pub fn backpack_ping_payload() -> Value {
    json!({ "method": "PING" })
}

pub fn backpack_pong_payload() -> Value {
    json!({ "method": "PONG" })
}

pub fn parse_backpack_stream_event(
    exchange_id: &ExchangeId,
    fallback: Option<&rustcta_exchange_api::SymbolScope>,
    text: &str,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let value =
        serde_json::from_str::<Value>(text).map_err(|error| ExchangeApiError::Serialization {
            message: error.to_string(),
        })?;
    match parse_backpack_public_stream_message(exchange_id, fallback, &value)? {
        BackpackPublicStreamMessage::Heartbeat => {
            return Ok(vec![ExchangeStreamEvent::Heartbeat {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                exchange: exchange_id.clone(),
                received_at: Utc::now(),
            }]);
        }
        BackpackPublicStreamMessage::OrderBook(order_book) => {
            return Ok(vec![ExchangeStreamEvent::OrderBookSnapshot(
                rustcta_exchange_api::OrderBookResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(exchange_id.clone(), None),
                    order_book,
                },
            )]);
        }
        _ => {}
    }
    let event_type = backpack_event_type(&value);
    let data = value.get("data").unwrap_or(&value);
    if event_type.contains("depth") || event_type.contains("bookticker") {
        let order_book = parse_backpack_order_book_stream(exchange_id, fallback, data)?;
        return Ok(vec![ExchangeStreamEvent::OrderBookSnapshot(
            rustcta_exchange_api::OrderBookResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                order_book,
            },
        )]);
    }
    if event_type.contains("order") {
        let market_type = fallback
            .map(|symbol| symbol.market_type)
            .unwrap_or(MarketType::Spot);
        if let Some(order) = parse_order(
            exchange_id,
            fallback,
            market_type,
            value.get("data").unwrap_or(&value),
        )? {
            return Ok(vec![ExchangeStreamEvent::OrderUpdate(order)]);
        }
    }
    if event_type.contains("fill") {
        let symbol = fallback.ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "backpack fill stream parser requires symbol hint".to_string(),
        })?;
        let tenant = rustcta_exchange_api::TenantId::new("stream").map_err(|error| {
            ExchangeApiError::InvalidRequest {
                message: error.to_string(),
            }
        })?;
        let account = rustcta_exchange_api::AccountId::new("stream").map_err(|error| {
            ExchangeApiError::InvalidRequest {
                message: error.to_string(),
            }
        })?;
        return Ok(parse_fills(
            exchange_id,
            tenant,
            account,
            Some(symbol),
            symbol.market_type,
            value.get("data").unwrap_or(&value),
        )?
        .into_iter()
        .map(ExchangeStreamEvent::Fill)
        .collect());
    }
    if event_type.contains("position") {
        let tenant = rustcta_exchange_api::TenantId::new("stream").map_err(|error| {
            ExchangeApiError::InvalidRequest {
                message: error.to_string(),
            }
        })?;
        let account = rustcta_exchange_api::AccountId::new("stream").map_err(|error| {
            ExchangeApiError::InvalidRequest {
                message: error.to_string(),
            }
        })?;
        let positions = parse_positions(
            exchange_id,
            tenant,
            account,
            &[],
            value.get("data").unwrap_or(&value),
        )?;
        return Ok(vec![ExchangeStreamEvent::PositionSnapshot(
            rustcta_exchange_api::PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(exchange_id.clone(), None),
                positions,
            },
        )]);
    }
    Ok(Vec::new())
}

pub fn parse_backpack_public_stream_message(
    exchange_id: &ExchangeId,
    fallback: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<BackpackPublicStreamMessage> {
    let event_type = backpack_event_type(value);
    if event_type.contains("pong")
        || event_type.contains("ping")
        || value
            .get("method")
            .and_then(Value::as_str)
            .is_some_and(|method| matches!(method.to_ascii_uppercase().as_str(), "PING" | "PONG"))
    {
        return Ok(BackpackPublicStreamMessage::Heartbeat);
    }
    if value.get("result").and_then(Value::as_str) == Some("success")
        || event_type.contains("subscribed")
    {
        return Ok(BackpackPublicStreamMessage::SubscriptionAck);
    }
    let data = value.get("data").unwrap_or(value);
    if event_type.contains("depth") || event_type.contains("bookticker") {
        return Ok(BackpackPublicStreamMessage::OrderBook(
            parse_backpack_order_book_stream(exchange_id, fallback, data)?,
        ));
    }
    if event_type.contains("trade") {
        return Ok(BackpackPublicStreamMessage::Trade(
            parse_backpack_public_trade(exchange_id, fallback, data)?,
        ));
    }
    if event_type.contains("ticker") {
        return Ok(BackpackPublicStreamMessage::Ticker(parse_backpack_ticker(
            exchange_id,
            fallback,
            data,
        )?));
    }
    if event_type.contains("kline") || event_type.contains("candle") {
        return Ok(BackpackPublicStreamMessage::Candle(parse_backpack_candle(
            exchange_id,
            fallback,
            data,
            value.get("stream").and_then(Value::as_str),
        )?));
    }
    Ok(BackpackPublicStreamMessage::Ignored)
}

pub fn backpack_public_stream(subscription: &PublicStreamSubscription) -> String {
    let symbol = normalize_backpack_symbol(&subscription.symbol.exchange_symbol.symbol);
    match &subscription.kind {
        PublicStreamKind::Trades => format!("trade.{symbol}"),
        PublicStreamKind::Ticker => format!("ticker.{symbol}"),
        PublicStreamKind::OrderBookDelta => format!("depth.{symbol}"),
        PublicStreamKind::OrderBookSnapshot => format!("bookTicker.{symbol}"),
        PublicStreamKind::Candles { interval } => format!("kline.{interval}.{symbol}"),
    }
}

pub fn backpack_private_stream(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<&'static str> {
    match subscription.kind {
        PrivateStreamKind::Orders => Ok("account.orderUpdate"),
        PrivateStreamKind::Positions => Ok("account.positionUpdate"),
        PrivateStreamKind::Fills => Err(ExchangeApiError::Unsupported {
            operation: "backpack.private_fills_stream_unverified",
        }),
        PrivateStreamKind::Balances => Err(ExchangeApiError::Unsupported {
            operation: "backpack.private_balances_stream_unverified",
        }),
        PrivateStreamKind::Account => Err(ExchangeApiError::Unsupported {
            operation: "backpack.private_account_stream_unverified",
        }),
    }
}

fn parse_backpack_order_book_stream(
    exchange_id: &ExchangeId,
    fallback: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<rustcta_types::OrderBookSnapshot> {
    let symbol = fallback
        .cloned()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "backpack order book stream parser requires symbol hint".to_string(),
        })?;
    let normalized = json!({
        "bids": value.get("bids").or_else(|| value.get("b")).cloned().unwrap_or_else(|| json!([])),
        "asks": value.get("asks").or_else(|| value.get("a")).cloned().unwrap_or_else(|| json!([])),
        "lastUpdateId": value
            .get("lastUpdateId")
            .or_else(|| value.get("u"))
            .or_else(|| value.get("U"))
            .cloned()
            .unwrap_or(Value::Null),
        "timestamp": value
            .get("timestamp")
            .or_else(|| value.get("E"))
            .or_else(|| value.get("T"))
            .cloned()
            .unwrap_or(Value::Null),
    });
    parse_orderbook_snapshot(exchange_id, symbol, &normalized)
}

fn parse_backpack_public_trade(
    exchange_id: &ExchangeId,
    fallback: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<BackpackPublicTrade> {
    let symbol = fallback
        .cloned()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "backpack trade stream parser requires symbol hint".to_string(),
        })?;
    Ok(BackpackPublicTrade {
        symbol,
        side: parse_trade_side(
            value
                .get("side")
                .or_else(|| value.get("S"))
                .or_else(|| value.get("m")),
        ),
        price: text(value.get("price").or_else(|| value.get("p"))).ok_or_else(|| {
            parse_error(exchange_id.clone(), "Backpack trade missing price", value)
        })?,
        quantity: text(value.get("quantity").or_else(|| value.get("q"))).ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Backpack trade missing quantity",
                value,
            )
        })?,
        trade_id: text(
            value
                .get("id")
                .or_else(|| value.get("tradeId"))
                .or_else(|| value.get("t")),
        ),
        traded_at: timestamp(value, Utc::now()),
    })
}

fn parse_backpack_ticker(
    _exchange_id: &ExchangeId,
    fallback: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<BackpackTicker> {
    let symbol = fallback
        .cloned()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "backpack ticker stream parser requires symbol hint".to_string(),
        })?;
    Ok(BackpackTicker {
        symbol,
        last_price: text(
            value
                .get("lastPrice")
                .or_else(|| value.get("last"))
                .or_else(|| value.get("c")),
        ),
        mark_price: text(value.get("markPrice")),
        index_price: text(value.get("indexPrice")),
        high_price: text(value.get("high").or_else(|| value.get("highPrice"))),
        low_price: text(value.get("low").or_else(|| value.get("lowPrice"))),
        volume: text(value.get("volume").or_else(|| value.get("v"))),
        quote_volume: text(value.get("quoteVolume").or_else(|| value.get("V"))),
        updated_at: timestamp(value, Utc::now()),
    })
}

fn parse_backpack_candle(
    exchange_id: &ExchangeId,
    fallback: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
    stream: Option<&str>,
) -> ExchangeApiResult<BackpackCandle> {
    let symbol = fallback
        .cloned()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "backpack candle stream parser requires symbol hint".to_string(),
        })?;
    Ok(BackpackCandle {
        symbol,
        interval: text(value.get("interval").or_else(|| value.get("i")))
            .or_else(|| stream.and_then(kline_interval_from_stream)),
        opened_at: timestamp_from_fields(value, &["start", "startTime", "t"]).ok_or_else(|| {
            parse_error(exchange_id.clone(), "Backpack candle missing start", value)
        })?,
        closed_at: timestamp_from_fields(value, &["end", "endTime", "T"]),
        open: text(value.get("open").or_else(|| value.get("o"))).ok_or_else(|| {
            parse_error(exchange_id.clone(), "Backpack candle missing open", value)
        })?,
        high: text(value.get("high").or_else(|| value.get("h"))).ok_or_else(|| {
            parse_error(exchange_id.clone(), "Backpack candle missing high", value)
        })?,
        low: text(value.get("low").or_else(|| value.get("l"))).ok_or_else(|| {
            parse_error(exchange_id.clone(), "Backpack candle missing low", value)
        })?,
        close: text(value.get("close").or_else(|| value.get("c"))).ok_or_else(|| {
            parse_error(exchange_id.clone(), "Backpack candle missing close", value)
        })?,
        volume: text(value.get("volume").or_else(|| value.get("v"))).ok_or_else(|| {
            parse_error(exchange_id.clone(), "Backpack candle missing volume", value)
        })?,
    })
}

fn backpack_event_type(value: &Value) -> String {
    value
        .get("e")
        .or_else(|| value.get("eventType"))
        .or_else(|| value.get("stream"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
}

fn parse_trade_side(value: Option<&Value>) -> OrderSide {
    match value {
        Some(Value::Bool(is_buyer_maker)) => {
            if *is_buyer_maker {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            }
        }
        Some(Value::String(text))
            if matches!(text.to_ascii_lowercase().as_str(), "ask" | "sell") =>
        {
            OrderSide::Sell
        }
        _ => OrderSide::Buy,
    }
}

fn timestamp(value: &Value, fallback: DateTime<Utc>) -> DateTime<Utc> {
    timestamp_from_fields(value, &["timestamp", "time", "E", "T"]).unwrap_or(fallback)
}

fn timestamp_from_fields(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field))
        .and_then(|value| text(Some(value)))
        .and_then(|text| text.parse::<i64>().ok())
        .and_then(timestamp_from_number)
}

fn timestamp_from_number(value: i64) -> Option<DateTime<Utc>> {
    if value > 10_000_000_000_000 {
        Utc.timestamp_micros(value).single()
    } else {
        Utc.timestamp_millis_opt(value).single()
    }
}

fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn kline_interval_from_stream(stream: &str) -> Option<String> {
    let mut parts = stream.split('.');
    if parts.next()?.eq_ignore_ascii_case("kline") {
        parts.next().map(str::to_string)
    } else {
        None
    }
}
