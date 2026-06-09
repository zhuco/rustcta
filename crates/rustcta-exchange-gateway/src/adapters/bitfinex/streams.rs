use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent,
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType, OrderBookSnapshot, TenantId};
use serde_json::{json, Map, Value};

use super::parser::{
    normalize_bitfinex_symbol, parse_error, parse_orderbook_snapshot, value_as_f64, value_as_i64,
};
use super::private_parser::{parse_balances, parse_order, parse_positions};
use super::signing::bitfinex_ws_auth_signature;
use super::BitfinexGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

pub const BITFINEX_WS_HEARTBEAT_TIMEOUT_SECONDS: u64 = 30;
pub const BITFINEX_WS_SEQ_ALL_FLAG: u64 = 65_536;
pub const BITFINEX_WS_OB_CHECKSUM_FLAG: u64 = 131_072;
pub const BITFINEX_WS_BULK_UPDATES_FLAG: u64 = 536_870_912;

const BITFINEX_BOOK_PRECISIONS: &[&str] = &["P0", "P1", "P2", "P3", "P4"];
const BITFINEX_BOOK_FREQUENCIES: &[&str] = &["F0", "F1"];
const BITFINEX_BOOK_LENGTHS: &[u16] = &[1, 25, 100, 250];
const BITFINEX_DEFAULT_BOOK_PRECISION: &str = "P0";
const BITFINEX_DEFAULT_BOOK_FREQUENCY: &str = "F0";
const BITFINEX_DEFAULT_BOOK_LENGTH: u16 = 100;

impl BitfinexGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitfinex.public_streams_disabled",
            });
        }
        let payload = bitfinex_public_subscribe_payload(&subscription)?;
        let pre_subscribe = if matches!(
            subscription.kind,
            PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot
        ) {
            vec![bitfinex_public_conf_payload(true, true, false)]
        } else {
            Vec::new()
        };
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "url": self.config.public_ws_url,
            "pre_subscribe": pre_subscribe,
            "payload": payload,
            "unsubscribe": bitfinex_public_unsubscribe_payload(&subscription)?,
            "heartbeat": bitfinex_ws_heartbeat_spec(),
            "order_book_policy": bitfinex_public_orderbook_policy().as_json(),
            "resync": {
                "order_book": "rest_snapshot_after_reconnect_or_sequence_gap"
            }
        })
        .to_string())
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_supported_market(market_type)?;
        }
        if !self.config.private_streams_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitfinex.private_streams_disabled",
            });
        }
        let (api_key, api_secret) = self.private_credentials("bitfinex.private_ws_auth")?;
        let nonce = Utc::now().timestamp_micros().to_string();
        let auth = bitfinex_private_auth_payload(api_key, api_secret, &nonce);
        Ok(json!({
            "exchange": self.exchange_id.to_string(),
            "url": self.config.private_ws_url,
            "mode": "auth_channel_zero",
            "initial_payloads": [auth],
            "stream_filter": bitfinex_private_filter(&subscription),
            "heartbeat": bitfinex_ws_heartbeat_spec(),
            "reconciliation": "REST wallets/positions/orders/trades after reconnect"
        })
        .to_string())
    }
}

pub fn bitfinex_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: true,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn bitfinex_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let mut payload = Map::new();
    payload.insert("event".to_string(), json!("subscribe"));
    payload.insert(
        "channel".to_string(),
        json!(bitfinex_public_channel(&subscription.kind)?),
    );
    payload.insert(
        "symbol".to_string(),
        json!(normalize_bitfinex_symbol(
            &subscription.symbol.exchange_symbol.symbol,
            subscription.symbol.market_type
        )?),
    );
    if matches!(
        subscription.kind,
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot
    ) {
        payload.insert("prec".to_string(), json!(BITFINEX_DEFAULT_BOOK_PRECISION));
        payload.insert("freq".to_string(), json!(BITFINEX_DEFAULT_BOOK_FREQUENCY));
        payload.insert("len".to_string(), json!(BITFINEX_DEFAULT_BOOK_LENGTH));
    }
    Ok(Value::Object(payload))
}

pub fn bitfinex_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "event": "unsubscribe",
        "channel": bitfinex_public_channel(&subscription.kind)?,
        "symbol": normalize_bitfinex_symbol(
            &subscription.symbol.exchange_symbol.symbol,
            subscription.symbol.market_type
        )?,
    }))
}

pub fn bitfinex_private_auth_payload(api_key: &str, api_secret: &str, nonce: &str) -> Value {
    json!({
        "event": "auth",
        "apiKey": api_key,
        "authNonce": nonce,
        "authPayload": format!("AUTH{nonce}"),
        "authSig": bitfinex_ws_auth_signature(api_secret, nonce),
    })
}

pub fn bitfinex_ws_heartbeat_spec() -> Value {
    json!({
        "server_heartbeat": "hb",
        "server_event": "heartbeat",
        "timeout_seconds": BITFINEX_WS_HEARTBEAT_TIMEOUT_SECONDS,
        "resubscribe_on_reconnect": true,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitfinexPublicBookPolicy {
    pub channel: &'static str,
    pub default_precision: &'static str,
    pub supported_precisions: &'static [&'static str],
    pub default_frequency: &'static str,
    pub supported_frequencies: &'static [&'static str],
    pub default_length: u16,
    pub supported_lengths: &'static [u16],
    pub sequence_flag: u64,
    pub checksum_flag: u64,
    pub bulk_updates_flag: u64,
    pub checksum_algorithm: &'static str,
    pub rebuild_strategy: &'static str,
}

impl BitfinexPublicBookPolicy {
    pub fn as_json(&self) -> Value {
        json!({
            "channel": self.channel,
            "prec": {
                "default": self.default_precision,
                "supported": self.supported_precisions,
            },
            "freq": {
                "default": self.default_frequency,
                "supported": self.supported_frequencies,
                "F0": "realtime",
                "F1": "two_seconds",
            },
            "len": {
                "default": self.default_length,
                "supported": self.supported_lengths,
            },
            "conf_flags": {
                "SEQ_ALL": self.sequence_flag,
                "OB_CHECKSUM": self.checksum_flag,
                "BULK_UPDATES": self.bulk_updates_flag,
            },
            "checksum": {
                "algorithm": self.checksum_algorithm,
                "scope": "top_25_bids_and_asks",
                "resync_on_mismatch": true,
            },
            "rebuild": self.rebuild_strategy,
        })
    }
}

pub fn bitfinex_public_orderbook_policy() -> BitfinexPublicBookPolicy {
    BitfinexPublicBookPolicy {
        channel: "book",
        default_precision: BITFINEX_DEFAULT_BOOK_PRECISION,
        supported_precisions: BITFINEX_BOOK_PRECISIONS,
        default_frequency: BITFINEX_DEFAULT_BOOK_FREQUENCY,
        supported_frequencies: BITFINEX_BOOK_FREQUENCIES,
        default_length: BITFINEX_DEFAULT_BOOK_LENGTH,
        supported_lengths: BITFINEX_BOOK_LENGTHS,
        sequence_flag: BITFINEX_WS_SEQ_ALL_FLAG,
        checksum_flag: BITFINEX_WS_OB_CHECKSUM_FLAG,
        bulk_updates_flag: BITFINEX_WS_BULK_UPDATES_FLAG,
        checksum_algorithm: "crc32_signed_int",
        rebuild_strategy:
            "first book array is snapshot; apply updates in SEQ_ALL order; REST snapshot after reconnect, sequence gap, or checksum mismatch",
    }
}

pub fn bitfinex_public_conf_payload(checksum: bool, sequence: bool, bulk_updates: bool) -> Value {
    let mut flags = 0_u64;
    if checksum {
        flags += BITFINEX_WS_OB_CHECKSUM_FLAG;
    }
    if sequence {
        flags += BITFINEX_WS_SEQ_ALL_FLAG;
    }
    if bulk_updates {
        flags += BITFINEX_WS_BULK_UPDATES_FLAG;
    }
    json!({
        "event": "conf",
        "flags": flags,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitfinexBookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitfinexBookUpdateAction {
    Upsert,
    Delete,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BitfinexBookLevelUpdate {
    pub price: f64,
    pub count: i64,
    pub amount: f64,
    pub quantity: f64,
    pub side: BitfinexBookSide,
    pub action: BitfinexBookUpdateAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BitfinexPublicBookMessage {
    Snapshot {
        book: OrderBookSnapshot,
        sequence: Option<u64>,
    },
    Update {
        update: BitfinexBookLevelUpdate,
        sequence: Option<u64>,
    },
    Checksum {
        checksum: i64,
        sequence: Option<u64>,
    },
    Heartbeat,
    Control,
}

pub fn parse_bitfinex_public_book_message(
    exchange_id: &ExchangeId,
    symbol_hint: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<BitfinexPublicBookMessage> {
    if is_heartbeat(value) || is_subscription_event(value) {
        return Ok(BitfinexPublicBookMessage::Control);
    }
    let Some(array) = value.as_array() else {
        return Err(parse_error(
            exchange_id.clone(),
            "Bitfinex book message missing array envelope",
            value,
        ));
    };
    let Some(payload) = array.get(1) else {
        return Err(parse_error(
            exchange_id.clone(),
            "Bitfinex book message missing payload",
            value,
        ));
    };
    if payload.as_str() == Some("hb") {
        return Ok(BitfinexPublicBookMessage::Heartbeat);
    }
    if payload.as_str() == Some("cs") {
        let checksum = array.get(2).and_then(value_as_i64).ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Bitfinex checksum message missing checksum",
                value,
            )
        })?;
        return Ok(BitfinexPublicBookMessage::Checksum {
            checksum,
            sequence: extract_bitfinex_sequence(array, 3),
        });
    }
    if payload
        .as_array()
        .is_some_and(|rows| rows.first().is_some_and(Value::is_array))
    {
        let sequence = extract_bitfinex_sequence(array, 2);
        let mut book = parse_orderbook_snapshot(exchange_id, symbol_hint, payload)?;
        book.sequence = sequence;
        return Ok(BitfinexPublicBookMessage::Snapshot { book, sequence });
    }
    if payload.as_array().is_some() {
        return Ok(BitfinexPublicBookMessage::Update {
            update: parse_bitfinex_book_level_update(exchange_id, payload)?,
            sequence: extract_bitfinex_sequence(array, 2),
        });
    }
    Err(parse_error(
        exchange_id.clone(),
        "unsupported Bitfinex book message",
        value,
    ))
}

pub fn parse_bitfinex_book_level_update(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<BitfinexBookLevelUpdate> {
    let array = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitfinex book update missing array",
            value,
        )
    })?;
    let price = array.first().and_then(value_as_f64).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitfinex book update missing price",
            value,
        )
    })?;
    let count = array.get(1).and_then(value_as_i64).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitfinex book update missing count",
            value,
        )
    })?;
    let amount = array.get(2).and_then(value_as_f64).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Bitfinex book update missing amount",
            value,
        )
    })?;
    let side = if amount >= 0.0 {
        BitfinexBookSide::Bid
    } else {
        BitfinexBookSide::Ask
    };
    let action = if count == 0 {
        BitfinexBookUpdateAction::Delete
    } else {
        BitfinexBookUpdateAction::Upsert
    };
    Ok(BitfinexBookLevelUpdate {
        price,
        count,
        amount,
        quantity: if action == BitfinexBookUpdateAction::Delete {
            0.0
        } else {
            amount.abs()
        },
        side,
        action,
    })
}

pub fn bitfinex_sequence_gap(
    previous_sequence: Option<u64>,
    received_sequence: Option<u64>,
) -> Option<(u64, u64)> {
    let (Some(previous), Some(received)) = (previous_sequence, received_sequence) else {
        return None;
    };
    let expected = previous.checked_add(1)?;
    (received != expected).then_some((expected, received))
}

pub fn parse_bitfinex_public_stream_message(
    exchange_id: &ExchangeId,
    symbol_hint: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Option<ExchangeStreamEvent>> {
    if is_heartbeat(value) || is_subscription_event(value) {
        return Ok(None);
    }
    let Some(array) = value.as_array() else {
        return Ok(None);
    };
    let Some(payload) = array.get(1) else {
        return Ok(None);
    };
    if payload.as_str() == Some("hb") {
        return Ok(Some(ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }));
    }
    if payload
        .as_array()
        .is_some_and(|rows| rows.first().is_some_and(Value::is_array))
    {
        let book = parse_orderbook_snapshot(exchange_id, symbol_hint, payload)?;
        return Ok(Some(ExchangeStreamEvent::OrderBookSnapshot(
            rustcta_exchange_api::OrderBookResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: crate::adapters::response_metadata(exchange_id.clone(), None),
                order_book: book,
            },
        )));
    }
    Ok(None)
}

fn extract_bitfinex_sequence(array: &[Value], index: usize) -> Option<u64> {
    array
        .get(index)
        .and_then(value_as_i64)
        .map(|value| value as u64)
}

pub enum BitfinexPrivateStreamMessage {
    Order(rustcta_exchange_api::OrderState),
    Balance(rustcta_exchange_api::BalancesResponse),
    Position(rustcta_exchange_api::PositionsResponse),
    Heartbeat,
    Ignored,
}

pub fn parse_bitfinex_private_stream_message(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type_hint: MarketType,
    value: &Value,
) -> ExchangeApiResult<BitfinexPrivateStreamMessage> {
    if is_heartbeat(value) || is_subscription_event(value) {
        return Ok(BitfinexPrivateStreamMessage::Ignored);
    }
    let Some(array) = value.as_array() else {
        return Ok(BitfinexPrivateStreamMessage::Ignored);
    };
    if array.get(1).and_then(Value::as_str) == Some("hb") {
        return Ok(BitfinexPrivateStreamMessage::Heartbeat);
    }
    let event = array.get(1).and_then(Value::as_str).unwrap_or_default();
    let payload = array.get(2).unwrap_or(&Value::Null);
    match event {
        "on" | "ou" | "oc" => Ok(BitfinexPrivateStreamMessage::Order(parse_order(
            exchange_id,
            market_type_hint,
            payload,
        )?)),
        "ws" | "wu" => Ok(BitfinexPrivateStreamMessage::Balance(
            rustcta_exchange_api::BalancesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: crate::adapters::response_metadata(exchange_id.clone(), None),
                balances: parse_balances(
                    exchange_id,
                    tenant_id,
                    account_id,
                    market_type_hint,
                    &json!([payload.clone()]),
                )?,
            },
        )),
        "ps" | "pu" | "pc" => Ok(BitfinexPrivateStreamMessage::Position(
            rustcta_exchange_api::PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: crate::adapters::response_metadata(exchange_id.clone(), None),
                positions: parse_positions(
                    exchange_id,
                    tenant_id,
                    account_id,
                    &json!([payload.clone()]),
                )?,
            },
        )),
        _ => Ok(BitfinexPrivateStreamMessage::Ignored),
    }
}

fn bitfinex_public_channel(kind: &PublicStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => Ok("book"),
        PublicStreamKind::Trades => Ok("trades"),
        PublicStreamKind::Ticker => Ok("ticker"),
        PublicStreamKind::Candles { .. } => Ok("candles"),
    }
}

fn bitfinex_private_filter(subscription: &PrivateStreamSubscription) -> Value {
    match subscription.kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills | PrivateStreamKind::Positions => {
            json!(["trading"])
        }
        PrivateStreamKind::Balances | PrivateStreamKind::Account => json!(["wallet", "balance"]),
    }
}

fn is_heartbeat(value: &Value) -> bool {
    value.get("event").and_then(Value::as_str) == Some("heartbeat")
}

fn is_subscription_event(value: &Value) -> bool {
    matches!(
        value.get("event").and_then(Value::as_str),
        Some("subscribed" | "unsubscribed" | "info" | "auth")
    )
}
