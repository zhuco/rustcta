#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse,
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Value};

use super::parser::{normalize_coinbaseexchange_symbol, parse_error, parse_orderbook_snapshot};
use super::signing::ws_auth_signature;
use super::CoinbaseExchangeGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::streams::StreamRuntimeState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinbaseExchangePublicOrderBookWsPolicy {
    pub public_channel: &'static str,
    pub public_interval_ms: u64,
    pub level2_channel: &'static str,
    pub full_channel: &'static str,
    pub level3_channel: &'static str,
    pub rest_snapshot_endpoint: &'static str,
    pub rest_snapshot_levels: &'static [u8],
    pub snapshot_sequence_field: &'static str,
    pub level2_sequence: &'static str,
    pub full_sequence: &'static str,
    pub level3_sequence: &'static str,
    pub checksum: Option<&'static str>,
    pub update_semantics: &'static str,
    pub resync: &'static str,
}

pub fn coinbaseexchange_public_order_book_ws_policy() -> CoinbaseExchangePublicOrderBookWsPolicy {
    CoinbaseExchangePublicOrderBookWsPolicy {
        public_channel: "level2_batch",
        public_interval_ms: 50,
        level2_channel: "level2",
        full_channel: "full",
        level3_channel: "level3",
        rest_snapshot_endpoint: "GET /products/{product_id}/book",
        rest_snapshot_levels: &[2, 3],
        snapshot_sequence_field: "sequence",
        level2_sequence: "level2/level2_batch snapshot and l2update schemas do not carry a sequence field; level2 guarantees delivery, while level2_batch is public unauthenticated batched level2 data",
        full_sequence: "full channel messages carry per-product sequence; build from REST level=3 snapshot, discard queued messages with sequence <= snapshot sequence, then require contiguous sequence increments",
        level3_sequence: "level3 compact messages include sequence in the schema for open/change/done/match/noop and follow the same REST level=3 snapshot replay boundary as full",
        checksum: None,
        update_semantics: "absolute quantity at changed price levels; size 0 removes the level",
        resync: "on reconnect or sequence gap for sequence-bearing full/level3 feeds, refetch REST /products/{product_id}/book snapshot and replay buffered messages after snapshot sequence; for level2_batch, refetch REST snapshot after reconnect or suspected loss",
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoinbaseExchangeWsSubscriptionSpec {
    pub url: String,
    pub channel: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
}

#[derive(Debug, Clone)]
pub struct CoinbaseExchangeWsSession {
    pub spec: CoinbaseExchangeWsSubscriptionSpec,
    pub state: StreamRuntimeState,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoinbaseExchangePublicStreamMessage {
    OrderBook(OrderBookResponse),
    SubscriptionAck,
    Heartbeat,
    Ignored,
}

pub fn coinbaseexchange_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: false,
        supports_positions: false,
        supports_account: true,
        order_event_kinds: vec![
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

impl CoinbaseExchangeGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let spec = coinbaseexchange_public_subscription_spec(&subscription, &self.config.ws_url)?;
        Ok(format!("coinbaseexchange:{}:{}", spec.url, spec.channel))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_spot(subscription.market_type.unwrap_or(MarketType::Spot))?;
        let (api_key, api_secret, api_passphrase) =
            self.private_credentials("coinbaseexchange.subscribe_private_stream")?;
        let timestamp = Utc::now().timestamp().to_string();
        let spec = coinbaseexchange_private_subscription_spec(
            &subscription,
            &self.config.ws_url,
            api_key,
            api_secret,
            api_passphrase,
            &timestamp,
        )?;
        Ok(format!("coinbaseexchange:{}:{}", spec.url, spec.channel))
    }
}

pub fn coinbaseexchange_public_subscription_spec(
    subscription: &PublicStreamSubscription,
    ws_url: &str,
) -> ExchangeApiResult<CoinbaseExchangeWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinbaseexchange.public_stream.market_type",
        });
    }
    let product_id =
        normalize_coinbaseexchange_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let channel = match &subscription.kind {
        PublicStreamKind::Trades => "matches",
        PublicStreamKind::Ticker => "ticker",
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "level2_batch",
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinbaseexchange.public_stream.candles",
            });
        }
    };
    Ok(subscription_spec(ws_url, channel, vec![product_id], None))
}

pub fn coinbaseexchange_private_subscription_spec(
    subscription: &PrivateStreamSubscription,
    ws_url: &str,
    api_key: &str,
    api_secret: &str,
    api_passphrase: &str,
    timestamp: &str,
) -> ExchangeApiResult<CoinbaseExchangeWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.market_type.unwrap_or(MarketType::Spot) != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinbaseexchange.private_stream.market_type",
        });
    }
    match subscription.kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills | PrivateStreamKind::Account => {}
        PrivateStreamKind::Balances => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinbaseexchange.private_stream.balances",
            });
        }
        PrivateStreamKind::Positions => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinbaseexchange.private_stream.positions",
            });
        }
    }
    let auth = json!({
        "signature": ws_auth_signature(api_secret, timestamp)?,
        "key": api_key,
        "passphrase": api_passphrase,
        "timestamp": timestamp
    });
    Ok(subscription_spec(ws_url, "user", Vec::new(), Some(auth)))
}

pub fn coinbaseexchange_public_ws_session(
    exchange: ExchangeId,
    subscription: PublicStreamSubscription,
    ws_url: &str,
) -> ExchangeApiResult<CoinbaseExchangeWsSession> {
    let spec = coinbaseexchange_public_subscription_spec(&subscription, ws_url)?;
    Ok(CoinbaseExchangeWsSession {
        spec,
        state: StreamRuntimeState::new(exchange, MarketType::Spot),
    })
}

pub fn coinbaseexchange_private_ws_session(
    exchange: ExchangeId,
    subscription: PrivateStreamSubscription,
    ws_url: &str,
    api_key: &str,
    api_secret: &str,
    api_passphrase: &str,
    timestamp: &str,
) -> ExchangeApiResult<CoinbaseExchangeWsSession> {
    let spec = coinbaseexchange_private_subscription_spec(
        &subscription,
        ws_url,
        api_key,
        api_secret,
        api_passphrase,
        timestamp,
    )?;
    Ok(CoinbaseExchangeWsSession {
        spec,
        state: StreamRuntimeState::new(exchange, MarketType::Spot),
    })
}

fn subscription_spec(
    ws_url: &str,
    channel: &str,
    product_ids: Vec<String>,
    auth: Option<Value>,
) -> CoinbaseExchangeWsSubscriptionSpec {
    let mut subscribe_payload = json!({
        "type": "subscribe",
        "channels": [channel],
    });
    if !product_ids.is_empty() {
        subscribe_payload["product_ids"] = json!(product_ids);
    }
    if let Some(auth) = auth {
        for key in ["signature", "key", "passphrase", "timestamp"] {
            subscribe_payload[key] = auth[key].clone();
        }
    }
    let mut unsubscribe_payload = json!({
        "type": "unsubscribe",
        "channels": [channel],
    });
    if let Some(product_ids) = subscribe_payload.get("product_ids").cloned() {
        unsubscribe_payload["product_ids"] = product_ids;
    }
    CoinbaseExchangeWsSubscriptionSpec {
        url: ws_url.to_string(),
        channel: channel.to_string(),
        subscribe_payload,
        unsubscribe_payload,
    }
}

pub fn coinbaseexchange_public_orderbook_subscription_spec(
    symbol: &SymbolScope,
    ws_url: &str,
    channel: Option<&str>,
) -> ExchangeApiResult<CoinbaseExchangeWsSubscriptionSpec> {
    if symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinbaseexchange.public_orderbook_stream.market_type",
        });
    }
    let product_id = normalize_coinbaseexchange_symbol(&symbol.exchange_symbol.symbol)?;
    let channel = channel.unwrap_or("level2_batch");
    match channel {
        "level2_batch" | "level2" | "full" | "level3" => {
            Ok(subscription_spec(ws_url, channel, vec![product_id], None))
        }
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinbaseexchange.public_orderbook_stream.channel",
        }),
    }
}

pub fn parse_coinbaseexchange_public_stream_events(
    exchange_id: &ExchangeId,
    fallback: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    match parse_coinbaseexchange_public_stream_message(exchange_id, fallback, value)? {
        CoinbaseExchangePublicStreamMessage::OrderBook(order_book) => {
            Ok(vec![ExchangeStreamEvent::OrderBookSnapshot(order_book)])
        }
        CoinbaseExchangePublicStreamMessage::Heartbeat => {
            Ok(vec![ExchangeStreamEvent::Heartbeat {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                exchange: exchange_id.clone(),
                received_at: Utc::now(),
            }])
        }
        CoinbaseExchangePublicStreamMessage::SubscriptionAck
        | CoinbaseExchangePublicStreamMessage::Ignored => Ok(Vec::new()),
    }
}

pub fn parse_coinbaseexchange_public_stream_message(
    exchange_id: &ExchangeId,
    fallback: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<CoinbaseExchangePublicStreamMessage> {
    match value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "subscriptions" => Ok(CoinbaseExchangePublicStreamMessage::SubscriptionAck),
        "heartbeat" => Ok(CoinbaseExchangePublicStreamMessage::Heartbeat),
        "snapshot" | "l2update" => Ok(CoinbaseExchangePublicStreamMessage::OrderBook(
            parse_coinbaseexchange_level2_book(exchange_id, fallback, value)?,
        )),
        _ => Ok(CoinbaseExchangePublicStreamMessage::Ignored),
    }
}

fn parse_coinbaseexchange_level2_book(
    exchange_id: &ExchangeId,
    fallback: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookResponse> {
    let normalized = match value.get("type").and_then(Value::as_str) {
        Some("snapshot") => json!({
            "sequence": value.get("sequence").cloned().unwrap_or(Value::Null),
            "bids": value.get("bids").cloned().unwrap_or_else(|| json!([])),
            "asks": value.get("asks").cloned().unwrap_or_else(|| json!([])),
        }),
        Some("l2update") => json!({
            "sequence": value.get("sequence").cloned().unwrap_or(Value::Null),
            "bids": coinbaseexchange_l2_changes(value, "buy"),
            "asks": coinbaseexchange_l2_changes(value, "sell"),
        }),
        _ => {
            return Err(parse_error(
                exchange_id.clone(),
                "unsupported coinbaseexchange level2 message",
                value,
            ));
        }
    };
    let mut order_book = parse_orderbook_snapshot(exchange_id, fallback, &normalized)?;
    order_book.exchange_timestamp = value
        .get("time")
        .and_then(Value::as_str)
        .and_then(|time| chrono::DateTime::parse_from_rfc3339(time).ok())
        .map(|time| time.with_timezone(&Utc));
    Ok(OrderBookResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: response_metadata(exchange_id.clone(), None),
        order_book,
    })
}

fn coinbaseexchange_l2_changes(value: &Value, side: &str) -> Value {
    let Some(changes) = value.get("changes").and_then(Value::as_array) else {
        return json!([]);
    };
    Value::Array(
        changes
            .iter()
            .filter_map(|change| {
                let values = change.as_array()?;
                let change_side = values.first()?.as_str()?;
                if !change_side.eq_ignore_ascii_case(side) {
                    return None;
                }
                let size = values.get(2)?.as_str().unwrap_or_default();
                if size.parse::<f64>().ok().is_some_and(|size| size == 0.0) {
                    return None;
                }
                Some(json!([
                    values.get(1).cloned().unwrap_or(Value::Null),
                    values.get(2).cloned().unwrap_or(Value::Null)
                ]))
            })
            .collect(),
    )
}
