#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderBookResponse, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::{json, Value};

use super::parser::{
    canonical_from_alpaca_symbol, normalize_alpaca_symbol, parse_orderbook_snapshot,
};
use super::AlpacaGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl AlpacaGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        self.ensure_credentials("alpaca.subscribe_public_stream")?;
        let payload = alpaca_public_subscribe_payload(&subscription)?;
        Ok(format!(
            "alpaca:{}:{}",
            self.config.public_ws_base_url,
            payload
                .get("orderbooks")
                .or_else(|| payload.get("trades"))
                .or_else(|| payload.get("quotes"))
                .or_else(|| payload.get("bars"))
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        ))
    }
}

pub fn alpaca_public_auth_payload(api_key: &str, api_secret: &str) -> Value {
    json!({
        "action": "auth",
        "key": api_key,
        "secret": api_secret,
    })
}

pub fn alpaca_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let symbol = normalize_alpaca_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let mut payload = json!({ "action": "subscribe" });
    match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            payload["orderbooks"] = json!([symbol]);
        }
        PublicStreamKind::Trades => {
            payload["trades"] = json!([symbol]);
        }
        PublicStreamKind::Ticker => {
            payload["quotes"] = json!([symbol]);
        }
        PublicStreamKind::Candles { .. } => {
            payload["bars"] = json!([symbol]);
        }
    }
    Ok(payload)
}

pub fn alpaca_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    let mut payload = alpaca_public_subscribe_payload(subscription)?;
    payload["action"] = json!("unsubscribe");
    Ok(payload)
}

pub fn parse_alpaca_public_ws_events(
    exchange_id: &ExchangeId,
    text: &str,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let value: Value = serde_json::from_str(text).map_err(|error| ExchangeApiError::Transport {
        message: error.to_string(),
    })?;
    let items = value.as_array().cloned().unwrap_or_else(|| vec![value]);
    let mut events = Vec::new();
    for item in items {
        match item.get("T").and_then(Value::as_str) {
            Some("o") => events.push(ExchangeStreamEvent::OrderBookSnapshot(
                parse_orderbook_event(exchange_id, &item)?,
            )),
            Some("success") | Some("subscription") => {}
            Some("error") => {
                return Err(ExchangeApiError::Unsupported {
                    operation: "alpaca.public_ws_error_event",
                });
            }
            _ => {}
        }
    }
    Ok(events)
}

pub fn parse_orderbook_event(
    exchange_id: &ExchangeId,
    event: &Value,
) -> ExchangeApiResult<OrderBookResponse> {
    let symbol =
        event
            .get("S")
            .and_then(Value::as_str)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "alpaca WS orderbook missing S".to_string(),
            })?;
    let normalized = normalize_alpaca_symbol(symbol)?;
    let canonical: CanonicalSymbol = canonical_from_alpaca_symbol(&normalized)?;
    let exchange_symbol =
        ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, normalized.clone())
            .map_err(super::parser::validation_error)?;
    let mut book = serde_json::Map::new();
    book.insert(
        "b".to_string(),
        event.get("b").cloned().unwrap_or_else(|| json!([])),
    );
    book.insert(
        "a".to_string(),
        event.get("a").cloned().unwrap_or_else(|| json!([])),
    );
    book.insert(
        "t".to_string(),
        event.get("t").cloned().unwrap_or(Value::Null),
    );
    let mut orderbooks = serde_json::Map::new();
    orderbooks.insert(normalized.clone(), Value::Object(book));
    let wrapped = json!({ "orderbooks": Value::Object(orderbooks) });
    parse_orderbook_snapshot(
        exchange_id,
        rustcta_exchange_api::SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical),
            exchange_symbol,
        },
        &wrapped,
    )
}

pub fn alpaca_private_stream_boundary() -> Value {
    json!({
        "support": "unsupported",
        "reason": "Alpaca Broker order updates are SSE (/v2/events/trades), not a JSON WebSocket private stream in the gateway contract",
    })
}

pub fn private_stream_capabilities(
    enabled: bool,
) -> rustcta_exchange_api::PrivateStreamCapabilities {
    let _ = enabled;
    rustcta_exchange_api::PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}
