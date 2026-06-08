use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription,
};
use serde_json::{json, Value};

use super::parser::normalize_bybit_symbol;
use super::signing::sign_ws_auth;
use super::BybitGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl BybitGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        let payload = bybit_public_subscribe_payload(&subscription)?;
        Ok(format!(
            "{}:{}:{}",
            self.exchange_id,
            self.config.public_ws_url,
            payload["args"][0].as_str().unwrap_or("unknown")
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        let operation = self.profile_operation(
            "bybit.subscribe_private_stream",
            "bybiteu.subscribe_private_stream",
        );
        let (api_key, api_secret) = self.private_credentials(operation)?;
        let expires = Utc::now().timestamp_millis() + 60_000;
        let auth = bybit_private_auth_payload(api_key, api_secret, expires)?;
        let subscribe = bybit_private_subscribe_payload(&subscription)?;
        Ok(format!(
            "{}:{}:{}:{}",
            self.exchange_id,
            self.config.private_ws_url,
            auth["op"].as_str().unwrap_or("auth"),
            subscribe["args"][0].as_str().unwrap_or("unknown")
        ))
    }
}

pub fn bybit_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let symbol = normalize_bybit_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let topic = match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            format!("orderbook.50.{symbol}")
        }
        PublicStreamKind::Trades => format!("publicTrade.{symbol}"),
        PublicStreamKind::Ticker => format!("tickers.{symbol}"),
        PublicStreamKind::Candles { interval } => format!("kline.{interval}.{symbol}"),
    };
    Ok(json!({
        "op": "subscribe",
        "args": [topic]
    }))
}

pub fn bybit_private_auth_payload(
    api_key: &str,
    api_secret: &str,
    expires_ms: i64,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "op": "auth",
        "args": [api_key, expires_ms, sign_ws_auth(api_secret, expires_ms)?]
    }))
}

pub fn bybit_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let topic = match subscription.kind {
        PrivateStreamKind::Orders => "order",
        PrivateStreamKind::Fills => "execution",
        PrivateStreamKind::Balances => "wallet",
        PrivateStreamKind::Positions => "position",
        PrivateStreamKind::Account => "wallet",
    };
    Ok(json!({
        "op": "subscribe",
        "args": [topic]
    }))
}

pub fn bybit_heartbeat_payload() -> Value {
    json!({"op": "ping"})
}

pub fn classify_ws_control(value: &Value) -> ExchangeApiResult<&'static str> {
    if value.get("op").and_then(Value::as_str) == Some("pong") {
        return Ok("pong");
    }
    if value.get("success").and_then(Value::as_bool) == Some(true) {
        return Ok("ack");
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("Bybit WS control failure: {value}"),
    })
}
