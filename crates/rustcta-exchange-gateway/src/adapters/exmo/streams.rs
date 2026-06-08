#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    CredentialScope, ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind,
    PrivateStreamCapabilities, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::{json, Value};

use super::parser::normalize_exmo_exchange_symbol;
use super::signing::ws_login_signature;
use super::ExmoGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

#[derive(Debug, Clone, PartialEq)]
pub struct ExmoWsSubscriptionSpec {
    pub url: String,
    pub channel: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
    pub auth_payload: Option<Value>,
}

pub fn exmo_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
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
            PrivateOrderStreamEventKind::Reject,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

impl ExmoGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let spec = exmo_public_subscription_spec(&self.config.public_ws_url, &subscription)?;
        Ok(format!("exmo:{}:{}", spec.url, spec.channel))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_rest("exmo.subscribe_private_stream")?;
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        self.ensure_spot(market_type)?;
        let channel = exmo_private_channel(&subscription.kind)?;
        Ok(format!(
            "exmo:private:{channel}:{}",
            subscription.account_id
        ))
    }
}

pub fn exmo_public_subscription_spec(
    ws_url: &str,
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<ExmoWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "exmo.public_stream.market_type",
        });
    }
    let pair = normalize_exmo_exchange_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let channel = match &subscription.kind {
        PublicStreamKind::Trades => format!("spot/trades:{pair}"),
        PublicStreamKind::Ticker => format!("spot/ticker:{pair}"),
        PublicStreamKind::OrderBookDelta => format!("spot/order_book_updates:{pair}"),
        PublicStreamKind::OrderBookSnapshot => format!("spot/order_book_snapshots:{pair}"),
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "exmo.public_stream.candles",
            });
        }
    };
    Ok(ExmoWsSubscriptionSpec {
        url: ws_url.trim_end_matches('/').to_string(),
        channel: channel.clone(),
        subscribe_payload: json!({
            "id": 1,
            "method": "subscribe",
            "topics": [channel]
        }),
        unsubscribe_payload: json!({
            "id": 2,
            "method": "unsubscribe",
            "topics": [channel]
        }),
        auth_payload: None,
    })
}

pub fn exmo_private_subscription_spec(
    ws_url: &str,
    subscription: &PrivateStreamSubscription,
    api_key: &str,
    api_secret: &str,
    nonce: &str,
) -> ExchangeApiResult<ExmoWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.market_type.unwrap_or(MarketType::Spot) != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "exmo.private_stream.market_type",
        });
    }
    let channel = exmo_private_channel(&subscription.kind)?.to_string();
    Ok(ExmoWsSubscriptionSpec {
        url: ws_url.trim_end_matches('/').to_string(),
        channel: channel.clone(),
        subscribe_payload: json!({
            "id": 2,
            "method": "subscribe",
            "topics": [channel]
        }),
        unsubscribe_payload: json!({
            "id": 3,
            "method": "unsubscribe",
            "topics": [channel]
        }),
        auth_payload: Some(json!({
            "id": 1,
            "method": "login",
            "api_key": api_key,
            "nonce": nonce,
            "sign": ws_login_signature(api_key, api_secret, nonce)?
        })),
    })
}

pub fn exmo_pong_response(value: &Value) -> Option<Value> {
    let method = value
        .get("method")
        .or_else(|| value.get("event"))
        .and_then(Value::as_str)?;
    if !method.eq_ignore_ascii_case("ping") {
        return None;
    }
    Some(json!({
        "id": value.get("id").cloned().unwrap_or_else(|| json!(1)),
        "method": "pong"
    }))
}

pub fn exmo_credential_scopes() -> Vec<CredentialScope> {
    vec![CredentialScope::ReadOnly, CredentialScope::Trade]
}

fn exmo_private_channel(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PrivateStreamKind::Orders => Ok("spot/orders"),
        PrivateStreamKind::Fills => Ok("spot/user_trades"),
        PrivateStreamKind::Balances | PrivateStreamKind::Account => Ok("spot/wallet"),
        PrivateStreamKind::Positions => Err(ExchangeApiError::Unsupported {
            operation: "exmo.private_stream.positions",
        }),
    }
}
