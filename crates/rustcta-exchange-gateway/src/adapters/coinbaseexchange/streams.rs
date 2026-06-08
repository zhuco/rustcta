#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Value};

use super::parser::normalize_coinbaseexchange_symbol;
use super::signing::ws_auth_signature;
use super::CoinbaseExchangeGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;
use crate::streams::StreamRuntimeState;

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
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "level2",
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
