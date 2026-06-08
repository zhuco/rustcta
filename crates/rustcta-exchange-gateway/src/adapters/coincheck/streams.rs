#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Value};

use super::parser::normalize_coincheck_symbol;
use super::CoincheckGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;
use crate::streams::StreamRuntimeState;

#[derive(Debug, Clone, PartialEq)]
pub struct CoincheckWsSubscriptionSpec {
    pub url: String,
    pub channel: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
}

#[derive(Debug, Clone)]
pub struct CoincheckWsSession {
    pub spec: CoincheckWsSubscriptionSpec,
    pub state: StreamRuntimeState,
}

pub fn coincheck_private_stream_capabilities(_enabled: bool) -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

impl CoincheckGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let spec = coincheck_public_subscription_spec(&subscription, &self.config.ws_url)?;
        Ok(format!("coincheck:{}:{}", spec.url, spec.channel))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "coincheck.private_stream.rest_reconciliation",
        })
    }
}

pub fn coincheck_public_subscription_spec(
    subscription: &PublicStreamSubscription,
    ws_url: &str,
) -> ExchangeApiResult<CoincheckWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "coincheck.public_stream.market_type",
        });
    }
    let pair = normalize_coincheck_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let channel = match &subscription.kind {
        PublicStreamKind::Trades => format!("{pair}-trades"),
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            format!("{pair}-orderbook")
        }
        PublicStreamKind::Ticker | PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coincheck.public_stream.kind",
            });
        }
    };
    Ok(subscription_spec(ws_url, channel))
}

pub fn coincheck_public_ws_session(
    exchange: ExchangeId,
    subscription: PublicStreamSubscription,
    ws_url: &str,
) -> ExchangeApiResult<CoincheckWsSession> {
    let spec = coincheck_public_subscription_spec(&subscription, ws_url)?;
    Ok(CoincheckWsSession {
        spec,
        state: StreamRuntimeState::new(exchange, MarketType::Spot),
    })
}

fn subscription_spec(ws_url: &str, channel: String) -> CoincheckWsSubscriptionSpec {
    CoincheckWsSubscriptionSpec {
        url: ws_url.to_string(),
        channel: channel.clone(),
        subscribe_payload: json!({
            "type": "subscribe",
            "channel": channel,
        }),
        unsubscribe_payload: json!({
            "type": "unsubscribe",
            "channel": channel,
        }),
    }
}
