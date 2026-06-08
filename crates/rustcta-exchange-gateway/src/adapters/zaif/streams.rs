#![allow(dead_code)]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::{json, Value};

use super::parser::normalize_zaif_pair;

#[derive(Debug, Clone, PartialEq)]
pub struct ZaifWsSubscriptionSpec {
    pub url: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
    pub heartbeat_policy: &'static str,
}

pub fn zaif_private_stream_capabilities(_enabled: bool) -> PrivateStreamCapabilities {
    PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
}

pub fn public_subscription_spec(
    subscription: &PublicStreamSubscription,
    url: &str,
) -> ExchangeApiResult<ZaifWsSubscriptionSpec> {
    if subscription.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "zaif.public_stream.market_type",
        });
    }
    match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot
        | PublicStreamKind::OrderBookDelta
        | PublicStreamKind::Trades
        | PublicStreamKind::Ticker => {}
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "zaif.public_stream.candles",
            })
        }
    }
    let pair = normalize_zaif_pair(&subscription.symbol.exchange_symbol.symbol)?;
    Ok(ZaifWsSubscriptionSpec {
        url: format!("{}?currency_pair={pair}", url.trim_end_matches('/')),
        subscribe_payload: json!({"currency_pair": pair}),
        unsubscribe_payload: json!({"unsupported": "connection_close"}),
        heartbeat_policy: "server_push_no_client_ping_documented",
    })
}

impl super::ZaifGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        let spec = public_subscription_spec(&subscription, &self.config.public_websocket_url)?;
        Ok(format!("zaif:public:{}", spec.url))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "zaif.private_stream.rest_reconciliation_fallback",
        })
    }
}
