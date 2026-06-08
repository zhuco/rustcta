#![allow(dead_code)]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::{json, Value};

use super::parser::normalize_bittrade_symbol;

#[derive(Debug, Clone, PartialEq)]
pub struct BittradeWsSubscriptionSpec {
    pub url: String,
    pub topic: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
}

pub fn bittrade_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if enabled {
        PrivateStreamCapabilities {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            supports_orders: true,
            supports_fills: true,
            supports_balances: true,
            supports_positions: false,
            supports_account: true,
            order_event_kinds: vec![],
            supports_client_order_id: true,
            supports_exchange_order_id: true,
        }
    } else {
        PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION)
    }
}

pub fn public_subscription_spec(
    subscription: &PublicStreamSubscription,
    url: &str,
) -> ExchangeApiResult<BittradeWsSubscriptionSpec> {
    if subscription.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "bittrade.public_stream.non_spot_market_type",
        });
    }
    let symbol = normalize_bittrade_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let topic = match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            format!("market.{symbol}.depth.step0")
        }
        PublicStreamKind::Trades => format!("market.{symbol}.trade.detail"),
        PublicStreamKind::Ticker => format!("market.{symbol}.bbo"),
        PublicStreamKind::Candles { interval } => {
            format!("market.{symbol}.kline.{}", normalize_interval(interval)?)
        }
    };
    Ok(BittradeWsSubscriptionSpec {
        url: url.to_string(),
        topic: topic.clone(),
        subscribe_payload: json!({
            "sub": topic,
            "id": "rustcta-bittrade-sub",
        }),
        unsubscribe_payload: json!({
            "unsub": topic,
            "id": "rustcta-bittrade-unsub",
        }),
    })
}

pub fn public_pong_payload(ping: i64) -> Value {
    json!({ "pong": ping })
}

pub fn private_ping_payload(timestamp_ms: i64) -> Value {
    json!({
        "action": "ping",
        "data": {
            "ts": timestamp_ms,
        }
    })
}

impl super::BittradeGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        let spec = public_subscription_spec(&subscription, &self.config.public_websocket_url)?;
        Ok(format!("bittrade:{}:{}", spec.url, spec.topic))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "bittrade.private_stream.rest_reconciliation_fallback_only",
        })
    }
}

fn normalize_interval(interval: &str) -> ExchangeApiResult<String> {
    match interval.trim() {
        "1m" | "1min" => Ok("1min".to_string()),
        "5m" | "5min" => Ok("5min".to_string()),
        "15m" | "15min" => Ok("15min".to_string()),
        "30m" | "30min" => Ok("30min".to_string()),
        "1h" | "60m" | "60min" => Ok("60min".to_string()),
        "1d" | "1day" => Ok("1day".to_string()),
        "1w" | "1week" => Ok("1week".to_string()),
        "1mo" | "1mon" => Ok("1mon".to_string()),
        "1y" | "1year" => Ok("1year".to_string()),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "bittrade.public_stream.candles.unsupported_interval",
        }),
    }
}
