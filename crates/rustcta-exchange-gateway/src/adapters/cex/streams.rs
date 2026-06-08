#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription,
};
use serde_json::{json, Value};

use super::parser::{cex_pair_string, normalize_cex_symbol};
use super::signing::cex_ws_signature;
use super::CexGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl CexGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "cex.public_stream_runtime_unverified",
            });
        }
        let pair = cex_pair_string(&subscription.symbol.exchange_symbol.symbol)?;
        Ok(format!(
            "cex:{}:{}:{}",
            self.config.websocket_url,
            cex_public_event(&subscription.kind)?,
            pair
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if let Some(market_type) = subscription.market_type {
            self.ensure_spot(market_type)?;
        }
        Err(ExchangeApiError::Unsupported {
            operation: "cex.private_streams_unverified",
        })
    }
}

pub fn cex_ws_auth_payload(
    api_key: &str,
    api_secret: &str,
    timestamp_seconds: &str,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "e": "auth",
        "auth": {
            "key": api_key,
            "signature": cex_ws_signature(timestamp_seconds, api_key, api_secret)?,
            "timestamp": timestamp_seconds.parse::<i64>().map_err(|error| {
                ExchangeApiError::InvalidRequest {
                    message: format!("invalid CEX.IO websocket timestamp: {error}"),
                }
            })?
        },
        "oid": "auth"
    }))
}

pub fn cex_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    oid: &str,
) -> ExchangeApiResult<Value> {
    let (base, quote) = normalize_cex_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    match subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => Ok(json!({
            "e": "order-book-subscribe",
            "data": {
                "pair": [base, quote],
                "subscribe": true,
                "depth": 25
            },
            "oid": oid
        })),
        PublicStreamKind::Trades => Ok(json!({
            "e": "subscribe",
            "rooms": [format!("pair-{base}-{quote}")]
        })),
        PublicStreamKind::Ticker => Ok(json!({
            "e": "subscribe",
            "rooms": ["tickers"]
        })),
        PublicStreamKind::Candles { .. } => Err(ExchangeApiError::Unsupported {
            operation: "cex.public_ws_candles_unverified",
        }),
    }
}

pub fn cex_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
    oid: &str,
) -> ExchangeApiResult<Value> {
    let (base, quote) = normalize_cex_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    Ok(json!({
        "e": "order-book-unsubscribe",
        "data": {"pair": [base, quote]},
        "oid": oid
    }))
}

pub fn cex_pong_payload() -> Value {
    json!({ "e": "pong" })
}

fn cex_public_event(kind: &PublicStreamKind) -> ExchangeApiResult<&'static str> {
    Ok(match kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            "order-book-subscribe"
        }
        PublicStreamKind::Trades => "subscribe:pair",
        PublicStreamKind::Ticker => "subscribe:tickers",
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "cex.public_ws_candles_unverified",
            })
        }
    })
}
