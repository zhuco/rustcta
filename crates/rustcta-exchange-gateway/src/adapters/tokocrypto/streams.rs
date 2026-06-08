#![allow(dead_code)]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::{json, Value};

use super::parser::market_data_symbol;

#[derive(Debug, Clone, PartialEq)]
pub struct TokocryptoWsSubscriptionSpec {
    pub url: String,
    pub stream: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
}

pub fn tokocrypto_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
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
) -> ExchangeApiResult<TokocryptoWsSubscriptionSpec> {
    if subscription.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "tokocrypto.public_stream.non_spot_market_type",
        });
    }
    let symbol =
        market_data_symbol(&subscription.symbol.exchange_symbol.symbol)?.to_ascii_lowercase();
    let stream = match &subscription.kind {
        PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => {
            format!("{symbol}@depth")
        }
        PublicStreamKind::Trades => format!("{symbol}@trade"),
        PublicStreamKind::Ticker => format!("{symbol}@ticker"),
        PublicStreamKind::Candles { interval } => {
            format!("{symbol}@kline_{}", normalize_interval(interval)?)
        }
    };
    Ok(TokocryptoWsSubscriptionSpec {
        url: url.to_string(),
        stream: stream.clone(),
        subscribe_payload: json!({
            "method": "SUBSCRIBE",
            "params": [stream],
            "id": 1,
        }),
        unsubscribe_payload: json!({
            "method": "UNSUBSCRIBE",
            "params": [stream],
            "id": 1,
        }),
    })
}

pub fn private_subscribe_payload(listen_token: &str) -> Value {
    json!({
        "method": "SUBSCRIBE",
        "params": [listen_token],
        "id": 1,
    })
}

impl super::TokocryptoGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "tokocrypto.public_stream.payload_spec_only",
        })
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.exchange)?;
        Err(ExchangeApiError::Unsupported {
            operation: "tokocrypto.private_stream.listen_token_request_spec_only",
        })
    }
}

fn normalize_interval(interval: &str) -> ExchangeApiResult<String> {
    match interval.trim() {
        "1m" | "1min" => Ok("1m".to_string()),
        "5m" | "5min" => Ok("5m".to_string()),
        "15m" | "15min" => Ok("15m".to_string()),
        "30m" | "30min" => Ok("30m".to_string()),
        "1h" | "60m" => Ok("1h".to_string()),
        "1d" => Ok("1d".to_string()),
        "1w" => Ok("1w".to_string()),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "tokocrypto.public_stream.candles.unsupported_interval",
        }),
    }
}
