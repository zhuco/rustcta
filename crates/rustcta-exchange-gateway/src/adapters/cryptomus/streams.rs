#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::cryptomus_symbol;
use super::CryptomusGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

impl CryptomusGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "cryptomus.public_streams_disabled",
            });
        }
        let payload = cryptomus_public_subscribe_payload(&subscription, 1)?;
        Ok(format!(
            "cryptomus:{}:{}",
            self.config.public_ws_url,
            payload["method"].as_str().unwrap_or("unknown")
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.enabled_private_streams || !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "cryptomus.private_streams_require_one_time_token",
            });
        }
        let payload = cryptomus_private_subscribe_payload(&subscription, 1)?;
        Ok(format!(
            "cryptomus:{}:{}",
            self.config.public_ws_url,
            payload["method"].as_str().unwrap_or("unknown")
        ))
    }
}

pub fn cryptomus_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
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
            PrivateOrderStreamEventKind::BalanceUpdate,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn cryptomus_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let market = cryptomus_symbol(&subscription.symbol);
    let (method, params) = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            ("depth_subscribe", json!([format!("{market}:1")]))
        }
        PublicStreamKind::Trades => ("trade_subscribe", json!([market])),
        PublicStreamKind::Ticker => ("ticker_subscribe", json!([market])),
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "cryptomus.candle_stream_unverified",
            })
        }
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": params
    }))
}

pub fn cryptomus_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let market = cryptomus_symbol(&subscription.symbol);
    let (method, params) = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            ("depth_unsubscribe", json!([format!("{market}:1")]))
        }
        PublicStreamKind::Trades => ("trade_unsubscribe", json!([market])),
        PublicStreamKind::Ticker => ("ticker_unsubscribe", json!([market])),
        PublicStreamKind::Candles { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "cryptomus.candle_stream_unverified",
            })
        }
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": params
    }))
}

pub fn cryptomus_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let method = match subscription.kind {
        PrivateStreamKind::Orders => "order_subscribe",
        PrivateStreamKind::Fills => "deal_subscribe",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "balance_subscribe",
        PrivateStreamKind::Positions => {
            return Err(ExchangeApiError::Unsupported {
                operation: "cryptomus.positions_stream_unsupported_for_spot",
            })
        }
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": ["all"]
    }))
}

pub fn cryptomus_ping_payload(id: u64) -> Value {
    json!({
        "id": id,
        "method": "ping",
        "params": []
    })
}

pub fn cryptomus_reconnect_policy_ms() -> (i64, i64, i64) {
    (50_000, 10_000, 60_000)
}
