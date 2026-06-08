use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
};
use rustcta_types::{MarketType, OrderBookLevel, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::{normalize_symbol, validation_error};
use super::signing::sign_request;
use super::BitmartGatewayAdapter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmartWsSpec {
    pub url: String,
    pub payload: Value,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_payload: Value,
}

impl BitmartGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_supported_market(subscription.symbol.market_type)?;
        if !self.config.enabled_public_streams {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitmart.public_streams_disabled",
            });
        }
        let spec = public_subscription_spec(&self.config, &subscription)?;
        Ok(format!(
            "bitmart:{:?}:{:?}:{}",
            subscription.symbol.market_type, subscription.kind, spec.payload
        ))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.private_streams_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitmart.private_streams_disabled",
            });
        }
        let spec = private_subscription_spec(&self.config, &subscription)?;
        Ok(format!(
            "bitmart:private:{:?}:{}",
            subscription.kind, spec.payload
        ))
    }
}

pub fn bitmart_private_stream_capabilities() -> PrivateStreamCapabilities {
    PrivateStreamCapabilities {
        schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: true,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::Ack,
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

pub fn private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if enabled {
        bitmart_private_stream_capabilities()
    } else {
        PrivateStreamCapabilities::unsupported(rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION)
    }
}

pub fn public_subscription_spec(
    config: &super::config::BitmartGatewayConfig,
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<BitmartWsSpec> {
    let symbol = normalize_symbol(
        &subscription.symbol.exchange_symbol.symbol,
        subscription.symbol.market_type,
    )?;
    let channel = match (subscription.symbol.market_type, &subscription.kind) {
        (
            MarketType::Spot,
            PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta,
        ) => {
            format!("spot/depth50:{symbol}")
        }
        (MarketType::Spot, PublicStreamKind::Trades) => format!("spot/trade:{symbol}"),
        (MarketType::Spot, PublicStreamKind::Ticker) => format!("spot/ticker:{symbol}"),
        (
            MarketType::Perpetual,
            PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta,
        ) => {
            format!("futures/depth50:{symbol}")
        }
        (MarketType::Perpetual, PublicStreamKind::Trades) => format!("futures/trade:{symbol}"),
        (MarketType::Perpetual, PublicStreamKind::Ticker) => format!("futures/ticker:{symbol}"),
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitmart.public_stream_kind",
            })
        }
    };
    Ok(BitmartWsSpec {
        url: match subscription.symbol.market_type {
            MarketType::Spot => config.spot_ws_url.clone(),
            _ => config.futures_ws_url.clone(),
        },
        payload: json!({"op": "subscribe", "args": [channel]}),
        heartbeat_interval_ms: 15_000,
        heartbeat_payload: json!("ping"),
    })
}

pub fn private_subscription_spec(
    config: &super::config::BitmartGatewayConfig,
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<BitmartWsSpec> {
    let api_key = config
        .api_key
        .as_deref()
        .ok_or(ExchangeApiError::Unsupported {
            operation: "bitmart.private_stream_credentials",
        })?;
    let api_secret = config
        .api_secret
        .as_deref()
        .ok_or(ExchangeApiError::Unsupported {
            operation: "bitmart.private_stream_credentials",
        })?;
    let memo = config
        .memo
        .as_deref()
        .ok_or(ExchangeApiError::Unsupported {
            operation: "bitmart.private_stream_credentials",
        })?;
    let timestamp = Utc::now().timestamp_millis().to_string();
    let sign = sign_request(api_secret, &timestamp, Some(memo), "bitmart.WebSocket")?;
    let channel = match (subscription.market_type, &subscription.kind) {
        (Some(MarketType::Spot) | None, PrivateStreamKind::Orders) => "spot/user/order",
        (Some(MarketType::Spot) | None, PrivateStreamKind::Fills) => "spot/user/trade",
        (Some(MarketType::Spot) | None, PrivateStreamKind::Balances) => "spot/user/balance",
        (Some(MarketType::Perpetual), PrivateStreamKind::Orders) => "futures/order",
        (Some(MarketType::Perpetual), PrivateStreamKind::Fills) => "futures/trade",
        (Some(MarketType::Perpetual), PrivateStreamKind::Balances) => "futures/asset",
        (Some(MarketType::Perpetual), PrivateStreamKind::Positions) => "futures/position",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitmart.private_stream_kind",
            })
        }
    };
    Ok(BitmartWsSpec {
        url: match subscription.market_type {
            Some(MarketType::Perpetual) => config.futures_ws_url.clone(),
            _ => config.spot_ws_url.clone(),
        },
        payload: json!({
            "op": "login",
            "args": [api_key, timestamp, sign],
            "subscribe": {"op": "subscribe", "args": [channel]},
        }),
        heartbeat_interval_ms: 15_000,
        heartbeat_payload: json!("ping"),
    })
}

pub fn parse_public_order_book_event(
    exchange_id: &rustcta_types::ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Option<OrderBookSnapshot>> {
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let Some(symbol) = data.get("symbol").and_then(Value::as_str) else {
        return Ok(None);
    };
    let Some((base, quote)) = split_stream_symbol(symbol) else {
        return Ok(None);
    };
    let canonical = rustcta_types::CanonicalSymbol::new(base, quote).map_err(validation_error)?;
    let bids = parse_ws_levels(data.get("bids").or_else(|| data.get("buys")))?;
    let asks = parse_ws_levels(data.get("asks").or_else(|| data.get("sells")))?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        market_type,
        canonical,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(
        rustcta_types::ExchangeSymbol::new(exchange_id.clone(), market_type, symbol)
            .map_err(validation_error)?,
    );
    Ok(Some(snapshot))
}

pub fn parse_control_message(value: &Value) -> Option<&'static str> {
    if value.as_str() == Some("pong") {
        return Some("pong");
    }
    match value
        .get("event")
        .or_else(|| value.get("op"))
        .and_then(Value::as_str)
    {
        Some("subscribe") | Some("subscribed") => Some("subscribed"),
        Some("error") => Some("error"),
        _ => None,
    }
}

fn parse_ws_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    levels
        .iter()
        .filter_map(|level| {
            let array = level.as_array()?;
            let price = array.first()?.as_str()?.parse().ok()?;
            let quantity = array.get(1)?.as_str()?.parse().ok()?;
            Some(OrderBookLevel::new(price, quantity).map_err(validation_error))
        })
        .collect()
}

fn split_stream_symbol(symbol: &str) -> Option<(String, String)> {
    if let Some((base, quote)) = symbol.split_once('_') {
        return Some((base.to_string(), quote.to_string()));
    }
    const QUOTES: [&str; 6] = ["USDT", "USDC", "USD", "BTC", "ETH", "EUR"];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}
