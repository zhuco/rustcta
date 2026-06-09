#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_api::{PrivateOrderStreamEventKind, PrivateStreamCapabilities};
use rustcta_types::MarketType;
use serde_json::{json, Value};

use super::parser::{normalize_coinex_symbol, parse_error, parse_orderbook_snapshot};
use super::signing::sign_request;
use super::CoinExGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;

pub const COINEX_PUBLIC_WS_URL: &str = "wss://socket.coinex.com/v2/spot";
pub const COINEX_PRIVATE_WS_URL: &str = "wss://socket.coinex.com/v2/spot";
pub const COINEX_PUBLIC_DEPTH_PUSH_DELAY_MS: u16 = 200;
pub const COINEX_PUBLIC_DEPTH_FULL_REFRESH_MS: u32 = 60_000;
pub const COINEX_PUBLIC_DEPTH_LIMITS: [u16; 4] = [5, 10, 20, 50];
pub const COINEX_PUBLIC_DEPTH_DEFAULT_LIMIT: u16 = 50;
pub const COINEX_PUBLIC_DEPTH_DEFAULT_INTERVAL: &str = "0";
pub const COINEX_PUBLIC_DEPTH_INTERVALS: [&str; 17] = [
    "0",
    "0.00000000001",
    "0.000000000001",
    "0.0000000001",
    "0.000000001",
    "0.00000001",
    "0.0000001",
    "0.000001",
    "0.00001",
    "0.0001",
    "0.001",
    "0.01",
    "0.1",
    "1",
    "10",
    "100",
    "1000",
];

impl CoinExGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        let policy = coinex_public_ws_policy(&subscription)?;
        let method = policy.subscribe_method().unwrap_or("unknown");
        Ok(format!("coinex:{COINEX_PUBLIC_WS_URL}:{method}"))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_rest("coinex.subscribe_private_stream")?;
        if subscription.market_type.unwrap_or(MarketType::Spot) != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinex.private_ws_market_type",
            });
        }
        let subscribe = coinex_private_subscribe_payload(&subscription, 1)?;
        Ok(format!(
            "coinex:{COINEX_PRIVATE_WS_URL}:{}:{}",
            subscribe["method"].as_str().unwrap_or("unknown"),
            subscription.account_id
        ))
    }
}

impl CoinExWsPolicy {
    fn subscribe_method(&self) -> Option<&str> {
        match self {
            Self::Public { subscribe, .. } | Self::Private { subscribe, .. } => {
                subscribe.get("method").and_then(Value::as_str)
            }
        }
    }
}

pub fn coinex_reconnect_policy_ms() -> (i64, i64, i64) {
    (30_000, 10_000, 45_000)
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoinExWsPolicy {
    Public {
        url: String,
        subscribe: Value,
        unsubscribe: Value,
        heartbeat: Value,
        resync: CoinExResyncPolicy,
    },
    Private {
        url: String,
        login: Value,
        subscribe: Value,
        unsubscribe: Value,
        heartbeat: Value,
        auth_renewal: CoinExAuthRenewalPolicy,
        reconciliation_fallback: CoinExReconciliationFallback,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinExResyncPolicy {
    pub snapshot_endpoint: &'static str,
    pub resubscribe_after_reconnect: bool,
    pub snapshot_required_after_reconnect: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinExAuthRenewalPolicy {
    pub renewal: &'static str,
    pub renewal_interval_ms: i64,
    pub relogin_before_reconnect: bool,
    pub resubscribe_after_relogin: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinExReconciliationFallback {
    pub query_order_endpoint: &'static str,
    pub open_orders_endpoint: &'static str,
    pub fills_endpoint: &'static str,
    pub use_when_private_ws_stale: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoinExStreamControlMessage {
    Pong,
    SubscriptionAck {
        id: Option<u64>,
        method: Option<String>,
    },
    AuthAck {
        id: Option<u64>,
    },
    Error {
        code: Option<i64>,
        message: Option<String>,
    },
    Data(Value),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinExPublicOrderBookWsPolicy {
    pub subscribe_method: &'static str,
    pub update_method: &'static str,
    pub push_delay_ms: u16,
    pub full_refresh_interval_ms: u32,
    pub supported_limits: &'static [u16],
    pub default_limit: u16,
    pub supported_merge_intervals: &'static [&'static str],
    pub default_merge_interval: &'static str,
    pub checksum_algorithm: &'static str,
    pub checksum_type: &'static str,
    pub resync_strategy: &'static str,
}

impl CoinExPublicOrderBookWsPolicy {
    pub fn as_json(&self) -> Value {
        json!({
            "subscribe_method": self.subscribe_method,
            "update_method": self.update_method,
            "push_delay_ms": self.push_delay_ms,
            "full_refresh_interval_ms": self.full_refresh_interval_ms,
            "limit": {
                "default": self.default_limit,
                "supported": self.supported_limits,
            },
            "merge_interval": {
                "default": self.default_merge_interval,
                "supported": self.supported_merge_intervals,
            },
            "params_shape": {
                "field": "market_list",
                "item": ["market", "limit", "interval", "is_full"],
            },
            "semantics": {
                "is_full_true": "every changed push carries the full subscribed depth; full market depth refresh is also sent about every minute",
                "is_full_false": "pushes incremental changed levels about every 200ms; quantity 0 deletes a level",
            },
            "checksum": {
                "algorithm": self.checksum_algorithm,
                "type": self.checksum_type,
                "string": "bid1_price:bid1_amount:bid2_price:bid2_amount:ask1_price:ask1_amount:...",
                "resync_on_mismatch": true,
            },
            "resync": self.resync_strategy,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinExDepthSubscriptionSpec {
    pub market: String,
    pub limit: u16,
    pub merge_interval: String,
    pub is_full: bool,
}

impl CoinExDepthSubscriptionSpec {
    pub fn params_entry(&self) -> Value {
        json!([self.market, self.limit, self.merge_interval, self.is_full])
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoinExPublicOrderBookUpdate {
    pub market: String,
    pub is_full: bool,
    pub order_book: rustcta_types::OrderBookSnapshot,
    pub deleted_bid_prices: Vec<String>,
    pub deleted_ask_prices: Vec<String>,
    pub checksum: Option<i64>,
    pub checksum_matches_payload: Option<bool>,
}

pub fn coinex_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
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
            PrivateOrderStreamEventKind::Expired,
            PrivateOrderStreamEventKind::BalanceUpdate,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn coinex_public_ws_policy(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<CoinExWsPolicy> {
    Ok(CoinExWsPolicy::Public {
        url: COINEX_PUBLIC_WS_URL.to_string(),
        subscribe: coinex_public_subscribe_payload(subscription, 1)?,
        unsubscribe: coinex_public_unsubscribe_payload(subscription, 2)?,
        heartbeat: coinex_ping_payload(3),
        resync: CoinExResyncPolicy {
            snapshot_endpoint: "/spot/depth",
            resubscribe_after_reconnect: true,
            snapshot_required_after_reconnect: true,
        },
    })
}

pub fn coinex_public_orderbook_ws_policy() -> CoinExPublicOrderBookWsPolicy {
    CoinExPublicOrderBookWsPolicy {
        subscribe_method: "depth.subscribe",
        update_method: "depth.update",
        push_delay_ms: COINEX_PUBLIC_DEPTH_PUSH_DELAY_MS,
        full_refresh_interval_ms: COINEX_PUBLIC_DEPTH_FULL_REFRESH_MS,
        supported_limits: &COINEX_PUBLIC_DEPTH_LIMITS,
        default_limit: COINEX_PUBLIC_DEPTH_DEFAULT_LIMIT,
        supported_merge_intervals: &COINEX_PUBLIC_DEPTH_INTERVALS,
        default_merge_interval: COINEX_PUBLIC_DEPTH_DEFAULT_INTERVAL,
        checksum_algorithm: "crc32",
        checksum_type: "signed_i32",
        resync_strategy: "build from REST /spot/depth, subscribe to depth.update, apply full or incremental pushes, and rebuild/resubscribe after reconnect, stale stream, parse error, or checksum mismatch",
    }
}

pub fn coinex_private_ws_policy(
    subscription: &PrivateStreamSubscription,
    api_key: &str,
    api_secret: &str,
    timestamp_ms: i64,
) -> ExchangeApiResult<CoinExWsPolicy> {
    Ok(CoinExWsPolicy::Private {
        url: COINEX_PRIVATE_WS_URL.to_string(),
        login: coinex_private_login_payload(api_key, api_secret, timestamp_ms, 1)?,
        subscribe: coinex_private_subscribe_payload(subscription, 2)?,
        unsubscribe: coinex_private_unsubscribe_payload(subscription, 3)?,
        heartbeat: coinex_ping_payload(4),
        auth_renewal: CoinExAuthRenewalPolicy {
            renewal: "relogin",
            renewal_interval_ms: 30 * 60 * 1000,
            relogin_before_reconnect: true,
            resubscribe_after_relogin: true,
        },
        reconciliation_fallback: CoinExReconciliationFallback {
            query_order_endpoint: "/spot/order-status",
            open_orders_endpoint: "/spot/pending-order",
            fills_endpoint: "/spot/finished-order",
            use_when_private_ws_stale: true,
        },
    })
}

pub fn coinex_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let market = normalize_coinex_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let (method, params) = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            let spec = coinex_depth_subscription_spec(&market, &subscription.kind)?;
            (
                "depth.subscribe",
                json!({ "market_list": [spec.params_entry()] }),
            )
        }
        PublicStreamKind::Trades => ("deals.subscribe", json!([market])),
        PublicStreamKind::Ticker => ("state.subscribe", json!([market])),
        PublicStreamKind::Candles { interval } => ("kline.subscribe", json!([market, interval])),
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": params,
    }))
}

pub fn coinex_depth_subscription_spec(
    market: &str,
    kind: &PublicStreamKind,
) -> ExchangeApiResult<CoinExDepthSubscriptionSpec> {
    let is_full = match kind {
        PublicStreamKind::OrderBookSnapshot => true,
        PublicStreamKind::OrderBookDelta => false,
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinex.depth_subscription_kind",
            });
        }
    };
    coinex_custom_depth_subscription_spec(
        market,
        COINEX_PUBLIC_DEPTH_DEFAULT_LIMIT,
        COINEX_PUBLIC_DEPTH_DEFAULT_INTERVAL,
        is_full,
    )
}

pub fn coinex_custom_depth_subscription_spec(
    market: &str,
    limit: u16,
    merge_interval: &str,
    is_full: bool,
) -> ExchangeApiResult<CoinExDepthSubscriptionSpec> {
    if !COINEX_PUBLIC_DEPTH_LIMITS.contains(&limit) {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinex.depth_limit",
        });
    }
    if !COINEX_PUBLIC_DEPTH_INTERVALS.contains(&merge_interval) {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinex.depth_merge_interval",
        });
    }
    Ok(CoinExDepthSubscriptionSpec {
        market: normalize_coinex_symbol(market)?,
        limit,
        merge_interval: merge_interval.to_string(),
        is_full,
    })
}

pub fn coinex_public_channel(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<&'static str> {
    Ok(match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => "depth.subscribe",
        PublicStreamKind::Trades => "deals.subscribe",
        PublicStreamKind::Ticker => "state.subscribe",
        PublicStreamKind::Candles { .. } => "kline.subscribe",
    })
}

pub fn coinex_public_unsubscribe_payload(
    subscription: &PublicStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let method = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            "depth.unsubscribe"
        }
        PublicStreamKind::Trades => "deals.unsubscribe",
        PublicStreamKind::Ticker => "state.unsubscribe",
        PublicStreamKind::Candles { .. } => "kline.unsubscribe",
    };
    let market = normalize_coinex_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let params = match &subscription.kind {
        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot => {
            json!({ "market_list": [market] })
        }
        _ => json!([market]),
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": params,
    }))
}

pub fn coinex_private_login_payload(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: i64,
    id: u64,
) -> ExchangeApiResult<Value> {
    if api_key.trim().is_empty() || api_secret.trim().is_empty() {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinex.private_ws_missing_credentials",
        });
    }
    let timestamp = timestamp_ms.to_string();
    let signature = sign_request(api_secret, "GET", "/v2/ws/auth", "", &timestamp);
    Ok(json!({
        "id": id,
        "method": "server.sign",
        "params": {
            "access_id": api_key,
            "signed_str": signature,
            "timestamp": timestamp_ms
        }
    }))
}

pub fn coinex_private_subscribe_payload(
    subscription: &PrivateStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let method = match subscription.kind {
        PrivateStreamKind::Orders => "order.subscribe",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "balance.subscribe",
        PrivateStreamKind::Fills => "deal.subscribe",
        PrivateStreamKind::Positions => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinex.private_ws_positions_spot",
            });
        }
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": ["SPOT"],
    }))
}

pub fn coinex_private_channel(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<&'static str> {
    Ok(match subscription.kind {
        PrivateStreamKind::Orders => "order.subscribe",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "balance.subscribe",
        PrivateStreamKind::Fills => "deal.subscribe",
        PrivateStreamKind::Positions => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinex.private_ws_positions_spot",
            });
        }
    })
}

pub fn coinex_private_unsubscribe_payload(
    subscription: &PrivateStreamSubscription,
    id: u64,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    let method = match subscription.kind {
        PrivateStreamKind::Orders => "order.unsubscribe",
        PrivateStreamKind::Balances | PrivateStreamKind::Account => "balance.unsubscribe",
        PrivateStreamKind::Fills => "deal.unsubscribe",
        PrivateStreamKind::Positions => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinex.private_ws_positions_spot",
            });
        }
    };
    Ok(json!({
        "id": id,
        "method": method,
        "params": ["SPOT"],
    }))
}

pub fn coinex_ping_payload(id: u64) -> Value {
    json!({
        "id": id,
        "method": "server.ping",
        "params": []
    })
}

pub fn parse_coinex_stream_control(value: &Value) -> CoinExStreamControlMessage {
    if value.get("method").and_then(Value::as_str) == Some("server.pong")
        || value.get("message").and_then(Value::as_str) == Some("pong")
    {
        return CoinExStreamControlMessage::Pong;
    }
    if value.get("error").is_some()
        || value
            .get("code")
            .and_then(Value::as_i64)
            .is_some_and(|code| code != 0)
    {
        let error = value.get("error").unwrap_or(value);
        return CoinExStreamControlMessage::Error {
            code: error
                .get("code")
                .or_else(|| value.get("code"))
                .and_then(value_as_i64),
            message: error
                .get("message")
                .or_else(|| error.get("msg"))
                .or_else(|| value.get("message"))
                .and_then(Value::as_str)
                .map(str::to_string),
        };
    }
    let id = value.get("id").and_then(Value::as_u64);
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_string);
    if method.as_deref() == Some("server.sign")
        || value.get("result").and_then(Value::as_str) == Some("success")
    {
        return CoinExStreamControlMessage::AuthAck { id };
    }
    if id.is_some()
        && (value.get("result").is_some()
            || method
                .as_deref()
                .is_some_and(|text| text.ends_with(".subscribe")))
    {
        return CoinExStreamControlMessage::SubscriptionAck { id, method };
    }
    CoinExStreamControlMessage::Data(value.clone())
}

pub fn parse_coinex_public_orderbook_update(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<CoinExPublicOrderBookUpdate> {
    if value.get("method").and_then(Value::as_str) != Some("depth.update") {
        return Err(parse_error(
            exchange_id.clone(),
            "CoinEx public order book message is not depth.update",
            value,
        ));
    }
    let data = value.get("data").ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "CoinEx depth.update missing data",
            value,
        )
    })?;
    let market = data
        .get("market")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| symbol.exchange_symbol.symbol.clone());
    let is_full = data
        .get("is_full")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let deleted_bid_prices =
        coinex_zero_quantity_prices(data.get("depth").unwrap_or(data), "bids")?;
    let deleted_ask_prices =
        coinex_zero_quantity_prices(data.get("depth").unwrap_or(data), "asks")?;
    let book_data = coinex_nonzero_depth_data(data);
    let order_book = parse_orderbook_snapshot(exchange_id, symbol, &book_data)?;
    let depth = data.get("depth").unwrap_or(data);
    let checksum = depth.get("checksum").and_then(value_as_i64);
    let checksum_matches_payload = coinex_depth_checksum_matches(depth)?;
    Ok(CoinExPublicOrderBookUpdate {
        market,
        is_full,
        order_book,
        deleted_bid_prices,
        deleted_ask_prices,
        checksum,
        checksum_matches_payload,
    })
}

pub fn coinex_depth_checksum_matches(depth: &Value) -> ExchangeApiResult<Option<bool>> {
    let expected = match depth.get("checksum").and_then(value_as_i64) {
        Some(checksum) => checksum,
        None => return Ok(None),
    };
    let checksum_string = coinex_depth_checksum_string(depth)?;
    let signed = coinex_crc32_signed(checksum_string.as_bytes());
    let unsigned = coinex_crc32_unsigned(checksum_string.as_bytes()) as i64;
    Ok(Some(expected == signed || expected == unsigned))
}

pub fn coinex_depth_checksum_string(depth: &Value) -> ExchangeApiResult<String> {
    let mut parts = Vec::new();
    append_coinex_checksum_levels(&mut parts, depth.get("bids"))?;
    append_coinex_checksum_levels(&mut parts, depth.get("asks"))?;
    Ok(parts.join(":"))
}

pub fn coinex_crc32_signed(bytes: &[u8]) -> i64 {
    coinex_crc32_unsigned(bytes) as i32 as i64
}

pub fn coinex_crc32_unsigned(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

pub fn coinex_ws_now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn append_coinex_checksum_levels(
    parts: &mut Vec<String>,
    levels: Option<&Value>,
) -> ExchangeApiResult<()> {
    let Some(levels) = levels.and_then(Value::as_array) else {
        return Ok(());
    };
    for level in levels {
        let array = level.as_array().ok_or_else(|| {
            parse_error(
                rustcta_types::ExchangeId::new("coinex").expect("coinex exchange id"),
                "CoinEx checksum level is not an array",
                level,
            )
        })?;
        let price = checksum_part(array.first()).ok_or_else(|| {
            parse_error(
                rustcta_types::ExchangeId::new("coinex").expect("coinex exchange id"),
                "CoinEx checksum level missing price",
                level,
            )
        })?;
        let amount = checksum_part(array.get(1)).ok_or_else(|| {
            parse_error(
                rustcta_types::ExchangeId::new("coinex").expect("coinex exchange id"),
                "CoinEx checksum level missing amount",
                level,
            )
        })?;
        parts.push(price);
        parts.push(amount);
    }
    Ok(())
}

fn coinex_zero_quantity_prices(depth: &Value, side: &str) -> ExchangeApiResult<Vec<String>> {
    let Some(levels) = depth.get(side).and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    let mut prices = Vec::new();
    for level in levels {
        let array = level.as_array().ok_or_else(|| {
            parse_error(
                rustcta_types::ExchangeId::new("coinex").expect("coinex exchange id"),
                "CoinEx depth level is not an array",
                level,
            )
        })?;
        if array.get(1).is_some_and(is_zero_quantity) {
            if let Some(price) = checksum_part(array.first()) {
                prices.push(price);
            }
        }
    }
    Ok(prices)
}

fn coinex_nonzero_depth_data(data: &Value) -> Value {
    let mut normalized = data.clone();
    let Some(depth) = normalized.get_mut("depth").and_then(Value::as_object_mut) else {
        return normalized;
    };
    for side in ["bids", "asks"] {
        if let Some(levels) = depth.get_mut(side).and_then(Value::as_array_mut) {
            levels.retain(|level| {
                level
                    .as_array()
                    .and_then(|array| array.get(1))
                    .is_none_or(|quantity| !is_zero_quantity(quantity))
            });
        }
    }
    normalized
}

fn is_zero_quantity(value: &Value) -> bool {
    match value {
        Value::String(text) => text.parse::<f64>().is_ok_and(|number| number == 0.0),
        Value::Number(number) => number.as_f64().is_some_and(|number| number == 0.0),
        _ => false,
    }
}

fn checksum_part(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}
