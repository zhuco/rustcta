#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Duration, Utc};
use rustcta_exchange_api::{
    AuthRenewalKind, AuthRenewalPolicy, ExchangeApiError, ExchangeApiResult,
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType, OrderBookLevel, OrderBookSnapshot};
use serde_json::{json, Value};

use super::parser::normalize_kucoin_symbol;
use super::KuCoinGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;
use crate::orderbook_state::{OrderBookDelta, OrderBookDeltaLevel};
use crate::streams::StreamRuntimeState;

const DEFAULT_BULLET_TOKEN_TTL_MS: i64 = 24 * 60 * 60 * 1_000;
const DEFAULT_BULLET_RENEW_BEFORE_MS: i64 = 60 * 60 * 1_000;

#[derive(Debug, Clone, PartialEq)]
pub struct KuCoinWsSubscriptionSpec {
    pub url: String,
    pub topic: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KuCoinBulletTokenLease {
    pub exchange: ExchangeId,
    pub token: String,
    pub endpoint: String,
    pub connect_id: String,
    pub ping_interval_ms: i64,
    pub ping_timeout_ms: i64,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub renewal_policy: AuthRenewalPolicy,
}

impl KuCoinBulletTokenLease {
    pub fn websocket_url(&self) -> String {
        format!(
            "{}?token={}&connectId={}",
            self.endpoint.trim_end_matches('/'),
            self.token,
            self.connect_id
        )
    }

    pub fn should_renew(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at - Duration::milliseconds(self.renewal_policy.renew_before_expiry_ms)
    }
}

#[derive(Debug, Clone)]
pub struct KuCoinWsSession {
    pub spec: KuCoinWsSubscriptionSpec,
    pub state: StreamRuntimeState,
}

pub fn kucoin_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
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
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

impl KuCoinGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let spec = kucoin_public_subscription_spec(&subscription, "1", None)?;
        Ok(format!("kucoin:{}:{}", spec.url, spec.topic))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "kucoin.subscribe_private_stream.credentials",
            });
        }
        let market_type = subscription.market_type.unwrap_or(MarketType::Spot);
        self.ensure_spot(market_type)?;
        let topic = kucoin_private_topic(&subscription.kind)?;
        Ok(format!(
            "kucoin:bullet-private:{topic}:{}",
            subscription.account_id
        ))
    }

    pub async fn request_private_bullet_token(
        &self,
        connect_id: &str,
    ) -> ExchangeApiResult<KuCoinBulletTokenLease> {
        let value = self
            .send_signed_post(
                "kucoin.private_stream.bullet_token",
                "/api/v1/bullet-private",
                &std::collections::HashMap::new(),
                &json!({}),
            )
            .await?;
        kucoin_bullet_token_lease(&self.exchange_id, connect_id, &value, Utc::now())
    }
}

pub fn kucoin_public_subscription_spec(
    subscription: &PublicStreamSubscription,
    request_id: &str,
    token_lease: Option<&KuCoinBulletTokenLease>,
) -> ExchangeApiResult<KuCoinWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.symbol.market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "kucoin.public_stream.market_type",
        });
    }
    let symbol = normalize_kucoin_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let spec = match &subscription.kind {
        PublicStreamKind::Ticker => kucoin_obu_subscription_spec(
            ws_url(token_lease, "wss://ws-api-spot.kucoin.com/endpoint"),
            symbol,
            "1",
            request_id,
        ),
        PublicStreamKind::OrderBookDelta => kucoin_obu_subscription_spec(
            ws_url(token_lease, "wss://ws-api-spot.kucoin.com/endpoint"),
            symbol,
            "increment",
            request_id,
        ),
        PublicStreamKind::OrderBookSnapshot => kucoin_obu_subscription_spec(
            ws_url(token_lease, "wss://ws-api-spot.kucoin.com/endpoint"),
            symbol,
            "50",
            request_id,
        ),
        PublicStreamKind::Trades => subscription_spec(
            ws_url(token_lease, "wss://ws-api-spot.kucoin.com/endpoint"),
            format!("/market/match:{symbol}"),
            request_id,
            false,
        ),
        PublicStreamKind::Candles { interval } => subscription_spec(
            ws_url(token_lease, "wss://ws-api-spot.kucoin.com/endpoint"),
            format!("/market/candles:{symbol}_{interval}"),
            request_id,
            false,
        ),
    };
    Ok(spec)
}

pub fn kucoin_private_subscription_spec(
    subscription: &PrivateStreamSubscription,
    request_id: &str,
    token_lease: &KuCoinBulletTokenLease,
) -> ExchangeApiResult<KuCoinWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.market_type.unwrap_or(MarketType::Spot) != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "kucoin.private_stream.market_type",
        });
    }
    let topic = kucoin_private_topic(&subscription.kind)?;
    Ok(subscription_spec(
        token_lease.websocket_url(),
        topic.to_string(),
        request_id,
        true,
    ))
}

pub fn kucoin_pong_response(value: &Value) -> Option<Value> {
    let kind = value.get("type").and_then(Value::as_str)?;
    if !kind.eq_ignore_ascii_case("ping") {
        return None;
    }
    Some(json!({
        "id": value.get("id").and_then(Value::as_str).unwrap_or("pong"),
        "type": "pong"
    }))
}

pub fn parse_kucoin_obu_delta(
    exchange: &ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookDelta> {
    let data = value.get("data").unwrap_or(value);
    let changes = data.get("changes").unwrap_or(data);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoin obu delta parser requires canonical_symbol".to_string(),
            })?;
    let mut delta = OrderBookDelta::new(
        exchange.clone(),
        symbol.market_type,
        canonical_symbol,
        Utc::now(),
    );
    delta.bids = parse_delta_levels(changes.get("bids").or_else(|| changes.get("b")))?;
    delta.asks = parse_delta_levels(changes.get("asks").or_else(|| changes.get("a")))?;
    delta.first_sequence = first_u64(
        data,
        &[
            "sequenceStart",
            "startingSequence",
            "startSeq",
            "start",
            "sequence",
        ],
    );
    delta.last_sequence = first_u64(
        data,
        &["sequenceEnd", "endingSequence", "endSeq", "end", "sequence"],
    );
    delta.exchange_timestamp = first_i64(data, &["time", "timestamp", "ts"])
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(delta)
}

pub fn parse_kucoin_obu_snapshot(
    exchange: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoin obu snapshot parser requires canonical_symbol".to_string(),
            })?;
    let bids = parse_snapshot_levels(data.get("bids").or_else(|| data.get("b")))?;
    let asks = parse_snapshot_levels(data.get("asks").or_else(|| data.get("a")))?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange.clone(),
        symbol.market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = first_u64(
        data,
        &["sequence", "sequenceEnd", "endingSequence", "endSeq"],
    );
    snapshot.exchange_timestamp = first_i64(data, &["time", "timestamp", "ts"])
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn kucoin_bullet_token_lease(
    exchange: &ExchangeId,
    connect_id: &str,
    value: &Value,
    issued_at: DateTime<Utc>,
) -> ExchangeApiResult<KuCoinBulletTokenLease> {
    let token = required_str(value, "token")?.to_string();
    let server = value
        .get("instanceServers")
        .and_then(Value::as_array)
        .and_then(|servers| servers.first())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "kucoin bullet response missing instanceServers[0]".to_string(),
        })?;
    let endpoint = required_str(server, "endpoint")?.to_string();
    let ping_interval_ms = value_i64(server.get("pingInterval")).unwrap_or(20_000);
    let ping_timeout_ms = value_i64(server.get("pingTimeout")).unwrap_or(10_000);
    Ok(KuCoinBulletTokenLease {
        exchange: exchange.clone(),
        token,
        endpoint,
        connect_id: connect_id.trim().to_string(),
        ping_interval_ms,
        ping_timeout_ms,
        issued_at,
        expires_at: issued_at + Duration::milliseconds(DEFAULT_BULLET_TOKEN_TTL_MS),
        renewal_policy: AuthRenewalPolicy {
            kind: AuthRenewalKind::TokenRefresh,
            renew_before_expiry_ms: DEFAULT_BULLET_RENEW_BEFORE_MS,
            renewal_interval_ms: Some(20 * 60 * 60 * 1_000),
            reconnect_on_renewal_failure: true,
            resubscribe_after_renewal: true,
        },
    })
}

pub fn kucoin_public_ws_session(
    exchange: ExchangeId,
    subscription: PublicStreamSubscription,
    request_id: &str,
    token_lease: Option<&KuCoinBulletTokenLease>,
) -> ExchangeApiResult<KuCoinWsSession> {
    let spec = kucoin_public_subscription_spec(&subscription, request_id, token_lease)?;
    Ok(KuCoinWsSession {
        spec,
        state: StreamRuntimeState::new(exchange, MarketType::Spot),
    })
}

pub fn kucoin_private_ws_session(
    exchange: ExchangeId,
    subscription: PrivateStreamSubscription,
    request_id: &str,
    token_lease: &KuCoinBulletTokenLease,
) -> ExchangeApiResult<KuCoinWsSession> {
    let spec = kucoin_private_subscription_spec(&subscription, request_id, token_lease)?;
    Ok(KuCoinWsSession {
        spec,
        state: StreamRuntimeState::new(exchange, MarketType::Spot),
    })
}

fn subscription_spec(
    url: String,
    topic: String,
    request_id: &str,
    private_channel: bool,
) -> KuCoinWsSubscriptionSpec {
    KuCoinWsSubscriptionSpec {
        url,
        topic: topic.clone(),
        subscribe_payload: json!({
            "id": request_id,
            "type": "subscribe",
            "topic": topic,
            "privateChannel": private_channel,
            "response": true
        }),
        unsubscribe_payload: json!({
            "id": request_id,
            "type": "unsubscribe",
            "topic": topic,
            "privateChannel": private_channel,
            "response": true
        }),
    }
}

fn kucoin_obu_subscription_spec(
    url: String,
    symbol: String,
    depth: &str,
    request_id: &str,
) -> KuCoinWsSubscriptionSpec {
    let topic = format!("obu:{symbol}:{depth}");
    KuCoinWsSubscriptionSpec {
        url,
        topic: topic.clone(),
        subscribe_payload: json!({
            "id": request_id,
            "type": "subscribe",
            "topic": "obu",
            "channel": "obu",
            "symbol": symbol,
            "depth": depth,
            "privateChannel": false,
            "response": true
        }),
        unsubscribe_payload: json!({
            "id": request_id,
            "type": "unsubscribe",
            "topic": "obu",
            "channel": "obu",
            "symbol": symbol,
            "depth": depth,
            "privateChannel": false,
            "response": true
        }),
    }
}

fn ws_url(token_lease: Option<&KuCoinBulletTokenLease>, fallback: &str) -> String {
    token_lease
        .map(KuCoinBulletTokenLease::websocket_url)
        .unwrap_or_else(|| fallback.to_string())
}

fn parse_delta_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookDeltaLevel>> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "kucoin obu message missing levels".to_string(),
        })?
        .iter()
        .map(|level| {
            let (price, quantity) = level_price_quantity(level)?;
            Ok(OrderBookDeltaLevel::new(price, quantity))
        })
        .collect()
}

fn parse_snapshot_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "kucoin obu message missing levels".to_string(),
        })?
        .iter()
        .map(|level| {
            let (price, quantity) = level_price_quantity(level)?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn level_price_quantity(value: &Value) -> ExchangeApiResult<(f64, f64)> {
    if let Some(array) = value.as_array() {
        let price =
            value_to_f64(array.first()).ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoin obu level missing price".to_string(),
            })?;
        let quantity =
            value_to_f64(array.get(1)).ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoin obu level missing quantity".to_string(),
            })?;
        return Ok((price, quantity));
    }
    let price = required_f64(value, &["price", "p"])?;
    let quantity = required_f64(value, &["size", "quantity", "q"])?;
    Ok((price, quantity))
}

fn required_f64(value: &Value, keys: &[&str]) -> ExchangeApiResult<f64> {
    value_to_f64(keys.iter().find_map(|key| value.get(*key))).ok_or_else(|| {
        ExchangeApiError::InvalidRequest {
            message: format!("kucoin obu message missing numeric field {keys:?}"),
        }
    })
}

fn value_to_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn first_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
    })
}

fn first_i64(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
    })
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn kucoin_private_topic(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => Ok("/spotMarket/tradeOrdersV2"),
        PrivateStreamKind::Balances | PrivateStreamKind::Account => Ok("/account/balance"),
        PrivateStreamKind::Positions => Err(ExchangeApiError::Unsupported {
            operation: "kucoin.private_stream.positions",
        }),
    }
}

fn required_str<'a>(value: &'a Value, field: &str) -> ExchangeApiResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("kucoin bullet response missing {field}"),
        })
}

fn value_i64(value: Option<&Value>) -> Option<i64> {
    value.and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}
