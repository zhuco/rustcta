#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Duration, Utc};
use rustcta_exchange_api::{
    AuthRenewalKind, AuthRenewalPolicy, ExchangeApiError, ExchangeApiResult,
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Value};

use super::parser::normalize_kucoinfutures_symbol;
use super::KuCoinFuturesGatewayAdapter;
use crate::adapters::ensure_exchange_api_schema;
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

pub fn kucoinfutures_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: true,
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

impl KuCoinFuturesGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let spec = kucoinfutures_public_subscription_spec(&subscription, "1", None)?;
        Ok(format!("kucoinfutures:{}:{}", spec.url, spec.topic))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "kucoinfutures.subscribe_private_stream.credentials",
            });
        }
        let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_perpetual(market_type)?;
        let topic = kucoinfutures_private_topic(&subscription.kind)?;
        Ok(format!(
            "kucoinfutures:bullet-private:{topic}:{}",
            subscription.account_id
        ))
    }

    pub async fn request_private_bullet_token(
        &self,
        connect_id: &str,
    ) -> ExchangeApiResult<KuCoinBulletTokenLease> {
        let value = self
            .send_signed_post(
                "kucoinfutures.private_stream.bullet_token",
                "/api/v1/bullet-private",
                &std::collections::HashMap::new(),
                &json!({}),
            )
            .await?;
        kucoinfutures_bullet_token_lease(&self.exchange_id, connect_id, &value, Utc::now())
    }
}

pub fn kucoinfutures_public_subscription_spec(
    subscription: &PublicStreamSubscription,
    request_id: &str,
    token_lease: Option<&KuCoinBulletTokenLease>,
) -> ExchangeApiResult<KuCoinWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.symbol.market_type != MarketType::Perpetual {
        return Err(ExchangeApiError::Unsupported {
            operation: "kucoinfutures.public_stream.market_type",
        });
    }
    let symbol = normalize_kucoinfutures_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let topic = match &subscription.kind {
        PublicStreamKind::Trades => format!("/contractMarket/execution:{symbol}"),
        PublicStreamKind::Ticker => format!("/contractMarket/ticker:{symbol}"),
        PublicStreamKind::OrderBookDelta => format!("/contractMarket/level2:{symbol}"),
        PublicStreamKind::OrderBookSnapshot => format!("/contractMarket/level2Depth50:{symbol}"),
        PublicStreamKind::Candles { interval } => {
            format!("/contractMarket/limitCandle:{symbol}_{interval}")
        }
    };
    let url = token_lease
        .map(KuCoinBulletTokenLease::websocket_url)
        .unwrap_or_else(|| "wss://ws-api-futures.kucoin.com/endpoint".to_string());
    Ok(subscription_spec(url, topic, request_id, false))
}

pub fn kucoinfutures_private_subscription_spec(
    subscription: &PrivateStreamSubscription,
    request_id: &str,
    token_lease: &KuCoinBulletTokenLease,
) -> ExchangeApiResult<KuCoinWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.market_type.unwrap_or(MarketType::Perpetual) != MarketType::Perpetual {
        return Err(ExchangeApiError::Unsupported {
            operation: "kucoinfutures.private_stream.market_type",
        });
    }
    let topic = kucoinfutures_private_topic(&subscription.kind)?;
    Ok(subscription_spec(
        token_lease.websocket_url(),
        topic.to_string(),
        request_id,
        true,
    ))
}

pub fn kucoinfutures_pong_response(value: &Value) -> Option<Value> {
    let kind = value.get("type").and_then(Value::as_str)?;
    if !kind.eq_ignore_ascii_case("ping") {
        return None;
    }
    Some(json!({
        "id": value.get("id").and_then(Value::as_str).unwrap_or("pong"),
        "type": "pong"
    }))
}

pub fn kucoinfutures_bullet_token_lease(
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
            message: "kucoinfutures bullet response missing instanceServers[0]".to_string(),
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

pub fn kucoinfutures_public_ws_session(
    exchange: ExchangeId,
    subscription: PublicStreamSubscription,
    request_id: &str,
    token_lease: Option<&KuCoinBulletTokenLease>,
) -> ExchangeApiResult<KuCoinWsSession> {
    let spec = kucoinfutures_public_subscription_spec(&subscription, request_id, token_lease)?;
    Ok(KuCoinWsSession {
        spec,
        state: StreamRuntimeState::new(exchange, MarketType::Perpetual),
    })
}

pub fn kucoinfutures_private_ws_session(
    exchange: ExchangeId,
    subscription: PrivateStreamSubscription,
    request_id: &str,
    token_lease: &KuCoinBulletTokenLease,
) -> ExchangeApiResult<KuCoinWsSession> {
    let spec = kucoinfutures_private_subscription_spec(&subscription, request_id, token_lease)?;
    Ok(KuCoinWsSession {
        spec,
        state: StreamRuntimeState::new(exchange, MarketType::Perpetual),
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

fn kucoinfutures_private_topic(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => Ok("/contractMarket/tradeOrders"),
        PrivateStreamKind::Balances | PrivateStreamKind::Account => Ok("/contractAccount/wallet"),
        PrivateStreamKind::Positions => Ok("/contract/position"),
    }
}

fn required_str<'a>(value: &'a Value, field: &str) -> ExchangeApiResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("kucoinfutures bullet response missing {field}"),
        })
}

fn value_i64(value: Option<&Value>) -> Option<i64> {
    value.and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}
