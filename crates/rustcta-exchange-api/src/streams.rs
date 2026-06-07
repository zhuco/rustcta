use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    capabilities::{CapabilitySupport, CredentialScope},
    AccountId, BalancesResponse, ExchangeId, Fill, MarketType, OrderBookResponse, OrderState,
    PositionsResponse, RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PublicStreamKind {
    Trades,
    Ticker,
    OrderBookDelta,
    OrderBookSnapshot,
    Candles { interval: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicStreamSubscription {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub kind: PublicStreamKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamChannelScope {
    Public,
    Private,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum StreamSubscriptionTopic {
    Public {
        exchange: ExchangeId,
        market_type: MarketType,
        exchange_symbol: String,
        stream: PublicStreamKind,
    },
    Private {
        exchange: ExchangeId,
        market_type: Option<MarketType>,
        account_id: AccountId,
        stream: PrivateStreamKind,
    },
}

impl StreamSubscriptionTopic {
    pub fn exchange(&self) -> &ExchangeId {
        match self {
            Self::Public { exchange, .. } | Self::Private { exchange, .. } => exchange,
        }
    }

    pub fn channel_scope(&self) -> StreamChannelScope {
        match self {
            Self::Public { .. } => StreamChannelScope::Public,
            Self::Private { .. } => StreamChannelScope::Private,
        }
    }

    pub fn subscription_key(&self) -> String {
        match self {
            Self::Public {
                exchange,
                market_type,
                exchange_symbol,
                stream,
            } => format!(
                "public:{}:{}:{}:{}",
                exchange,
                market_type_key(*market_type),
                exchange_symbol.to_ascii_lowercase(),
                public_stream_key(stream)
            ),
            Self::Private {
                exchange,
                market_type,
                account_id,
                stream,
            } => format!(
                "private:{}:{}:{}:{}",
                exchange,
                optional_market_type_key(*market_type),
                account_id,
                private_stream_key(stream)
            ),
        }
    }
}

fn market_type_key(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "spot",
        MarketType::Margin => "margin",
        MarketType::Futures => "futures",
        MarketType::Perpetual => "perpetual",
        MarketType::Option => "option",
    }
}

fn optional_market_type_key(market_type: Option<MarketType>) -> &'static str {
    market_type.map(market_type_key).unwrap_or("any")
}

fn public_stream_key(stream: &PublicStreamKind) -> String {
    match stream {
        PublicStreamKind::Trades => "trades".to_string(),
        PublicStreamKind::Ticker => "ticker".to_string(),
        PublicStreamKind::OrderBookDelta => "order_book_delta".to_string(),
        PublicStreamKind::OrderBookSnapshot => "order_book_snapshot".to_string(),
        PublicStreamKind::Candles { interval } => {
            format!("candles:{}", interval.to_ascii_lowercase())
        }
    }
}

fn private_stream_key(stream: &PrivateStreamKind) -> &'static str {
    match stream {
        PrivateStreamKind::Orders => "orders",
        PrivateStreamKind::Fills => "fills",
        PrivateStreamKind::Balances => "balances",
        PrivateStreamKind::Positions => "positions",
        PrivateStreamKind::Account => "account",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionAckStatus {
    Pending,
    Accepted,
    Rejected,
    TimedOut,
    Unsubscribed,
}

impl Default for SubscriptionAckStatus {
    fn default() -> Self {
        Self::Pending
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionAck {
    #[serde(default = "default_stream_schema_version")]
    pub schema_version: u16,
    #[serde(default)]
    pub subscription_id: String,
    pub topic: StreamSubscriptionTopic,
    #[serde(default)]
    pub status: SubscriptionAckStatus,
    #[serde(default)]
    pub exchange_request_id: Option<String>,
    #[serde(default)]
    pub exchange_channel: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default = "Utc::now")]
    pub received_at: DateTime<Utc>,
}

impl SubscriptionAck {
    pub fn accepted(
        schema_version: u16,
        subscription_id: impl Into<String>,
        topic: StreamSubscriptionTopic,
        received_at: DateTime<Utc>,
    ) -> Self {
        Self {
            schema_version,
            subscription_id: subscription_id.into(),
            topic,
            status: SubscriptionAckStatus::Accepted,
            exchange_request_id: None,
            exchange_channel: None,
            message: None,
            received_at,
        }
    }

    pub fn rejected(
        schema_version: u16,
        subscription_id: impl Into<String>,
        topic: StreamSubscriptionTopic,
        message: impl Into<String>,
        received_at: DateTime<Utc>,
    ) -> Self {
        Self {
            schema_version,
            subscription_id: subscription_id.into(),
            topic,
            status: SubscriptionAckStatus::Rejected,
            exchange_request_id: None,
            exchange_channel: None,
            message: Some(message.into()),
            received_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
    #[serde(default = "default_stream_schema_version")]
    pub schema_version: u16,
    #[serde(default = "default_request_context")]
    pub context: RequestContext,
    #[serde(default)]
    pub subscription_id: String,
    #[serde(default)]
    pub topic: Option<StreamSubscriptionTopic>,
    #[serde(default)]
    pub exchange_request_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PrivateStreamKind {
    Orders,
    Fills,
    Balances,
    Positions,
    Account,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PrivateOrderStreamEventKind {
    Ack,
    New,
    PartialFill,
    Fill,
    Cancel,
    Reject,
    Expired,
    BalanceUpdate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateStreamCapabilities {
    pub schema_version: u16,
    pub supports_orders: bool,
    pub supports_fills: bool,
    pub supports_balances: bool,
    pub supports_positions: bool,
    pub supports_account: bool,
    pub order_event_kinds: Vec<PrivateOrderStreamEventKind>,
    pub supports_client_order_id: bool,
    pub supports_exchange_order_id: bool,
}

impl PrivateStreamCapabilities {
    pub fn unsupported(schema_version: u16) -> Self {
        Self {
            schema_version,
            supports_orders: false,
            supports_fills: false,
            supports_balances: false,
            supports_positions: false,
            supports_account: false,
            order_event_kinds: Vec::new(),
            supports_client_order_id: false,
            supports_exchange_order_id: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamHeartbeatDirection {
    ClientPing,
    ServerPing,
    Bidirectional,
    None,
}

impl Default for StreamHeartbeatDirection {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatCapability {
    #[serde(default)]
    pub supported: bool,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub direction: StreamHeartbeatDirection,
    #[serde(default)]
    pub interval_ms: Option<u64>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

impl Default for HeartbeatCapability {
    fn default() -> Self {
        Self {
            supported: false,
            required: false,
            direction: StreamHeartbeatDirection::default(),
            interval_ms: None,
            timeout_ms: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReconnectCapability {
    pub supported: bool,
    pub requires_resubscribe: bool,
    pub preserves_session: bool,
    pub max_reconnect_attempts: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StreamResyncCapability {
    pub order_book: bool,
    pub balances: bool,
    pub positions: bool,
    pub orders: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StreamAuthCapability {
    pub required: bool,
    pub credential_scopes: Vec<CredentialScope>,
    pub renewal_ms: Option<u64>,
    pub uses_listen_key: bool,
    pub requires_relogin_on_reconnect: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRuntimeCapability {
    #[serde(default)]
    pub public: CapabilitySupport,
    #[serde(default)]
    pub private: CapabilitySupport,
    #[serde(default)]
    pub supports_subscribe: bool,
    #[serde(default)]
    pub supports_unsubscribe: bool,
    #[serde(default)]
    pub supports_public_subscribe: bool,
    #[serde(default)]
    pub supports_public_unsubscribe: bool,
    #[serde(default)]
    pub supports_private_subscribe: bool,
    #[serde(default)]
    pub supports_private_unsubscribe: bool,
    #[serde(default)]
    pub heartbeat: HeartbeatCapability,
    #[serde(default)]
    pub reconnect: ReconnectCapability,
    #[serde(default)]
    pub resync: StreamResyncCapability,
    #[serde(default)]
    pub auth: StreamAuthCapability,
    #[serde(default = "default_separate_stream_connections")]
    pub public_private_separate_connections: bool,
    #[serde(default)]
    pub heartbeat_policy: HeartbeatPolicy,
    #[serde(default)]
    pub auth_renewal_policy: AuthRenewalPolicy,
    #[serde(default)]
    pub auth_renewal: AuthRenewalPolicy,
    #[serde(default = "default_reconnect_requires_login")]
    pub reconnect_requires_login: bool,
    #[serde(default = "default_reconnect_requires_resubscribe")]
    pub reconnect_requires_resubscribe: bool,
    #[serde(default = "default_orderbook_requires_snapshot_after_reconnect")]
    pub orderbook_requires_snapshot_after_reconnect: bool,
}

impl Default for StreamRuntimeCapability {
    fn default() -> Self {
        Self {
            public: CapabilitySupport::default(),
            private: CapabilitySupport::default(),
            supports_subscribe: false,
            supports_unsubscribe: false,
            supports_public_subscribe: false,
            supports_public_unsubscribe: false,
            supports_private_subscribe: false,
            supports_private_unsubscribe: false,
            heartbeat: HeartbeatCapability::default(),
            reconnect: ReconnectCapability::default(),
            resync: StreamResyncCapability::default(),
            auth: StreamAuthCapability::default(),
            public_private_separate_connections: true,
            heartbeat_policy: HeartbeatPolicy::default(),
            auth_renewal_policy: AuthRenewalPolicy::default(),
            auth_renewal: AuthRenewalPolicy::default(),
            reconnect_requires_login: true,
            reconnect_requires_resubscribe: true,
            orderbook_requires_snapshot_after_reconnect: true,
        }
    }
}

fn default_separate_stream_connections() -> bool {
    true
}

fn default_reconnect_requires_login() -> bool {
    true
}

fn default_reconnect_requires_resubscribe() -> bool {
    true
}

fn default_orderbook_requires_snapshot_after_reconnect() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateStreamSubscription {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub market_type: Option<MarketType>,
    pub account_id: AccountId,
    pub kind: PrivateStreamKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeartbeatDirection {
    ClientPing,
    ServerPing,
    ApplicationMessage,
    None,
}

impl Default for HeartbeatDirection {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatPolicy {
    #[serde(default)]
    pub direction: HeartbeatDirection,
    #[serde(default)]
    pub ping_interval_ms: i64,
    #[serde(default)]
    pub pong_timeout_ms: i64,
    #[serde(default)]
    pub stale_message_ms: i64,
    #[serde(default)]
    pub requires_pong_payload_echo: bool,
}

impl Default for HeartbeatPolicy {
    fn default() -> Self {
        Self {
            direction: HeartbeatDirection::None,
            ping_interval_ms: 0,
            pong_timeout_ms: 0,
            stale_message_ms: 0,
            requires_pong_payload_echo: false,
        }
    }
}

impl HeartbeatPolicy {
    pub fn disabled() -> Self {
        Self {
            direction: HeartbeatDirection::None,
            ping_interval_ms: 0,
            pong_timeout_ms: 0,
            stale_message_ms: 0,
            requires_pong_payload_echo: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthRenewalKind {
    None,
    ReLogin,
    ListenKeyKeepAlive,
    TokenRefresh,
    JwtRefresh,
}

impl Default for AuthRenewalKind {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthRenewalPolicy {
    #[serde(default)]
    pub kind: AuthRenewalKind,
    #[serde(default)]
    pub renew_before_expiry_ms: i64,
    #[serde(default)]
    pub renewal_interval_ms: Option<i64>,
    #[serde(default)]
    pub reconnect_on_renewal_failure: bool,
    #[serde(default)]
    pub resubscribe_after_renewal: bool,
}

impl Default for AuthRenewalPolicy {
    fn default() -> Self {
        Self {
            kind: AuthRenewalKind::None,
            renew_before_expiry_ms: 60_000,
            renewal_interval_ms: None,
            reconnect_on_renewal_failure: false,
            resubscribe_after_renewal: false,
        }
    }
}

fn default_stream_schema_version() -> u16 {
    EXCHANGE_API_SCHEMA_VERSION
}

fn default_request_context() -> RequestContext {
    RequestContext::new(Utc::now())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenKeyLease {
    pub exchange: ExchangeId,
    pub account_id: AccountId,
    pub listen_key_id: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    #[serde(default)]
    pub last_renewed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub renewal_policy: AuthRenewalPolicy,
}

impl ListenKeyLease {
    pub fn should_renew(&self, now: DateTime<Utc>) -> bool {
        if self.renewal_policy.kind == AuthRenewalKind::None {
            return false;
        }

        let renew_at = self.expires_at
            - chrono::Duration::milliseconds(self.renewal_policy.renew_before_expiry_ms);
        if now >= renew_at {
            return true;
        }

        match (
            self.renewal_policy.renewal_interval_ms,
            self.last_renewed_at,
        ) {
            (Some(interval_ms), Some(last_renewed_at)) => {
                now.signed_duration_since(last_renewed_at)
                    >= chrono::Duration::milliseconds(interval_ms)
            }
            (Some(interval_ms), None) => {
                now.signed_duration_since(self.issued_at)
                    >= chrono::Duration::milliseconds(interval_ms)
            }
            _ => false,
        }
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WsLivenessState {
    Healthy,
    AwaitingPong,
    Stale,
    AuthRenewalDue,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceGap {
    pub exchange: ExchangeId,
    pub market_type: MarketType,
    pub exchange_symbol: String,
    pub expected_sequence: Option<u64>,
    pub received_sequence: Option<u64>,
    pub detected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResyncReason {
    SequenceGap,
    ChecksumMismatch,
    StaleStream,
    Reconnected,
    AuthRenewed,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExchangeStreamEvent {
    OrderBookSnapshot(OrderBookResponse),
    BalanceSnapshot(BalancesResponse),
    PositionSnapshot(PositionsResponse),
    OrderUpdate(OrderState),
    Fill(Fill),
    Heartbeat {
        schema_version: u16,
        exchange: ExchangeId,
        received_at: DateTime<Utc>,
    },
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::*;
    use crate::{RequestContext, EXCHANGE_API_SCHEMA_VERSION};

    fn exchange_id() -> ExchangeId {
        ExchangeId::new("binance").expect("exchange")
    }

    fn account_id() -> AccountId {
        AccountId::new("main").expect("account")
    }

    #[test]
    fn stream_subscription_topic_should_round_trip_public_and_private_targets() {
        let public = StreamSubscriptionTopic::Public {
            exchange: exchange_id(),
            market_type: MarketType::Spot,
            exchange_symbol: "BTCUSDT".to_string(),
            stream: PublicStreamKind::OrderBookDelta,
        };
        let private = StreamSubscriptionTopic::Private {
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: account_id(),
            stream: PrivateStreamKind::Orders,
        };

        assert_eq!(public.channel_scope(), StreamChannelScope::Public);
        assert_eq!(private.channel_scope(), StreamChannelScope::Private);
        assert_eq!(
            public.subscription_key(),
            "public:binance:spot:btcusdt:order_book_delta"
        );
        assert_eq!(
            private.subscription_key(),
            "private:binance:perpetual:main:orders"
        );
        assert_eq!(
            serde_json::from_str::<StreamSubscriptionTopic>(
                &serde_json::to_string(&private).expect("serialize")
            )
            .expect("deserialize"),
            private
        );
    }

    #[test]
    fn stream_subscription_topic_should_use_stable_wire_tags() {
        let topic = StreamSubscriptionTopic::Public {
            exchange: exchange_id(),
            market_type: MarketType::Spot,
            exchange_symbol: "BTCUSDT".to_string(),
            stream: PublicStreamKind::OrderBookDelta,
        };

        let value = serde_json::to_value(&topic).expect("serialize");

        assert_eq!(value["kind"], "public");
        assert_eq!(value["market_type"], "spot");
        assert_eq!(value["exchange_symbol"], "BTCUSDT");
        assert_eq!(value["stream"], "OrderBookDelta");
    }

    #[test]
    fn subscription_ack_should_preserve_status_and_exchange_channel() {
        let topic = StreamSubscriptionTopic::Public {
            exchange: exchange_id(),
            market_type: MarketType::Spot,
            exchange_symbol: "BTCUSDT".to_string(),
            stream: PublicStreamKind::Trades,
        };
        let mut ack = SubscriptionAck::accepted(
            EXCHANGE_API_SCHEMA_VERSION,
            topic.subscription_key(),
            topic,
            Utc::now(),
        );
        ack.exchange_channel = Some("trades.BTCUSDT".to_string());

        let restored: SubscriptionAck =
            serde_json::from_str(&serde_json::to_string(&ack).expect("serialize"))
                .expect("deserialize");

        assert_eq!(restored.status, SubscriptionAckStatus::Accepted);
        assert_eq!(restored.exchange_channel.as_deref(), Some("trades.BTCUSDT"));
    }

    #[test]
    fn unsubscribe_request_should_round_trip() {
        let topic = StreamSubscriptionTopic::Private {
            exchange: exchange_id(),
            market_type: None,
            account_id: account_id(),
            stream: PrivateStreamKind::Balances,
        };
        let request = UnsubscribeRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: RequestContext::new(Utc::now()),
            subscription_id: topic.subscription_key(),
            topic: Some(topic),
            exchange_request_id: Some("req-1".to_string()),
        };

        let restored: UnsubscribeRequest =
            serde_json::from_str(&serde_json::to_string(&request).expect("serialize"))
                .expect("deserialize");

        assert_eq!(restored.subscription_id, request.subscription_id);
        assert_eq!(restored.exchange_request_id.as_deref(), Some("req-1"));
    }

    #[test]
    fn stream_runtime_policy_defaults_should_deserialize_from_missing_fields() {
        let heartbeat: HeartbeatPolicy = serde_json::from_str("{}").expect("heartbeat");
        let auth: AuthRenewalPolicy = serde_json::from_str("{}").expect("auth");
        let ack: SubscriptionAck = serde_json::from_value(serde_json::json!({
            "topic": {
                "kind": "public",
                "exchange": "binance",
                "market_type": "spot",
                "exchange_symbol": "BTCUSDT",
                "stream": "Trades"
            }
        }))
        .expect("ack");

        assert_eq!(heartbeat.direction, HeartbeatDirection::None);
        assert_eq!(heartbeat.ping_interval_ms, 0);
        assert_eq!(auth.kind, AuthRenewalKind::None);
        assert!(!auth.reconnect_on_renewal_failure);
        assert!(!auth.resubscribe_after_renewal);
        assert_eq!(ack.schema_version, EXCHANGE_API_SCHEMA_VERSION);
        assert_eq!(ack.status, SubscriptionAckStatus::Pending);
    }

    #[test]
    fn listen_key_lease_should_compute_renewal_and_expiry() {
        let now = Utc::now();
        let lease = ListenKeyLease {
            exchange: exchange_id(),
            account_id: account_id(),
            listen_key_id: "lease-label".to_string(),
            issued_at: now,
            expires_at: now + Duration::minutes(60),
            last_renewed_at: None,
            renewal_policy: AuthRenewalPolicy {
                kind: AuthRenewalKind::ListenKeyKeepAlive,
                renew_before_expiry_ms: 5 * 60_000,
                renewal_interval_ms: Some(30 * 60_000),
                reconnect_on_renewal_failure: true,
                resubscribe_after_renewal: true,
            },
        };

        assert!(!lease.should_renew(now + Duration::minutes(10)));
        assert!(lease.should_renew(now + Duration::minutes(31)));
        assert!(lease.should_renew(now + Duration::minutes(56)));
        assert!(!lease.is_expired(now + Duration::minutes(59)));
        assert!(lease.is_expired(now + Duration::minutes(60)));
    }

    #[test]
    fn stream_runtime_capability_should_default_to_conservative_runtime() {
        let capability = StreamRuntimeCapability::default();

        assert!(!capability.supports_subscribe);
        assert!(!capability.supports_unsubscribe);
        assert!(!capability.heartbeat.required);
        assert!(!capability.auth.required);
        assert!(!capability.reconnect.supported);

        let heartbeat = HeartbeatPolicy::default();
        let auth = AuthRenewalPolicy::default();
        assert_eq!(heartbeat.direction, HeartbeatDirection::None);
        assert_eq!(auth.kind, AuthRenewalKind::None);
    }
}
