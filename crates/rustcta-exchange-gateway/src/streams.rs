use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use rustcta_exchange_api::{
    AuthRenewalPolicy, ExchangeApiError, ExchangeApiResult, HeartbeatDirection, HeartbeatPolicy,
    ListenKeyLease, PublicStreamKind, PublicStreamSubscription, RequestContext, ResyncReason,
    SequenceGap, StreamChannelScope, StreamSubscriptionTopic, SubscriptionAck,
    SubscriptionAckStatus, UnsubscribeRequest, WsLivenessState, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde::{Deserialize, Serialize};

use crate::orderbook_state::{OrderBookApplyOutcome, OrderBookResyncRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Recovering,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamSupervisorAction {
    None,
    Connect,
    SendPing,
    Reconnect,
    Resubscribe,
    RecoverSnapshot,
    MarkOffline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamReconnectPolicy {
    pub ping_interval_ms: i64,
    pub pong_timeout_ms: i64,
    pub stale_message_ms: i64,
    pub reconnect_backoff_ms: i64,
    pub max_reconnect_attempts: Option<u32>,
}

impl Default for StreamReconnectPolicy {
    fn default() -> Self {
        Self {
            ping_interval_ms: 30_000,
            pong_timeout_ms: 10_000,
            stale_message_ms: 10_000,
            reconnect_backoff_ms: 1_000,
            max_reconnect_attempts: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRuntimeState {
    pub exchange: ExchangeId,
    pub market_type: MarketType,
    pub connection_state: StreamConnectionState,
    pub subscription_count: usize,
    pub reconnect_attempts: u32,
    pub reconnect_count: u64,
    pub last_connected_at: Option<DateTime<Utc>>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub last_ping_at: Option<DateTime<Utc>>,
    pub last_pong_at: Option<DateTime<Utc>>,
    pub last_disconnect_at: Option<DateTime<Utc>>,
    pub sequence_gap_detected: bool,
    pub last_resync_reason: Option<ResyncReason>,
}

impl StreamRuntimeState {
    pub fn new(exchange: ExchangeId, market_type: MarketType) -> Self {
        Self {
            exchange,
            market_type,
            connection_state: StreamConnectionState::Disconnected,
            subscription_count: 0,
            reconnect_attempts: 0,
            reconnect_count: 0,
            last_connected_at: None,
            last_message_at: None,
            last_ping_at: None,
            last_pong_at: None,
            last_disconnect_at: None,
            sequence_gap_detected: false,
            last_resync_reason: None,
        }
    }

    pub fn on_connecting(&mut self) {
        self.connection_state = StreamConnectionState::Connecting;
    }

    pub fn on_connected(&mut self, now: DateTime<Utc>) {
        self.connection_state = StreamConnectionState::Connected;
        self.last_connected_at = Some(now);
        self.last_message_at = Some(now);
        self.last_pong_at = Some(now);
        self.reconnect_attempts = 0;
        self.sequence_gap_detected = false;
        self.last_resync_reason = None;
    }

    pub fn on_message(&mut self, now: DateTime<Utc>) {
        self.last_message_at = Some(now);
        if self.connection_state != StreamConnectionState::Recovering {
            self.connection_state = StreamConnectionState::Connected;
        }
    }

    pub fn on_ping_sent(&mut self, now: DateTime<Utc>) {
        self.last_ping_at = Some(now);
    }

    pub fn on_pong(&mut self, now: DateTime<Utc>) {
        self.last_pong_at = Some(now);
    }

    pub fn on_sequence_gap(&mut self) {
        self.on_resync_required(ResyncReason::SequenceGap);
    }

    pub fn on_resync_required(&mut self, reason: ResyncReason) {
        self.sequence_gap_detected = matches!(reason, ResyncReason::SequenceGap);
        self.last_resync_reason = Some(reason);
        self.connection_state = StreamConnectionState::Recovering;
    }

    pub fn on_orderbook_apply_outcome(&mut self, outcome: &OrderBookApplyOutcome) {
        if let OrderBookApplyOutcome::ResyncRequired(request) = outcome {
            self.on_orderbook_resync_request(request);
        }
    }

    pub fn on_orderbook_resync_request(&mut self, request: &OrderBookResyncRequest) {
        self.on_resync_required(request.reason);
    }

    pub fn on_disconnected(&mut self, now: DateTime<Utc>) {
        self.connection_state = StreamConnectionState::Reconnecting;
        self.last_disconnect_at = Some(now);
        self.reconnect_attempts = self.reconnect_attempts.saturating_add(1);
        self.reconnect_count = self.reconnect_count.saturating_add(1);
    }

    pub fn mark_offline(&mut self) {
        self.connection_state = StreamConnectionState::Offline;
    }

    pub fn decide(
        &self,
        now: DateTime<Utc>,
        policy: &StreamReconnectPolicy,
    ) -> StreamSupervisorAction {
        match self.connection_state {
            StreamConnectionState::Disconnected => return StreamSupervisorAction::Connect,
            StreamConnectionState::Offline => return StreamSupervisorAction::MarkOffline,
            StreamConnectionState::Recovering => return StreamSupervisorAction::RecoverSnapshot,
            StreamConnectionState::Connecting
            | StreamConnectionState::Connected
            | StreamConnectionState::Reconnecting => {}
        }

        if let Some(max_attempts) = policy.max_reconnect_attempts {
            if self.reconnect_attempts >= max_attempts {
                return StreamSupervisorAction::MarkOffline;
            }
        }

        if matches!(self.connection_state, StreamConnectionState::Reconnecting) {
            return StreamSupervisorAction::Reconnect;
        }

        if is_stale(self.last_message_at, now, policy.stale_message_ms) {
            return StreamSupervisorAction::Reconnect;
        }

        if is_stale(self.last_pong_at, now, policy.pong_timeout_ms) {
            return StreamSupervisorAction::Reconnect;
        }

        if is_stale(self.last_ping_at, now, policy.ping_interval_ms) {
            return StreamSupervisorAction::SendPing;
        }

        StreamSupervisorAction::None
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StreamRuntimeEvent {
    Connected {
        session_id: String,
        at: DateTime<Utc>,
    },
    Disconnected {
        session_id: String,
        at: DateTime<Utc>,
    },
    SubscriptionRegistered {
        session_id: String,
        subscription_id: String,
    },
    SubscriptionAcked {
        session_id: String,
        ack: SubscriptionAck,
    },
    SubscriptionRejected {
        session_id: String,
        ack: SubscriptionAck,
    },
    Unsubscribed {
        session_id: String,
        request: UnsubscribeRequest,
    },
    HeartbeatDue {
        session_id: String,
        at: DateTime<Utc>,
    },
    PongDue {
        session_id: String,
        at: DateTime<Utc>,
    },
    AuthRenewalDue {
        session_id: String,
        at: DateTime<Utc>,
    },
    ReconnectDue {
        session_id: String,
        at: DateTime<Utc>,
    },
    ResyncRequired {
        session_id: String,
        reason: ResyncReason,
        gap: Option<SequenceGap>,
    },
    Closed {
        session_id: String,
        at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisteredSubscription {
    pub subscription_id: String,
    pub topic: StreamSubscriptionTopic,
    pub status: SubscriptionAckStatus,
    pub subscribe_payload: Option<String>,
    pub unsubscribe_payload: Option<String>,
    pub registered_at: DateTime<Utc>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub last_ack: Option<SubscriptionAck>,
}

impl RegisteredSubscription {
    fn new(
        topic: StreamSubscriptionTopic,
        subscribe_payload: Option<String>,
        unsubscribe_payload: Option<String>,
        registered_at: DateTime<Utc>,
    ) -> Self {
        let subscription_id = topic.subscription_key();
        Self {
            subscription_id,
            topic,
            status: SubscriptionAckStatus::Pending,
            subscribe_payload,
            unsubscribe_payload,
            registered_at,
            acknowledged_at: None,
            last_ack: None,
        }
    }

    pub fn is_recoverable(&self) -> bool {
        matches!(
            self.status,
            SubscriptionAckStatus::Pending
                | SubscriptionAckStatus::Accepted
                | SubscriptionAckStatus::TimedOut
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionRegistry {
    subscriptions: BTreeMap<String, RegisteredSubscription>,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    pub fn get(&self, subscription_id: &str) -> Option<&RegisteredSubscription> {
        self.subscriptions.get(subscription_id)
    }

    pub fn register(
        &mut self,
        topic: StreamSubscriptionTopic,
        subscribe_payload: Option<String>,
        unsubscribe_payload: Option<String>,
        now: DateTime<Utc>,
    ) -> String {
        let subscription_id = topic.subscription_key();
        match self.subscriptions.get_mut(&subscription_id) {
            Some(existing) if existing.status != SubscriptionAckStatus::Unsubscribed => {
                existing.subscribe_payload =
                    subscribe_payload.or_else(|| existing.subscribe_payload.clone());
                existing.unsubscribe_payload =
                    unsubscribe_payload.or_else(|| existing.unsubscribe_payload.clone());
            }
            _ => {
                self.subscriptions.insert(
                    subscription_id.clone(),
                    RegisteredSubscription::new(topic, subscribe_payload, unsubscribe_payload, now),
                );
            }
        }
        subscription_id
    }

    pub fn acknowledge(&mut self, ack: SubscriptionAck) -> ExchangeApiResult<()> {
        let Some(subscription) = self.subscriptions.get_mut(&ack.subscription_id) else {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("unknown stream subscription {}", ack.subscription_id),
            });
        };
        if subscription.topic != ack.topic {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "stream subscription {} ack topic mismatch",
                    ack.subscription_id
                ),
            });
        }
        subscription.status = ack.status;
        subscription.acknowledged_at = Some(ack.received_at);
        subscription.last_ack = Some(ack);
        Ok(())
    }

    pub fn mark_timed_out(
        &mut self,
        subscription_id: &str,
        now: DateTime<Utc>,
    ) -> ExchangeApiResult<SubscriptionAck> {
        let Some(subscription) = self.subscriptions.get_mut(subscription_id) else {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("unknown stream subscription {subscription_id}"),
            });
        };
        subscription.status = SubscriptionAckStatus::TimedOut;
        let ack = SubscriptionAck {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            subscription_id: subscription_id.to_string(),
            topic: subscription.topic.clone(),
            status: SubscriptionAckStatus::TimedOut,
            exchange_request_id: None,
            exchange_channel: None,
            message: Some("subscription acknowledgement timed out".to_string()),
            received_at: now,
        };
        subscription.acknowledged_at = Some(now);
        subscription.last_ack = Some(ack.clone());
        Ok(ack)
    }

    pub fn mark_unsubscribed(
        &mut self,
        subscription_id: &str,
        context: RequestContext,
        exchange_request_id: Option<String>,
    ) -> ExchangeApiResult<UnsubscribeRequest> {
        let Some(subscription) = self.subscriptions.get_mut(subscription_id) else {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("unknown stream subscription {subscription_id}"),
            });
        };
        subscription.status = SubscriptionAckStatus::Unsubscribed;
        Ok(UnsubscribeRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context,
            subscription_id: subscription_id.to_string(),
            topic: Some(subscription.topic.clone()),
            exchange_request_id,
        })
    }

    pub fn active_subscriptions(&self) -> Vec<&RegisteredSubscription> {
        self.subscriptions
            .values()
            .filter(|subscription| subscription.is_recoverable())
            .collect()
    }

    pub fn resubscribe_topics(&self) -> Vec<StreamSubscriptionTopic> {
        self.active_subscriptions()
            .into_iter()
            .map(|subscription| subscription.topic.clone())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamSession {
    pub session_id: String,
    pub exchange: ExchangeId,
    pub market_type: MarketType,
    pub url: String,
    pub channel_scope: StreamChannelScope,
    pub state: StreamRuntimeState,
    pub heartbeat_policy: HeartbeatPolicy,
    pub auth_renewal_policy: AuthRenewalPolicy,
    pub listen_key_lease: Option<ListenKeyLease>,
    pub registry: SubscriptionRegistry,
    pub created_at: DateTime<Utc>,
    pub last_renewal_failure_at: Option<DateTime<Utc>>,
}

impl StreamSession {
    pub fn new(
        session_id: impl Into<String>,
        exchange: ExchangeId,
        market_type: MarketType,
        url: impl Into<String>,
        channel_scope: StreamChannelScope,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            exchange: exchange.clone(),
            market_type,
            url: url.into(),
            channel_scope,
            state: StreamRuntimeState::new(exchange, market_type),
            heartbeat_policy: HeartbeatPolicy::default(),
            auth_renewal_policy: AuthRenewalPolicy::default(),
            listen_key_lease: None,
            registry: SubscriptionRegistry::new(),
            created_at: now,
            last_renewal_failure_at: None,
        }
    }

    pub fn with_heartbeat_policy(mut self, policy: HeartbeatPolicy) -> Self {
        self.heartbeat_policy = policy;
        self
    }

    pub fn with_auth_renewal_policy(mut self, policy: AuthRenewalPolicy) -> Self {
        self.auth_renewal_policy = policy;
        self
    }

    pub fn with_listen_key_lease(mut self, lease: ListenKeyLease) -> Self {
        self.listen_key_lease = Some(lease);
        self
    }

    pub fn on_connected(&mut self, now: DateTime<Utc>) -> StreamRuntimeEvent {
        self.state.on_connected(now);
        StreamRuntimeEvent::Connected {
            session_id: self.session_id.clone(),
            at: now,
        }
    }

    pub fn on_disconnected(&mut self, now: DateTime<Utc>) -> StreamRuntimeEvent {
        self.state.on_disconnected(now);
        StreamRuntimeEvent::Disconnected {
            session_id: self.session_id.clone(),
            at: now,
        }
    }

    pub fn on_server_ping(&mut self, now: DateTime<Utc>) -> StreamRuntimeEvent {
        self.state.on_message(now);
        StreamRuntimeEvent::PongDue {
            session_id: self.session_id.clone(),
            at: now,
        }
    }

    pub fn on_application_heartbeat(&mut self, now: DateTime<Utc>) {
        self.state.on_message(now);
        self.state.on_pong(now);
    }

    pub fn close(&mut self, now: DateTime<Utc>) -> StreamRuntimeEvent {
        self.state.mark_offline();
        StreamRuntimeEvent::Closed {
            session_id: self.session_id.clone(),
            at: now,
        }
    }

    pub fn register_subscription(
        &mut self,
        topic: StreamSubscriptionTopic,
        subscribe_payload: Option<String>,
        unsubscribe_payload: Option<String>,
        now: DateTime<Utc>,
    ) -> StreamRuntimeEvent {
        let subscription_id =
            self.registry
                .register(topic, subscribe_payload, unsubscribe_payload, now);
        self.state.subscription_count = self.registry.active_subscriptions().len();
        StreamRuntimeEvent::SubscriptionRegistered {
            session_id: self.session_id.clone(),
            subscription_id,
        }
    }

    pub fn acknowledge_subscription(
        &mut self,
        ack: SubscriptionAck,
    ) -> ExchangeApiResult<StreamRuntimeEvent> {
        let status = ack.status;
        self.registry.acknowledge(ack.clone())?;
        self.state.subscription_count = self.registry.active_subscriptions().len();
        Ok(match status {
            SubscriptionAckStatus::Rejected => StreamRuntimeEvent::SubscriptionRejected {
                session_id: self.session_id.clone(),
                ack,
            },
            _ => StreamRuntimeEvent::SubscriptionAcked {
                session_id: self.session_id.clone(),
                ack,
            },
        })
    }

    pub fn unsubscribe(
        &mut self,
        subscription_id: &str,
        context: RequestContext,
        exchange_request_id: Option<String>,
    ) -> ExchangeApiResult<StreamRuntimeEvent> {
        let request =
            self.registry
                .mark_unsubscribed(subscription_id, context, exchange_request_id)?;
        self.state.subscription_count = self.registry.active_subscriptions().len();
        Ok(StreamRuntimeEvent::Unsubscribed {
            session_id: self.session_id.clone(),
            request,
        })
    }

    pub fn liveness_state(&self, now: DateTime<Utc>) -> WsLivenessState {
        if self
            .listen_key_lease
            .as_ref()
            .is_some_and(|lease| lease.is_expired(now))
        {
            return WsLivenessState::Expired;
        }

        if self
            .listen_key_lease
            .as_ref()
            .is_some_and(|lease| lease.should_renew(now))
        {
            return WsLivenessState::AuthRenewalDue;
        }

        if self.auth_renewal_due(now) {
            return WsLivenessState::AuthRenewalDue;
        }

        if self.heartbeat_policy.direction == HeartbeatDirection::None {
            return WsLivenessState::Healthy;
        }

        if is_stale(
            self.state.last_message_at,
            now,
            self.heartbeat_policy.stale_message_ms,
        ) {
            return WsLivenessState::Stale;
        }

        if let Some(last_ping_at) = self.state.last_ping_at {
            let pong_missing_or_old = self
                .state
                .last_pong_at
                .map(|last_pong_at| last_pong_at < last_ping_at)
                .unwrap_or(true);
            if pong_missing_or_old
                && now.signed_duration_since(last_ping_at)
                    > Duration::milliseconds(self.heartbeat_policy.pong_timeout_ms)
            {
                return WsLivenessState::Stale;
            }
            if pong_missing_or_old {
                return WsLivenessState::AwaitingPong;
            }
        }

        WsLivenessState::Healthy
    }

    pub fn next_runtime_event(&self, now: DateTime<Utc>) -> Option<StreamRuntimeEvent> {
        match self.liveness_state(now) {
            WsLivenessState::AuthRenewalDue => Some(StreamRuntimeEvent::AuthRenewalDue {
                session_id: self.session_id.clone(),
                at: now,
            }),
            WsLivenessState::Expired | WsLivenessState::Stale => {
                Some(StreamRuntimeEvent::ReconnectDue {
                    session_id: self.session_id.clone(),
                    at: now,
                })
            }
            WsLivenessState::Healthy | WsLivenessState::AwaitingPong => {
                match self.heartbeat_policy.direction {
                    HeartbeatDirection::ClientPing
                        if is_stale(
                            self.state.last_ping_at,
                            now,
                            self.heartbeat_policy.ping_interval_ms,
                        ) =>
                    {
                        Some(StreamRuntimeEvent::HeartbeatDue {
                            session_id: self.session_id.clone(),
                            at: now,
                        })
                    }
                    HeartbeatDirection::ServerPing | HeartbeatDirection::ApplicationMessage => None,
                    HeartbeatDirection::ClientPing | HeartbeatDirection::None => None,
                }
            }
        }
    }

    pub fn on_sequence_gap(&mut self, gap: SequenceGap) -> StreamRuntimeEvent {
        self.state.on_sequence_gap();
        StreamRuntimeEvent::ResyncRequired {
            session_id: self.session_id.clone(),
            reason: ResyncReason::SequenceGap,
            gap: Some(gap),
        }
    }

    pub fn on_orderbook_apply_outcome(
        &mut self,
        outcome: &OrderBookApplyOutcome,
        gap: Option<SequenceGap>,
    ) -> Option<StreamRuntimeEvent> {
        match outcome {
            OrderBookApplyOutcome::ResyncRequired(request) => {
                Some(self.on_orderbook_resync_request(request, gap))
            }
            _ => None,
        }
    }

    pub fn on_orderbook_resync_request(
        &mut self,
        request: &OrderBookResyncRequest,
        gap: Option<SequenceGap>,
    ) -> StreamRuntimeEvent {
        self.state.on_orderbook_resync_request(request);
        StreamRuntimeEvent::ResyncRequired {
            session_id: self.session_id.clone(),
            reason: request.reason,
            gap: gap.filter(|_| request.reason == ResyncReason::SequenceGap),
        }
    }

    fn auth_renewal_due(&self, now: DateTime<Utc>) -> bool {
        if self.auth_renewal_policy.kind == rustcta_exchange_api::AuthRenewalKind::None {
            return false;
        }

        self.auth_renewal_policy
            .renewal_interval_ms
            .is_some_and(|interval_ms| {
                let since = self.state.last_connected_at.unwrap_or(self.created_at);
                now.signed_duration_since(since) >= Duration::milliseconds(interval_ms)
            })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamSubscriptionKey {
    pub exchange: ExchangeId,
    pub market_type: MarketType,
    pub exchange_symbol: String,
    pub kind: PublicStreamKind,
}

impl StreamSubscriptionKey {
    pub fn public_order_book(subscription: &PublicStreamSubscription) -> ExchangeApiResult<Self> {
        if subscription.schema_version != EXCHANGE_API_SCHEMA_VERSION {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "unsupported public stream schema_version {}, expected {}",
                    subscription.schema_version, EXCHANGE_API_SCHEMA_VERSION
                ),
            });
        }
        match subscription.kind {
            PublicStreamKind::OrderBookSnapshot | PublicStreamKind::OrderBookDelta => Ok(Self {
                exchange: subscription.symbol.exchange.clone(),
                market_type: subscription.symbol.market_type,
                exchange_symbol: subscription.symbol.exchange_symbol.symbol.clone(),
                kind: subscription.kind.clone(),
            }),
            _ => Err(ExchangeApiError::Unsupported {
                operation: "public_stream.non_order_book",
            }),
        }
    }

    pub fn subscription_id(&self) -> String {
        format!(
            "{}:{:?}:{}:{:?}",
            self.exchange, self.market_type, self.exchange_symbol, self.kind
        )
        .to_ascii_lowercase()
    }
}

pub fn websocket_dependency_ready() -> bool {
    let _ = tokio_tungstenite::tungstenite::Message::Ping(Vec::new());
    true
}

fn is_stale(last_seen: Option<DateTime<Utc>>, now: DateTime<Utc>, max_age_ms: i64) -> bool {
    match last_seen {
        Some(last_seen) => {
            now.signed_duration_since(last_seen) > Duration::milliseconds(max_age_ms)
        }
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use rustcta_exchange_api::{
        AuthRenewalKind, AuthRenewalPolicy, HeartbeatDirection, HeartbeatPolicy, ListenKeyLease,
        PrivateStreamKind, PublicStreamKind, PublicStreamSubscription, RequestContext,
        StreamChannelScope, StreamSubscriptionTopic, SubscriptionAck, SubscriptionAckStatus,
        SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
    };
    use rustcta_types::{AccountId, ExchangeId, ExchangeSymbol, MarketType};

    use super::*;

    fn exchange_id() -> ExchangeId {
        ExchangeId::new("binance").expect("exchange")
    }

    fn account_id() -> AccountId {
        AccountId::new("main").expect("account")
    }

    fn subscription(kind: PublicStreamKind) -> PublicStreamSubscription {
        let exchange = exchange_id();
        let market_type = MarketType::Spot;
        PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: RequestContext::new(Utc::now()),
            symbol: SymbolScope {
                exchange: exchange.clone(),
                market_type,
                canonical_symbol: None,
                exchange_symbol: ExchangeSymbol::new(exchange, market_type, "BTCUSDT")
                    .expect("symbol"),
            },
            kind,
        }
    }

    fn topic() -> StreamSubscriptionTopic {
        StreamSubscriptionTopic::Public {
            exchange: exchange_id(),
            market_type: MarketType::Spot,
            exchange_symbol: "BTCUSDT".to_string(),
            stream: PublicStreamKind::OrderBookDelta,
        }
    }

    fn private_topic() -> StreamSubscriptionTopic {
        StreamSubscriptionTopic::Private {
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: account_id(),
            stream: PrivateStreamKind::Orders,
        }
    }

    #[test]
    fn stream_state_should_request_connect_when_disconnected() {
        let state = StreamRuntimeState::new(exchange_id(), MarketType::Spot);

        assert_eq!(
            state.decide(Utc::now(), &StreamReconnectPolicy::default()),
            StreamSupervisorAction::Connect
        );
    }

    #[test]
    fn stream_state_should_reconnect_on_stale_message() {
        let now = Utc::now();
        let mut state = StreamRuntimeState::new(exchange_id(), MarketType::Spot);
        state.on_connected(now - Duration::seconds(20));

        assert_eq!(
            state.decide(now, &StreamReconnectPolicy::default()),
            StreamSupervisorAction::Reconnect
        );
    }

    #[test]
    fn stream_state_should_request_ping_after_interval() {
        let now = Utc::now();
        let mut state = StreamRuntimeState::new(exchange_id(), MarketType::Spot);
        state.on_connected(now);
        state.on_message(now);
        state.on_pong(now);

        assert_eq!(
            state.decide(
                now + Duration::seconds(31),
                &StreamReconnectPolicy {
                    stale_message_ms: 60_000,
                    pong_timeout_ms: 60_000,
                    ..StreamReconnectPolicy::default()
                }
            ),
            StreamSupervisorAction::SendPing
        );
    }

    #[test]
    fn stream_state_should_recover_snapshot_on_sequence_gap() {
        let now = Utc::now();
        let mut state = StreamRuntimeState::new(exchange_id(), MarketType::Spot);
        state.on_connected(now);
        state.on_sequence_gap();

        assert_eq!(
            state.decide(now, &StreamReconnectPolicy::default()),
            StreamSupervisorAction::RecoverSnapshot
        );
    }

    #[test]
    fn stream_state_should_recover_snapshot_on_resync_reason() {
        let now = Utc::now();
        let mut state = StreamRuntimeState::new(exchange_id(), MarketType::Spot);
        state.on_connected(now);
        state.on_resync_required(ResyncReason::ChecksumMismatch);

        assert_eq!(
            state.last_resync_reason,
            Some(ResyncReason::ChecksumMismatch)
        );
        assert!(!state.sequence_gap_detected);
        assert_eq!(
            state.decide(now, &StreamReconnectPolicy::default()),
            StreamSupervisorAction::RecoverSnapshot
        );
    }

    #[test]
    fn stream_state_should_recover_snapshot_on_orderbook_resync_outcome() {
        let now = Utc::now();
        let mut state = StreamRuntimeState::new(exchange_id(), MarketType::Spot);
        state.on_connected(now);
        state.on_orderbook_apply_outcome(&OrderBookApplyOutcome::ResyncRequired(
            OrderBookResyncRequest {
                reason: ResyncReason::ChecksumMismatch,
                expected_next_sequence: Some(11),
                received_first_sequence: Some(11),
                received_last_sequence: Some(11),
            },
        ));

        assert_eq!(
            state.last_resync_reason,
            Some(ResyncReason::ChecksumMismatch)
        );
        assert_eq!(
            state.decide(now, &StreamReconnectPolicy::default()),
            StreamSupervisorAction::RecoverSnapshot
        );
    }

    #[test]
    fn stream_state_should_mark_offline_after_max_reconnect_attempts() {
        let now = Utc::now();
        let mut state = StreamRuntimeState::new(exchange_id(), MarketType::Spot);
        state.on_disconnected(now);

        assert_eq!(
            state.decide(
                now,
                &StreamReconnectPolicy {
                    max_reconnect_attempts: Some(1),
                    ..StreamReconnectPolicy::default()
                }
            ),
            StreamSupervisorAction::MarkOffline
        );
    }

    #[test]
    fn stream_subscription_key_should_accept_only_order_books() {
        let book = StreamSubscriptionKey::public_order_book(&subscription(
            PublicStreamKind::OrderBookDelta,
        ))
        .expect("book subscription");
        assert_eq!(
            book.subscription_id(),
            "binance:spot:btcusdt:orderbookdelta"
        );

        let error =
            StreamSubscriptionKey::public_order_book(&subscription(PublicStreamKind::Trades))
                .expect_err("trades unsupported for book stream");
        assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
    }

    #[test]
    fn websocket_dependency_should_be_linked() {
        assert!(websocket_dependency_ready());
    }

    #[test]
    fn subscription_registry_should_deduplicate_equivalent_subscriptions() {
        let now = Utc::now();
        let mut registry = SubscriptionRegistry::new();
        let first = registry.register(
            topic(),
            Some("{\"op\":\"subscribe\"}".to_string()),
            None,
            now,
        );
        let second = registry.register(
            topic(),
            None,
            Some("{\"op\":\"unsubscribe\"}".to_string()),
            now,
        );

        assert_eq!(first, second);
        assert_eq!(registry.len(), 1);
        let registered = registry.get(&first).expect("registered");
        assert_eq!(registered.status, SubscriptionAckStatus::Pending);
        assert_eq!(
            registered.unsubscribe_payload.as_deref(),
            Some("{\"op\":\"unsubscribe\"}")
        );
    }

    #[test]
    fn subscription_registry_should_track_accepted_and_rejected_acks() {
        let now = Utc::now();
        let mut registry = SubscriptionRegistry::new();
        let accepted_id = registry.register(topic(), None, None, now);
        let rejected_id = registry.register(private_topic(), None, None, now);

        registry
            .acknowledge(SubscriptionAck::accepted(
                EXCHANGE_API_SCHEMA_VERSION,
                accepted_id.clone(),
                topic(),
                now,
            ))
            .expect("accepted ack");
        registry
            .acknowledge(SubscriptionAck::rejected(
                EXCHANGE_API_SCHEMA_VERSION,
                rejected_id.clone(),
                private_topic(),
                "auth failed",
                now,
            ))
            .expect("rejected ack");

        assert_eq!(
            registry.get(&accepted_id).expect("accepted").status,
            SubscriptionAckStatus::Accepted
        );
        assert_eq!(
            registry.get(&rejected_id).expect("rejected").status,
            SubscriptionAckStatus::Rejected
        );
        assert_eq!(registry.active_subscriptions().len(), 1);
    }

    #[test]
    fn subscription_registry_should_reject_ack_topic_mismatch() {
        let now = Utc::now();
        let mut registry = SubscriptionRegistry::new();
        let subscription_id = registry.register(topic(), None, None, now);

        let error = registry
            .acknowledge(SubscriptionAck::accepted(
                EXCHANGE_API_SCHEMA_VERSION,
                subscription_id,
                private_topic(),
                now,
            ))
            .expect_err("topic mismatch");

        assert!(matches!(error, ExchangeApiError::InvalidRequest { .. }));
    }

    #[test]
    fn subscription_registry_should_resubscribe_timed_out_subscription() {
        let now = Utc::now();
        let mut registry = SubscriptionRegistry::new();
        let subscription_id = registry.register(topic(), None, None, now);

        let ack = registry
            .mark_timed_out(&subscription_id, now + Duration::seconds(5))
            .expect("timeout ack");

        assert_eq!(ack.status, SubscriptionAckStatus::TimedOut);
        assert_eq!(registry.resubscribe_topics(), vec![topic()]);
    }

    #[test]
    fn subscription_registry_should_not_resubscribe_after_manual_unsubscribe() {
        let now = Utc::now();
        let mut registry = SubscriptionRegistry::new();
        let keep_id = registry.register(topic(), None, None, now);
        let drop_id = registry.register(private_topic(), None, None, now);
        registry
            .acknowledge(SubscriptionAck::accepted(
                EXCHANGE_API_SCHEMA_VERSION,
                keep_id.clone(),
                topic(),
                now,
            ))
            .expect("ack keep");
        registry
            .mark_unsubscribed(
                &drop_id,
                RequestContext::new(now),
                Some("unsub-1".to_string()),
            )
            .expect("unsubscribe");

        let topics = registry.resubscribe_topics();
        assert_eq!(topics, vec![topic()]);
    }

    #[test]
    fn stream_session_should_register_ack_and_unsubscribe() {
        let now = Utc::now();
        let mut session = StreamSession::new(
            "session-1",
            exchange_id(),
            MarketType::Spot,
            "wss://stream.example",
            StreamChannelScope::Public,
            now,
        );
        let event = session.register_subscription(topic(), None, None, now);
        let StreamRuntimeEvent::SubscriptionRegistered {
            subscription_id, ..
        } = event
        else {
            panic!("expected subscription registration event");
        };

        let ack_event = session
            .acknowledge_subscription(SubscriptionAck::accepted(
                EXCHANGE_API_SCHEMA_VERSION,
                subscription_id.clone(),
                topic(),
                now,
            ))
            .expect("ack");
        assert!(matches!(
            ack_event,
            StreamRuntimeEvent::SubscriptionAcked { .. }
        ));
        assert_eq!(session.state.subscription_count, 1);

        let unsubscribe_event = session
            .unsubscribe(&subscription_id, RequestContext::new(now), None)
            .expect("unsubscribe");
        assert!(matches!(
            unsubscribe_event,
            StreamRuntimeEvent::Unsubscribed { .. }
        ));
        assert_eq!(session.state.subscription_count, 0);
        assert!(session.registry.resubscribe_topics().is_empty());
    }

    #[test]
    fn stream_session_should_request_heartbeat_after_interval() {
        let now = Utc::now();
        let mut session = StreamSession::new(
            "session-1",
            exchange_id(),
            MarketType::Spot,
            "wss://stream.example",
            StreamChannelScope::Public,
            now,
        )
        .with_heartbeat_policy(HeartbeatPolicy {
            direction: HeartbeatDirection::ClientPing,
            ping_interval_ms: 1_000,
            pong_timeout_ms: 10_000,
            stale_message_ms: 60_000,
            requires_pong_payload_echo: false,
        });
        session.on_connected(now);
        session.state.on_ping_sent(now);
        session.state.on_pong(now);

        assert!(matches!(
            session.next_runtime_event(now + Duration::milliseconds(1_001)),
            Some(StreamRuntimeEvent::HeartbeatDue { .. })
        ));
    }

    #[test]
    fn stream_session_should_reconnect_on_pong_timeout() {
        let now = Utc::now();
        let mut session = StreamSession::new(
            "session-1",
            exchange_id(),
            MarketType::Spot,
            "wss://stream.example",
            StreamChannelScope::Public,
            now,
        )
        .with_heartbeat_policy(HeartbeatPolicy {
            direction: HeartbeatDirection::ClientPing,
            ping_interval_ms: 1_000,
            pong_timeout_ms: 500,
            stale_message_ms: 60_000,
            requires_pong_payload_echo: false,
        });
        session.on_connected(now);
        session.state.on_message(now + Duration::milliseconds(100));
        session
            .state
            .on_ping_sent(now + Duration::milliseconds(200));

        assert_eq!(
            session.liveness_state(now + Duration::milliseconds(701)),
            WsLivenessState::Stale
        );
        assert!(matches!(
            session.next_runtime_event(now + Duration::milliseconds(701)),
            Some(StreamRuntimeEvent::ReconnectDue { .. })
        ));
    }

    #[test]
    fn stream_session_should_answer_server_ping_with_pong_due_event() {
        let now = Utc::now();
        let mut session = StreamSession::new(
            "session-1",
            exchange_id(),
            MarketType::Spot,
            "wss://stream.example",
            StreamChannelScope::Public,
            now,
        )
        .with_heartbeat_policy(HeartbeatPolicy {
            direction: HeartbeatDirection::ServerPing,
            ping_interval_ms: 0,
            pong_timeout_ms: 10_000,
            stale_message_ms: 30_000,
            requires_pong_payload_echo: false,
        });

        let event = session.on_server_ping(now + Duration::seconds(1));

        assert!(matches!(event, StreamRuntimeEvent::PongDue { .. }));
        assert_eq!(
            session.state.last_message_at,
            Some(now + Duration::seconds(1))
        );
    }

    #[test]
    fn stream_session_should_refresh_liveness_on_application_heartbeat() {
        let now = Utc::now();
        let mut session = StreamSession::new(
            "session-1",
            exchange_id(),
            MarketType::Spot,
            "wss://stream.example",
            StreamChannelScope::Public,
            now,
        )
        .with_heartbeat_policy(HeartbeatPolicy {
            direction: HeartbeatDirection::ApplicationMessage,
            ping_interval_ms: 0,
            pong_timeout_ms: 10_000,
            stale_message_ms: 5_000,
            requires_pong_payload_echo: false,
        });
        session.on_connected(now);
        session.on_application_heartbeat(now + Duration::seconds(10));

        assert_eq!(
            session.liveness_state(now + Duration::seconds(14)),
            WsLivenessState::Healthy
        );
    }

    #[test]
    fn stream_session_should_request_auth_renewal_before_listen_key_expiry() {
        let now = Utc::now();
        let lease = ListenKeyLease {
            exchange: exchange_id(),
            account_id: account_id(),
            listen_key_id: "listen-key-label".to_string(),
            issued_at: now,
            expires_at: now + Duration::minutes(60),
            last_renewed_at: None,
            renewal_policy: AuthRenewalPolicy {
                kind: AuthRenewalKind::ListenKeyKeepAlive,
                renew_before_expiry_ms: 5 * 60_000,
                renewal_interval_ms: None,
                reconnect_on_renewal_failure: true,
                resubscribe_after_renewal: true,
            },
        };
        let session = StreamSession::new(
            "session-1",
            exchange_id(),
            MarketType::Spot,
            "wss://stream.example",
            StreamChannelScope::Private,
            now,
        )
        .with_listen_key_lease(lease);

        assert_eq!(
            session.liveness_state(now + Duration::minutes(56)),
            WsLivenessState::AuthRenewalDue
        );
        assert!(matches!(
            session.next_runtime_event(now + Duration::minutes(56)),
            Some(StreamRuntimeEvent::AuthRenewalDue { .. })
        ));
    }

    #[test]
    fn stream_session_should_request_auth_renewal_from_session_policy() {
        let now = Utc::now();
        let mut session = StreamSession::new(
            "session-1",
            exchange_id(),
            MarketType::Spot,
            "wss://stream.example",
            StreamChannelScope::Private,
            now,
        )
        .with_auth_renewal_policy(AuthRenewalPolicy {
            kind: AuthRenewalKind::ReLogin,
            renew_before_expiry_ms: 0,
            renewal_interval_ms: Some(30_000),
            reconnect_on_renewal_failure: true,
            resubscribe_after_renewal: true,
        });
        session.on_connected(now);

        assert_eq!(
            session.liveness_state(now + Duration::seconds(31)),
            WsLivenessState::AuthRenewalDue
        );
        assert!(matches!(
            session.next_runtime_event(now + Duration::seconds(31)),
            Some(StreamRuntimeEvent::AuthRenewalDue { .. })
        ));
    }

    #[test]
    fn stream_session_should_emit_resync_event_on_sequence_gap() {
        let now = Utc::now();
        let mut session = StreamSession::new(
            "session-1",
            exchange_id(),
            MarketType::Spot,
            "wss://stream.example",
            StreamChannelScope::Public,
            now,
        );
        let event = session.on_sequence_gap(SequenceGap {
            exchange: exchange_id(),
            market_type: MarketType::Spot,
            exchange_symbol: "BTCUSDT".to_string(),
            expected_sequence: Some(42),
            received_sequence: Some(44),
            detected_at: now,
        });

        assert!(matches!(
            event,
            StreamRuntimeEvent::ResyncRequired {
                reason: ResyncReason::SequenceGap,
                gap: Some(_),
                ..
            }
        ));
        assert_eq!(
            session.state.decide(now, &StreamReconnectPolicy::default()),
            StreamSupervisorAction::RecoverSnapshot
        );
    }

    #[test]
    fn stream_session_should_emit_resync_event_on_orderbook_resync_request() {
        let now = Utc::now();
        let mut session = StreamSession::new(
            "session-1",
            exchange_id(),
            MarketType::Spot,
            "wss://stream.example",
            StreamChannelScope::Public,
            now,
        );
        let gap = SequenceGap {
            exchange: exchange_id(),
            market_type: MarketType::Spot,
            exchange_symbol: "BTCUSDT".to_string(),
            expected_sequence: Some(42),
            received_sequence: Some(44),
            detected_at: now,
        };
        let request = OrderBookResyncRequest {
            reason: ResyncReason::SequenceGap,
            expected_next_sequence: Some(42),
            received_first_sequence: Some(44),
            received_last_sequence: Some(44),
        };
        let outcome = OrderBookApplyOutcome::ResyncRequired(request);

        let event = session
            .on_orderbook_apply_outcome(&outcome, Some(gap))
            .expect("sequence gap should emit resync event");

        assert!(matches!(
            event,
            StreamRuntimeEvent::ResyncRequired {
                reason: ResyncReason::SequenceGap,
                gap: Some(_),
                ..
            }
        ));
        assert_eq!(
            session.state.last_resync_reason,
            Some(ResyncReason::SequenceGap)
        );
        assert_eq!(
            session.state.decide(now, &StreamReconnectPolicy::default()),
            StreamSupervisorAction::RecoverSnapshot
        );
    }

    #[test]
    fn stream_session_should_not_attach_gap_to_non_sequence_resync() {
        let now = Utc::now();
        let mut session = StreamSession::new(
            "session-1",
            exchange_id(),
            MarketType::Spot,
            "wss://stream.example",
            StreamChannelScope::Public,
            now,
        );
        let gap = SequenceGap {
            exchange: exchange_id(),
            market_type: MarketType::Spot,
            exchange_symbol: "BTCUSDT".to_string(),
            expected_sequence: Some(42),
            received_sequence: Some(44),
            detected_at: now,
        };
        let request = OrderBookResyncRequest {
            reason: ResyncReason::ChecksumMismatch,
            expected_next_sequence: Some(42),
            received_first_sequence: Some(42),
            received_last_sequence: Some(42),
        };

        let event = session.on_orderbook_resync_request(&request, Some(gap));

        assert!(matches!(
            event,
            StreamRuntimeEvent::ResyncRequired {
                reason: ResyncReason::ChecksumMismatch,
                gap: None,
                ..
            }
        ));
        assert_eq!(
            session.state.last_resync_reason,
            Some(ResyncReason::ChecksumMismatch)
        );
        assert!(!session.state.sequence_gap_detected);
    }
}
