use chrono::{DateTime, Duration, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};
use serde::{Deserialize, Serialize};

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
        self.sequence_gap_detected = true;
        self.connection_state = StreamConnectionState::Recovering;
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
        PublicStreamKind, PublicStreamSubscription, RequestContext, SymbolScope,
        EXCHANGE_API_SCHEMA_VERSION,
    };
    use rustcta_types::{ExchangeId, ExchangeSymbol, MarketType};

    use super::*;

    fn exchange_id() -> ExchangeId {
        ExchangeId::new("binance").expect("exchange")
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
}
