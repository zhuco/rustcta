use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{ExchangeId, RouteStatus, RouteType};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WsConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Recovering,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WsSupervisorAction {
    None,
    Connect,
    SendHeartbeat,
    Reconnect,
    Resubscribe,
    FetchSnapshot,
    Failover,
    MarkOffline,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WsSupervisorConfig {
    pub heartbeat_interval_ms: i64,
    pub heartbeat_timeout_ms: i64,
    pub stale_message_ms: i64,
    pub max_reconnect_attempts: u32,
}

impl Default for WsSupervisorConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 30_000,
            heartbeat_timeout_ms: 10_000,
            stale_message_ms: 3_000,
            max_reconnect_attempts: 10,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WsSupervisorState {
    pub exchange: ExchangeId,
    pub route_type: RouteType,
    pub status: RouteStatus,
    pub connection_state: WsConnectionState,
    pub last_message_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub last_pong_at: Option<DateTime<Utc>>,
    pub reconnect_attempts: u32,
    pub sequence_gap_detected: bool,
}

impl WsSupervisorState {
    pub fn new(exchange: ExchangeId, route_type: RouteType) -> Self {
        Self {
            exchange,
            route_type,
            status: RouteStatus::Healthy,
            connection_state: WsConnectionState::Disconnected,
            last_message_at: None,
            last_heartbeat_at: None,
            last_pong_at: None,
            reconnect_attempts: 0,
            sequence_gap_detected: false,
        }
    }

    pub fn on_connected(&mut self, now: DateTime<Utc>) {
        self.connection_state = WsConnectionState::Connected;
        self.status = RouteStatus::Healthy;
        self.last_message_at = Some(now);
        self.reconnect_attempts = 0;
    }

    pub fn on_message(&mut self, now: DateTime<Utc>) {
        self.last_message_at = Some(now);
        if self.connection_state != WsConnectionState::Recovering {
            self.connection_state = WsConnectionState::Connected;
        }
    }

    pub fn on_sequence_gap(&mut self) {
        self.sequence_gap_detected = true;
        self.connection_state = WsConnectionState::Recovering;
        self.status = RouteStatus::Degraded;
    }

    pub fn decide(&self, now: DateTime<Utc>, config: &WsSupervisorConfig) -> WsSupervisorAction {
        if self.connection_state == WsConnectionState::Disconnected {
            return WsSupervisorAction::Connect;
        }
        if self.connection_state == WsConnectionState::Offline
            || self.status == RouteStatus::Offline
        {
            return WsSupervisorAction::MarkOffline;
        }
        if self.sequence_gap_detected {
            return WsSupervisorAction::FetchSnapshot;
        }
        if self.reconnect_attempts >= config.max_reconnect_attempts {
            return WsSupervisorAction::Failover;
        }
        if let Some(last_message_at) = self.last_message_at {
            let age_ms = now
                .signed_duration_since(last_message_at)
                .num_milliseconds();
            if age_ms > config.stale_message_ms {
                return WsSupervisorAction::Reconnect;
            }
        }
        if let Some(last_heartbeat_at) = self.last_heartbeat_at {
            let age_ms = now
                .signed_duration_since(last_heartbeat_at)
                .num_milliseconds();
            if age_ms > config.heartbeat_interval_ms {
                return WsSupervisorAction::SendHeartbeat;
            }
        } else {
            return WsSupervisorAction::SendHeartbeat;
        }

        WsSupervisorAction::None
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;

    #[test]
    fn supervisor_should_request_snapshot_on_sequence_gap() {
        let now = Utc::now();
        let mut state = WsSupervisorState::new(ExchangeId::Binance, RouteType::MarketWs);
        state.on_connected(now);
        state.on_sequence_gap();

        assert_eq!(
            state.decide(now, &WsSupervisorConfig::default()),
            WsSupervisorAction::FetchSnapshot
        );
    }

    #[test]
    fn supervisor_should_reconnect_after_stale_messages() {
        let now = Utc::now();
        let mut state = WsSupervisorState::new(ExchangeId::Okx, RouteType::MarketWs);
        state.on_connected(now - Duration::seconds(5));

        assert_eq!(
            state.decide(now, &WsSupervisorConfig::default()),
            WsSupervisorAction::Reconnect
        );
    }
}
