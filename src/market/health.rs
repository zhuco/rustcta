use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{ExchangeId, RouteStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RouteType {
    MarketWs,
    PrivateWs,
    RestPublic,
    RestPrivate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteHealth {
    pub exchange: ExchangeId,
    pub route_type: RouteType,
    pub endpoint: String,
    pub status: RouteStatus,
    pub last_ok_at: Option<DateTime<Utc>>,
    pub last_error_at: Option<DateTime<Utc>>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub consecutive_errors: u32,
    pub reconnect_count: u64,
    pub sequence_gap_count: u64,
    pub rate_limit_hits: u64,
    pub latency_ms_p50: Option<f64>,
    pub latency_ms_p95: Option<f64>,
    pub latency_ms_p99: Option<f64>,
    pub latency_ms_p999: Option<f64>,
    pub latency_ms_max: Option<f64>,
    pub latency_sample_count: usize,
    pub degraded_reason: Option<String>,
}

impl RouteHealth {
    pub fn new(exchange: ExchangeId, route_type: RouteType, endpoint: impl Into<String>) -> Self {
        Self {
            exchange,
            route_type,
            endpoint: endpoint.into(),
            status: RouteStatus::Healthy,
            last_ok_at: None,
            last_error_at: None,
            last_message_at: None,
            consecutive_errors: 0,
            reconnect_count: 0,
            sequence_gap_count: 0,
            rate_limit_hits: 0,
            latency_ms_p50: None,
            latency_ms_p95: None,
            latency_ms_p99: None,
            latency_ms_p999: None,
            latency_ms_max: None,
            latency_sample_count: 0,
            degraded_reason: None,
        }
    }

    pub fn mark_ok(&mut self, now: DateTime<Utc>) {
        self.last_ok_at = Some(now);
        self.last_message_at = Some(now);
        self.consecutive_errors = 0;
        if self.status != RouteStatus::CloseOnly {
            self.status = RouteStatus::Healthy;
            self.degraded_reason = None;
        }
    }

    pub fn mark_error(&mut self, now: DateTime<Utc>, reason: impl Into<String>) {
        self.last_error_at = Some(now);
        self.consecutive_errors += 1;
        if self.status == RouteStatus::Healthy {
            self.status = RouteStatus::Degraded;
        }
        self.degraded_reason = Some(reason.into());
    }

    pub fn mark_offline(&mut self, reason: impl Into<String>) {
        self.status = RouteStatus::Offline;
        self.degraded_reason = Some(reason.into());
    }

    pub fn allows_new_entries(&self) -> bool {
        self.status.allows_new_entries()
    }

    pub fn allows_closes(&self) -> bool {
        self.status.allows_closes()
    }

    pub fn set_latency_summary(
        &mut self,
        sample_count: usize,
        p50_ms: Option<f64>,
        p95_ms: Option<f64>,
        p99_ms: Option<f64>,
        p999_ms: Option<f64>,
        max_ms: Option<f64>,
    ) {
        self.latency_sample_count = sample_count;
        self.latency_ms_p50 = p50_ms;
        self.latency_ms_p95 = p95_ms;
        self.latency_ms_p99 = p99_ms;
        self.latency_ms_p999 = p999_ms;
        self.latency_ms_max = max_ms;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_health_should_degrade_after_error() {
        let mut health = RouteHealth::new(ExchangeId::Binance, RouteType::MarketWs, "wss://x");
        health.mark_error(Utc::now(), "timeout");

        assert_eq!(health.status, RouteStatus::Degraded);
        assert!(!health.allows_new_entries());
        assert!(health.allows_closes());
    }

    #[test]
    fn route_health_should_expose_tail_latency_percentiles() {
        let mut health = RouteHealth::new(ExchangeId::Binance, RouteType::MarketWs, "wss://x");

        health.set_latency_summary(
            1_000,
            Some(40.0),
            Some(55.0),
            Some(75.0),
            Some(120.0),
            Some(140.0),
        );

        assert_eq!(health.latency_sample_count, 1_000);
        assert_eq!(health.latency_ms_p95, Some(55.0));
        assert_eq!(health.latency_ms_p99, Some(75.0));
        assert_eq!(health.latency_ms_p999, Some(120.0));
        assert_eq!(health.latency_ms_max, Some(140.0));
    }
}
