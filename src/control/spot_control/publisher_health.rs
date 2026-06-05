use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::RuntimePublisherLimitsConfig;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuntimePublisherHealth {
    pub running: bool,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_snapshot_at: Option<DateTime<Utc>>,
    pub snapshots_generated: u64,
    pub snapshots_persisted: u64,
    pub snapshot_persist_failures: u64,
    pub per_exchange_health: BTreeMap<String, RuntimePublisherExchangeHealth>,
    #[serde(default)]
    pub last_critical_error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuntimePublisherExchangeHealth {
    pub exchange: String,
    #[serde(default)]
    pub last_balance_poll: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_open_orders_poll: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_recent_fills_poll: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_symbol_rules_refresh: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_fee_refresh: Option<DateTime<Utc>>,
    #[serde(default)]
    pub request_latency_ms: Option<i64>,
    pub request_failures: u64,
    pub rate_limit_events: u64,
    #[serde(default)]
    pub backoff_until: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_successful_request: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuntimePublisherStartupReport {
    pub loaded_snapshot_count: usize,
    pub restored_symbols: Vec<String>,
    pub fresh_exchanges: Vec<String>,
    pub stale_exchanges: Vec<String>,
    pub unknown_orders: Vec<String>,
    pub balance_mismatches: Vec<String>,
    pub ownership_changes: Vec<String>,
    pub write_actions_unblocked: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimePublisherHealthHandle {
    inner: Arc<RwLock<RuntimePublisherHealth>>,
}

impl RuntimePublisherHealthHandle {
    pub async fn snapshot(&self) -> RuntimePublisherHealth {
        self.inner.read().await.clone()
    }

    pub async fn mark_running(&self) {
        let mut guard = self.inner.write().await;
        guard.running = true;
        guard.started_at.get_or_insert_with(Utc::now);
    }

    pub async fn mark_stopped(&self) {
        self.inner.write().await.running = false;
    }

    pub async fn mark_snapshot_generated(&self) {
        let mut guard = self.inner.write().await;
        guard.snapshots_generated += 1;
        guard.last_snapshot_at = Some(Utc::now());
    }

    pub async fn mark_snapshot_persisted(&self) {
        self.inner.write().await.snapshots_persisted += 1;
    }

    pub async fn mark_snapshot_persist_failed(&self, error: impl Into<String>) {
        let mut guard = self.inner.write().await;
        guard.snapshot_persist_failures += 1;
        guard.last_critical_error = Some(error.into());
    }

    pub async fn mark_success(
        &self,
        exchange: &str,
        component: RuntimePollComponent,
        latency_ms: i64,
    ) {
        let mut guard = self.inner.write().await;
        let now = Utc::now();
        let entry = guard
            .per_exchange_health
            .entry(normalize_exchange(exchange))
            .or_insert_with(|| RuntimePublisherExchangeHealth {
                exchange: normalize_exchange(exchange),
                ..RuntimePublisherExchangeHealth::default()
            });
        match component {
            RuntimePollComponent::Balances => entry.last_balance_poll = Some(now),
            RuntimePollComponent::OpenOrders => entry.last_open_orders_poll = Some(now),
            RuntimePollComponent::RecentFills => entry.last_recent_fills_poll = Some(now),
            RuntimePollComponent::SymbolRules => entry.last_symbol_rules_refresh = Some(now),
            RuntimePollComponent::Fees => entry.last_fee_refresh = Some(now),
        }
        entry.request_latency_ms = Some(latency_ms);
        entry.last_successful_request = Some(now);
        entry.last_error = None;
    }

    pub async fn mark_failure(
        &self,
        exchange: &str,
        component: RuntimePollComponent,
        error: impl Into<String>,
        rate_limited: bool,
        limits: &RuntimePublisherLimitsConfig,
    ) {
        let mut guard = self.inner.write().await;
        let error = error.into();
        let entry = guard
            .per_exchange_health
            .entry(normalize_exchange(exchange))
            .or_insert_with(|| RuntimePublisherExchangeHealth {
                exchange: normalize_exchange(exchange),
                ..RuntimePublisherExchangeHealth::default()
            });
        entry.request_failures += 1;
        entry.last_error = Some(format!("{component:?}: {error}"));
        if rate_limited {
            entry.rate_limit_events += 1;
            entry.backoff_until = Some(
                Utc::now()
                    + chrono::Duration::milliseconds(limits.pause_after_rate_limit_ms as i64),
            );
        } else {
            let backoff = (limits.exponential_backoff_initial_ms
                * 2_u64.saturating_pow(entry.request_failures.min(16) as u32))
            .min(limits.exponential_backoff_max_ms);
            entry.backoff_until = Some(Utc::now() + chrono::Duration::milliseconds(backoff as i64));
        }
        guard.last_critical_error = Some(format!("{} {component:?}: {error}", exchange));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RuntimePollComponent {
    Balances,
    OpenOrders,
    RecentFills,
    SymbolRules,
    Fees,
}

fn normalize_exchange(exchange: &str) -> String {
    exchange.trim().to_ascii_lowercase()
}
