use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::exchanges::unified::MarketType;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExchangeMarketHealth {
    pub exchange: String,
    pub market_type: Option<MarketType>,
    pub connected: bool,
    pub last_message_at: Option<DateTime<Utc>>,
    pub last_book_update_at: Option<DateTime<Utc>>,
    pub stale_symbols: Vec<String>,
    pub fresh_symbols: Vec<String>,
    pub reconnect_count: u64,
    pub parse_error_count: u64,
    pub sequence_gap_count: u64,
    pub heartbeat_timeout_count: u64,
    pub avg_latency_ms: Option<f64>,
    pub max_latency_ms: Option<i64>,
    pub recorder_dropped_events: u64,
}

#[derive(Debug, Clone, Default)]
pub struct BookHealth {
    inner: Arc<RwLock<HashMap<String, ExchangeMarketHealth>>>,
}

impl BookHealth {
    pub async fn mark_connected(&self, exchange: &str, market_type: MarketType) {
        let mut guard = self.inner.write().await;
        let health = guard
            .entry(normalize_exchange(exchange))
            .or_insert_with(|| new_health(exchange, market_type));
        health.connected = true;
        health.market_type = Some(market_type);
    }

    pub async fn mark_disconnected(&self, exchange: &str, reason: &str) {
        let mut guard = self.inner.write().await;
        let health = guard
            .entry(normalize_exchange(exchange))
            .or_insert_with(|| new_health(exchange, MarketType::Spot));
        health.connected = false;
        log::warn!(
            "market websocket disconnected exchange={} reason={}",
            exchange,
            reason
        );
    }

    pub async fn mark_message(
        &self,
        exchange: &str,
        market_type: MarketType,
        symbol: &str,
        latency_ms: Option<i64>,
        now: DateTime<Utc>,
    ) {
        let mut guard = self.inner.write().await;
        let health = guard
            .entry(normalize_exchange(exchange))
            .or_insert_with(|| new_health(exchange, market_type));
        health.connected = true;
        health.market_type = Some(market_type);
        health.last_message_at = Some(now);
        health.last_book_update_at = Some(now);
        insert_unique(&mut health.fresh_symbols, symbol);
        health.stale_symbols.retain(|item| item != symbol);
        if let Some(latency) = latency_ms {
            health.max_latency_ms = Some(health.max_latency_ms.unwrap_or(latency).max(latency));
            health.avg_latency_ms = Some(match health.avg_latency_ms {
                Some(avg) => avg * 0.9 + latency as f64 * 0.1,
                None => latency as f64,
            });
        }
    }

    pub async fn mark_symbol_stale(&self, exchange: &str, market_type: MarketType, symbol: &str) {
        let mut guard = self.inner.write().await;
        let health = guard
            .entry(normalize_exchange(exchange))
            .or_insert_with(|| new_health(exchange, market_type));
        health.market_type = Some(market_type);
        insert_unique(&mut health.stale_symbols, symbol);
        health.fresh_symbols.retain(|item| item != symbol);
    }

    pub async fn mark_reconnect(&self, exchange: &str, market_type: MarketType) {
        let mut guard = self.inner.write().await;
        let health = guard
            .entry(normalize_exchange(exchange))
            .or_insert_with(|| new_health(exchange, market_type));
        health.reconnect_count += 1;
        health.connected = false;
    }

    pub async fn mark_parse_error(&self, exchange: &str, market_type: MarketType) {
        let mut guard = self.inner.write().await;
        let health = guard
            .entry(normalize_exchange(exchange))
            .or_insert_with(|| new_health(exchange, market_type));
        health.parse_error_count += 1;
    }

    pub async fn mark_sequence_gap(&self, exchange: &str, market_type: MarketType) {
        let mut guard = self.inner.write().await;
        let health = guard
            .entry(normalize_exchange(exchange))
            .or_insert_with(|| new_health(exchange, market_type));
        health.sequence_gap_count += 1;
    }

    pub async fn mark_heartbeat_timeout(&self, exchange: &str, market_type: MarketType) {
        let mut guard = self.inner.write().await;
        let health = guard
            .entry(normalize_exchange(exchange))
            .or_insert_with(|| new_health(exchange, market_type));
        health.heartbeat_timeout_count += 1;
        health.connected = false;
    }

    pub async fn set_recorder_dropped_events(&self, exchange: &str, dropped: u64) {
        let mut guard = self.inner.write().await;
        let health = guard
            .entry(normalize_exchange(exchange))
            .or_insert_with(|| new_health(exchange, MarketType::Spot));
        health.recorder_dropped_events = dropped;
    }

    pub async fn snapshot(&self) -> Vec<ExchangeMarketHealth> {
        let mut values = self
            .inner
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        values.sort_by(|left, right| left.exchange.cmp(&right.exchange));
        values
    }
}

fn new_health(exchange: &str, market_type: MarketType) -> ExchangeMarketHealth {
    ExchangeMarketHealth {
        exchange: normalize_exchange(exchange),
        market_type: Some(market_type),
        ..ExchangeMarketHealth::default()
    }
}

fn insert_unique(values: &mut Vec<String>, value: &str) {
    let mut set = values.iter().cloned().collect::<HashSet<_>>();
    let normalized = value.trim().to_ascii_uppercase();
    if set.insert(normalized.clone()) {
        values.push(normalized);
        values.sort();
    }
}

fn normalize_exchange(exchange: &str) -> String {
    exchange.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn book_health_should_track_heartbeat_timeout_and_stale_symbols() {
        let health = BookHealth::default();
        health
            .mark_message("mexc", MarketType::Spot, "BTCUSDT", Some(2), Utc::now())
            .await;
        health
            .mark_heartbeat_timeout("mexc", MarketType::Spot)
            .await;
        health
            .mark_symbol_stale("mexc", MarketType::Spot, "BTCUSDT")
            .await;

        let snapshot = health.snapshot().await;
        assert_eq!(snapshot[0].heartbeat_timeout_count, 1);
        assert_eq!(snapshot[0].stale_symbols, vec!["BTCUSDT".to_string()]);
    }
}
