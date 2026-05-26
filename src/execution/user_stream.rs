use crate::execution::{
    classify_position_drift, recommended_actions_for_severity, OrderCommandStatus, OrderSnapshot,
    PositionSide, PositionSnapshot, ReconcileReport,
};
use crate::market::{CanonicalSymbol, ExchangeId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PrivateStreamEvent {
    OrderUpdate(OrderSnapshot),
    PositionUpdate(PositionSnapshot),
    FundingSettlement {
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        position_side: PositionSide,
        funding_pnl_usdt: f64,
        funding_rate: f64,
        settled_at: DateTime<Utc>,
    },
    Heartbeat {
        exchange: ExchangeId,
        received_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountSyncConfig {
    pub quantity_tolerance: f64,
    pub orphan_tolerance: f64,
    pub stale_after_ms: i64,
}

impl Default for AccountSyncConfig {
    fn default() -> Self {
        Self {
            quantity_tolerance: 1e-8,
            orphan_tolerance: 1e-6,
            stale_after_ms: 10_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountSyncState {
    config: AccountSyncConfig,
    positions: HashMap<PositionKey, PositionSnapshot>,
    open_orders: HashMap<OrderKey, OrderSnapshot>,
    last_event_at: HashMap<ExchangeId, DateTime<Utc>>,
    funding_events: Vec<PrivateStreamEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct PositionKey {
    exchange: ExchangeId,
    canonical_symbol: CanonicalSymbol,
    position_side: PositionSide,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct OrderKey {
    exchange: ExchangeId,
    canonical_symbol: CanonicalSymbol,
    id: String,
}

impl AccountSyncState {
    pub fn new(config: AccountSyncConfig) -> Self {
        Self {
            config,
            positions: HashMap::new(),
            open_orders: HashMap::new(),
            last_event_at: HashMap::new(),
            funding_events: Vec::new(),
        }
    }

    pub fn apply_event(&mut self, event: PrivateStreamEvent) {
        match event {
            PrivateStreamEvent::OrderUpdate(order) => {
                self.last_event_at
                    .insert(order.exchange.clone(), order.updated_at);
                let key = OrderKey::from_snapshot(&order);
                if order.is_open() {
                    self.open_orders.insert(key, order);
                } else {
                    self.open_orders.remove(&key);
                }
            }
            PrivateStreamEvent::PositionUpdate(position) => {
                self.last_event_at
                    .insert(position.exchange.clone(), position.updated_at);
                self.positions
                    .insert(PositionKey::from_snapshot(&position), position);
            }
            PrivateStreamEvent::FundingSettlement { ref exchange, .. } => {
                self.last_event_at.insert(exchange.clone(), Utc::now());
                self.funding_events.push(event);
            }
            PrivateStreamEvent::Heartbeat {
                exchange,
                received_at,
            } => {
                self.last_event_at.insert(exchange, received_at);
            }
        }
    }

    pub fn position(
        &self,
        exchange: &ExchangeId,
        symbol: &CanonicalSymbol,
        side: PositionSide,
    ) -> Option<&PositionSnapshot> {
        self.positions.get(&PositionKey {
            exchange: exchange.clone(),
            canonical_symbol: symbol.clone(),
            position_side: side,
        })
    }

    pub fn open_orders(&self) -> Vec<&OrderSnapshot> {
        self.open_orders.values().collect()
    }

    pub fn is_stale(&self, exchange: &ExchangeId, now: DateTime<Utc>) -> bool {
        self.last_event_at
            .get(exchange)
            .map(|last| {
                now.signed_duration_since(*last).num_milliseconds() > self.config.stale_after_ms
            })
            .unwrap_or(true)
    }

    pub fn reconcile_position(
        &self,
        local_position: Option<&PositionSnapshot>,
        exchange_position: Option<&PositionSnapshot>,
        exchange: ExchangeId,
        symbol: CanonicalSymbol,
        checked_at: DateTime<Utc>,
    ) -> ReconcileReport {
        let severity = classify_position_drift(
            local_position,
            exchange_position,
            self.config.quantity_tolerance,
            self.config.orphan_tolerance,
        );
        ReconcileReport::new(
            exchange,
            symbol,
            severity,
            local_position.cloned(),
            exchange_position.cloned(),
            Vec::new(),
            Vec::new(),
            (severity != crate::execution::ReconcileSeverity::Ok)
                .then(|| format!("private stream position drift: {:?}", severity)),
            recommended_actions_for_severity(severity),
            None,
            checked_at,
        )
    }
}

impl Default for AccountSyncState {
    fn default() -> Self {
        Self::new(AccountSyncConfig::default())
    }
}

impl PositionKey {
    fn from_snapshot(snapshot: &PositionSnapshot) -> Self {
        Self {
            exchange: snapshot.exchange.clone(),
            canonical_symbol: snapshot.canonical_symbol.clone(),
            position_side: snapshot.position_side,
        }
    }
}

impl OrderKey {
    fn from_snapshot(snapshot: &OrderSnapshot) -> Self {
        let id = snapshot
            .client_order_id
            .clone()
            .or_else(|| snapshot.exchange_order_id.clone())
            .unwrap_or_else(|| format!("unknown-{}", snapshot.updated_at.timestamp_millis()));
        Self {
            exchange: snapshot.exchange.clone(),
            canonical_symbol: snapshot.canonical_symbol.clone(),
            id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::ExchangeSymbol;

    fn position(qty: f64, updated_at: DateTime<Utc>) -> PositionSnapshot {
        PositionSnapshot {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: Some(ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT")),
            position_side: PositionSide::Long,
            quantity: qty,
            entry_price: Some(65000.0),
            unrealized_pnl: Some(1.0),
            updated_at,
        }
    }

    fn order(status: OrderCommandStatus, updated_at: DateTime<Utc>) -> OrderSnapshot {
        OrderSnapshot {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: Some(ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT")),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("exchange-1".to_string()),
            status,
            quantity: 1.0,
            filled_quantity: 0.0,
            updated_at,
        }
    }

    #[test]
    fn account_sync_should_apply_order_and_position_updates() {
        let now = Utc::now();
        let mut state = AccountSyncState::default();

        state.apply_event(PrivateStreamEvent::OrderUpdate(order(
            OrderCommandStatus::Accepted,
            now,
        )));
        assert_eq!(state.open_orders().len(), 1);

        state.apply_event(PrivateStreamEvent::OrderUpdate(order(
            OrderCommandStatus::Filled,
            now,
        )));
        assert!(state.open_orders().is_empty());

        state.apply_event(PrivateStreamEvent::PositionUpdate(position(0.1, now)));
        assert_eq!(
            state
                .position(
                    &ExchangeId::Binance,
                    &CanonicalSymbol::new("BTC", "USDT"),
                    PositionSide::Long
                )
                .expect("position")
                .quantity,
            0.1
        );
    }

    #[test]
    fn account_sync_should_reconcile_private_stream_position() {
        let now = Utc::now();
        let state = AccountSyncState::new(AccountSyncConfig {
            quantity_tolerance: 0.001,
            orphan_tolerance: 0.01,
            stale_after_ms: 10_000,
        });

        let report = state.reconcile_position(
            Some(&position(0.1, now)),
            Some(&position(0.2, now)),
            ExchangeId::Binance,
            CanonicalSymbol::new("BTC", "USDT"),
            now,
        );

        assert!(report.alert_required);
        assert!(report
            .recommended_actions
            .contains(&crate::execution::ReconcileAction::EnterCloseOnly));
    }
}
