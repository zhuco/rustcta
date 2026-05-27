use crate::execution::{
    classify_position_drift, recommended_actions_for_severity, ExchangeBalance, ExchangeErrorClass,
    ExchangePosition, FillEvent, OrderCommandStatus, OrderSnapshot, OrderState, PositionSide,
    PositionSnapshot, RateLimitState, ReconcileReport,
};
use crate::market::{CanonicalSymbol, ExchangeId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateEvent {
    pub exchange: ExchangeId,
    pub account_id: Option<String>,
    pub event_id: Option<String>,
    pub exchange_sequence: Option<u64>,
    pub transaction_time: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub kind: PrivateEventKind,
    pub raw: Option<serde_json::Value>,
}

impl PrivateEvent {
    pub fn new(exchange: ExchangeId, kind: PrivateEventKind, received_at: DateTime<Utc>) -> Self {
        Self {
            exchange,
            account_id: None,
            event_id: None,
            exchange_sequence: None,
            transaction_time: None,
            received_at,
            kind,
            raw: None,
        }
    }

    pub fn order(order: OrderState, received_at: DateTime<Utc>) -> Self {
        Self::new(
            order.exchange.clone(),
            PrivateEventKind::Order(order),
            received_at,
        )
    }

    pub fn fill(fill: FillEvent, received_at: DateTime<Utc>) -> Self {
        Self::new(
            fill.exchange.clone(),
            PrivateEventKind::Fill(fill),
            received_at,
        )
    }

    pub fn position(position: ExchangePosition, received_at: DateTime<Utc>) -> Self {
        Self::new(
            position.exchange.clone(),
            PrivateEventKind::Position(position),
            received_at,
        )
    }

    pub fn balance(balance: ExchangeBalance, received_at: DateTime<Utc>) -> Self {
        Self::new(
            balance.exchange.clone(),
            PrivateEventKind::Balance(balance),
            received_at,
        )
    }

    pub fn with_account_id(mut self, account_id: impl Into<String>) -> Self {
        self.account_id = Some(account_id.into());
        self
    }

    pub fn with_event_id(mut self, event_id: impl Into<String>) -> Self {
        self.event_id = Some(event_id.into());
        self
    }

    pub fn with_exchange_sequence(mut self, exchange_sequence: u64) -> Self {
        self.exchange_sequence = Some(exchange_sequence);
        self
    }

    pub fn with_raw(mut self, raw: serde_json::Value) -> Self {
        self.raw = Some(raw);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PrivateEventKind {
    Order(OrderState),
    Fill(FillEvent),
    Position(ExchangePosition),
    Balance(ExchangeBalance),
    FundingSettlement {
        canonical_symbol: CanonicalSymbol,
        position_side: PositionSide,
        funding_pnl_usdt: f64,
        funding_rate: f64,
        settled_at: DateTime<Utc>,
    },
    Error(PrivateErrorEvent),
    RateLimit(RateLimitState),
    Heartbeat,
    StreamDisconnected {
        reason: Option<String>,
        disconnected_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateErrorEvent {
    pub class: ExchangeErrorClass,
    pub endpoint: Option<String>,
    pub code: Option<String>,
    pub message: String,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub retry_after_ms: Option<u64>,
    pub occurred_at: DateTime<Utc>,
}

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
    use crate::execution::{FillLiquidity, OrderSide, OrderType, TimeInForce};
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

    fn order_state(status: OrderCommandStatus, updated_at: DateTime<Utc>) -> OrderState {
        OrderState {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("exchange-1".to_string()),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            order_type: OrderType::Limit,
            quantity: 0.1,
            price: Some(65000.0),
            filled_quantity: 0.05,
            average_fill_price: Some(65000.0),
            time_in_force: TimeInForce::PostOnly,
            reduce_only: false,
            status,
            updated_at,
        }
    }

    fn fill_event(filled_at: DateTime<Utc>) -> FillEvent {
        FillEvent {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            trade_id: "trade-1".to_string(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("exchange-1".to_string()),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            liquidity: FillLiquidity::Maker,
            price: 65000.0,
            quantity: 0.05,
            quote_quantity: 3250.0,
            fee: Some(0.65),
            fee_asset: Some("USDT".to_string()),
            fee_rate: Some(0.0002),
            realized_pnl: Some(0.0),
            reduce_only: Some(false),
            filled_at,
            received_at: filled_at,
        }
    }

    fn exchange_position(updated_at: DateTime<Utc>) -> ExchangePosition {
        ExchangePosition {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "BTCUSDT"),
            position_side: PositionSide::Long,
            quantity: 0.05,
            entry_price: Some(65000.0),
            mark_price: Some(65100.0),
            unrealized_pnl: Some(5.0),
            updated_at,
        }
    }

    fn balance(updated_at: DateTime<Utc>) -> ExchangeBalance {
        ExchangeBalance {
            exchange: ExchangeId::Binance,
            asset: "USDT".to_string(),
            total: 10_000.0,
            available: 8_000.0,
            locked: 2_000.0,
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

    #[test]
    fn private_event_should_represent_order_fill_position_and_balance_updates() {
        let now = Utc::now();

        let order_event =
            PrivateEvent::order(order_state(OrderCommandStatus::PartiallyFilled, now), now)
                .with_account_id("arb-main")
                .with_event_id("evt-order-1")
                .with_exchange_sequence(42);
        let fill_event = PrivateEvent::fill(fill_event(now), now);
        let position_event = PrivateEvent::position(exchange_position(now), now);
        let balance_event = PrivateEvent::balance(balance(now), now);

        assert_eq!(order_event.exchange, ExchangeId::Binance);
        assert_eq!(order_event.account_id.as_deref(), Some("arb-main"));
        assert_eq!(order_event.event_id.as_deref(), Some("evt-order-1"));
        assert_eq!(order_event.exchange_sequence, Some(42));

        match order_event.kind {
            PrivateEventKind::Order(order) => {
                assert_eq!(order.status, OrderCommandStatus::PartiallyFilled);
                assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
            }
            other => panic!("expected order event, got {other:?}"),
        }

        match fill_event.kind {
            PrivateEventKind::Fill(fill) => {
                assert_eq!(fill.trade_id, "trade-1");
                assert_eq!(fill.notional(), 3250.0);
                assert_eq!(fill.liquidity.is_maker(), Some(true));
            }
            other => panic!("expected fill event, got {other:?}"),
        }

        match position_event.kind {
            PrivateEventKind::Position(position) => {
                assert_eq!(position.position_side, PositionSide::Long);
                assert_eq!(position.quantity, 0.05);
            }
            other => panic!("expected position event, got {other:?}"),
        }

        match balance_event.kind {
            PrivateEventKind::Balance(balance) => {
                assert_eq!(balance.asset, "USDT");
                assert_eq!(balance.available, 8_000.0);
                assert_eq!(balance.locked, 2_000.0);
            }
            other => panic!("expected balance event, got {other:?}"),
        }
    }
}
