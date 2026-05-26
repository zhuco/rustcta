use crate::execution::{OrderCommandStatus, PositionSide};
use crate::market::{CanonicalSymbol, ExchangeId, ExchangeSymbol};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReconcileSeverity {
    Ok,
    MinorDrift,
    OrderDrift,
    PositionDrift,
    OrphanExposure,
    UnknownCritical,
}

impl ReconcileSeverity {
    pub fn alert_required(self) -> bool {
        matches!(
            self,
            Self::PositionDrift | Self::OrphanExposure | Self::UnknownCritical
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReconcileAction {
    None,
    AdjustLocalLedger,
    ImportMissingOrder(String),
    CancelUnknownOrder(String),
    BlockNewEntries,
    EnterCloseOnly,
    RunOrphanRecovery,
    AlertImmediately,
    ManualReview,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: Option<ExchangeSymbol>,
    pub position_side: PositionSide,
    pub quantity: f64,
    pub entry_price: Option<f64>,
    pub unrealized_pnl: Option<f64>,
    pub updated_at: DateTime<Utc>,
}

impl PositionSnapshot {
    pub fn abs_quantity(&self) -> f64 {
        self.quantity.abs()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderSnapshot {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: Option<ExchangeSymbol>,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub status: OrderCommandStatus,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub updated_at: DateTime<Utc>,
}

impl OrderSnapshot {
    pub fn is_open(&self) -> bool {
        matches!(
            self.status,
            OrderCommandStatus::Submitted
                | OrderCommandStatus::Accepted
                | OrderCommandStatus::PartiallyFilled
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReconcileReport {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub severity: ReconcileSeverity,
    pub local_position: Option<PositionSnapshot>,
    pub exchange_position: Option<PositionSnapshot>,
    pub local_open_orders: Vec<OrderSnapshot>,
    pub exchange_open_orders: Vec<OrderSnapshot>,
    pub detected_drift: Option<String>,
    pub recommended_actions: Vec<ReconcileAction>,
    pub auto_action_taken: Option<ReconcileAction>,
    pub alert_required: bool,
    pub checked_at: DateTime<Utc>,
}

impl ReconcileReport {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        exchange: ExchangeId,
        canonical_symbol: CanonicalSymbol,
        severity: ReconcileSeverity,
        local_position: Option<PositionSnapshot>,
        exchange_position: Option<PositionSnapshot>,
        local_open_orders: Vec<OrderSnapshot>,
        exchange_open_orders: Vec<OrderSnapshot>,
        detected_drift: Option<String>,
        recommended_actions: Vec<ReconcileAction>,
        auto_action_taken: Option<ReconcileAction>,
        checked_at: DateTime<Utc>,
    ) -> Self {
        Self {
            exchange,
            canonical_symbol,
            severity,
            local_position,
            exchange_position,
            local_open_orders,
            exchange_open_orders,
            detected_drift,
            recommended_actions,
            auto_action_taken,
            alert_required: severity.alert_required(),
            checked_at,
        }
    }
}

pub fn classify_position_drift(
    local_position: Option<&PositionSnapshot>,
    exchange_position: Option<&PositionSnapshot>,
    quantity_tolerance: f64,
    orphan_tolerance: f64,
) -> ReconcileSeverity {
    match (local_position, exchange_position) {
        (None, None) => ReconcileSeverity::Ok,
        (Some(local), Some(exchange)) => {
            let drift = (local.quantity - exchange.quantity).abs();
            if drift <= quantity_tolerance {
                ReconcileSeverity::Ok
            } else if local.abs_quantity() <= orphan_tolerance
                && exchange.abs_quantity() > orphan_tolerance
            {
                ReconcileSeverity::OrphanExposure
            } else if exchange.abs_quantity() <= orphan_tolerance
                && local.abs_quantity() > orphan_tolerance
            {
                ReconcileSeverity::PositionDrift
            } else if drift <= orphan_tolerance {
                ReconcileSeverity::MinorDrift
            } else {
                ReconcileSeverity::PositionDrift
            }
        }
        (None, Some(exchange)) if exchange.abs_quantity() > orphan_tolerance => {
            ReconcileSeverity::OrphanExposure
        }
        (Some(local), None) if local.abs_quantity() > orphan_tolerance => {
            ReconcileSeverity::PositionDrift
        }
        _ => ReconcileSeverity::MinorDrift,
    }
}

pub fn recommended_actions_for_severity(severity: ReconcileSeverity) -> Vec<ReconcileAction> {
    match severity {
        ReconcileSeverity::Ok => vec![ReconcileAction::None],
        ReconcileSeverity::MinorDrift => vec![ReconcileAction::AdjustLocalLedger],
        ReconcileSeverity::OrderDrift => {
            vec![
                ReconcileAction::EnterCloseOnly,
                ReconcileAction::ManualReview,
            ]
        }
        ReconcileSeverity::PositionDrift => vec![
            ReconcileAction::BlockNewEntries,
            ReconcileAction::EnterCloseOnly,
            ReconcileAction::ManualReview,
        ],
        ReconcileSeverity::OrphanExposure => vec![
            ReconcileAction::BlockNewEntries,
            ReconcileAction::EnterCloseOnly,
            ReconcileAction::RunOrphanRecovery,
            ReconcileAction::AlertImmediately,
        ],
        ReconcileSeverity::UnknownCritical => vec![
            ReconcileAction::BlockNewEntries,
            ReconcileAction::EnterCloseOnly,
            ReconcileAction::AlertImmediately,
            ReconcileAction::ManualReview,
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pos(qty: f64) -> PositionSnapshot {
        PositionSnapshot {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("btc", "usdt"),
            exchange_symbol: None,
            position_side: PositionSide::Long,
            quantity: qty,
            entry_price: Some(100.0),
            unrealized_pnl: None,
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn reconciler_should_classify_position_drift() {
        assert_eq!(
            classify_position_drift(Some(&pos(1.0)), Some(&pos(1.00001)), 0.001, 0.01),
            ReconcileSeverity::Ok
        );
        assert_eq!(
            classify_position_drift(Some(&pos(1.0)), Some(&pos(1.02)), 0.001, 0.05),
            ReconcileSeverity::MinorDrift
        );
        assert_eq!(
            classify_position_drift(Some(&pos(1.0)), Some(&pos(1.2)), 0.001, 0.05),
            ReconcileSeverity::PositionDrift
        );
        assert_eq!(
            classify_position_drift(None, Some(&pos(1.0)), 0.001, 0.05),
            ReconcileSeverity::OrphanExposure
        );
    }
}
