use crate::execution::{
    BundleLeg, OrderCommand, OrderIntent, OrderSide, OrderType, OrphanRecoveryAction,
    OrphanRecoveryRecommendation, PositionSide, TimeInForce,
};
use crate::market::{CanonicalSymbol, ExchangeId, ExchangeSymbol, RuntimeMode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MakerFill {
    pub bundle_id: String,
    pub mode: RuntimeMode,
    pub canonical_symbol: CanonicalSymbol,
    pub taker_exchange: ExchangeId,
    pub taker_exchange_symbol: ExchangeSymbol,
    pub maker_side: OrderSide,
    pub filled_quantity: f64,
    pub hedge_price: Option<f64>,
    pub max_slippage_pct: Option<f64>,
    pub filled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrphanRecoveryContext {
    pub mode: RuntimeMode,
    pub canonical_symbol: CanonicalSymbol,
    pub recovery_exchange: ExchangeId,
    pub recovery_exchange_symbol: ExchangeSymbol,
    pub exposed_position_side: PositionSide,
    pub price: Option<f64>,
    pub max_slippage_pct: Option<f64>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct HedgePlanner;

impl HedgePlanner {
    pub fn hedge_for_maker_fill(fill: &MakerFill) -> Option<OrderCommand> {
        if fill.filled_quantity <= 0.0 || !fill.filled_quantity.is_finite() {
            return None;
        }

        let (intent, side, position_side) = match fill.maker_side {
            OrderSide::Sell => (
                OrderIntent::HedgeLongTaker,
                OrderSide::Buy,
                PositionSide::Long,
            ),
            OrderSide::Buy => (
                OrderIntent::HedgeShortTaker,
                OrderSide::Sell,
                PositionSide::Short,
            ),
        };

        Some(OrderCommand::new(
            fill.mode,
            fill.bundle_id.clone(),
            BundleLeg::Hedge,
            1,
            fill.taker_exchange.clone(),
            fill.canonical_symbol.clone(),
            fill.taker_exchange_symbol.clone(),
            intent,
            side,
            position_side,
            OrderType::Market,
            fill.filled_quantity,
            fill.hedge_price,
            TimeInForce::Ioc,
            false,
            false,
            fill.max_slippage_pct,
            fill.filled_at,
        ))
    }

    pub fn command_for_orphan_recovery(
        recommendation: &OrphanRecoveryRecommendation,
        context: &OrphanRecoveryContext,
    ) -> Option<OrderCommand> {
        if recommendation.unhedged_qty <= 0.0 || !recommendation.unhedged_qty.is_finite() {
            return None;
        }

        match recommendation.action {
            OrphanRecoveryAction::RetryHedgeWithWiderSlippage { leg, attempt } => {
                let (intent, side, position_side) =
                    hedge_fields_for_exposure(context.exposed_position_side);
                Some(OrderCommand::new(
                    context.mode,
                    recommendation.bundle_id.clone(),
                    leg,
                    attempt,
                    context.recovery_exchange.clone(),
                    context.canonical_symbol.clone(),
                    context.recovery_exchange_symbol.clone(),
                    intent,
                    side,
                    position_side,
                    OrderType::Market,
                    recommendation.unhedged_qty,
                    context.price,
                    TimeInForce::Ioc,
                    false,
                    false,
                    context.max_slippage_pct.map(|slippage| slippage * 1.5),
                    context.created_at,
                ))
            }
            OrphanRecoveryAction::ReverseMakerLegWithTaker { leg } => {
                let (intent, side, position_side) =
                    emergency_close_fields_for_exposure(context.exposed_position_side);
                Some(OrderCommand::new(
                    context.mode,
                    recommendation.bundle_id.clone(),
                    leg,
                    1,
                    context.recovery_exchange.clone(),
                    context.canonical_symbol.clone(),
                    context.recovery_exchange_symbol.clone(),
                    intent,
                    side,
                    position_side,
                    OrderType::Market,
                    recommendation.unhedged_qty,
                    context.price,
                    TimeInForce::Ioc,
                    false,
                    true,
                    context.max_slippage_pct,
                    context.created_at,
                ))
            }
            OrphanRecoveryAction::EnterCloseOnlyAndAlert
            | OrphanRecoveryAction::ManualIntervention => None,
        }
    }
}

fn hedge_fields_for_exposure(exposed_side: PositionSide) -> (OrderIntent, OrderSide, PositionSide) {
    match exposed_side {
        PositionSide::Long => (
            OrderIntent::HedgeShortTaker,
            OrderSide::Sell,
            PositionSide::Short,
        ),
        PositionSide::Short => (
            OrderIntent::HedgeLongTaker,
            OrderSide::Buy,
            PositionSide::Long,
        ),
        PositionSide::Net => (
            OrderIntent::HedgeShortTaker,
            OrderSide::Sell,
            PositionSide::Net,
        ),
    }
}

fn emergency_close_fields_for_exposure(
    exposed_side: PositionSide,
) -> (OrderIntent, OrderSide, PositionSide) {
    match exposed_side {
        PositionSide::Long => (
            OrderIntent::EmergencyCloseLongTaker,
            OrderSide::Sell,
            PositionSide::Long,
        ),
        PositionSide::Short => (
            OrderIntent::EmergencyCloseShortTaker,
            OrderSide::Buy,
            PositionSide::Short,
        ),
        PositionSide::Net => (
            OrderIntent::EmergencyCloseLongTaker,
            OrderSide::Sell,
            PositionSide::Net,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::BundleLeg;
    use crate::market::RouteStatus;

    #[test]
    fn hedge_should_create_taker_command_for_maker_fill() {
        let now = Utc::now();
        let command = HedgePlanner::hedge_for_maker_fill(&MakerFill {
            bundle_id: "bundle-1".to_string(),
            mode: RuntimeMode::Simulation,
            canonical_symbol: CanonicalSymbol::new("btc", "usdt"),
            taker_exchange: ExchangeId::Okx,
            taker_exchange_symbol: ExchangeSymbol::new(ExchangeId::Okx, "BTC-USDT-SWAP"),
            maker_side: OrderSide::Sell,
            filled_quantity: 0.25,
            hedge_price: Some(100.0),
            max_slippage_pct: Some(0.001),
            filled_at: now,
        })
        .unwrap();

        assert_eq!(command.intent, OrderIntent::HedgeLongTaker);
        assert_eq!(command.side, OrderSide::Buy);
        assert_eq!(command.time_in_force, TimeInForce::Ioc);
        assert_eq!(command.quantity, 0.25);
    }

    #[test]
    fn hedge_should_create_reverse_command_for_orphan_recovery() {
        let now = Utc::now();
        let recommendation = OrphanRecoveryRecommendation {
            bundle_id: "bundle-2".to_string(),
            action: OrphanRecoveryAction::ReverseMakerLegWithTaker {
                leg: BundleLeg::EmergencyCloseLong,
            },
            unhedged_qty: 1.0,
            route_status: RouteStatus::Healthy,
            alert_required: true,
            reason: "reverse".to_string(),
            recommended_at: now,
        };

        let command = HedgePlanner::command_for_orphan_recovery(
            &recommendation,
            &OrphanRecoveryContext {
                mode: RuntimeMode::Simulation,
                canonical_symbol: CanonicalSymbol::new("eth", "usdt"),
                recovery_exchange: ExchangeId::Binance,
                recovery_exchange_symbol: ExchangeSymbol::new(ExchangeId::Binance, "ETHUSDT"),
                exposed_position_side: PositionSide::Long,
                price: None,
                max_slippage_pct: Some(0.002),
                created_at: now,
            },
        )
        .unwrap();

        assert_eq!(command.intent, OrderIntent::EmergencyCloseLongTaker);
        assert_eq!(command.side, OrderSide::Sell);
        assert!(command.reduce_only);
    }
}
