use crate::strategies::common::snapshot::{
    StrategyPerformanceSnapshot, StrategyRiskLimits, StrategySnapshot,
};
use crate::strategies::common::{RiskLevel, StrategyExposureSnapshot};

use super::exposure::PortfolioExposure;
use super::inventory::HedgeInventoryLedger;
use super::BetaHedgeMarketMakerConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HedgePhase {
    BuildLong,
    ReduceLong,
    BuildShort,
    ReduceShort,
    Flat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtectionState {
    Normal,
    SoftLimit,
    HardLimit,
    Emergency,
}

#[derive(Debug, Clone)]
pub struct ProtectionDecision {
    pub phase: HedgePhase,
    pub state: ProtectionState,
    pub same_side_scale: f64,
    pub reason: String,
}

impl ProtectionDecision {
    pub fn risk_level(&self) -> RiskLevel {
        match self.state {
            ProtectionState::Normal => RiskLevel::Normal,
            ProtectionState::SoftLimit => RiskLevel::Warning,
            ProtectionState::HardLimit => RiskLevel::Danger,
            ProtectionState::Emergency => RiskLevel::Halt,
        }
    }
}

pub fn evaluate_protection(
    config: &BetaHedgeMarketMakerConfig,
    exposure: &PortfolioExposure,
    inventory: &HedgeInventoryLedger,
) -> ProtectionDecision {
    let target = exposure.hedge_target_notional_usd;
    let gross = inventory.gross_notional_usd.abs();
    let phase = determine_phase(target, inventory);

    if gross >= config.hedge.emergency_inventory_notional {
        return ProtectionDecision {
            phase,
            state: ProtectionState::Emergency,
            same_side_scale: 0.0,
            reason: format!(
                "gross hedge inventory {:.2} exceeds emergency limit {:.2}",
                gross, config.hedge.emergency_inventory_notional
            ),
        };
    }

    if gross >= config.hedge.hard_inventory_notional {
        return ProtectionDecision {
            phase,
            state: ProtectionState::HardLimit,
            same_side_scale: 0.0,
            reason: format!(
                "gross hedge inventory {:.2} exceeds hard limit {:.2}",
                gross, config.hedge.hard_inventory_notional
            ),
        };
    }

    if gross >= config.hedge.soft_inventory_notional {
        let range =
            (config.hedge.hard_inventory_notional - config.hedge.soft_inventory_notional).max(1.0);
        let progress = ((gross - config.hedge.soft_inventory_notional) / range).clamp(0.0, 1.0);
        let same_side_scale = 0.5 - progress * 0.3;
        return ProtectionDecision {
            phase,
            state: ProtectionState::SoftLimit,
            same_side_scale,
            reason: format!(
                "gross hedge inventory {:.2} exceeds soft limit {:.2}",
                gross, config.hedge.soft_inventory_notional
            ),
        };
    }

    ProtectionDecision {
        phase,
        state: ProtectionState::Normal,
        same_side_scale: 1.0,
        reason: "normal".to_string(),
    }
}

pub fn determine_phase(target_notional: f64, inventory: &HedgeInventoryLedger) -> HedgePhase {
    const EPS: f64 = 1e-6;

    let gap_notional = target_notional - inventory.net_notional_usd;

    if target_notional > EPS {
        if inventory.short_leg.quantity > EPS {
            HedgePhase::ReduceShort
        } else if gap_notional < -EPS && inventory.long_leg.quantity > EPS {
            HedgePhase::ReduceLong
        } else {
            HedgePhase::BuildLong
        }
    } else if target_notional < -EPS {
        if inventory.long_leg.quantity > EPS {
            HedgePhase::ReduceLong
        } else if gap_notional > EPS && inventory.short_leg.quantity > EPS {
            HedgePhase::ReduceShort
        } else {
            HedgePhase::BuildShort
        }
    } else if inventory.long_leg.quantity > EPS {
        HedgePhase::ReduceLong
    } else if inventory.short_leg.quantity > EPS {
        HedgePhase::ReduceShort
    } else {
        HedgePhase::Flat
    }
}

pub fn build_strategy_limits(config: &BetaHedgeMarketMakerConfig) -> StrategyRiskLimits {
    StrategyRiskLimits {
        warning_scale_factor: Some(0.7),
        danger_scale_factor: Some(0.3),
        stop_loss_pct: None,
        max_inventory_notional: Some(config.hedge.emergency_inventory_notional),
        max_daily_loss: None,
        max_consecutive_losses: None,
        inventory_skew_limit: Some(
            config.hedge.soft_inventory_notional / config.hedge.emergency_inventory_notional,
        ),
        max_unrealized_loss: None,
    }
}

pub fn build_strategy_snapshot(
    config: &BetaHedgeMarketMakerConfig,
    exposure: &PortfolioExposure,
    inventory: &HedgeInventoryLedger,
) -> StrategySnapshot {
    let mut snapshot = StrategySnapshot::new(config.strategy.name.clone());
    snapshot.exposure = StrategyExposureSnapshot {
        notional: inventory.net_notional_usd,
        net_inventory: inventory.net_quantity,
        long_position: inventory.long_leg.quantity,
        short_position: inventory.short_leg.quantity,
        inventory_ratio: Some(
            inventory.gross_notional_usd / config.hedge.emergency_inventory_notional.max(1.0),
        ),
    };
    snapshot.performance = StrategyPerformanceSnapshot {
        realized_pnl: inventory.realized_pnl,
        unrealized_pnl: inventory.unrealized_pnl,
        drawdown_pct: None,
        daily_pnl: None,
        consecutive_losses: None,
        timestamp: exposure.measured_at,
    };
    snapshot.risk_limits = Some(build_strategy_limits(config));
    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::MarketType;
    use crate::strategies::beta_hedge_market_maker::{
        BetaHedgeMarketMakerConfig, HedgeConfig, QuotingConfig, StrategyAccount, StrategyMeta,
    };

    fn config() -> BetaHedgeMarketMakerConfig {
        BetaHedgeMarketMakerConfig {
            strategy: StrategyMeta {
                name: "beta_hedge".to_string(),
                log_level: None,
            },
            account: StrategyAccount {
                account_id: "binance_main".to_string(),
                market_type: MarketType::Futures,
            },
            hedge: HedgeConfig {
                hedge_symbol: "DOGE/USDC".to_string(),
                hedge_ratio: 0.4,
                target_inventory_notional_cap: 650.0,
                soft_inventory_notional: 400.0,
                hard_inventory_notional: 650.0,
                emergency_inventory_notional: 850.0,
                max_portfolio_exposure_multiple: 10.0,
            },
            quoting: QuotingConfig::default(),
            dry_run: true,
        }
    }

    #[test]
    fn soft_limit_scales_same_side_add_orders() {
        let exposure = PortfolioExposure {
            hedge_target_notional_usd: 500.0,
            ..PortfolioExposure::default()
        };
        let inventory = HedgeInventoryLedger {
            gross_notional_usd: 500.0,
            ..HedgeInventoryLedger::default()
        };
        let decision = evaluate_protection(&config(), &exposure, &inventory);
        assert_eq!(decision.state, ProtectionState::SoftLimit);
        assert!(decision.same_side_scale <= 0.5);
        assert!(decision.same_side_scale >= 0.2);
    }

    #[test]
    fn phase_unwinds_opposite_leg_before_opening_new_direction() {
        let inventory = HedgeInventoryLedger {
            short_leg: super::super::inventory::HedgeLegInventory {
                quantity: 200.0,
                entry_price: 0.1,
                notional_usd: 20.0,
            },
            ..HedgeInventoryLedger::default()
        };
        assert_eq!(determine_phase(100.0, &inventory), HedgePhase::ReduceShort);
    }

    #[test]
    fn phase_reduces_long_when_positive_target_is_exceeded() {
        let inventory = HedgeInventoryLedger {
            long_leg: super::super::inventory::HedgeLegInventory {
                quantity: 8_000.0,
                entry_price: 0.1,
                notional_usd: 800.0,
            },
            net_quantity: 8_000.0,
            net_notional_usd: 800.0,
            gross_notional_usd: 800.0,
            mark_price: 0.1,
            ..HedgeInventoryLedger::default()
        };

        assert_eq!(determine_phase(623.34, &inventory), HedgePhase::ReduceLong);
    }

    #[test]
    fn phase_reduces_short_when_negative_target_is_exceeded() {
        let inventory = HedgeInventoryLedger {
            short_leg: super::super::inventory::HedgeLegInventory {
                quantity: 8_000.0,
                entry_price: 0.1,
                notional_usd: 800.0,
            },
            net_quantity: -8_000.0,
            net_notional_usd: -800.0,
            gross_notional_usd: 800.0,
            mark_price: 0.1,
            ..HedgeInventoryLedger::default()
        };

        assert_eq!(
            determine_phase(-623.34, &inventory),
            HedgePhase::ReduceShort
        );
    }
}
