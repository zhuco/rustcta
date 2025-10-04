use std::sync::Arc;

use chrono::Utc;
use tokio::sync::{Mutex, RwLock};

use crate::strategies::common::{
    RiskAction, RiskDecision, RiskNotifyLevel, StrategyRiskLimits, StrategySnapshot,
    UnifiedRiskEvaluator,
};
use crate::strategies::trend_grid::domain::config::TrendGridConfigV2;
use crate::strategies::trend_grid::domain::state::ConfigState;

use crate::core::error::ExchangeError;

pub(crate) fn build_limits_from_config(config: &TrendGridConfigV2) -> StrategyRiskLimits {
    StrategyRiskLimits {
        warning_scale_factor: Some(0.85),
        danger_scale_factor: Some(0.6),
        stop_loss_pct: Some(config.risk_control.max_drawdown),
        max_inventory_notional: Some(config.risk_control.position_limit_per_symbol),
        max_daily_loss: Some(config.risk_control.daily_loss_limit),
        max_consecutive_losses: None,
        inventory_skew_limit: None,
        max_unrealized_loss: None,
    }
}

pub(crate) fn build_risk_snapshot(
    name: String,
    state: &ConfigState,
    risk_limits: &StrategyRiskLimits,
    position_limit: f64,
) -> StrategySnapshot {
    let mut snapshot = StrategySnapshot::new(name);
    let notional = state.net_position * state.current_price;
    snapshot.exposure.notional = notional;
    snapshot.exposure.net_inventory = state.net_position;
    if position_limit > 0.0 {
        snapshot.exposure.inventory_ratio = Some((notional.abs() / position_limit).min(10.0));
    }

    snapshot.performance.realized_pnl = state.realized_pnl;
    snapshot.performance.unrealized_pnl = state.unrealized_pnl;
    snapshot.performance.daily_pnl = Some(state.pnl);
    snapshot.performance.timestamp = Utc::now();
    snapshot.risk_limits = Some(risk_limits.clone());
    snapshot
}

pub(crate) async fn evaluate_risk(
    risk_evaluator: &Arc<dyn UnifiedRiskEvaluator>,
    snapshot: &StrategySnapshot,
) -> RiskDecision {
    risk_evaluator.evaluate(snapshot).await
}

pub(crate) async fn apply_risk_decision(
    config_id: &str,
    decision: RiskDecision,
    state: &Arc<Mutex<ConfigState>>,
    running: &Arc<RwLock<bool>>,
) -> Result<(), ExchangeError> {
    match decision.action {
        RiskAction::None => Ok(()),
        RiskAction::Notify { level, message } => {
            match level {
                RiskNotifyLevel::Info => log::info!("ℹ️ [{}] 风险提示: {}", config_id, message),
                RiskNotifyLevel::Warning => {
                    log::warn!("⚠️ [{}] 风险警告: {}", config_id, message)
                }
                RiskNotifyLevel::Danger => {
                    log::error!("❗ [{}] 风险危险: {}", config_id, message)
                }
            }
            Ok(())
        }
        RiskAction::ScaleDown {
            scale_factor,
            reason,
        } => {
            log::warn!(
                "⚠️ [{}] 风险缩减触发: {} (scale={:.2})",
                config_id,
                reason,
                scale_factor
            );
            let mut guard = state.lock().await;
            guard.need_grid_reset = true;
            Ok(())
        }
        RiskAction::Halt { reason } => {
            log::error!("❌ [{}] 风险停机: {}", config_id, reason);
            *running.write().await = false;
            Ok(())
        }
    }
}
