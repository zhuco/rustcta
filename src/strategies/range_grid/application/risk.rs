use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{Mutex, RwLock};

use crate::strategies::common::{
    RiskAction, RiskDecision, RiskNotifyLevel, StrategyRiskLimits, StrategySnapshot,
    UnifiedRiskEvaluator,
};

use crate::strategies::range_grid::domain::config::RangeGridConfig;
use crate::strategies::range_grid::domain::model::PairRuntimeState;

pub fn build_limits_from_config(config: &RangeGridConfig) -> StrategyRiskLimits {
    StrategyRiskLimits {
        warning_scale_factor: Some(0.85),
        danger_scale_factor: Some(0.6),
        stop_loss_pct: Some(config.risk_control.max_drawdown),
        max_inventory_notional: Some(config.risk_control.position_limit_per_symbol),
        max_daily_loss: Some(config.risk_control.daily_loss_limit),
        max_consecutive_losses: None,
        inventory_skew_limit: None,
        max_unrealized_loss: Some(config.risk_control.max_unrealized_loss_per_symbol),
    }
}

pub fn build_snapshot(
    strategy_name: &str,
    state: &PairRuntimeState,
    limits: &StrategyRiskLimits,
) -> StrategySnapshot {
    let mut snapshot = StrategySnapshot::new(strategy_name);
    snapshot.exposure.notional = state.current_price * state.net_position;
    snapshot.exposure.net_inventory = state.net_position;
    snapshot.performance.realized_pnl = state.realized_pnl;
    snapshot.performance.unrealized_pnl = state.unrealized_pnl;
    snapshot.performance.daily_pnl = Some(state.realized_pnl + state.unrealized_pnl);
    snapshot.performance.timestamp = chrono::Utc::now();
    snapshot.risk_limits = Some(limits.clone());
    snapshot
}

pub async fn evaluate_risk(
    evaluator: &Arc<dyn UnifiedRiskEvaluator>,
    snapshot: &StrategySnapshot,
) -> RiskDecision {
    evaluator.evaluate(snapshot).await
}

pub async fn apply_risk_decision(
    state: &Arc<Mutex<PairRuntimeState>>,
    running: &Arc<RwLock<bool>>,
    decision: RiskDecision,
) -> Result<()> {
    match decision.action {
        RiskAction::None => {}
        RiskAction::Notify { level, message } => match level {
            RiskNotifyLevel::Info => log::info!("ℹ️ 风控提示: {}", message),
            RiskNotifyLevel::Warning => log::warn!("⚠️ 风控警告: {}", message),
            RiskNotifyLevel::Danger => log::error!("❗ 风控危险: {}", message),
        },
        RiskAction::ScaleDown {
            scale_factor,
            reason,
        } => {
            log::warn!("⚠️ 建议缩减仓位: {} (scale={:.2})", reason, scale_factor);
            let mut guard = state.lock().await;
            guard.need_rebuild = true;
        }
        RiskAction::Halt { reason } => {
            log::error!("⛔ 风控停机: {}", reason);
            *running.write().await = false;
            let mut guard = state.lock().await;
            guard.deactivate_grid();
        }
    }

    Ok(())
}
