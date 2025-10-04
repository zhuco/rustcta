use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::core::risk_manager::{GlobalRiskManager, RiskStatus};

use super::snapshot::{StrategyRiskLimits, StrategySnapshot};

/// 动作枚举：策略层可据此做减仓、停策略等操作
#[derive(Debug, Clone, PartialEq)]
pub enum RiskAction {
    /// 无需动作
    None,
    /// 建议减仓，scale_factor 为剩余仓位比例（0.0~1.0）
    ScaleDown { scale_factor: f64, reason: String },
    /// 立即停止策略
    Halt { reason: String },
    /// 仅告警
    Notify {
        level: RiskNotifyLevel,
        message: String,
    },
}

/// 告警级别
#[derive(Debug, Clone, PartialEq)]
pub enum RiskNotifyLevel {
    Info,
    Warning,
    Danger,
}

/// 风控评估结果
#[derive(Debug, Clone, PartialEq)]
pub struct RiskDecision {
    pub action: RiskAction,
    pub status: RiskStatus,
    pub checked_at: DateTime<Utc>,
}

impl RiskDecision {
    pub fn none(status: RiskStatus) -> Self {
        Self {
            action: RiskAction::None,
            status,
            checked_at: Utc::now(),
        }
    }
}

/// 统一风控接口
#[async_trait]
pub trait UnifiedRiskEvaluator: Send + Sync {
    async fn evaluate(&self, snapshot: &StrategySnapshot) -> RiskDecision;
}

/// 默认实现：桥接到 core::risk_manager::GlobalRiskManager
pub struct GlobalRiskAdapter {
    strategy_name: String,
    global_risk: Arc<GlobalRiskManager>,
}

impl GlobalRiskAdapter {
    pub fn new(strategy_name: impl Into<String>, global_risk: Arc<GlobalRiskManager>) -> Self {
        Self {
            strategy_name: strategy_name.into(),
            global_risk,
        }
    }
}

#[async_trait]
impl UnifiedRiskEvaluator for GlobalRiskAdapter {
    async fn evaluate(&self, snapshot: &StrategySnapshot) -> RiskDecision {
        let perf = &snapshot.performance;
        let exposure = &snapshot.exposure;

        self.global_risk
            .update_strategy_risk(
                self.strategy_name.clone(),
                exposure.notional,
                perf.unrealized_pnl,
                perf.realized_pnl,
            )
            .await;

        let status = self.global_risk.check_risk().await;

        let action = match status {
            RiskStatus::Normal => RiskAction::None,
            RiskStatus::Warning => {
                let factor = snapshot
                    .risk_limits
                    .as_ref()
                    .and_then(|limits| limits.warning_scale_factor)
                    .unwrap_or(0.8);
                RiskAction::ScaleDown {
                    scale_factor: factor,
                    reason: "达到风险警告阈值".to_string(),
                }
            }
            RiskStatus::Danger => {
                let factor = snapshot
                    .risk_limits
                    .as_ref()
                    .and_then(|limits| limits.danger_scale_factor)
                    .unwrap_or(0.5);
                RiskAction::ScaleDown {
                    scale_factor: factor,
                    reason: "达到风险危险阈值".to_string(),
                }
            }
            RiskStatus::StopAll => RiskAction::Halt {
                reason: "触发全局风险停止".to_string(),
            },
        };

        RiskDecision {
            action,
            status,
            checked_at: Utc::now(),
        }
    }
}

/// 基于策略自身阈值的轻量风控，用于未配置全局风控时的兜底
pub struct LocalRiskEvaluator {
    strategy_name: String,
}

impl LocalRiskEvaluator {
    pub fn new(strategy_name: impl Into<String>) -> Self {
        Self {
            strategy_name: strategy_name.into(),
        }
    }
}

#[async_trait]
impl UnifiedRiskEvaluator for LocalRiskEvaluator {
    async fn evaluate(&self, snapshot: &StrategySnapshot) -> RiskDecision {
        let now = Utc::now();
        if let Some(limits) = &snapshot.risk_limits {
            if let Some(stop_loss) = limits.stop_loss_pct {
                if snapshot.performance.drawdown_pct.unwrap_or(0.0) <= -stop_loss {
                    return RiskDecision {
                        action: RiskAction::Halt {
                            reason: format!(
                                "{} 触发本地止损 {:.2}%",
                                self.strategy_name,
                                stop_loss * 100.0
                            ),
                        },
                        status: RiskStatus::Danger,
                        checked_at: now,
                    };
                }
            }

            if let Some(max_inventory) = limits.max_inventory_notional {
                if snapshot.exposure.notional.abs() > max_inventory {
                    return RiskDecision {
                        action: RiskAction::ScaleDown {
                            scale_factor: limits.danger_scale_factor.unwrap_or(0.5),
                            reason: format!(
                                "{} 仓位超过本地限额 {:.2} > {:.2}",
                                self.strategy_name, snapshot.exposure.notional, max_inventory
                            ),
                        },
                        status: RiskStatus::Warning,
                        checked_at: now,
                    };
                }
            }

            if let Some(inventory_ratio) = snapshot.exposure.inventory_ratio {
                if let Some(limit) = limits.inventory_skew_limit {
                    if inventory_ratio > limit {
                        return RiskDecision {
                            action: RiskAction::ScaleDown {
                                scale_factor: limits.danger_scale_factor.unwrap_or(0.5),
                                reason: format!(
                                    "{} 库存偏斜比例 {:.2}% 超过 {:.2}%",
                                    self.strategy_name,
                                    inventory_ratio * 100.0,
                                    limit * 100.0
                                ),
                            },
                            status: RiskStatus::Warning,
                            checked_at: now,
                        };
                    }
                }
            }

            if let Some(max_unrealized_loss) = limits.max_unrealized_loss {
                if snapshot.performance.unrealized_pnl < -max_unrealized_loss {
                    return RiskDecision {
                        action: RiskAction::ScaleDown {
                            scale_factor: limits.danger_scale_factor.unwrap_or(0.5),
                            reason: format!(
                                "{} 未实现亏损 {:.2} 超过限制 {:.2}",
                                self.strategy_name,
                                snapshot.performance.unrealized_pnl,
                                max_unrealized_loss
                            ),
                        },
                        status: RiskStatus::Warning,
                        checked_at: now,
                    };
                }
            }

            if let Some(max_daily_loss) = limits.max_daily_loss {
                if let Some(daily_pnl) = snapshot.performance.daily_pnl {
                    if daily_pnl < -max_daily_loss {
                        return RiskDecision {
                            action: RiskAction::Halt {
                                reason: format!(
                                    "{} 触发本地日亏损限制 {:.2} < -{:.2}",
                                    self.strategy_name, daily_pnl, max_daily_loss
                                ),
                            },
                            status: RiskStatus::Danger,
                            checked_at: now,
                        };
                    }
                }
            }

            if let Some(max_losses) = limits.max_consecutive_losses {
                if let Some(losses) = snapshot.performance.consecutive_losses {
                    if losses >= max_losses {
                        return RiskDecision {
                            action: RiskAction::Halt {
                                reason: format!(
                                    "{} 连续亏损 {} 次触发限制 {}",
                                    self.strategy_name, losses, max_losses
                                ),
                            },
                            status: RiskStatus::Danger,
                            checked_at: now,
                        };
                    }
                }
            }
        }

        RiskDecision::none(RiskStatus::Normal)
    }
}

/// 组合多个 evaluator，按顺序返回第一个需要动作的决策
pub struct CompositeRiskEvaluator {
    evaluators: Vec<Arc<dyn UnifiedRiskEvaluator>>,
}

impl CompositeRiskEvaluator {
    pub fn new(evaluators: Vec<Arc<dyn UnifiedRiskEvaluator>>) -> Self {
        Self { evaluators }
    }
}

#[async_trait]
impl UnifiedRiskEvaluator for CompositeRiskEvaluator {
    async fn evaluate(&self, snapshot: &StrategySnapshot) -> RiskDecision {
        let mut last_decision = RiskDecision::none(RiskStatus::Normal);
        for evaluator in &self.evaluators {
            let decision = evaluator.evaluate(snapshot).await;
            if !matches!(decision.action, RiskAction::None) {
                return decision;
            }
            last_decision = decision;
        }
        last_decision
    }
}

/// 根据配置选择合适的 evaluator
pub fn build_unified_risk_evaluator(
    strategy_name: impl Into<String>,
    global: Option<Arc<GlobalRiskManager>>,
    limits: Option<StrategyRiskLimits>,
) -> Arc<dyn UnifiedRiskEvaluator> {
    let strategy_name = strategy_name.into();

    let mut evaluators: Vec<Arc<dyn UnifiedRiskEvaluator>> = Vec::new();

    if let Some(global_risk) = global {
        evaluators.push(Arc::new(GlobalRiskAdapter::new(
            strategy_name.clone(),
            global_risk,
        )));
    }

    if limits.is_some() {
        evaluators.push(Arc::new(LocalRiskEvaluator::new(strategy_name.clone())));
    }

    if evaluators.is_empty() {
        evaluators.push(Arc::new(LocalRiskEvaluator::new(strategy_name)));
    }

    Arc::new(CompositeRiskEvaluator::new(evaluators))
}
