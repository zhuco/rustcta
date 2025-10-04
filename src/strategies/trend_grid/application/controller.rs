//! # 趋势网格策略 (Trend Grid Trading Strategy V2)
//!
//! 结合趋势跟踪和网格交易的混合策略，在趋势中进行网格交易。
//!
//! ## 主要功能
//! - 自动识别市场趋势方向
//! - 在趋势方向上布置网格订单
//! - 动态调整网格间距和数量
//! - WebSocket实时订单管理

use super::risk;
use super::tasks;
use crate::strategies::trend_grid::domain::config::*;
use crate::strategies::trend_grid::domain::state::ConfigState;

use crate::analysis::TradeCollector;
use crate::core::error::ExchangeError;
use crate::cta::account_manager::AccountManager;
use crate::strategies::common::{
    build_unified_risk_evaluator, RiskLevel, Strategy, StrategyDeps, StrategyInstance,
    StrategyPosition, StrategyRiskLimits, StrategyState, StrategyStatus, UnifiedRiskEvaluator,
};
use anyhow::Result as AnyResult;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// 趋势网格策略V2
pub struct TrendGridStrategyV2 {
    config: TrendGridConfigV2,
    account_manager: Arc<AccountManager>,
    config_states: Arc<RwLock<HashMap<String, Arc<Mutex<ConfigState>>>>>,
    running: Arc<RwLock<bool>>,
    collector: Option<Arc<TradeCollector>>,
    risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    risk_limits: StrategyRiskLimits,
    started_at: Arc<RwLock<Option<DateTime<Utc>>>>,
    last_error: Arc<RwLock<Option<String>>>,
}

impl TrendGridStrategyV2 {
    /// 创建策略实例
    pub fn new(config: TrendGridConfigV2, account_manager: Arc<AccountManager>) -> Self {
        let risk_limits = risk::build_limits_from_config(&config);
        let risk_evaluator = build_unified_risk_evaluator(
            config.strategy.name.clone(),
            None,
            Some(risk_limits.clone()),
        );

        let deps = StrategyDeps::builder()
            .with_account_manager(account_manager.clone())
            .with_risk_evaluator(risk_evaluator.clone())
            .build()
            .expect("TrendGridStrategyV2 依赖构建失败");

        Self::new_with_deps(config, deps, risk_limits)
    }

    /// 创建策略实例（带数据收集器）
    pub fn with_collector(
        config: TrendGridConfigV2,
        account_manager: Arc<AccountManager>,
        collector: Arc<TradeCollector>,
    ) -> Self {
        let risk_limits = risk::build_limits_from_config(&config);
        let risk_evaluator = build_unified_risk_evaluator(
            config.strategy.name.clone(),
            None,
            Some(risk_limits.clone()),
        );

        let deps = StrategyDeps::builder()
            .with_account_manager(account_manager.clone())
            .with_risk_evaluator(risk_evaluator.clone())
            .with_trade_collector(collector.clone())
            .build()
            .expect("TrendGridStrategyV2 依赖构建失败");

        Self::new_with_deps(config, deps, risk_limits)
    }

    /// 使用注入依赖创建策略实例
    pub fn new_with_deps(
        config: TrendGridConfigV2,
        deps: StrategyDeps,
        risk_limits: StrategyRiskLimits,
    ) -> Self {
        let _ = std::fs::create_dir_all("logs/strategies");

        Self {
            config,
            account_manager: deps.account_manager.clone(),
            config_states: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(RwLock::new(false)),
            collector: deps.trade_collector.clone(),
            risk_evaluator: deps.risk_evaluator.clone(),
            risk_limits,
            started_at: Arc::new(RwLock::new(None)),
            last_error: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn evaluate_risk_for_config(&self, config_id: &str) -> Result<()> {
        let states = self.config_states.read().await;
        let Some(state_arc) = states.get(config_id) else {
            return Ok(());
        };
        let state_arc = state_arc.clone();
        drop(states);

        let snapshot = {
            let guard = state_arc.lock().await;
            risk::build_risk_snapshot(
                format!("trend_grid::{}", config_id),
                &guard,
                &self.risk_limits,
                self.config.risk_control.position_limit_per_symbol,
            )
        };

        let decision = risk::evaluate_risk(&self.risk_evaluator, &snapshot).await;
        risk::apply_risk_decision(config_id, decision, &state_arc, &self.running).await
    }

    /// 启动策略
    pub async fn start(&self) -> Result<()> {
        self.start_internal().await
    }

    async fn start_internal(&self) -> Result<()> {
        *self.last_error.write().await = None;
        *self.started_at.write().await = Some(Utc::now());
        log::info!("🚀 启动趋势网格策略");

        let result: Result<()> = async {
            *self.running.write().await = true;

            if self.config.execution.startup_cancel_all {
                self.cancel_all_orders().await?;
            }

            for trading_config in &self.config.trading_configs {
                if !trading_config.enabled {
                    log::info!("⏭️ 跳过禁用配置: {}", trading_config.config_id);
                    continue;
                }

                if self.config.execution.thread_per_config {
                    let handle = tasks::spawn_config_task(
                        trading_config.clone(),
                        self.account_manager.clone(),
                        self.config_states.clone(),
                        self.running.clone(),
                        self.config.trend_adjustment.clone(),
                        self.config.batch_settings.clone(),
                        self.config.websocket.clone(),
                        self.config.logging.clone(),
                        self.config.grid_management.clone(),
                        self.collector.clone(),
                    );
                    let _ = handle;
                } else {
                    if let Err(e) = tasks::run_config_task_once(
                        trading_config.clone(),
                        self.account_manager.clone(),
                        self.config_states.clone(),
                        self.running.clone(),
                        self.config.trend_adjustment.clone(),
                        self.config.batch_settings.clone(),
                        self.config.websocket.clone(),
                        self.config.logging.clone(),
                        self.config.grid_management.clone(),
                        self.collector.clone(),
                    )
                    .await
                    {
                        log::error!("❌ 配置 {} 运行错误: {}", trading_config.config_id, e);
                    }
                }
            }

            let _ = tasks::spawn_grid_check_task(
                self.config.grid_management.check_interval,
                self.config.grid_management.show_grid_status,
                self.config_states.clone(),
                self.running.clone(),
                self.account_manager.clone(),
                self.config.trading_configs.clone(),
                self.config.batch_settings.clone(),
                self.config.trend_adjustment.clone(),
                self.config.grid_management.clone(),
                self.risk_evaluator.clone(),
                self.risk_limits.clone(),
                self.config.risk_control.position_limit_per_symbol,
            );

            let _ = tasks::spawn_trend_monitoring_task(
                self.config_states.clone(),
                self.running.clone(),
                self.account_manager.clone(),
                self.config.trading_configs.clone(),
                self.config.batch_settings.clone(),
                self.config.trend_adjustment.clone(),
                self.config.grid_management.clone(),
                self.risk_evaluator.clone(),
                self.risk_limits.clone(),
                self.config.risk_control.position_limit_per_symbol,
            );

            Ok(())
        }
        .await;

        if let Err(err) = &result {
            *self.last_error.write().await = Some(err.to_string());
            *self.running.write().await = false;
        }

        result
    }

    /// 停止策略
    pub async fn stop(&self) -> Result<()> {
        self.stop_internal().await
    }

    async fn stop_internal(&self) -> Result<()> {
        let result: Result<()> = async {
            *self.running.write().await = false;

            if self.config.execution.shutdown_cancel_all {
                self.cancel_all_orders().await?;
            }

            Ok(())
        }
        .await;

        if let Err(err) = &result {
            *self.last_error.write().await = Some(err.to_string());
        }

        result
    }

    /// 查询策略状态快照
    pub async fn current_status(&self) -> StrategyStatus {
        let mut status = StrategyStatus::new(self.config.strategy.name.clone());

        let running = *self.running.read().await;
        let last_error = { self.last_error.read().await.clone() };
        let started_at = { self.started_at.read().await.clone() };

        let state = if running {
            StrategyState::Running
        } else if last_error.is_some() {
            StrategyState::Error
        } else {
            StrategyState::Stopped
        };

        status = status.with_state(state);

        if let Some(started_at) = started_at {
            if let Ok(uptime) = (Utc::now() - started_at).to_std() {
                status = status.with_uptime(uptime);
            }
        }

        if let Some(err) = last_error.clone() {
            status = status.with_last_error(err);
        }

        let risk_level = if last_error.is_some() {
            RiskLevel::Danger
        } else {
            RiskLevel::Normal
        };

        status = status.with_risk_level(risk_level);

        let positions = {
            let states = self.config_states.read().await;
            let mut positions = Vec::with_capacity(states.len());
            for state in states.values() {
                let guard = state.lock().await;
                positions.push(StrategyPosition {
                    symbol: guard.config.symbol.clone(),
                    net_position: guard.net_position,
                    notional: guard.net_position * guard.current_price,
                });
            }
            positions
        };

        status = status.with_positions(positions);

        status
    }

    /// 取消所有订单
    pub async fn cancel_all_orders(&self) -> Result<()> {
        for config in &self.config.trading_configs {
            if !config.enabled {
                continue;
            }

            match self
                .account_manager
                .cancel_all_orders(&config.account.id, Some(&config.symbol))
                .await
            {
                Ok(cancelled) => {
                    log::info!("✅ {} 取消了 {} 个订单", config.config_id, cancelled.len());
                }
                Err(e) => {
                    log::error!("❌ {} 取消订单失败: {}", config.config_id, e);
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl StrategyInstance for TrendGridStrategyV2 {
    async fn start(&self) -> AnyResult<()> {
        self.start_internal().await.map_err(Into::into)
    }

    async fn stop(&self) -> AnyResult<()> {
        self.stop_internal().await.map_err(Into::into)
    }

    async fn status(&self) -> AnyResult<StrategyStatus> {
        Ok(self.current_status().await)
    }
}

impl Strategy for TrendGridStrategyV2 {
    type Config = TrendGridConfigV2;

    fn create(config: Self::Config, deps: StrategyDeps) -> AnyResult<Self> {
        let risk_limits = risk::build_limits_from_config(&config);
        Ok(Self::new_with_deps(config, deps, risk_limits))
    }
}

type Result<T> = std::result::Result<T, ExchangeError>;
