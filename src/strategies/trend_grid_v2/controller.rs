//! # 趋势网格策略 (Trend Grid Trading Strategy V2)
//!
//! 结合趋势跟踪和网格交易的混合策略，在趋势中进行网格交易。
//!
//! ## 主要功能
//! - 自动识别市场趋势方向
//! - 在趋势方向上布置网格订单
//! - 动态调整网格间距和数量
//! - WebSocket实时订单管理

use super::config::*;
use super::risk;
use super::state::ConfigState;
use super::tasks;

use crate::analysis::TradeCollector;
use crate::core::error::ExchangeError;
use crate::cta::account_manager::AccountManager;
use crate::strategies::common::{
    build_unified_risk_evaluator, StrategyRiskLimits, UnifiedRiskEvaluator,
};
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
}

impl TrendGridStrategyV2 {
    /// 创建策略实例
    pub fn new(config: TrendGridConfigV2, account_manager: Arc<AccountManager>) -> Self {
        // 创建日志目录
        let _ = std::fs::create_dir_all("logs/strategies");

        let risk_limits = risk::build_limits_from_config(&config);
        let risk_evaluator = build_unified_risk_evaluator(
            config.strategy.name.clone(),
            None,
            Some(risk_limits.clone()),
        );

        Self {
            config,
            account_manager,
            config_states: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(RwLock::new(false)),
            collector: None,
            risk_evaluator,
            risk_limits,
        }
    }

    /// 创建策略实例（带数据收集器）
    pub fn with_collector(
        config: TrendGridConfigV2,
        account_manager: Arc<AccountManager>,
        collector: Arc<TradeCollector>,
    ) -> Self {
        // 创建日志目录
        let _ = std::fs::create_dir_all("logs/strategies");

        let risk_limits = risk::build_limits_from_config(&config);
        let risk_evaluator = build_unified_risk_evaluator(
            config.strategy.name.clone(),
            None,
            Some(risk_limits.clone()),
        );

        Self {
            config,
            account_manager,
            config_states: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(RwLock::new(false)),
            collector: Some(collector),
            risk_evaluator,
            risk_limits,
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
        log::info!("🚀 启动趋势网格策略");

        *self.running.write().await = true;

        // 启动时取消所有订单
        if self.config.execution.startup_cancel_all {
            self.cancel_all_orders().await?;
        }

        // 为每个启用的交易配置创建独立线程
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

        // 启动网格检查任务（每2分钟）
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

        // 策略启动完成
        Ok(())
    }

    /// 停止策略
    pub async fn stop(&self) -> Result<()> {
        // 停止策略

        *self.running.write().await = false;

        // 停止时取消所有订单
        if self.config.execution.shutdown_cancel_all {
            self.cancel_all_orders().await?;
        }

        // 策略已停止
        Ok(())
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

type Result<T> = std::result::Result<T, ExchangeError>;
