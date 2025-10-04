use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::{Mutex, RwLock};

use crate::core::types::MarketType;
use crate::cta::account_manager::AccountManager;
use crate::strategies::common::application::{
    deps::StrategyDeps,
    status::{RiskLevel, StrategyPosition, StrategyState, StrategyStatus},
    strategy::{Strategy, StrategyInstance},
};
use crate::strategies::common::{StrategyRiskLimits, UnifiedRiskEvaluator};

use super::notifications::RangeGridNotifier;
use super::risk;
use super::tasks::{spawn_symbol_loop, SymbolTaskParams};
use crate::strategies::range_grid::domain::config::RangeGridConfig;
use crate::strategies::range_grid::domain::model::PairRuntimeState;
use crate::strategies::range_grid::infrastructure::services::{IndicatorService, PrecisionService};
use crate::strategies::range_grid::infrastructure::websocket::RangeGridWebsocket;

pub struct RangeGridStrategy {
    config: Arc<RangeGridConfig>,
    account_manager: Arc<AccountManager>,
    risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    risk_limits: StrategyRiskLimits,
    notifier: RangeGridNotifier,
    indicator_service: IndicatorService,
    precision_service: PrecisionService,
    running: Arc<RwLock<bool>>,
    started_at: Arc<RwLock<Option<DateTime<Utc>>>>,
    last_error: Arc<RwLock<Option<String>>>,
    states: Arc<RwLock<HashMap<String, Arc<Mutex<PairRuntimeState>>>>>,
    handles: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
    ws_handles: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
    market_type: MarketType,
}

impl RangeGridStrategy {
    fn parse_market_type(value: &str) -> Result<MarketType> {
        match value.to_lowercase().as_str() {
            "futures" | "future" => Ok(MarketType::Futures),
            "spot" => Ok(MarketType::Spot),
            other => Err(anyhow!("不支持的市场类型: {}", other)),
        }
    }

    async fn ensure_states(&self) {
        let mut guard = self.states.write().await;
        for symbol_cfg in &self.config.symbols {
            guard
                .entry(symbol_cfg.config_id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(PairRuntimeState::new(symbol_cfg.clone()))));
        }
    }

    async fn cancel_all_on_startup(&self) {
        if !self.config.execution.startup_cancel_all {
            return;
        }

        for symbol_cfg in &self.config.symbols {
            if !symbol_cfg.enabled {
                continue;
            }
            if let Err(err) = self
                .account_manager
                .cancel_all_orders(&symbol_cfg.account.id, Some(&symbol_cfg.symbol))
                .await
            {
                log::warn!(
                    "[range_grid] 启动时取消 {} 挂单失败: {}",
                    symbol_cfg.config_id,
                    err
                );
            }
        }
    }

    async fn cancel_all_on_shutdown(&self) {
        if !self.config.execution.shutdown_cancel_all {
            return;
        }

        for symbol_cfg in &self.config.symbols {
            if !symbol_cfg.enabled {
                continue;
            }
            if let Err(err) = self
                .account_manager
                .cancel_all_orders(&symbol_cfg.account.id, Some(&symbol_cfg.symbol))
                .await
            {
                log::warn!(
                    "[range_grid] 停止时取消 {} 挂单失败: {}",
                    symbol_cfg.config_id,
                    err
                );
            }
        }
    }
}

#[async_trait]
impl StrategyInstance for RangeGridStrategy {
    async fn start(&self) -> Result<()> {
        if !self.config.strategy.enabled {
            log::warn!("[range_grid] 策略未启用，跳过启动");
            return Ok(());
        }

        self.ensure_states().await;
        self.cancel_all_on_startup().await;

        *self.running.write().await = true;
        *self.started_at.write().await = Some(Utc::now());
        *self.last_error.write().await = None;

        let mut account_configs: HashMap<String, HashSet<String>> = HashMap::new();
        let mut symbol_map: HashMap<String, String> = HashMap::new();
        for symbol_cfg in &self.config.symbols {
            if !symbol_cfg.enabled {
                continue;
            }
            account_configs
                .entry(symbol_cfg.account.id.clone())
                .or_insert_with(HashSet::new)
                .insert(symbol_cfg.config_id.clone());
            symbol_map.insert(
                normalize_symbol(&symbol_cfg.symbol),
                symbol_cfg.config_id.clone(),
            );
        }
        let symbol_map = Arc::new(symbol_map);

        let mut handles_guard = self.handles.write().await;
        handles_guard.clear();

        for symbol_cfg in &self.config.symbols {
            if !symbol_cfg.enabled {
                continue;
            }

            let state = {
                let guard = self.states.read().await;
                guard
                    .get(&symbol_cfg.config_id)
                    .cloned()
                    .expect("缺少运行状态")
            };

            let params = SymbolTaskParams {
                global_config: self.config.clone(),
                symbol_config: symbol_cfg.clone(),
                state,
                running: self.running.clone(),
                account_manager: self.account_manager.clone(),
                indicator_service: self.indicator_service.clone(),
                precision_service: self.precision_service.clone(),
                notifier: self.notifier.clone(),
                risk_evaluator: self.risk_evaluator.clone(),
                risk_limits: self.risk_limits.clone(),
                market_type: self.market_type,
            };

            let handle = spawn_symbol_loop(params);
            handles_guard.push(handle);
        }
        drop(handles_guard);

        let mut ws_handles = self.ws_handles.write().await;
        ws_handles.clear();
        for (account_id, configs) in account_configs {
            let allowed = Arc::new(configs);
            let handle = RangeGridWebsocket::spawn_for_account(
                account_id.clone(),
                self.account_manager.clone(),
                self.market_type,
                symbol_map.clone(),
                allowed,
                self.states.clone(),
                self.running.clone(),
            );
            ws_handles.push(handle);
        }

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        *self.running.write().await = false;

        self.cancel_all_on_shutdown().await;

        let mut handles = self.handles.write().await;
        for handle in handles.iter() {
            handle.abort();
        }
        while let Some(handle) = handles.pop() {
            if let Err(err) = handle.await {
                if err.is_cancelled() {
                    log::debug!("[range_grid] 后台任务已中断: {:?}", err);
                } else {
                    log::warn!("[range_grid] 任务结束时出现错误: {:?}", err);
                }
            }
        }

        let mut ws_handles = self.ws_handles.write().await;
        for handle in ws_handles.iter() {
            handle.abort();
        }
        while let Some(handle) = ws_handles.pop() {
            if let Err(err) = handle.await {
                if err.is_cancelled() {
                    log::debug!("[range_grid] WebSocket任务已中断: {:?}", err);
                } else {
                    log::warn!("[range_grid] WebSocket任务结束时出现错误: {:?}", err);
                }
            }
        }

        Ok(())
    }

    async fn status(&self) -> Result<StrategyStatus> {
        let running = *self.running.read().await;
        let mut status = StrategyStatus::new(self.config.strategy.name.clone());
        status.state = if running {
            StrategyState::Running
        } else {
            StrategyState::Stopped
        };

        if let Some(started_at) = *self.started_at.read().await {
            if running {
                let uptime = Utc::now().signed_duration_since(started_at);
                status.uptime = Some(uptime.to_std().unwrap_or_default());
            }
        }

        let positions = {
            let guard = self.states.read().await;
            let mut list = Vec::new();
            for state in guard.values() {
                let state_guard = state.lock().await;
                list.push(StrategyPosition {
                    symbol: state_guard.config.symbol.clone(),
                    net_position: state_guard.net_position,
                    notional: state_guard.current_price * state_guard.net_position,
                });
            }
            list
        };
        status.positions = positions;
        status.risk_level = RiskLevel::Normal;
        status.updated_at = Utc::now();
        status.last_error = self.last_error.read().await.clone();
        Ok(status)
    }
}

impl Strategy for RangeGridStrategy {
    type Config = RangeGridConfig;

    fn create(config: Self::Config, deps: StrategyDeps) -> Result<Self>
    where
        Self: Sized,
    {
        let market_type = Self::parse_market_type(&config.strategy.market_type)?;
        let config = Arc::new(config);

        let risk_limits = risk::build_limits_from_config(&config);
        let risk_evaluator = deps.risk_evaluator.clone();

        let accounts = deps.account_manager.clone();
        let indicator_service = IndicatorService::new(accounts.clone(), market_type);
        let precision_service =
            PrecisionService::new(accounts.clone(), market_type, &config.precision_management);
        let notifier = RangeGridNotifier::new(&config.notifications);

        Ok(Self {
            config,
            account_manager: accounts,
            risk_evaluator,
            risk_limits,
            notifier,
            indicator_service,
            precision_service,
            running: Arc::new(RwLock::new(false)),
            started_at: Arc::new(RwLock::new(None)),
            last_error: Arc::new(RwLock::new(None)),
            states: Arc::new(RwLock::new(HashMap::new())),
            handles: Arc::new(RwLock::new(Vec::new())),
            ws_handles: Arc::new(RwLock::new(Vec::new())),
            market_type,
        })
    }
}

fn normalize_symbol(symbol: &str) -> String {
    symbol.replace('/', "").to_uppercase()
}
