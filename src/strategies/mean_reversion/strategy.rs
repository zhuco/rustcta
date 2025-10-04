use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};

use crate::core::exchange::Exchange;
use crate::core::types::MarketType;
use crate::cta::account_manager::{AccountInfo, AccountManager};
use crate::strategies::common::{
    Strategy, StrategyDeps, StrategyInstance, StrategyState, StrategyStatus, UnifiedRiskEvaluator,
};

use super::config::MeanReversionConfig;
use super::logging;
use super::model::{PerformanceTracker, SymbolMeta, SymbolState};
use super::utils::CancelRateLimiter;

#[derive(Clone)]
pub struct MeanReversionStrategy {
    pub(super) config: MeanReversionConfig,
    pub(super) account_manager: Arc<AccountManager>,
    pub(super) account: Arc<AccountInfo>,
    pub(super) exchange: Arc<Box<dyn Exchange>>,
    pub(super) market_type: MarketType,
    pub(super) risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    pub(super) running: Arc<RwLock<bool>>,
    pub(super) status: Arc<RwLock<StrategyStatus>>,
    pub(super) symbol_states: Arc<RwLock<HashMap<String, SymbolState>>>,
    pub(super) performance: Arc<RwLock<PerformanceTracker>>,
    pub(super) symbol_meta: Arc<RwLock<HashMap<String, SymbolMeta>>>,
    pub(super) symbol_aliases: Arc<RwLock<HashMap<String, String>>>,
    pub(super) task_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    pub(super) cancel_limiter: Arc<Mutex<CancelRateLimiter>>,
}

impl MeanReversionStrategy {
    fn new(config: MeanReversionConfig, deps: StrategyDeps) -> Result<Self> {
        let account_manager = deps.account_manager.clone();
        let account = account_manager
            .get_account(&config.account.account_id)
            .ok_or_else(|| anyhow!("account {} not found", config.account.account_id))?;

        let mut symbol_states = HashMap::new();
        let mut symbol_aliases = HashMap::new();
        for symbol_cfg in &config.symbols {
            if symbol_cfg.enabled {
                let canonical = symbol_cfg.symbol.clone();
                let normalized = super::utils::normalize_symbol(&symbol_cfg.symbol);
                symbol_states.insert(
                    canonical.clone(),
                    SymbolState::new(symbol_cfg.clone(), &config.cache),
                );
                symbol_aliases.insert(normalized, canonical);
            }
        }

        let status = StrategyStatus::new(config.strategy.name.clone())
            .with_state(StrategyState::Initializing);

        Ok(Self {
            config: config.clone(),
            account_manager,
            account: account.clone(),
            exchange: account.exchange.clone(),
            market_type: config.account.market_type,
            risk_evaluator: deps.risk_evaluator.clone(),
            running: Arc::new(RwLock::new(false)),
            status: Arc::new(RwLock::new(status)),
            symbol_states: Arc::new(RwLock::new(symbol_states)),
            symbol_aliases: Arc::new(RwLock::new(symbol_aliases)),
            performance: Arc::new(RwLock::new(PerformanceTracker::default())),
            symbol_meta: Arc::new(RwLock::new(HashMap::new())),
            task_handles: Arc::new(Mutex::new(Vec::new())),
            cancel_limiter: Arc::new(Mutex::new(CancelRateLimiter::new(
                config.execution.cancel_rate_limit_per_min,
            ))),
        })
    }
}

impl Strategy for MeanReversionStrategy {
    type Config = MeanReversionConfig;

    fn create(config: Self::Config, deps: StrategyDeps) -> Result<Self>
    where
        Self: Sized,
    {
        Self::new(config, deps)
    }
}

#[async_trait]
impl StrategyInstance for MeanReversionStrategy {
    async fn start(&self) -> Result<()> {
        let mut running_guard = self.running.write().await;
        if *running_guard {
            return Ok(());
        }
        *running_guard = true;
        drop(running_guard);

        {
            let mut status = self.status.write().await;
            status.state = StrategyState::Running;
            status.updated_at = chrono::Utc::now();
        }

        let enabled_symbols = self.config.symbols.iter().filter(|s| s.enabled).count();
        logging::info(
            None,
            format!("均值回归策略启动，启用交易对数量={}", enabled_symbols),
        );

        self.spawn_tasks().await?;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        {
            let mut running = self.running.write().await;
            if !*running {
                return Ok(());
            }
            *running = false;
        }

        let mut handles = self.task_handles.lock().await;
        for handle in handles.drain(..) {
            handle.abort();
        }

        let mut status = self.status.write().await;
        status.state = StrategyState::Stopped;
        status.updated_at = chrono::Utc::now();
        logging::info(None, "均值回归策略已停止");
        Ok(())
    }

    async fn status(&self) -> Result<StrategyStatus> {
        let status = self.status.read().await;
        Ok(status.clone())
    }
}
