use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};

use crate::core::exchange::Exchange;
use crate::core::types::MarketType;
use crate::cta::account_manager::{AccountInfo, AccountManager};
use crate::strategies::common::{
    RiskLevel, Strategy, StrategyDeps, StrategyInstance, StrategyPosition, StrategyState,
    StrategyStatus, UnifiedRiskEvaluator,
};

use super::config::SidewaysMartingaleConfig;
use super::logging;
use super::model::{RuntimeState, SymbolState};

#[derive(Clone)]
pub struct SidewaysMartingaleStrategy {
    pub(super) config: SidewaysMartingaleConfig,
    pub(super) account_manager: Arc<AccountManager>,
    pub(super) account: Arc<AccountInfo>,
    pub(super) exchange: Arc<Box<dyn Exchange>>,
    pub(super) market_type: MarketType,
    pub(super) risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    pub(super) running: Arc<RwLock<bool>>,
    pub(super) started_at: Arc<RwLock<Option<Instant>>>,
    pub(super) status: Arc<RwLock<StrategyStatus>>,
    pub(super) runtime: Arc<RwLock<RuntimeState>>,
    pub(super) task_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl SidewaysMartingaleStrategy {
    fn new(config: SidewaysMartingaleConfig, deps: StrategyDeps) -> Result<Self> {
        let account_manager = deps.account_manager.clone();
        let account = account_manager
            .get_account(&config.account.account_id)
            .ok_or_else(|| anyhow!("account {} not found", config.account.account_id))?;

        let mut symbols = HashMap::new();
        for symbol_cfg in &config.symbols {
            if symbol_cfg.enabled {
                symbols.insert(
                    symbol_cfg.symbol.clone(),
                    SymbolState::new(symbol_cfg.clone()),
                );
            }
        }

        let status = StrategyStatus::new(config.strategy.name.clone())
            .with_state(StrategyState::Initializing)
            .with_risk_level(RiskLevel::Normal);

        Ok(Self {
            config: config.clone(),
            account_manager,
            account: account.clone(),
            exchange: account.exchange.clone(),
            market_type: config.account.market_type,
            risk_evaluator: deps.risk_evaluator.clone(),
            running: Arc::new(RwLock::new(false)),
            started_at: Arc::new(RwLock::new(None)),
            status: Arc::new(RwLock::new(status)),
            runtime: Arc::new(RwLock::new(RuntimeState {
                symbols,
                ..RuntimeState::default()
            })),
            task_handles: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

impl Strategy for SidewaysMartingaleStrategy {
    type Config = SidewaysMartingaleConfig;

    fn create(config: Self::Config, deps: StrategyDeps) -> Result<Self>
    where
        Self: Sized,
    {
        Self::new(config, deps)
    }
}

#[async_trait]
impl StrategyInstance for SidewaysMartingaleStrategy {
    async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        *running = true;
        drop(running);

        *self.started_at.write().await = Some(Instant::now());

        {
            let mut status = self.status.write().await;
            status.state = StrategyState::Running;
            status.updated_at = chrono::Utc::now();
        }

        logging::info(
            None,
            format!(
                "SidewaysMartingaleStrategy 启动，交易对数量={}",
                self.config.symbols.iter().filter(|s| s.enabled).count()
            ),
        );

        self.bootstrap().await?;
        self.spawn_tasks().await?;
        self.update_status().await;
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

        {
            let mut status = self.status.write().await;
            status.state = StrategyState::Stopped;
            status.updated_at = chrono::Utc::now();
        }

        logging::info(None, "SidewaysMartingaleStrategy 已停止");
        Ok(())
    }

    async fn status(&self) -> Result<StrategyStatus> {
        self.update_status().await;
        Ok(self.status.read().await.clone())
    }
}

impl SidewaysMartingaleStrategy {
    pub(super) async fn update_status(&self) {
        let runtime = self.runtime.read().await;
        let mut positions = Vec::new();
        for (symbol, state) in &runtime.symbols {
            let mut net = 0.0;
            let mut notional = 0.0;
            if let Some(long) = &state.long {
                net += long.total_quantity;
                notional += long.total_quantity * long.average_price;
            }
            if let Some(short) = &state.short {
                net -= short.total_quantity;
                notional += short.total_quantity * short.average_price;
            }
            if net.abs() > 1e-9 || notional.abs() > 1e-9 {
                positions.push(StrategyPosition {
                    symbol: symbol.clone(),
                    net_position: net,
                    notional,
                });
            }
        }
        let last_error = runtime.last_error.clone();
        let paused = runtime.trading_paused_until;
        drop(runtime);

        let uptime = self
            .started_at
            .read()
            .await
            .map(|started| started.elapsed());
        let mut status = self.status.write().await;
        status.positions = positions;
        status.updated_at = chrono::Utc::now();
        status.uptime = uptime;
        status.last_error = last_error;
        status.risk_level = if paused
            .map(|until| until > chrono::Utc::now())
            .unwrap_or(false)
        {
            RiskLevel::Warning
        } else {
            RiskLevel::Normal
        };
    }

    pub(super) async fn mark_error(&self, message: String) {
        {
            let mut runtime = self.runtime.write().await;
            runtime.last_error = Some(message.clone());
        }
        {
            let mut status = self.status.write().await;
            status.state = StrategyState::Error;
            status.last_error = Some(message.clone());
            status.updated_at = chrono::Utc::now();
        }
        logging::error(None, message);
    }
}
