use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;

use crate::cta::account_manager::AccountManager;

use super::config::{
    AccountConfig, PollingConfig, RuntimeConfig, RuntimeExecutionConfig, StrategyConfig,
    StrategyMeta, WebSocketRuntimeConfig,
};
use super::controller::SolusdcHedgedGridStrategy;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MultiRuntimeConfig {
    pub strategy: StrategyMeta,
    pub account: AccountConfig,
    pub symbols: Vec<MultiSymbolConfig>,
    #[serde(default)]
    pub polling: PollingConfig,
    #[serde(default)]
    pub websocket: WebSocketRuntimeConfig,
    #[serde(default)]
    pub execution: RuntimeExecutionConfig,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MultiSymbolConfig {
    #[serde(default)]
    pub enabled: bool,
    pub engine: StrategyConfig,
    #[serde(default)]
    pub polling: Option<PollingConfig>,
    #[serde(default)]
    pub websocket: Option<WebSocketRuntimeConfig>,
    #[serde(default)]
    pub execution: Option<RuntimeExecutionConfig>,
}

pub struct MultiHedgedGridStrategy {
    config: MultiRuntimeConfig,
    account_manager: Arc<AccountManager>,
    running: Arc<AtomicBool>,
    children: Arc<Mutex<Vec<SolusdcHedgedGridStrategy>>>,
}

impl MultiHedgedGridStrategy {
    pub fn new(config: MultiRuntimeConfig, account_manager: Arc<AccountManager>) -> Self {
        Self {
            config,
            account_manager,
            running: Arc::new(AtomicBool::new(false)),
            children: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let mut children = self.children.lock().await;
        for symbol in self.config.symbols.iter().filter(|symbol| symbol.enabled) {
            let runtime = RuntimeConfig {
                strategy: StrategyMeta {
                    name: format!(
                        "{}_{}",
                        self.config.strategy.name,
                        symbol.engine.symbol.replace('/', "")
                    ),
                    log_level: self.config.strategy.log_level.clone(),
                },
                account: self.config.account.clone(),
                engine: symbol.engine.clone(),
                polling: symbol
                    .polling
                    .clone()
                    .unwrap_or_else(|| self.config.polling.clone()),
                websocket: symbol
                    .websocket
                    .clone()
                    .unwrap_or_else(|| self.config.websocket.clone()),
                execution: symbol
                    .execution
                    .clone()
                    .unwrap_or_else(|| self.config.execution.clone()),
            };
            let strategy = SolusdcHedgedGridStrategy::new(runtime, self.account_manager.clone());
            strategy.start().await?;
            children.push(strategy);
        }

        log::info!(
            "[multi_hedged_grid] started enabled_symbols={}",
            children.len()
        );
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(());
        }

        let mut children = self.children.lock().await;
        for strategy in children.iter() {
            strategy.stop().await?;
        }
        children.clear();
        Ok(())
    }
}
