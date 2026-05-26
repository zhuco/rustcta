use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::{mpsc::UnboundedSender, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration, Instant};

use crate::core::types::{MarketType, WsMessage};
use crate::core::websocket::WebSocketClient;
use crate::cta::account_manager::AccountManager;
use crate::exchanges::binance::{BinanceExchange, BinanceWebSocketClient};

use super::config::{
    AccountConfig, PollingConfig, RuntimeConfig, RuntimeExecutionConfig, StrategyConfig,
    StrategyMeta, WebSocketRuntimeConfig,
};
use super::controller::{run_loop, UserStreamEvent};

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
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl MultiHedgedGridStrategy {
    pub fn new(config: MultiRuntimeConfig, account_manager: Arc<AccountManager>) -> Self {
        Self {
            config,
            account_manager,
            running: Arc::new(AtomicBool::new(false)),
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let mut routes = HashMap::new();
        let mut handles = self.handles.lock().await;
        let mut enabled_count = 0usize;

        for symbol in self.config.symbols.iter().filter(|symbol| symbol.enabled) {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            routes.insert(normalize_symbol(&symbol.engine.symbol), tx);
            enabled_count += 1;

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
            let account_manager = self.account_manager.clone();
            let running = self.running.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    if !running.load(Ordering::SeqCst) {
                        break;
                    }
                    match run_loop(
                        runtime.clone(),
                        account_manager.clone(),
                        running.clone(),
                        Some(rx),
                    )
                    .await
                    {
                        Ok(_) => break,
                        Err(err) => {
                            log::error!("[multi_hedged_grid] symbol run_loop异常: {}", err);
                            if !running.load(Ordering::SeqCst) {
                                break;
                            }
                            sleep(Duration::from_secs(30)).await;
                        }
                    }
                    break;
                }
            }));
        }

        let listener_config = self.config.clone();
        let account_manager = self.account_manager.clone();
        let running = self.running.clone();
        handles.push(tokio::spawn(async move {
            if let Err(err) =
                shared_user_stream_task(listener_config, account_manager, running, routes).await
            {
                log::error!("[multi_hedged_grid] shared user stream stopped: {}", err);
            }
        }));

        log::info!(
            "[multi_hedged_grid] started enabled_symbols={}",
            enabled_count
        );
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(());
        }

        let mut handles = self.handles.lock().await;
        for handle in handles.drain(..) {
            handle.abort();
        }
        Ok(())
    }
}

async fn shared_user_stream_task(
    config: MultiRuntimeConfig,
    account_manager: Arc<AccountManager>,
    running: Arc<AtomicBool>,
    routes: HashMap<String, UnboundedSender<UserStreamEvent>>,
) -> Result<()> {
    let account = account_manager
        .get_account(&config.account.account_id)
        .ok_or_else(|| anyhow!("账户不存在: {}", config.account.account_id))?;
    if account.exchange_name.to_lowercase() != "binance" {
        return Err(anyhow!(
            "multi_hedged_grid shared user stream currently supports Binance only"
        ));
    }

    let market_type = config.account.market_type;
    let mut backoff = Duration::from_millis(config.websocket.reconnect_delay_ms);
    let max_backoff = Duration::from_millis(config.websocket.max_reconnect_delay_ms);

    while running.load(Ordering::SeqCst) {
        let listen_key = match account.exchange.create_user_data_stream(market_type).await {
            Ok(key) => key,
            Err(err) => {
                log::warn!("[multi_hedged_grid] 获取 listenKey 失败: {}", err);
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };
        let ws_url = match market_type {
            MarketType::Spot => format!("wss://stream.binance.com:9443/ws/{}", listen_key),
            MarketType::Futures => format!("wss://fstream.binance.com/private/ws/{}", listen_key),
        };
        let mut ws_client = BinanceWebSocketClient::new(ws_url, market_type);
        if let Err(err) = ws_client.connect().await {
            log::warn!("[multi_hedged_grid] WebSocket连接失败: {}", err);
            sleep(backoff).await;
            backoff = (backoff * 2).min(max_backoff);
            continue;
        }
        backoff = Duration::from_millis(config.websocket.reconnect_delay_ms);
        log::info!(
            "[multi_hedged_grid] shared WebSocket用户流已连接 account={}",
            config.account.account_id
        );

        while running.load(Ordering::SeqCst) {
            match ws_client.receive().await {
                Ok(Some(raw)) => match ws_client.parse_binance_message(&raw) {
                    Ok(WsMessage::ExecutionReport(report)) => {
                        let key = normalize_symbol(&report.symbol);
                        if let Some(tx) = routes.get(&key) {
                            let _ = tx.send(UserStreamEvent::ExecutionReport {
                                report,
                                ws_receive_at: Instant::now(),
                            });
                        }
                    }
                    Ok(WsMessage::Error(err)) => {
                        log::warn!("[multi_hedged_grid] WebSocket错误: {}", err);
                        if err.contains("listenKeyExpired") {
                            break;
                        }
                    }
                    _ => {}
                },
                Ok(None) => break,
                Err(err) => {
                    log::warn!("[multi_hedged_grid] WebSocket接收失败: {}", err);
                    break;
                }
            }
        }
    }

    Ok(())
}

fn normalize_symbol(symbol: &str) -> String {
    symbol.replace('/', "").to_ascii_uppercase()
}
