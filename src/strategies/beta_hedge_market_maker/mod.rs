mod execution;
pub mod exposure;
pub mod inventory;
pub mod quoting;
pub mod risk;

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, Notify, RwLock};

use crate::core::exchange::Exchange;
use crate::core::types::{
    Balance, ExecutionReport as WsExecutionReport, MarketType, OrderBook, OrderStatus, Position,
    WsMessage,
};
use crate::core::websocket::{BaseWebSocketClient, WebSocketClient};
use crate::cta::account_manager::{AccountInfo, AccountManager};
use crate::exchanges::binance::{BinanceExchange, BinanceWebSocketClient};
use crate::strategies::common::application::strategy::{Strategy as AppStrategy, StrategyInstance};
use crate::strategies::common::{
    RiskAction, RiskLevel, StrategyDeps, StrategyPosition, StrategyState, StrategyStatus,
    UnifiedRiskEvaluator,
};

use self::execution::{
    apply_order_update, cancel_symbol_orders, reconcile_orders, LiveOrder, ReconcileParams,
};
use self::exposure::{compute_portfolio_exposure, PortfolioExposure};
use self::inventory::{extract_hedge_position_snapshot, HedgeInventoryLedger};
use self::quoting::{build_quotes, MarketQuote, QuoteParams, QuotePlan};
use self::risk::{build_strategy_snapshot, evaluate_protection, HedgePhase};

const SUMMARY_LOG_INTERVAL_SECS: i64 = 5;
const FALLBACK_SYNC_INTERVAL_SECS: u64 = 15;
const KNOWN_QUOTES: &[&str] = &[
    "FDUSD", "USDC", "USDT", "BUSD", "TUSD", "USDP", "DAI", "BTC", "ETH", "BNB", "TRY", "USD",
];

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StrategyMeta {
    pub name: String,
    #[serde(default)]
    pub log_level: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StrategyAccount {
    pub account_id: String,
    pub market_type: MarketType,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HedgeConfig {
    pub hedge_symbol: String,
    pub hedge_ratio: f64,
    pub target_inventory_notional_cap: f64,
    pub soft_inventory_notional: f64,
    pub hard_inventory_notional: f64,
    pub emergency_inventory_notional: f64,
    #[serde(default = "default_max_portfolio_exposure_multiple")]
    pub max_portfolio_exposure_multiple: f64,
}

fn default_max_portfolio_exposure_multiple() -> f64 {
    10.0
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QuotingConfig {
    pub base_order_notional: f64,
    pub levels_per_side: usize,
    pub refresh_interval_ms: u64,
    pub order_ttl_ms: u64,
    #[serde(default = "default_min_requote_interval_ms")]
    pub min_requote_interval_ms: u64,
    #[serde(default = "default_reprice_threshold_ticks")]
    pub reprice_threshold_ticks: u32,
    pub post_only: bool,
    pub time_in_force: String,
    pub reduce_risk_taker_enabled: bool,
    #[serde(default = "default_base_spread_bps")]
    pub base_spread_bps: f64,
    #[serde(default = "default_level_spacing_bps")]
    pub level_spacing_bps: f64,
    #[serde(default = "default_min_spread_bps")]
    pub min_spread_bps: f64,
    #[serde(default = "default_max_inventory_bias_bps")]
    pub max_inventory_bias_bps: f64,
    #[serde(default = "default_level_size_decay")]
    pub level_size_decay: f64,
    #[serde(default)]
    pub independent_dual_side: bool,
}

impl Default for QuotingConfig {
    fn default() -> Self {
        Self {
            base_order_notional: 8.0,
            levels_per_side: 2,
            refresh_interval_ms: 500,
            order_ttl_ms: 15_000,
            min_requote_interval_ms: default_min_requote_interval_ms(),
            reprice_threshold_ticks: default_reprice_threshold_ticks(),
            post_only: true,
            time_in_force: "GTX".to_string(),
            reduce_risk_taker_enabled: false,
            base_spread_bps: default_base_spread_bps(),
            level_spacing_bps: default_level_spacing_bps(),
            min_spread_bps: default_min_spread_bps(),
            max_inventory_bias_bps: default_max_inventory_bias_bps(),
            level_size_decay: default_level_size_decay(),
            independent_dual_side: false,
        }
    }
}

fn default_base_spread_bps() -> f64 {
    6.0
}

fn default_min_requote_interval_ms() -> u64 {
    8_000
}

fn default_reprice_threshold_ticks() -> u32 {
    4
}

fn default_level_spacing_bps() -> f64 {
    4.0
}

fn default_min_spread_bps() -> f64 {
    2.0
}

fn default_max_inventory_bias_bps() -> f64 {
    4.0
}

fn default_level_size_decay() -> f64 {
    0.8
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BetaHedgeMarketMakerConfig {
    pub strategy: StrategyMeta,
    pub account: StrategyAccount,
    pub hedge: HedgeConfig,
    pub quoting: QuotingConfig,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct SymbolPrecision {
    pub tick_size: f64,
    pub step_size: f64,
    pub price_digits: u32,
    pub qty_digits: u32,
    pub min_notional: f64,
}

#[derive(Debug, Clone, Default)]
struct RuntimeState {
    precision: Option<SymbolPrecision>,
    inventory: HedgeInventoryLedger,
    live_orders: Vec<LiveOrder>,
    last_quotes: Vec<QuotePlan>,
    last_error: Option<String>,
    started_at: Option<Instant>,
    balances: Vec<Balance>,
    positions: Vec<Position>,
    market: Option<MarketQuote>,
    last_exposure: Option<PortfolioExposure>,
    last_quote_at: Option<DateTime<Utc>>,
    last_summary_log_at: Option<DateTime<Utc>>,
    public_message_count: u64,
    user_message_count: u64,
    public_reconnects: u64,
    user_reconnects: u64,
    public_ws_ready: bool,
    user_ws_ready: bool,
}

pub struct BetaHedgeMarketMaker {
    config: BetaHedgeMarketMakerConfig,
    account: Arc<AccountInfo>,
    risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    status: Arc<RwLock<StrategyStatus>>,
    runtime: Arc<RwLock<RuntimeState>>,
    running: Arc<RwLock<bool>>,
    dual_position_mode: Arc<RwLock<bool>>,
    quote_notifier: Arc<Notify>,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl BetaHedgeMarketMaker {
    fn new(config: BetaHedgeMarketMakerConfig, deps: StrategyDeps) -> Result<Self> {
        let account_manager: Arc<AccountManager> = deps.account_manager.clone();
        let account = account_manager
            .get_account(&config.account.account_id)
            .ok_or_else(|| anyhow!("account {} not found", config.account.account_id))?;

        Ok(Self {
            status: Arc::new(RwLock::new(
                StrategyStatus::new(config.strategy.name.clone())
                    .with_state(StrategyState::Initializing),
            )),
            runtime: Arc::new(RwLock::new(RuntimeState::default())),
            running: Arc::new(RwLock::new(false)),
            dual_position_mode: Arc::new(RwLock::new(false)),
            quote_notifier: Arc::new(Notify::new()),
            tasks: Arc::new(Mutex::new(Vec::new())),
            risk_evaluator: deps.risk_evaluator,
            account,
            config,
        })
    }

    async fn init_precision(&self) -> Result<()> {
        let info = self
            .account
            .exchange
            .get_symbol_info(
                &self.config.hedge.hedge_symbol,
                self.config.account.market_type,
            )
            .await?;
        let precision = SymbolPrecision {
            tick_size: info.tick_size,
            step_size: info.step_size,
            price_digits: infer_digits(info.tick_size),
            qty_digits: infer_digits(info.step_size),
            min_notional: info.min_notional.unwrap_or(5.0),
        };
        self.runtime.write().await.precision = Some(precision);
        Ok(())
    }

    async fn detect_dual_position_mode(&self) -> Result<bool> {
        if let Some(binance) = self
            .account
            .exchange
            .as_any()
            .downcast_ref::<BinanceExchange>()
        {
            return Ok(binance.get_position_mode().await?);
        }
        Ok(false)
    }

    async fn bootstrap_state(&self) -> Result<()> {
        let balances = self
            .account
            .exchange
            .get_balance(self.config.account.market_type)
            .await?;
        let positions = self.account.exchange.get_positions(None).await?;
        let live_orders = self.sync_live_orders_from_exchange().await?;
        let market = fetch_market_quote(
            self.account.exchange.as_ref().as_ref(),
            &self.config.hedge.hedge_symbol,
            self.config.account.market_type,
        )
        .await?;

        let exposure = compute_portfolio_exposure(
            &balances,
            &positions,
            &self.config.hedge.hedge_symbol,
            self.config.hedge.hedge_ratio,
            self.config.hedge.target_inventory_notional_cap,
            self.config.hedge.max_portfolio_exposure_multiple,
        );

        {
            let mut runtime = self.runtime.write().await;
            runtime.balances = balances;
            runtime.positions = positions;
            runtime.live_orders = live_orders;
            runtime.market = Some(market);
            runtime.last_exposure = Some(exposure.clone());
        }

        log::info!(
            "[beta_hedge_mm] bootstrap equity={:.2} core_exposure={:.2} hedge_target={:.2} dry_run={}",
            exposure.account_equity_usd,
            exposure.portfolio_core_exposure_usd,
            exposure.hedge_target_notional_usd,
            self.config.dry_run
        );
        self.quote_notifier.notify_one();
        Ok(())
    }

    async fn sync_live_orders_from_exchange(&self) -> Result<Vec<LiveOrder>> {
        if self.config.dry_run {
            return Ok(Vec::new());
        }

        let exchange_orders = self
            .account
            .exchange
            .get_open_orders(
                Some(&self.config.hedge.hedge_symbol),
                self.config.account.market_type,
            )
            .await?;

        let mut live_orders = Vec::new();
        for order in exchange_orders {
            let client_order_id = extract_client_order_id(&order.info);
            let Some(client_order_id) = client_order_id else {
                continue;
            };
            if !client_order_id.starts_with("betahedge_") {
                continue;
            }

            live_orders.push(LiveOrder {
                order_id: order.id,
                client_order_id: Some(client_order_id.clone()),
                level: parse_level_from_client_order_id(&client_order_id).unwrap_or(0),
                side: order.side,
                position_side: extract_position_side(&order.info),
                reduce_only: extract_reduce_only(&order.info),
                price: order.price.unwrap_or(0.0),
                quantity: order.remaining.max(0.0),
                created_at: order.last_trade_timestamp.unwrap_or(order.timestamp),
            });
        }

        Ok(live_orders)
    }

    async fn quote_loop(self: Arc<Self>) {
        let fallback = Duration::from_secs(FALLBACK_SYNC_INTERVAL_SECS);
        let refresh_interval =
            Duration::from_millis(self.config.quoting.refresh_interval_ms.max(1));
        let mut fallback_sync = tokio::time::interval(fallback);
        fallback_sync.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut first_run = true;
        let mut last_reconcile_at: Option<Instant> = None;

        loop {
            if !*self.running.read().await {
                break;
            }

            if !first_run {
                tokio::select! {
                    _ = self.quote_notifier.notified() => {},
                    _ = fallback_sync.tick() => {
                        if let Err(err) = self.bootstrap_state().await {
                            log::warn!("[beta_hedge_mm] fallback snapshot sync failed: {}", err);
                        }
                    }
                }
            } else {
                first_run = false;
                fallback_sync.reset();
            }

            if let Some(last_at) = last_reconcile_at {
                let elapsed = last_at.elapsed();
                if elapsed < refresh_interval {
                    tokio::time::sleep(refresh_interval - elapsed).await;
                }
            }

            if !self.config.dry_run && !self.runtime.read().await.user_ws_ready {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            last_reconcile_at = Some(Instant::now());

            if let Err(err) = self.reconcile_from_runtime().await {
                log::warn!("[beta_hedge_mm] reconcile failed: {}", err);
                self.runtime.write().await.last_error = Some(err.to_string());
            }
        }
    }

    async fn run_public_stream(self: Arc<Self>) {
        let stream_symbol = normalize_symbol(&self.config.hedge.hedge_symbol).to_lowercase();
        let params = vec![
            format!("{}@ticker", stream_symbol),
            format!("{}@depth5@100ms", stream_symbol),
        ];
        let channels = params
            .iter()
            .map(|param| format!(r#""{}""#, param))
            .collect::<Vec<_>>()
            .join(",");
        let payload = format!(
            r#"{{"method":"SUBSCRIBE","params":[{}],"id":{}}}"#,
            channels,
            Utc::now().timestamp_millis()
        );
        let ws_url = match self.config.account.market_type {
            MarketType::Spot => "wss://stream.binance.com:9443/ws",
            MarketType::Futures => "wss://fstream.binance.com/ws",
        };

        loop {
            if !*self.running.read().await {
                break;
            }

            let mut client = BaseWebSocketClient::new(ws_url.to_string(), "binance".to_string());
            if let Err(err) = client.connect().await {
                log::warn!("[beta_hedge_mm] public ws connect failed: {}", err);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
            if let Err(err) = client.send(payload.clone()).await {
                log::warn!("[beta_hedge_mm] public ws subscribe failed: {}", err);
                let _ = client.disconnect().await;
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }

            {
                let mut runtime = self.runtime.write().await;
                runtime.public_reconnects += 1;
                runtime.public_ws_ready = true;
            }
            log::info!("[beta_hedge_mm] public ws connected {}", ws_url);

            loop {
                if !*self.running.read().await {
                    let _ = client.disconnect().await;
                    return;
                }

                match client.receive().await {
                    Ok(Some(message)) => {
                        if let Err(err) = self.handle_public_ws(&message).await {
                            log::debug!("[beta_hedge_mm] public ws parse failed: {}", err);
                        }
                    }
                    Ok(None) => tokio::time::sleep(Duration::from_millis(20)).await,
                    Err(err) => {
                        self.runtime.write().await.public_ws_ready = false;
                        log::warn!("[beta_hedge_mm] public ws receive error: {}", err);
                        break;
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    async fn run_user_stream(self: Arc<Self>) {
        loop {
            if !*self.running.read().await {
                break;
            }

            let listen_key = match self
                .account
                .exchange
                .create_user_data_stream(self.config.account.market_type)
                .await
            {
                Ok(key) => key,
                Err(err) => {
                    log::warn!("[beta_hedge_mm] listen_key acquisition failed: {}", err);
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };

            let ws_url = match self.config.account.market_type {
                MarketType::Spot => format!("wss://stream.binance.com:9443/ws/{}", listen_key),
                MarketType::Futures => format!("wss://fstream.binance.com/ws/{}", listen_key),
            };
            let mut client =
                BinanceWebSocketClient::new(ws_url.clone(), self.config.account.market_type);
            if let Err(err) = client.connect().await {
                log::warn!("[beta_hedge_mm] user ws connect failed: {}", err);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }

            {
                let mut runtime = self.runtime.write().await;
                runtime.user_reconnects += 1;
                runtime.user_ws_ready = true;
            }
            log::info!("[beta_hedge_mm] user ws connected {}", ws_url);

            loop {
                if !*self.running.read().await {
                    let _ = client.disconnect().await;
                    return;
                }

                match client.receive().await {
                    Ok(Some(message)) => match client.parse_binance_message(&message) {
                        Ok(WsMessage::ExecutionReport(report)) => {
                            if let Err(err) = self.handle_execution_report(report).await {
                                log::debug!(
                                    "[beta_hedge_mm] user ws execution report failed: {}",
                                    err
                                );
                            }
                        }
                        Ok(WsMessage::Error(err))
                            if err.contains("listenKeyExpired")
                                || err.contains("ListenKeyExpired") =>
                        {
                            log::warn!(
                                "[beta_hedge_mm] listen key expired, user stream will reconnect"
                            );
                            break;
                        }
                        _ => {
                            if let Err(err) = self.handle_user_ws(&message).await {
                                log::debug!("[beta_hedge_mm] user ws parse failed: {}", err);
                            }
                        }
                    },
                    Ok(None) => tokio::time::sleep(Duration::from_millis(20)).await,
                    Err(err) => {
                        self.runtime.write().await.user_ws_ready = false;
                        log::warn!("[beta_hedge_mm] user ws receive error: {}", err);
                        break;
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    async fn handle_public_ws(&self, payload: &str) -> Result<()> {
        let value: Value = serde_json::from_str(payload)?;
        if value.get("result").is_some() {
            return Ok(());
        }
        let data = value.get("data").unwrap_or(&value);
        let event_type = data.get("e").and_then(|field| field.as_str()).unwrap_or("");
        if event_type.is_empty() {
            return Ok(());
        }

        let symbol = data
            .get("s")
            .and_then(|field| field.as_str())
            .unwrap_or_default();
        if !symbol.is_empty() && !same_symbol(symbol, &self.config.hedge.hedge_symbol) {
            return Ok(());
        }

        let mut updated_market = None;
        match event_type {
            "24hrTicker" => {
                let bid = parse_str_f64(data.get("b")).unwrap_or(0.0);
                let ask = parse_str_f64(data.get("a")).unwrap_or(0.0);
                if bid > 0.0 && ask > 0.0 {
                    updated_market = Some(MarketQuote {
                        best_bid: bid,
                        best_ask: ask,
                        mid: (bid + ask) / 2.0,
                    });
                }
            }
            "depthUpdate" => {
                let best_bid = extract_best_level(data.get("b"));
                let best_ask = extract_best_level(data.get("a"));
                if let (Some((bid, _)), Some((ask, _))) = (best_bid, best_ask) {
                    if bid > 0.0 && ask > 0.0 {
                        updated_market = Some(MarketQuote {
                            best_bid: bid,
                            best_ask: ask,
                            mid: (bid + ask) / 2.0,
                        });
                    }
                }
            }
            _ => {}
        }

        if let Some(market) = updated_market {
            let mut runtime = self.runtime.write().await;
            runtime.public_message_count += 1;
            runtime.market = Some(market);
            drop(runtime);
            self.quote_notifier.notify_one();
        }

        Ok(())
    }

    async fn handle_execution_report(&self, report: WsExecutionReport) -> Result<()> {
        if !same_symbol(&report.symbol, &self.config.hedge.hedge_symbol) {
            return Ok(());
        }

        let remaining_qty = (report.amount - report.executed_amount).max(0.0);
        let is_beta_order = report
            .client_order_id
            .as_deref()
            .map(|id| id.starts_with("betahedge_"))
            .unwrap_or(false);

        {
            let mut runtime = self.runtime.write().await;
            runtime.user_message_count += 1;
            apply_order_update(
                &mut runtime.live_orders,
                &report.order_id,
                report.client_order_id.as_deref(),
                report.status.clone(),
                Some(remaining_qty),
                report.timestamp,
            );
        }

        if is_beta_order
            && matches!(
                report.status,
                OrderStatus::Closed | OrderStatus::PartiallyFilled
            )
        {
            log::info!(
                "[beta_hedge_mm] order update status={:?} id={} client_id={} remaining={:.4} avg_price={:.6}",
                report.status,
                report.order_id,
                report.client_order_id.as_deref().unwrap_or(""),
                remaining_qty,
                report.executed_price,
            );
        }

        self.quote_notifier.notify_one();
        Ok(())
    }

    async fn handle_user_ws(&self, payload: &str) -> Result<()> {
        let value: Value = serde_json::from_str(payload)?;
        let data = value.get("data").unwrap_or(&value);
        let event_type = data.get("e").and_then(|field| field.as_str()).unwrap_or("");
        if event_type.is_empty() {
            return Ok(());
        }

        {
            let mut runtime = self.runtime.write().await;
            runtime.user_message_count += 1;
        }

        match event_type {
            "ORDER_TRADE_UPDATE" => self.handle_order_trade_update(data).await?,
            "ACCOUNT_UPDATE" => self.handle_account_update(data).await?,
            "listenKeyExpired" => {
                log::warn!("[beta_hedge_mm] listen key expired, user stream will reconnect");
            }
            _ => {}
        }

        Ok(())
    }

    async fn handle_order_trade_update(&self, event: &Value) -> Result<()> {
        let order = event
            .get("o")
            .ok_or_else(|| anyhow!("ORDER_TRADE_UPDATE missing order payload"))?;
        let order_id = order
            .get("i")
            .and_then(|field| field.as_i64())
            .map(|id| id.to_string())
            .ok_or_else(|| anyhow!("ORDER_TRADE_UPDATE missing order id"))?;
        let client_order_id = order.get("c").and_then(|field| field.as_str());
        let status = parse_binance_order_status(order.get("X").and_then(|field| field.as_str()));
        let is_fill_status = matches!(status, OrderStatus::Closed | OrderStatus::PartiallyFilled);
        let orig_qty = parse_str_f64(order.get("q")).unwrap_or(0.0);
        let executed_qty = parse_str_f64(order.get("z")).unwrap_or(0.0);
        let remaining_qty = (orig_qty - executed_qty).max(0.0);
        let event_time = event
            .get("E")
            .and_then(|field| field.as_i64())
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now);

        let symbol = order
            .get("s")
            .and_then(|field| field.as_str())
            .unwrap_or_default();
        let is_hedge_symbol = same_symbol(symbol, &self.config.hedge.hedge_symbol);
        let is_beta_order = client_order_id
            .map(|id| id.starts_with("betahedge_"))
            .unwrap_or(false);

        {
            let mut runtime = self.runtime.write().await;
            apply_order_update(
                &mut runtime.live_orders,
                &order_id,
                client_order_id,
                status.clone(),
                Some(remaining_qty),
                event_time,
            );
        }

        if is_hedge_symbol && is_beta_order {
            let avg_price = parse_str_f64(order.get("ap")).unwrap_or(0.0);
            let last_fill = parse_str_f64(order.get("l")).unwrap_or(0.0);
            if last_fill > 0.0 || is_fill_status {
                log::info!(
                    "[beta_hedge_mm] order update status={:?} id={} client_id={} remaining={:.4} avg_price={:.6}",
                    status,
                    order_id,
                    client_order_id.unwrap_or(""),
                    remaining_qty,
                    avg_price
                );
            }
        }

        if is_hedge_symbol {
            self.quote_notifier.notify_one();
        }
        Ok(())
    }

    async fn handle_account_update(&self, event: &Value) -> Result<()> {
        let account = event
            .get("a")
            .ok_or_else(|| anyhow!("ACCOUNT_UPDATE missing account payload"))?;
        let market = self.runtime.read().await.market;
        let mut runtime = self.runtime.write().await;

        if let Some(balance_items) = account.get("B").and_then(|field| field.as_array()) {
            for item in balance_items {
                if let Some(balance) =
                    parse_account_balance_update(item, self.config.account.market_type)
                {
                    merge_balance(&mut runtime.balances, balance);
                }
            }
        }

        if let Some(position_items) = account.get("P").and_then(|field| field.as_array()) {
            for item in position_items {
                if let Some(position) = parse_account_position_update(item, market) {
                    merge_position(&mut runtime.positions, position);
                }
            }
        }

        drop(runtime);
        self.quote_notifier.notify_one();
        Ok(())
    }

    async fn reconcile_from_runtime(&self) -> Result<()> {
        let (balances, positions, market, precision, mut inventory, live_orders, last_error) = {
            let runtime = self.runtime.read().await;
            (
                runtime.balances.clone(),
                runtime.positions.clone(),
                runtime.market,
                runtime.precision,
                runtime.inventory.clone(),
                runtime.live_orders.clone(),
                runtime.last_error.clone(),
            )
        };

        let Some(market) = market else {
            return Ok(());
        };
        let Some(precision) = precision else {
            return Ok(());
        };
        if balances.is_empty() {
            return Ok(());
        }

        let exposure = compute_portfolio_exposure(
            &balances,
            &positions,
            &self.config.hedge.hedge_symbol,
            self.config.hedge.hedge_ratio,
            self.config.hedge.target_inventory_notional_cap,
            self.config.hedge.max_portfolio_exposure_multiple,
        );
        let hedge_snapshot =
            extract_hedge_position_snapshot(&positions, &self.config.hedge.hedge_symbol);
        inventory.sync_from_snapshot(&hedge_snapshot);
        let protection = evaluate_protection(&self.config, &exposure, &inventory);
        let strategy_snapshot = build_strategy_snapshot(&self.config, &exposure, &inventory);
        let risk_decision = self.risk_evaluator.evaluate(&strategy_snapshot).await;
        let risk_level = merge_risk_level(&protection, &risk_decision.action);
        let cycle_error = match &risk_decision.action {
            RiskAction::Halt { reason } => Some(reason.clone()),
            RiskAction::ScaleDown { reason, .. } => Some(reason.clone()),
            RiskAction::Notify { message, .. } => Some(message.clone()),
            RiskAction::None => None,
        };

        if matches!(risk_decision.action, RiskAction::Halt { .. })
            || protection.state == risk::ProtectionState::Emergency
        {
            cancel_symbol_orders(
                self.account.exchange.as_ref().as_ref(),
                &self.config.hedge.hedge_symbol,
                self.config.account.market_type,
                self.config.dry_run,
            )
            .await?;
            let mut runtime = self.runtime.write().await;
            runtime.inventory = inventory.clone();
            runtime.live_orders.clear();
            runtime.last_quotes.clear();
            runtime.last_exposure = Some(exposure.clone());
            runtime.last_error = cycle_error
                .or_else(|| Some(protection.reason.clone()))
                .or(last_error);
            drop(runtime);
            self.update_status(&inventory, risk_level, Some(protection.reason.clone()))
                .await;
            return Ok(());
        }

        let inventory_gap_notional_usd =
            exposure.hedge_target_notional_usd - inventory.net_notional_usd;
        let dual_position_mode = *self.dual_position_mode.read().await;
        let quotes = build_quotes(
            market,
            precision,
            &inventory,
            &protection,
            &QuoteParams {
                target_notional_usd: exposure.hedge_target_notional_usd,
                inventory_gap_notional_usd,
                base_order_notional: self.config.quoting.base_order_notional,
                levels_per_side: self.config.quoting.levels_per_side,
                base_spread_bps: self.config.quoting.base_spread_bps,
                level_spacing_bps: self.config.quoting.level_spacing_bps,
                min_spread_bps: self.config.quoting.min_spread_bps,
                max_inventory_bias_bps: self.config.quoting.max_inventory_bias_bps,
                level_size_decay: self.config.quoting.level_size_decay,
                dual_position_mode,
                independent_dual_side: self.config.quoting.independent_dual_side,
            },
        );

        let mut live_orders = live_orders;
        let exec_report = reconcile_orders(
            ReconcileParams {
                exchange: self.account.exchange.as_ref().as_ref(),
                symbol: &self.config.hedge.hedge_symbol,
                market_type: self.config.account.market_type,
                post_only: self.config.quoting.post_only,
                time_in_force: &self.config.quoting.time_in_force,
                tick_size: precision.tick_size,
                step_size: precision.step_size,
                order_ttl_ms: self.config.quoting.order_ttl_ms,
                min_requote_interval_ms: self.config.quoting.min_requote_interval_ms,
                reprice_threshold_ticks: self.config.quoting.reprice_threshold_ticks,
                dual_position_mode,
                dry_run: self.config.dry_run,
                now: Utc::now(),
            },
            &mut live_orders,
            &quotes,
        )
        .await?;

        {
            let mut runtime = self.runtime.write().await;
            runtime.inventory = inventory.clone();
            runtime.live_orders = live_orders;
            runtime.last_quotes = quotes.clone();
            runtime.last_exposure = Some(exposure.clone());
            runtime.last_quote_at = Some(Utc::now());
            runtime.last_error = cycle_error.clone().or(last_error);
            maybe_log_summary(
                &mut runtime,
                &exposure,
                &inventory,
                protection.phase,
                &quotes,
                &exec_report,
            );
        }

        self.update_status(&inventory, risk_level, cycle_error)
            .await;
        Ok(())
    }

    async fn update_status(
        &self,
        inventory: &HedgeInventoryLedger,
        risk_level: RiskLevel,
        last_error: Option<String>,
    ) {
        let mut status = StrategyStatus::new(self.config.strategy.name.clone())
            .with_state(if *self.running.read().await {
                StrategyState::Running
            } else {
                StrategyState::Stopped
            })
            .with_risk_level(risk_level)
            .with_positions(vec![StrategyPosition {
                symbol: self.config.hedge.hedge_symbol.clone(),
                net_position: inventory.net_quantity,
                notional: inventory.net_notional_usd,
            }]);

        if let Some(started_at) = self.runtime.read().await.started_at {
            status = status.with_uptime(started_at.elapsed());
        }
        if let Some(error) = last_error {
            status = status.with_last_error(error);
        }

        *self.status.write().await = status;
    }
}

#[async_trait]
impl StrategyInstance for BetaHedgeMarketMaker {
    async fn start(&self) -> Result<()> {
        if *self.running.read().await {
            return Ok(());
        }

        *self.running.write().await = true;
        self.runtime.write().await.started_at = Some(Instant::now());
        self.init_precision().await?;

        let dual_position_mode = self.detect_dual_position_mode().await?;
        *self.dual_position_mode.write().await = dual_position_mode;
        log::info!(
            "[beta_hedge_mm] account={} hedge_symbol={} dual_position_mode={} independent_dual_side={} maker_only={} dry_run={}",
            self.config.account.account_id,
            self.config.hedge.hedge_symbol,
            dual_position_mode,
            self.config.quoting.independent_dual_side,
            self.config.quoting.post_only,
            self.config.dry_run
        );

        self.bootstrap_state().await?;

        if !self.config.dry_run {
            cancel_symbol_orders(
                self.account.exchange.as_ref().as_ref(),
                &self.config.hedge.hedge_symbol,
                self.config.account.market_type,
                false,
            )
            .await?;
            self.runtime.write().await.live_orders.clear();
        }

        let strategy = Arc::new(self.clone_for_task());
        let public_handle = tokio::spawn(strategy.clone().run_public_stream());
        let user_handle = tokio::spawn(strategy.clone().run_user_stream());
        let quote_handle = tokio::spawn(strategy.clone().quote_loop());

        let mut handles = self.tasks.lock().await;
        handles.push(public_handle);
        handles.push(user_handle);
        handles.push(quote_handle);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        *self.running.write().await = false;
        self.quote_notifier.notify_waiters();

        let mut handles = self.tasks.lock().await;
        while let Some(handle) = handles.pop() {
            handle.abort();
            let _ = handle.await;
        }

        cancel_symbol_orders(
            self.account.exchange.as_ref().as_ref(),
            &self.config.hedge.hedge_symbol,
            self.config.account.market_type,
            self.config.dry_run,
        )
        .await?;

        let (inventory, last_error) = {
            let runtime = self.runtime.read().await;
            (runtime.inventory.clone(), runtime.last_error.clone())
        };
        self.update_status(&inventory, RiskLevel::Normal, last_error)
            .await;
        Ok(())
    }

    async fn status(&self) -> Result<StrategyStatus> {
        Ok(self.status.read().await.clone())
    }
}

impl Clone for BetaHedgeMarketMaker {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            account: self.account.clone(),
            risk_evaluator: self.risk_evaluator.clone(),
            status: self.status.clone(),
            runtime: self.runtime.clone(),
            running: self.running.clone(),
            dual_position_mode: self.dual_position_mode.clone(),
            quote_notifier: self.quote_notifier.clone(),
            tasks: self.tasks.clone(),
        }
    }
}

impl BetaHedgeMarketMaker {
    fn clone_for_task(&self) -> Self {
        self.clone()
    }
}

#[async_trait]
impl AppStrategy for BetaHedgeMarketMaker {
    type Config = BetaHedgeMarketMakerConfig;

    fn create(config: Self::Config, deps: StrategyDeps) -> Result<Self> {
        Ok(Self::new(config, deps)?)
    }
}

fn infer_digits(step: f64) -> u32 {
    if step <= 0.0 {
        return 4;
    }
    let mut digits = 0;
    let mut value = step;
    while value.fract() != 0.0 && digits < 8 {
        value *= 10.0;
        digits += 1;
    }
    digits
}

async fn fetch_market_quote(
    exchange: &dyn Exchange,
    symbol: &str,
    market_type: MarketType,
) -> Result<MarketQuote> {
    let book: OrderBook = exchange.get_orderbook(symbol, market_type, Some(5)).await?;
    let best_bid = book.bids.first().map(|level| level[0]).unwrap_or(0.0);
    let best_ask = book.asks.first().map(|level| level[0]).unwrap_or(0.0);
    let mid = if best_bid > 0.0 && best_ask > 0.0 {
        (best_bid + best_ask) / 2.0
    } else {
        let ticker = exchange.get_ticker(symbol, market_type).await?;
        ((ticker.bid + ticker.ask) / 2.0).max(ticker.last)
    };
    Ok(MarketQuote {
        best_bid,
        best_ask,
        mid,
    })
}

fn merge_risk_level(protection: &risk::ProtectionDecision, risk_action: &RiskAction) -> RiskLevel {
    let action_level = match risk_action {
        RiskAction::None => RiskLevel::Normal,
        RiskAction::Notify { .. } | RiskAction::ScaleDown { .. } => RiskLevel::Warning,
        RiskAction::Halt { .. } => RiskLevel::Halt,
    };

    match (protection.risk_level(), action_level) {
        (RiskLevel::Halt, _) | (_, RiskLevel::Halt) => RiskLevel::Halt,
        (RiskLevel::Danger, _) | (_, RiskLevel::Danger) => RiskLevel::Danger,
        (RiskLevel::Warning, _) | (_, RiskLevel::Warning) => RiskLevel::Warning,
        _ => RiskLevel::Normal,
    }
}

fn maybe_log_summary(
    runtime: &mut RuntimeState,
    exposure: &PortfolioExposure,
    inventory: &HedgeInventoryLedger,
    phase: HedgePhase,
    quotes: &[QuotePlan],
    exec_report: &execution::ExecutionReport,
) {
    let now = Utc::now();
    let should_log = runtime
        .last_summary_log_at
        .map(|logged_at| (now - logged_at).num_seconds() >= SUMMARY_LOG_INTERVAL_SECS)
        .unwrap_or(true);

    if !should_log {
        return;
    }

    runtime.last_summary_log_at = Some(now);
    log::info!(
        "[beta_hedge_mm] phase={:?} core_exposure={:.2} target={:.2} hedge={:.2} gap={:.2} long={:.4} short={:.4} upnl={:.2} quotes={} placed={} canceled={} skipped={} public_msgs={} user_msgs={}",
        phase,
        exposure.portfolio_core_exposure_usd,
        exposure.hedge_target_notional_usd,
        inventory.net_notional_usd,
        exposure.hedge_target_notional_usd - inventory.net_notional_usd,
        inventory.long_leg.quantity,
        inventory.short_leg.quantity,
        inventory.unrealized_pnl,
        quotes.len(),
        exec_report.placed,
        exec_report.canceled,
        exec_report.skipped,
        runtime.public_message_count,
        runtime.user_message_count
    );
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .replace('/', "")
        .replace('-', "")
        .to_ascii_uppercase()
}

fn same_symbol(lhs: &str, rhs: &str) -> bool {
    normalize_symbol(lhs) == normalize_symbol(rhs)
}

fn to_standard_symbol(symbol: &str) -> String {
    if symbol.contains('/') {
        let upper = symbol.to_ascii_uppercase();
        let parts: Vec<&str> = upper.split('/').collect();
        if parts.len() == 2 {
            return format!("{}/{}", parts[0], parts[1]);
        }
        return upper;
    }

    let compact = normalize_symbol(symbol);
    for quote in KNOWN_QUOTES {
        if compact.len() > quote.len() && compact.ends_with(quote) {
            let base = &compact[..compact.len() - quote.len()];
            return format!("{}/{}", base, quote);
        }
    }
    compact
}

fn parse_str_f64(value: Option<&Value>) -> Option<f64> {
    value.and_then(|field| match field {
        Value::String(string) => string.parse::<f64>().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    })
}

fn extract_best_level(levels: Option<&Value>) -> Option<(f64, f64)> {
    let first = levels?.as_array()?.first()?.as_array()?;
    if first.len() < 2 {
        return None;
    }
    let price = parse_str_f64(first.first())?;
    let quantity = parse_str_f64(first.get(1))?;
    Some((price, quantity))
}

fn extract_client_order_id(info: &Value) -> Option<String> {
    info.get("clientOrderId")
        .or_else(|| info.get("client_order_id"))
        .and_then(|field| field.as_str())
        .map(|value| value.to_string())
}

fn extract_position_side(info: &Value) -> Option<String> {
    info.get("positionSide")
        .or_else(|| info.get("position_side"))
        .and_then(|field| field.as_str())
        .map(|value| value.to_string())
}

fn extract_reduce_only(info: &Value) -> bool {
    info.get("reduceOnly")
        .or_else(|| info.get("reduce_only"))
        .and_then(|field| match field {
            Value::Bool(value) => Some(*value),
            Value::String(value) => value.parse::<bool>().ok(),
            _ => None,
        })
        .unwrap_or(false)
}

fn parse_level_from_client_order_id(client_order_id: &str) -> Option<usize> {
    client_order_id
        .rsplit('_')
        .next()
        .and_then(|suffix| u16::from_str_radix(suffix, 16).ok())
        .map(|value| value as usize)
}

fn parse_binance_order_status(status: Option<&str>) -> OrderStatus {
    match status.unwrap_or_default() {
        "NEW" => OrderStatus::Open,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Closed,
        "CANCELED" => OrderStatus::Canceled,
        "EXPIRED" => OrderStatus::Expired,
        "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::Pending,
    }
}

fn parse_account_balance_update(item: &Value, market_type: MarketType) -> Option<Balance> {
    let currency = item.get("a")?.as_str()?.to_string();
    let total = parse_str_f64(item.get("wb")).unwrap_or(0.0);
    let free = parse_str_f64(item.get("cw")).unwrap_or(total);
    Some(Balance {
        currency,
        total,
        free,
        used: (total - free).max(0.0),
        market_type,
    })
}

fn parse_account_position_update(item: &Value, market: Option<MarketQuote>) -> Option<Position> {
    let raw_symbol = item.get("s")?.as_str()?;
    let symbol = to_standard_symbol(raw_symbol);
    let position_side = item
        .get("ps")
        .and_then(|field| field.as_str())
        .unwrap_or("BOTH")
        .to_string();
    let raw_qty = parse_str_f64(item.get("pa")).unwrap_or(0.0);
    let signed_qty = match position_side.as_str() {
        "LONG" => raw_qty.abs(),
        "SHORT" => -raw_qty.abs(),
        _ => raw_qty,
    };
    let entry_price = parse_str_f64(item.get("ep")).unwrap_or(0.0);
    let unrealized_pnl = parse_str_f64(item.get("up")).unwrap_or(0.0);
    let mark_price = if signed_qty.abs() > 1e-9 {
        (entry_price + unrealized_pnl / signed_qty).max(0.0)
    } else if same_symbol(&symbol, "DOGE/USDC") {
        market.map(|quote| quote.mid).unwrap_or(entry_price)
    } else {
        entry_price
    };

    Some(Position {
        symbol,
        side: position_side,
        contracts: raw_qty.abs(),
        contract_size: 1.0,
        entry_price,
        mark_price,
        unrealized_pnl,
        percentage: 0.0,
        margin: 0.0,
        margin_ratio: 0.0,
        leverage: None,
        margin_type: item
            .get("mt")
            .and_then(|field| field.as_str())
            .map(|value| value.to_string()),
        size: signed_qty,
        amount: signed_qty,
        timestamp: Utc::now(),
    })
}

fn merge_balance(balances: &mut Vec<Balance>, balance: Balance) {
    if let Some(existing) = balances
        .iter_mut()
        .find(|existing| existing.currency.eq_ignore_ascii_case(&balance.currency))
    {
        *existing = balance;
    } else {
        balances.push(balance);
    }
}

fn merge_position(positions: &mut Vec<Position>, position: Position) {
    let key_symbol = position.symbol.clone();
    let key_side = position.side.clone();
    if position.amount.abs() < 1e-9 && position.contracts.abs() < 1e-9 && position.size.abs() < 1e-9
    {
        positions.retain(|existing| !(existing.symbol == key_symbol && existing.side == key_side));
        return;
    }

    if let Some(existing) = positions
        .iter_mut()
        .find(|existing| existing.symbol == key_symbol && existing.side == key_side)
    {
        *existing = position;
    } else {
        positions.push(position);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standardizes_binance_symbols() {
        assert_eq!(to_standard_symbol("DOGEUSDC"), "DOGE/USDC");
        assert_eq!(to_standard_symbol("BTCUSDT"), "BTC/USDT");
        assert_eq!(to_standard_symbol("DOGE/USDC"), "DOGE/USDC");
    }

    #[test]
    fn parses_account_update_position_into_signed_dual_mode_snapshot() {
        let value = serde_json::json!({
            "s": "DOGEUSDC",
            "pa": "1200",
            "ep": "0.145",
            "up": "6",
            "ps": "LONG",
            "mt": "cross"
        });

        let position = parse_account_position_update(
            &value,
            Some(MarketQuote {
                best_bid: 0.149,
                best_ask: 0.151,
                mid: 0.15,
            }),
        )
        .unwrap();

        assert_eq!(position.symbol, "DOGE/USDC");
        assert_eq!(position.side, "LONG");
        assert_eq!(position.amount, 1200.0);
        assert!(position.mark_price > 0.149);
    }
}
