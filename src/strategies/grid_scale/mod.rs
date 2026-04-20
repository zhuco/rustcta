use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, RwLock};

use crate::core::types::{
    MarketType, Order, OrderBook, OrderRequest, OrderSide, OrderStatus, OrderType, Ticker, Trade,
};
use crate::core::websocket::{BaseWebSocketClient, WebSocketClient};
use crate::cta::account_manager::{AccountInfo, AccountManager};
use crate::strategies::common::application::strategy::{Strategy as AppStrategy, StrategyInstance};
use crate::strategies::common::{
    StrategyDeps, StrategyState, StrategyStatus, UnifiedRiskEvaluator,
};
use crate::strategies::Strategy as LegacyStrategy;

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
pub struct GridScaleConfig {
    pub strategy: StrategyMeta,
    pub account: StrategyAccount,
    pub symbol: String,
    #[serde(default = "default_total_quote")]
    pub total_amount_quote: f64,
    /// 基础档距（bps，单边），相对 mid
    #[serde(default = "default_base_spread_bps")]
    pub base_spread_bps: f64,
    /// 网格步长（bps，单边）
    #[serde(default = "default_grid_step_bps")]
    pub grid_step_bps: f64,
    /// 每侧档位数上限
    #[serde(default = "default_max_levels_per_side")]
    pub levels_per_side: usize,
    /// 激活带（mid 上下的相对比例）
    #[serde(default = "default_activation_bounds")]
    pub activation_bounds: f64,
    /// 最小档距（可选），相对 mid 的比例
    #[serde(default)]
    pub min_spread_between_orders: Option<f64>,
    /// 最小下单量
    #[serde(default)]
    pub min_order_amount: Option<f64>,
    /// 总挂单上限
    #[serde(default = "default_max_open_orders")]
    pub max_open_orders: usize,
    /// 网格刷新周期（秒）
    #[serde(default = "default_update_interval")]
    pub grid_update_interval: u64,
    /// 趋势偏移灵敏度（每单位趋势偏移 bps 增减）
    #[serde(default = "default_trend_sensitivity")]
    pub trend_sensitivity_bps: f64,
    /// 库存 skew 强度（按仓位占 max_inventory 的比例调整价差）
    #[serde(default = "default_inventory_skew")]
    pub inventory_skew_strength: f64,
    /// 最大库存（基础币）
    #[serde(default = "default_max_inventory")]
    pub max_inventory: f64,
}

fn default_base_spread_bps() -> f64 {
    10.0
}

fn default_grid_step_bps() -> f64 {
    5.0
}

fn default_max_levels_per_side() -> usize {
    8
}

fn default_trend_sensitivity() -> f64 {
    5.0
}

fn default_inventory_skew() -> f64 {
    0.2
}

fn default_max_inventory() -> f64 {
    10.0
}

fn default_true() -> bool {
    true
}

fn default_buy() -> OrderSide {
    OrderSide::Buy
}

fn default_open_type() -> OrderType {
    OrderType::Limit
}

fn default_take_profit_type() -> OrderType {
    OrderType::Limit
}

fn default_total_quote() -> f64 {
    1000.0
}

fn default_activation_bounds() -> f64 {
    0.01
}

fn default_max_open_orders() -> usize {
    5
}

fn default_update_interval() -> u64 {
    60
}

#[derive(Debug, Clone)]
struct SymbolPrecision {
    tick_size: f64,
    step_size: f64,
    price_digits: u32,
    qty_digits: u32,
    min_notional: f64,
}

#[derive(Debug, Clone)]
struct GridLevel {
    id: String,
    price: f64,
    quantity: f64,
    side: OrderSide,
}

#[derive(Debug, Clone)]
struct PlacedOrder {
    level_id: String,
    order_id: Option<String>,
    client_id: String,
    side: OrderSide,
    price: f64,
    quantity: f64,
}

#[derive(Default)]
struct SymbolState {
    precision: Option<SymbolPrecision>,
    last_ticker: Option<Ticker>,
    last_orderbook: Option<OrderBook>,
    last_mid: Option<f64>,
    grid_levels: Vec<GridLevel>,
    placed_orders: Vec<PlacedOrder>,
    trend_bias: f64,
    position: f64,
}

pub struct GridScaleStrategy {
    config: GridScaleConfig,
    account: Arc<AccountInfo>,
    account_manager: Arc<AccountManager>,
    risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    state: Arc<RwLock<SymbolState>>,
    running: Arc<Notify>,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl GridScaleStrategy {
    pub fn new(config: GridScaleConfig, deps: StrategyDeps) -> Result<Self> {
        let account_manager = deps.account_manager.clone();
        let account = account_manager
            .get_account(&config.account.account_id)
            .ok_or_else(|| anyhow!("account {} not found", config.account.account_id))?;

        Ok(Self {
            config,
            account,
            account_manager,
            risk_evaluator: deps.risk_evaluator.clone(),
            state: Arc::new(RwLock::new(SymbolState::default())),
            running: Arc::new(Notify::new()),
            tasks: Arc::new(Mutex::new(Vec::new())),
        })
    }

    async fn init_precision(&self) -> Result<()> {
        let info = self
            .account
            .exchange
            .get_symbol_info(&self.config.symbol, self.config.account.market_type)
            .await?;
        let precision = SymbolPrecision {
            tick_size: info.tick_size,
            step_size: info.step_size,
            price_digits: infer_digits(info.tick_size),
            qty_digits: infer_digits(info.step_size),
            min_notional: info.min_notional.unwrap_or(0.0),
        };
        let mut state = self.state.write().await;
        state.precision = Some(precision);
        Ok(())
    }

    async fn run_public_stream(self: Arc<Self>) {
        let stream_symbol = self.config.symbol.replace('/', "").to_lowercase();
        let params = vec![
            format!("{}@ticker", stream_symbol),
            format!("{}@depth5@100ms", stream_symbol),
        ];
        let channels = params
            .iter()
            .map(|p| format!(r#""{}""#, p))
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
            let mut client = BaseWebSocketClient::new(ws_url.to_string(), "binance".to_string());
            if let Err(err) = client.connect().await {
                log::warn!("public ws connect failed: {}", err);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
            if let Err(err) = client.send(payload.clone()).await {
                log::warn!("public ws subscribe failed: {}", err);
                let _ = client.disconnect().await;
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
            log::info!("公共行情流已连接 {}", ws_url);
            loop {
                match client.receive().await {
                    Ok(Some(msg)) => {
                        if let Err(err) = self.handle_public_ws(&msg).await {
                            log::debug!("解析公共行情失败: {}", err);
                        }
                    }
                    Ok(None) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
                    Err(err) => {
                        log::warn!("公共行情流错误: {}", err);
                        break;
                    }
                }
            }
        }
    }

    async fn run_user_stream(self: Arc<Self>) {
        loop {
            let listen_key = match self
                .account
                .exchange
                .create_user_data_stream(self.config.account.market_type)
                .await
            {
                Ok(key) => key,
                Err(err) => {
                    log::warn!("获取listen_key失败: {}", err);
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    continue;
                }
            };

            let ws_url = match self.config.account.market_type {
                MarketType::Spot => format!("wss://stream.binance.com:9443/ws/{}", listen_key),
                MarketType::Futures => format!("wss://fstream.binance.com/ws/{}", listen_key),
            };

            let mut client = BaseWebSocketClient::new(ws_url.clone(), "binance".to_string());
            if let Err(err) = client.connect().await {
                log::warn!("用户流连接失败: {}", err);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
            log::info!("用户流已连接 {}", ws_url);

            loop {
                match client.receive().await {
                    Ok(Some(msg)) => {
                        if let Err(err) = self.handle_user_event(&msg).await {
                            log::debug!("用户流处理失败: {}", err);
                        }
                    }
                    Ok(None) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
                    Err(err) => {
                        log::warn!("用户流接收错误: {}", err);
                        break;
                    }
                }
            }
        }
    }

    async fn grid_loop(self: Arc<Self>) {
        let mut last_update =
            Utc::now() - Duration::seconds(self.config.grid_update_interval as i64);
        loop {
            let now = Utc::now();
            if (now - last_update).num_seconds() as u64 >= self.config.grid_update_interval {
                last_update = now;
                if let Err(err) = self.refresh_levels_and_orders().await {
                    log::warn!("刷新网格失败: {}", err);
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    async fn refresh_levels_and_orders(&self) -> Result<()> {
        let mid = {
            let mut state = self.state.write().await;
            let mid = Self::calc_mid(state.last_orderbook.as_ref(), state.last_ticker.as_ref());
            state.last_mid = mid;
            mid
        };
        let Some(mid) = mid else {
            return Ok(());
        };

        let precision = {
            let state = self.state.read().await;
            state
                .precision
                .clone()
                .ok_or_else(|| anyhow!("precision not ready"))?
        };

        let grid_levels = self.compute_levels(mid, &precision).await?;
        {
            let mut state = self.state.write().await;
            state.grid_levels = grid_levels.clone();
        }

        let allowed = self.filter_levels(&grid_levels, mid);
        self.reconcile_orders(&allowed, &precision).await?;
        Ok(())
    }

    fn filter_levels(&self, levels: &[GridLevel], mid: f64) -> Vec<GridLevel> {
        let band = self.config.activation_bounds.max(0.0);
        let buy_min = mid * (1.0 - band);
        let sell_max = mid * (1.0 + band);

        let mut buys: Vec<_> = levels
            .iter()
            .filter(|lvl| lvl.side == OrderSide::Buy && lvl.price >= buy_min && lvl.price <= mid)
            .cloned()
            .collect();
        let mut sells: Vec<_> = levels
            .iter()
            .filter(|lvl| lvl.side == OrderSide::Sell && lvl.price <= sell_max && lvl.price >= mid)
            .cloned()
            .collect();

        buys.sort_by(|a, b| {
            (mid - a.price)
                .abs()
                .partial_cmp(&(mid - b.price).abs())
                .unwrap()
        });
        sells.sort_by(|a, b| {
            (a.price - mid)
                .abs()
                .partial_cmp(&(b.price - mid).abs())
                .unwrap()
        });

        let per_side_cap = (self.config.max_open_orders / 2).max(1);
        buys.truncate(per_side_cap);
        sells.truncate(per_side_cap);

        buys.into_iter().chain(sells.into_iter()).collect()
    }

    async fn compute_levels(
        &self,
        mid: f64,
        precision: &SymbolPrecision,
    ) -> Result<Vec<GridLevel>> {
        let mut levels = Vec::new();
        let step_tick = precision.tick_size;
        let step_between = self
            .config
            .min_spread_between_orders
            .map(|s| s * mid)
            .unwrap_or(step_tick);
        let base_spread = self.config.base_spread_bps * mid / 10_000.0;
        let grid_step = self.config.grid_step_bps * mid / 10_000.0;
        let step = step_tick.max(step_between);
        let min_qty = if let Some(m) = self.config.min_order_amount {
            m
        } else {
            (precision.min_notional / mid).max(precision.step_size)
        };

        let (trend_bias, position) = {
            let state = self.state.read().await;
            (state.trend_bias, state.position)
        };
        let inv_ratio = (position / self.config.max_inventory).clamp(-1.0, 1.0);
        let inv_offset = inv_ratio * self.config.inventory_skew_strength * base_spread;
        let trend_offset = trend_bias * self.config.trend_sensitivity_bps * mid / 10_000.0;

        let buy_base = base_spread - inv_offset - trend_offset;
        let sell_base = base_spread + inv_offset + trend_offset;

        for i in 0..self.config.levels_per_side {
            let step_mult = i as f64;
            let buy_price = mid - (buy_base + grid_step * step_mult);
            let sell_price = mid + (sell_base + grid_step * step_mult);

            let buy_price_q = quantize(buy_price, step, precision.price_digits);
            let sell_price_q = quantize(sell_price, step, precision.price_digits);
            let qty = quantize(min_qty, precision.step_size, precision.qty_digits);

            if qty > 0.0 && buy_price_q > 0.0 {
                levels.push(GridLevel {
                    id: format!("B{}", i),
                    price: buy_price_q,
                    quantity: qty,
                    side: OrderSide::Buy,
                });
            }
            if qty > 0.0 && sell_price_q > 0.0 {
                levels.push(GridLevel {
                    id: format!("S{}", i),
                    price: sell_price_q,
                    quantity: qty,
                    side: OrderSide::Sell,
                });
            }
        }

        Ok(levels)
    }

    async fn reconcile_orders(
        &self,
        allowed: &[GridLevel],
        precision: &SymbolPrecision,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        let symbol = self.config.symbol.clone();
        let allowed_ids: HashSet<_> = allowed.iter().map(|l| l.id.clone()).collect();

        // collect stale for cancellation
        let mut stale: Vec<PlacedOrder> = Vec::new();
        state.placed_orders.retain(|po| {
            if allowed_ids.contains(&po.level_id) {
                true
            } else {
                stale.push(po.clone());
                false
            }
        });
        drop(state);
        for order in stale {
            let _ = self.cancel_order(&symbol, &order).await;
        }

        let mut state = self.state.write().await;

        for level in allowed {
            let desired_side = level.side;
            let desired_price = level.price;
            let desired_qty = level.quantity;
            if desired_qty <= 0.0 || desired_price * desired_qty < precision.min_notional {
                continue;
            }
            if let Some(existing) = state
                .placed_orders
                .iter()
                .find(|o| o.level_id == level.id)
                .cloned()
            {
                let price_diff = (existing.price - desired_price).abs();
                let qty_diff = (existing.quantity - desired_qty).abs();
                if price_diff < precision.tick_size * 0.5 && qty_diff < precision.step_size * 0.5 {
                    continue;
                } else {
                    let _ = self.cancel_order(&symbol, &existing).await;
                    state
                        .placed_orders
                        .retain(|o| o.level_id != level.id || o.client_id != existing.client_id);
                }
            }
            if let Some(order) = self
                .place_order(&symbol, level, desired_side, desired_price, desired_qty)
                .await?
            {
                state.placed_orders.push(order);
            }
        }
        Ok(())
    }

    async fn place_order(
        &self,
        symbol: &str,
        level: &GridLevel,
        side: OrderSide,
        price: f64,
        qty: f64,
    ) -> Result<Option<PlacedOrder>> {
        let client_id: String = format!(
            "gs-{}-{}-{}",
            symbol.replace('/', "").to_lowercase(),
            level.id,
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(4)
                .map(char::from)
                .collect::<String>()
        );
        let mut req = OrderRequest::new(
            symbol.to_string(),
            side,
            OrderType::Limit,
            qty,
            Some(price),
            self.config.account.market_type,
        );
        req.client_order_id = Some(client_id.clone());
        req.post_only = Some(true);
        if self.config.account.market_type == MarketType::Futures {
            req.time_in_force = Some("GTX".to_string());
        } else {
            req.time_in_force = Some("GTC".to_string());
        }
        match self.account.exchange.create_order(req).await {
            Ok(order) => Ok(Some(PlacedOrder {
                level_id: level.id.clone(),
                order_id: Some(order.id),
                client_id,
                side,
                price,
                quantity: qty,
            })),
            Err(err) => {
                log::warn!("提交订单失败 {}: {}", level.id, err);
                Ok(None)
            }
        }
    }

    async fn cancel_order(&self, symbol: &str, order: &PlacedOrder) -> Result<()> {
        if let Some(id) = &order.order_id {
            self.account
                .exchange
                .cancel_order(id, symbol, self.config.account.market_type)
                .await
                .map_err(|e| anyhow!("cancel_order failed: {}", e))?;
        }
        Ok(())
    }

    async fn handle_public_ws(&self, payload: &str) -> Result<()> {
        let value: Value = serde_json::from_str(payload)?;
        let data = value.get("data").unwrap_or(&value);
        let event_type = data.get("e").and_then(|v| v.as_str()).unwrap_or("");
        match event_type {
            "24hrTicker" => {
                let ticker = Self::parse_ticker(&self.config.symbol, data)?;
                let mut state = self.state.write().await;
                state.last_ticker = Some(ticker);
                // 简单趋势估计：使用 ticker 24h 变化作为方向
                state.trend_bias = Self::parse_num(data.get("P")) / 100.0;
            }
            "depthUpdate" => {
                let book = Self::parse_orderbook(&self.config.symbol, data)?;
                let mut state = self.state.write().await;
                state.last_orderbook = Some(book);
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_user_event(&self, payload: &str) -> Result<()> {
        let value: Value = serde_json::from_str(payload)?;
        let event_type = value.get("e").and_then(|v| v.as_str()).unwrap_or("");
        if event_type != "ORDER_TRADE_UPDATE" {
            return Ok(());
        }
        let order_obj = value
            .get("o")
            .ok_or_else(|| anyhow!("用户流缺少订单字段"))?;
        let raw_symbol = order_obj.get("s").and_then(|v| v.as_str()).unwrap_or("");
        let symbol = self.config.symbol.clone();
        if raw_symbol.replace('/', "").to_uppercase() != symbol.replace('/', "").to_uppercase() {
            return Ok(());
        }
        let status = match order_obj.get("X").and_then(|v| v.as_str()) {
            Some("FILLED") => OrderStatus::Closed,
            Some("CANCELED") => OrderStatus::Canceled,
            Some("REJECTED") => OrderStatus::Rejected,
            _ => OrderStatus::Open,
        };
        let order_id = order_obj
            .get("i")
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())
            .unwrap_or_default();
        let client_id = order_obj
            .get("c")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let last_fill_qty = order_obj
            .get("l")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        {
            let mut state = self.state.write().await;
            state
                .placed_orders
                .retain(|o| o.order_id.as_deref() != Some(&order_id) && o.client_id != client_id);
            if let Some(filled) = order_obj
                .get("z")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
            {
                if let Some(avg) = order_obj
                    .get("ap")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    // 以已成交量更新 position 简易统计
                    let signed =
                        if matches!(order_obj.get("S").and_then(|v| v.as_str()), Some("BUY")) {
                            filled
                        } else {
                            -filled
                        };
                    state.position += signed;
                    state.last_mid = Some(avg);
                }
            }
        }
        if last_fill_qty > 0.0 || matches!(status, OrderStatus::Closed | OrderStatus::Rejected) {
            // 触发补单
            tokio::spawn({
                let this = self.clone_for_task();
                async move {
                    let _ = this.refresh_levels_and_orders().await;
                }
            });
        }
        Ok(())
    }

    fn parse_ticker(symbol: &str, data: &Value) -> Result<Ticker> {
        Ok(Ticker {
            symbol: symbol.to_string(),
            high: Self::parse_num(data.get("h")),
            low: Self::parse_num(data.get("l")),
            bid: Self::parse_num(data.get("b")),
            ask: Self::parse_num(data.get("a")),
            last: Self::parse_num(data.get("c")),
            volume: Self::parse_num(data.get("v")),
            timestamp: Self::timestamp_from_ms(data.get("E").and_then(|v| v.as_i64())),
        })
    }

    fn parse_orderbook(symbol: &str, data: &Value) -> Result<OrderBook> {
        let bids = Self::parse_levels(data.get("b"));
        let asks = Self::parse_levels(data.get("a"));
        Ok(OrderBook {
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp: Self::timestamp_from_ms(data.get("E").and_then(|v| v.as_i64())),
            info: serde_json::Value::Null,
        })
    }

    fn parse_levels(value: Option<&Value>) -> Vec<[f64; 2]> {
        value
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|entry| {
                        entry.as_array().and_then(|pair| {
                            let price = Self::parse_num(pair.get(0));
                            let qty = Self::parse_num(pair.get(1));
                            Some([price, qty])
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    fn parse_num(v: Option<&Value>) -> f64 {
        v.and_then(|val| {
            if let Some(n) = val.as_f64() {
                Some(n)
            } else if let Some(s) = val.as_str() {
                s.parse::<f64>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0.0)
    }

    fn timestamp_from_ms(ms: Option<i64>) -> DateTime<Utc> {
        ms.and_then(|v| DateTime::<Utc>::from_timestamp_millis(v))
            .unwrap_or_else(Utc::now)
    }

    fn calc_mid(book: Option<&OrderBook>, ticker: Option<&Ticker>) -> Option<f64> {
        if let Some(b) = book {
            if !b.bids.is_empty() && !b.asks.is_empty() {
                return Some((b.bids[0][0] + b.asks[0][0]) * 0.5);
            }
        }
        ticker.map(|t| (t.bid + t.ask) * 0.5)
    }

    fn clone_for_task(&self) -> Arc<Self> {
        Arc::new(Self {
            config: self.config.clone(),
            account: self.account.clone(),
            account_manager: self.account_manager.clone(),
            risk_evaluator: self.risk_evaluator.clone(),
            state: self.state.clone(),
            running: self.running.clone(),
            tasks: self.tasks.clone(),
        })
    }
}

#[async_trait]
impl StrategyInstance for GridScaleStrategy {
    async fn start(&self) -> Result<()> {
        self.init_precision().await?;

        let public = self.clone_for_task();
        let pub_handle = tokio::spawn(async move {
            public.run_public_stream().await;
        });
        self.tasks.lock().await.push(pub_handle);

        let user = self.clone_for_task();
        let user_handle = tokio::spawn(async move {
            user.run_user_stream().await;
        });
        self.tasks.lock().await.push(user_handle);

        let grid = self.clone_for_task();
        let grid_handle = tokio::spawn(async move {
            grid.grid_loop().await;
        });
        self.tasks.lock().await.push(grid_handle);

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let mut tasks = self.tasks.lock().await;
        for handle in tasks.drain(..) {
            handle.abort();
        }
        let mut state = self.state.write().await;
        for order in state.placed_orders.drain(..) {
            let _ = self.cancel_order(&self.config.symbol, &order).await;
        }
        Ok(())
    }

    async fn status(&self) -> Result<StrategyStatus> {
        let mut status = StrategyStatus::new(self.config.strategy.name.clone());
        status.state = StrategyState::Running;
        Ok(status)
    }
}

#[async_trait]
impl LegacyStrategy for GridScaleStrategy {
    async fn name(&self) -> String {
        self.config.strategy.name.clone()
    }

    async fn on_tick(
        &self,
        _ticker: Ticker,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn on_order_update(
        &self,
        _order: Order,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn on_trade(
        &self,
        _trade: Trade,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn get_status(
        &self,
    ) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok("grid_scale".to_string())
    }
}

impl AppStrategy for GridScaleStrategy {
    type Config = GridScaleConfig;

    fn create(config: Self::Config, deps: StrategyDeps) -> Result<Self> {
        Ok(Self::new(config, deps)?)
    }
}

fn infer_digits(step: f64) -> u32 {
    if step <= 0.0 {
        return 4;
    }
    let mut s = step;
    let mut d = 0;
    while (s.fract() != 0.0) && d < 8 {
        s *= 10.0;
        d += 1;
    }
    d
}

fn quantize(value: f64, step: f64, digits: u32) -> f64 {
    if step > 0.0 {
        let floored = (value / step).floor() * step;
        truncate_digits(floored, digits)
    } else {
        truncate_digits(value, digits)
    }
}

fn truncate_digits(val: f64, digits: u32) -> f64 {
    let factor = 10f64.powi(digits as i32);
    (val * factor).floor() / factor
}

fn linspace(start: f64, end: f64, count: usize) -> Vec<f64> {
    if count == 0 {
        return Vec::new();
    }
    if count == 1 {
        return vec![start];
    }
    let step = (end - start) / (count as f64 - 1.0);
    (0..count).map(|i| start + step * i as f64).collect()
}
