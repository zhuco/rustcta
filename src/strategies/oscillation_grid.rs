/// 震荡网格策略
/// 基于 trend_grid_v2.rs 修改，去掉趋势跟踪部分
/// 专门用于震荡市场的固定网格交易

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, Mutex};
use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::core::{
    types::*,
    error::ExchangeError,
    websocket::{WebSocketClient, ConnectionState, MessageHandler},
    exchange::Exchange,
};
use crate::cta::account_manager::{AccountManager, AccountInfo};
use crate::utils::{generate_order_id, generate_order_id_with_tag};
use crate::analysis::{TradeCollector, TradeData};
use rust_decimal::Decimal;

/// 交易配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingConfig {
    pub config_id: String,
    pub enabled: bool,
    pub account: AccountConfig,
    pub symbol: String,
    pub grid: GridConfig,
    pub market_condition: MarketCondition,  // 替换趋势配置为市场条件
}

/// 账户配置（保持不变）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    pub id: String,
    pub exchange: String,
    pub env_prefix: String,
}

/// 网格配置（简化版，去掉动态调整）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridConfig {
    pub spacing: f64,                // 固定网格间距
    pub spacing_type: String,        // arithmetic/geometric
    pub order_amount: f64,           // 固定每单金额 (USDT)
    pub orders_per_side: u32,        // 每边订单数
    pub max_position: f64,           // 最大持仓
}

/// 市场条件（替代趋势配置）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketCondition {
    pub max_trend_score: f64,        // 最大趋势评分（绝对值），超过则停止
    pub min_volatility: f64,         // 最小波动率
    pub max_volatility: f64,         // 最大波动率
    pub check_interval: u64,         // 检查间隔（秒）
    pub auto_stop: bool,             // 是否自动停止
    pub stop_loss_ratio: f64,        // 止损比例
}

/// 策略配置（顶层）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OscillationGridConfig {
    pub strategy: StrategyInfo,
    pub trading_configs: Vec<TradingConfig>,
    pub batch_settings: BatchSettings,
    pub grid_management: GridManagement,
    pub websocket: WebSocketConfig,
    pub risk_control: RiskControl,
    pub execution: ExecutionConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyInfo {
    pub name: String,
    pub version: String,
    pub enabled: bool,
    pub strategy_type: String,
    pub market_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSettings {
    pub binance_batch_size: usize,
    pub okx_batch_size: usize,
    pub hyperliquid_batch_size: usize,
    pub default_batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridManagement {
    pub check_interval: u32,
    pub rebalance_threshold: f64,
    pub cancel_and_replace: bool,
    pub show_grid_status: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    pub subscribe_order_updates: bool,
    pub subscribe_trade_updates: bool,
    pub subscribe_ticker: bool,
    pub reconnect_on_disconnect: bool,
    pub heartbeat_interval: u32,
    pub log_all_trades: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskControl {
    pub max_leverage: f64,
    pub max_drawdown: f64,
    pub daily_loss_limit: f64,
    pub position_limit_per_symbol: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub startup_cancel_all: bool,
    pub shutdown_cancel_all: bool,
    pub thread_per_config: bool,
    pub startup_delay: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub file: String,
    pub console: bool,
    pub show_pnl: bool,
    pub show_position: bool,
}

/// 策略状态
#[derive(Debug, Clone)]
struct GridState {
    is_running: bool,
    grid_active: bool,
    center_price: f64,
    
    // 网格订单
    buy_orders: HashMap<String, GridOrder>,
    sell_orders: HashMap<String, GridOrder>,
    
    // 缓存
    order_cache: OrderCache,
    
    // 统计
    total_trades: u32,
    total_profit: f64,
    position_quantity: f64,
    position_value: f64,
    
    // 市场状态
    last_price: f64,
    last_trend_score: f64,
    last_check_time: DateTime<Utc>,
    
    // 风控
    daily_loss: f64,
    max_drawdown: f64,
    consecutive_losses: u32,
    
    // 标志
    need_grid_reset: bool,
    last_grid_reset: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct GridOrder {
    order_id: String,
    client_order_id: String,
    price: f64,
    quantity: f64,
    side: OrderSide,
    status: OrderStatus,
    placed_at: DateTime<Utc>,
}

/// 订单缓存
#[derive(Debug, Clone)]
struct OrderCache {
    open_orders: HashMap<String, Vec<Order>>,
    last_update: HashMap<String, DateTime<Utc>>,
}

/// 震荡网格策略主体
pub struct OscillationGrid {
    config: TradingConfig,
    account_manager: Arc<AccountManager>,
    exchange: Arc<dyn Exchange>,
    state: Arc<RwLock<GridState>>,
    ws_client: Option<Arc<Mutex<WebSocketClient>>>,
    user_stream_client: Option<Arc<Mutex<WebSocketClient>>>,
    trade_collector: Option<Arc<RwLock<TradeCollector>>>,
    running: Arc<RwLock<bool>>,
}

impl OscillationGrid {
    /// 创建新的震荡网格策略
    pub async fn new(
        config: TradingConfig,
        account_manager: Arc<AccountManager>,
    ) -> Result<Self, ExchangeError> {
        // 获取交易所实例
        let exchange = account_manager.get_exchange(&config.account.exchange)?;
        
        let state = GridState {
            is_running: false,
            grid_active: false,
            center_price: 0.0,
            buy_orders: HashMap::new(),
            sell_orders: HashMap::new(),
            order_cache: OrderCache {
                open_orders: HashMap::new(),
                last_update: HashMap::new(),
            },
            total_trades: 0,
            total_profit: 0.0,
            position_quantity: 0.0,
            position_value: 0.0,
            last_price: 0.0,
            last_trend_score: 0.0,
            last_check_time: Utc::now(),
            daily_loss: 0.0,
            max_drawdown: 0.0,
            consecutive_losses: 0,
            need_grid_reset: false,
            last_grid_reset: Utc::now(),
        };
        
        Ok(Self {
            config,
            account_manager,
            exchange,
            state: Arc::new(RwLock::new(state)),
            ws_client: None,
            user_stream_client: None,
            trade_collector: None,
            running: Arc::new(RwLock::new(false)),
        })
    }
    
    /// 启动策略
    pub async fn start(&mut self) -> Result<(), ExchangeError> {
        log::info!("🚀 启动震荡网格策略: {}", self.config.config_id);
        
        *self.running.write().await = true;
        
        // 取消所有现有订单
        if let Err(e) = self.cancel_all_orders().await {
            log::warn!("⚠️ 取消现有订单失败: {}", e);
        }
        
        // 初始化WebSocket连接
        self.init_websocket().await?;
        
        // 初始化用户数据流
        self.init_user_stream().await?;
        
        // 获取当前价格并初始化网格
        let ticker = self.exchange.get_ticker(&self.config.symbol).await?;
        let current_price = ticker.last_price;
        
        {
            let mut state = self.state.write().await;
            state.center_price = current_price;
            state.last_price = current_price;
            state.is_running = true;
        }
        
        // 放置初始网格订单
        self.place_grid_orders(current_price).await?;
        
        // 启动主循环
        let config = self.config.clone();
        let state = self.state.clone();
        let exchange = self.exchange.clone();
        let running = self.running.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                tokio::time::Duration::from_secs(config.market_condition.check_interval)
            );
            
            while *running.read().await {
                interval.tick().await;
                
                // 检查市场条件
                if config.market_condition.auto_stop {
                    // TODO: 从市场分析器获取趋势评分
                    // 如果趋势评分超过阈值，停止策略
                }
                
                // 检查是否需要重置网格
                let need_reset = state.read().await.need_grid_reset;
                if need_reset {
                    log::info!("📊 重置网格");
                    // TODO: 实现网格重置逻辑
                }
            }
        });
        
        log::info!("✅ 震荡网格策略启动成功");
        Ok(())
    }
    
    /// 停止策略
    pub async fn stop(&mut self) -> Result<(), ExchangeError> {
        log::info!("🛑 停止震荡网格策略: {}", self.config.config_id);
        
        *self.running.write().await = false;
        
        // 取消所有订单
        self.cancel_all_orders().await?;
        
        // 关闭WebSocket连接
        if let Some(ws) = &self.ws_client {
            let ws = ws.lock().await;
            ws.disconnect().await?;
        }
        
        if let Some(ws) = &self.user_stream_client {
            let ws = ws.lock().await;
            ws.disconnect().await?;
        }
        
        // 更新状态
        {
            let mut state = self.state.write().await;
            state.is_running = false;
            state.grid_active = false;
        }
        
        log::info!("✅ 震荡网格策略已停止");
        Ok(())
    }
    
    /// 放置网格订单
    async fn place_grid_orders(&mut self, center_price: f64) -> Result<(), ExchangeError> {
        log::info!("📊 开始放置网格订单，中心价格: {:.4}", center_price);
        
        let mut buy_orders = Vec::new();
        let mut sell_orders = Vec::new();
        
        // 计算网格价格
        for i in 1..=self.config.grid.orders_per_side {
            let spacing_multiplier = self.config.grid.spacing * i as f64;
            
            // 买单价格（下方）
            let buy_price = if self.config.grid.spacing_type == "arithmetic" {
                center_price * (1.0 - spacing_multiplier)
            } else {
                center_price * (1.0 - self.config.grid.spacing).powi(i as i32)
            };
            
            // 卖单价格（上方）
            let sell_price = if self.config.grid.spacing_type == "arithmetic" {
                center_price * (1.0 + spacing_multiplier)
            } else {
                center_price * (1.0 + self.config.grid.spacing).powi(i as i32)
            };
            
            // 固定订单金额
            let buy_quantity = self.config.grid.order_amount / buy_price;
            let sell_quantity = self.config.grid.order_amount / sell_price;
            
            buy_orders.push((buy_price, buy_quantity));
            sell_orders.push((sell_price, sell_quantity));
        }
        
        // 批量下单
        let mut state = self.state.write().await;
        
        // 下买单
        for (price, quantity) in buy_orders {
            match self.place_limit_order(OrderSide::Buy, price, quantity, false).await {
                Ok(order) => {
                    let grid_order = GridOrder {
                        order_id: order.order_id.clone(),
                        client_order_id: order.client_order_id.clone().unwrap_or_default(),
                        price,
                        quantity,
                        side: OrderSide::Buy,
                        status: order.status,
                        placed_at: Utc::now(),
                    };
                    state.buy_orders.insert(order.order_id.clone(), grid_order);
                    log::info!("✅ 买单下单成功: 价格={:.4}, 数量={:.4}", price, quantity);
                }
                Err(e) => {
                    log::error!("❌ 买单下单失败: {}", e);
                }
            }
        }
        
        // 下卖单
        for (price, quantity) in sell_orders {
            match self.place_limit_order(OrderSide::Sell, price, quantity, false).await {
                Ok(order) => {
                    let grid_order = GridOrder {
                        order_id: order.order_id.clone(),
                        client_order_id: order.client_order_id.clone().unwrap_or_default(),
                        price,
                        quantity,
                        side: OrderSide::Sell,
                        status: order.status,
                        placed_at: Utc::now(),
                    };
                    state.sell_orders.insert(order.order_id.clone(), grid_order);
                    log::info!("✅ 卖单下单成功: 价格={:.4}, 数量={:.4}", price, quantity);
                }
                Err(e) => {
                    log::error!("❌ 卖单下单失败: {}", e);
                }
            }
        }
        
        state.grid_active = true;
        log::info!("✅ 网格订单放置完成: {} 买单, {} 卖单", 
                  state.buy_orders.len(), state.sell_orders.len());
        
        Ok(())
    }
    
    /// 下限价单
    async fn place_limit_order(
        &self,
        side: OrderSide,
        price: f64,
        quantity: f64,
        reduce_only: bool,
    ) -> Result<Order, ExchangeError> {
        let order_request = OrderRequest {
            symbol: self.config.symbol.clone(),
            side,
            order_type: OrderType::Limit,
            quantity,
            price: Some(price),
            time_in_force: Some(TimeInForce::GTC),
            reduce_only,
            post_only: true,
            client_order_id: Some(generate_order_id_with_tag("OSC")),
        };
        
        self.exchange.place_order(order_request).await
    }
    
    /// 取消所有订单
    async fn cancel_all_orders(&self) -> Result<(), ExchangeError> {
        log::info!("🚫 取消所有订单");
        
        let open_orders = self.exchange.get_open_orders(&self.config.symbol).await?;
        
        for order in open_orders {
            if let Err(e) = self.exchange.cancel_order(&self.config.symbol, &order.order_id).await {
                log::warn!("⚠️ 取消订单 {} 失败: {}", order.order_id, e);
            }
        }
        
        // 清空本地订单记录
        let mut state = self.state.write().await;
        state.buy_orders.clear();
        state.sell_orders.clear();
        
        Ok(())
    }
    
    /// 处理订单成交
    async fn handle_order_fill(&self, order: &Order) -> Result<(), ExchangeError> {
        let mut state = self.state.write().await;
        
        // 更新统计
        state.total_trades += 1;
        
        // 更新持仓
        let fill_quantity = order.filled_quantity.unwrap_or(0.0);
        let fill_price = order.avg_fill_price.unwrap_or(order.price.unwrap_or(0.0));
        
        match order.side {
            OrderSide::Buy => {
                state.position_quantity += fill_quantity;
                state.position_value += fill_quantity * fill_price;
                
                // 移除已成交的买单
                state.buy_orders.remove(&order.order_id);
                
                // 补充新的买单（价格更低）
                let new_price = fill_price * (1.0 - self.config.grid.spacing);
                let new_quantity = self.config.grid.order_amount / new_price;
                
                drop(state);
                if let Ok(new_order) = self.place_limit_order(OrderSide::Buy, new_price, new_quantity, false).await {
                    let mut state = self.state.write().await;
                    let grid_order = GridOrder {
                        order_id: new_order.order_id.clone(),
                        client_order_id: new_order.client_order_id.clone().unwrap_or_default(),
                        price: new_price,
                        quantity: new_quantity,
                        side: OrderSide::Buy,
                        status: new_order.status,
                        placed_at: Utc::now(),
                    };
                    state.buy_orders.insert(new_order.order_id, grid_order);
                    log::info!("📈 买单成交，补充新买单: 价格={:.4}", new_price);
                }
            }
            OrderSide::Sell => {
                state.position_quantity -= fill_quantity;
                state.position_value -= fill_quantity * fill_price;
                
                // 计算利润（简化计算）
                let profit = fill_quantity * self.config.grid.spacing * fill_price;
                state.total_profit += profit;
                
                // 移除已成交的卖单
                state.sell_orders.remove(&order.order_id);
                
                // 补充新的卖单（价格更高）
                let new_price = fill_price * (1.0 + self.config.grid.spacing);
                let new_quantity = self.config.grid.order_amount / new_price;
                
                drop(state);
                if let Ok(new_order) = self.place_limit_order(OrderSide::Sell, new_price, new_quantity, false).await {
                    let mut state = self.state.write().await;
                    let grid_order = GridOrder {
                        order_id: new_order.order_id.clone(),
                        client_order_id: new_order.client_order_id.clone().unwrap_or_default(),
                        price: new_price,
                        quantity: new_quantity,
                        side: OrderSide::Sell,
                        status: new_order.status,
                        placed_at: Utc::now(),
                    };
                    state.sell_orders.insert(new_order.order_id, grid_order);
                    log::info!("📉 卖单成交，补充新卖单: 价格={:.4}, 利润={:.2}", new_price, profit);
                }
            }
        }
        
        Ok(())
    }
    
    /// 初始化WebSocket连接
    async fn init_websocket(&mut self) -> Result<(), ExchangeError> {
        log::info!("🔌 初始化WebSocket连接");
        
        let symbol = self.config.symbol.clone();
        let state = self.state.clone();
        
        // 创建WebSocket客户端
        let mut ws_client = WebSocketClient::new(
            format!("wss://fstream.binance.com/ws/{}@ticker", symbol.to_lowercase().replace("/", "")),
            "OscillationGrid".to_string(),
        );
        
        // 设置消息处理器
        let handler = OscillationGridHandler {
            symbol: symbol.clone(),
            state: state.clone(),
        };
        
        ws_client.set_handler(Arc::new(handler));
        
        // 连接
        ws_client.connect().await?;
        
        self.ws_client = Some(Arc::new(Mutex::new(ws_client)));
        
        Ok(())
    }
    
    /// 初始化用户数据流
    async fn init_user_stream(&mut self) -> Result<(), ExchangeError> {
        log::info!("🔌 初始化用户数据流");
        
        // 创建监听密钥
        if let Some(binance_exchange) = self.exchange.as_any().downcast_ref::<crate::exchanges::binance::BinanceExchange>() {
            let listen_key = binance_exchange.create_listen_key_with_auto_renewal(MarketType::Futures).await?;
            
            let mut user_stream_client = WebSocketClient::new(
                format!("wss://fstream.binance.com/ws/{}", listen_key),
                "OscillationGridUserStream".to_string(),
            );
            
            // 设置消息处理器
            let handler = UserStreamHandler {
                symbol: self.config.symbol.clone(),
                state: self.state.clone(),
                exchange: self.exchange.clone(),
            };
            
            user_stream_client.set_handler(Arc::new(handler));
            user_stream_client.connect().await?;
            
            self.user_stream_client = Some(Arc::new(Mutex::new(user_stream_client)));
        }
        
        Ok(())
    }
}

/// WebSocket消息处理器
struct OscillationGridHandler {
    symbol: String,
    state: Arc<RwLock<GridState>>,
}

#[async_trait]
impl MessageHandler for OscillationGridHandler {
    async fn handle_message(&self, message: &str) -> Result<(), ExchangeError> {
        // 处理ticker更新
        if message.contains("24hrTicker") {
            if let Ok(ticker_data) = serde_json::from_str::<serde_json::Value>(message) {
                if let Some(price) = ticker_data["c"].as_str() {
                    if let Ok(price) = price.parse::<f64>() {
                        let mut state = self.state.write().await;
                        state.last_price = price;
                    }
                }
            }
        }
        Ok(())
    }
    
    async fn on_connect(&self) -> Result<(), ExchangeError> {
        log::info!("✅ WebSocket已连接: {}", self.symbol);
        Ok(())
    }
    
    async fn on_disconnect(&self) -> Result<(), ExchangeError> {
        log::warn!("⚠️ WebSocket已断开: {}", self.symbol);
        Ok(())
    }
}

/// 用户数据流处理器
struct UserStreamHandler {
    symbol: String,
    state: Arc<RwLock<GridState>>,
    exchange: Arc<dyn Exchange>,
}

#[async_trait]
impl MessageHandler for UserStreamHandler {
    async fn handle_message(&self, message: &str) -> Result<(), ExchangeError> {
        // 处理订单更新
        if message.contains("ORDER_TRADE_UPDATE") {
            if let Ok(event) = serde_json::from_str::<serde_json::Value>(message) {
                if let Some(order_data) = event["o"].as_object() {
                    // 解析订单状态
                    let order_id = order_data["i"].as_str().unwrap_or("");
                    let status = order_data["X"].as_str().unwrap_or("");
                    
                    if status == "FILLED" {
                        // 订单完全成交
                        log::info!("📊 订单成交: {}", order_id);
                        
                        // 构造Order对象
                        let order = Order {
                            order_id: order_id.to_string(),
                            client_order_id: order_data["c"].as_str().map(|s| s.to_string()),
                            symbol: order_data["s"].as_str().unwrap_or("").to_string(),
                            side: if order_data["S"].as_str().unwrap_or("") == "BUY" {
                                OrderSide::Buy
                            } else {
                                OrderSide::Sell
                            },
                            order_type: OrderType::Limit,
                            quantity: order_data["q"].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                            price: order_data["p"].as_str().map(|p| p.parse().unwrap_or(0.0)),
                            status: OrderStatus::Filled,
                            filled_quantity: order_data["z"].as_str().map(|q| q.parse().unwrap_or(0.0)),
                            avg_fill_price: order_data["ap"].as_str().map(|p| p.parse().unwrap_or(0.0)),
                            created_at: Utc::now(),
                            updated_at: Utc::now(),
                            time_in_force: None,
                            reduce_only: false,
                            post_only: false,
                        };
                        
                        // 处理成交
                        // TODO: 需要在OscillationGrid中暴露handle_order_fill方法
                    }
                }
            }
        }
        Ok(())
    }
    
    async fn on_connect(&self) -> Result<(), ExchangeError> {
        log::info!("✅ 用户数据流已连接");
        Ok(())
    }
    
    async fn on_disconnect(&self) -> Result<(), ExchangeError> {
        log::warn!("⚠️ 用户数据流已断开");
        Ok(())
    }
}