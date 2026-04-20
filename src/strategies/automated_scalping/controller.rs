//! # AS策略 (Automated Scalping)
//!
//! 自动化剥头皮做市策略，专门针对免手续费交易对设计
//!
//! ## 主要特性
//! - 高频小额交易
//! - 动态价差调整
//! - 库存风险管理
//! - 市场微观结构分析
//! - 实时数据处理

use super::config::ASConfig;
use super::engine;
use super::state::{ASStrategyState, IndicatorState, LocalOrderBook, MarketSnapshot, SymbolInfo};
use crate::strategies::common::{
    build_unified_risk_evaluator, RiskAction, RiskDecision, RiskNotifyLevel, StrategyRiskLimits,
    StrategySnapshot, UnifiedRiskEvaluator,
};

use chrono::{DateTime, Duration, Utc};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::analysis::{TradeCollector, TradeData};
use crate::core::{
    error::ExchangeError,
    exchange::Exchange,
    order_cache::OrderCache,
    types::*,
    websocket::{BaseWebSocketClient, WebSocketClient},
};
use crate::cta::account_manager::AccountManager;
use rust_decimal::Decimal;
use std::str::FromStr;

pub struct AutomatedScalpingStrategy {
    config: ASConfig,
    account_manager: Arc<AccountManager>,
    state: Arc<Mutex<ASStrategyState>>,
    market_buffer: Arc<RwLock<VecDeque<MarketSnapshot>>>,
    indicators: Arc<RwLock<IndicatorState>>,
    ws_client: Arc<RwLock<Option<Arc<Mutex<BaseWebSocketClient>>>>>,
    running: Arc<RwLock<bool>>,
    current_price: Arc<RwLock<f64>>,
    orderbook: Arc<RwLock<LocalOrderBook>>,
    collector: Option<Arc<TradeCollector>>,
    symbol_info: Arc<RwLock<Option<SymbolInfo>>>,
    is_dual_mode: Arc<RwLock<bool>>,
    listen_key: Arc<RwLock<Option<String>>>,
    request_weight: Arc<RwLock<u32>>,
    last_weight_reset: Arc<RwLock<DateTime<Utc>>>,
    order_cache: Arc<OrderCache>,
    user_stream_client: Arc<RwLock<Option<Arc<Mutex<BaseWebSocketClient>>>>>,
    last_order_fetch: Arc<RwLock<DateTime<Utc>>>,
    risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    risk_limits: StrategyRiskLimits,
}

impl AutomatedScalpingStrategy {
    /// 创建策略实例
    pub fn new(config: ASConfig, account_manager: Arc<AccountManager>) -> Self {
        Self::with_collector(config, account_manager, None)
    }

    /// 创建带数据收集器的策略实例
    pub fn with_collector(
        config: ASConfig,
        account_manager: Arc<AccountManager>,
        collector: Option<Arc<TradeCollector>>,
    ) -> Self {
        let state = ASStrategyState {
            inventory: 0.0,
            long_position: 0.0,
            short_position: 0.0,
            avg_cost: 0.0,
            long_avg_cost: 0.0,
            short_avg_cost: 0.0,
            active_buy_orders: HashMap::new(),
            active_sell_orders: HashMap::new(),
            total_pnl: 0.0,
            daily_pnl: 0.0,
            trade_count: 0,
            consecutive_losses: 0,
            start_time: Utc::now(),
            last_order_time: Utc::now() - Duration::hours(1),
            orders_this_minute: 0,
            current_minute: Utc::now().timestamp() / 60,
        };

        let indicators = IndicatorState {
            ema_fast: 0.0,
            ema_slow: 0.0,
            rsi: 50.0,
            vwap: 0.0,
            atr: 0.0,
            price_history: VecDeque::with_capacity(200),
            volume_history: VecDeque::with_capacity(200),
            prev_avg_gain: 0.0,
            prev_avg_loss: 0.0,
        };

        let risk_limits = Self::build_risk_limits(&config);
        let risk_evaluator = build_unified_risk_evaluator(
            config.strategy.name.clone(),
            None,
            Some(risk_limits.clone()),
        );

        Self {
            config,
            account_manager,
            state: Arc::new(Mutex::new(state)),
            market_buffer: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            indicators: Arc::new(RwLock::new(indicators)),
            ws_client: Arc::new(RwLock::new(None)),
            running: Arc::new(RwLock::new(false)),
            current_price: Arc::new(RwLock::new(0.0)),
            orderbook: Arc::new(RwLock::new(LocalOrderBook {
                bids: Vec::new(),
                asks: Vec::new(),
                last_update: Utc::now(),
            })),
            collector,
            symbol_info: Arc::new(RwLock::new(None)),
            is_dual_mode: Arc::new(RwLock::new(false)),
            listen_key: Arc::new(RwLock::new(None)),
            request_weight: Arc::new(RwLock::new(0)),
            last_weight_reset: Arc::new(RwLock::new(Utc::now())),
            order_cache: Arc::new(OrderCache::new(600)), // 10分钟缓存
            last_order_fetch: Arc::new(RwLock::new(Utc::now() - Duration::hours(1))),
            user_stream_client: Arc::new(RwLock::new(None)),
            risk_evaluator,
            risk_limits,
        }
    }

    /// 启动策略
    pub async fn start(&self) -> Result<()> {
        log::info!("🚀 ========== 启动AS自动化剥头皮策略 ==========");
        log::info!("📋 策略配置:");
        log::info!("  - 交易对: {}", self.config.trading.symbol);
        log::info!("  - 每单金额: {} USDC", self.config.trading.order_size_usdc);
        log::info!(
            "  - 最大库存: {} {}",
            self.config.trading.max_inventory,
            self.get_base_asset().await
        );
        log::info!(
            "  - 价差范围: {}-{} bp",
            self.config.trading.min_spread_bp,
            self.config.trading.max_spread_bp
        );

        // 初始化用户数据流
        if let Err(e) = self.init_user_stream().await {
            log::warn!("无法初始化用户数据流: {}, 将使用轮询模式", e);
        }

        // 获取交易对信息
        self.fetch_symbol_info().await?;

        // 检查持仓模式
        self.check_position_mode().await?;

        // 设置运行标志
        *self.running.write().await = true;

        // 取消所有现有订单
        log::info!("🔄 取消所有现有订单...");
        if let Err(e) = self.cancel_all_orders().await {
            log::warn!("取消订单时出现警告: {}", e);
        }

        // 初始化订单缓存
        log::info!("📋 初始化订单缓存...");
        if let Err(e) = self.get_cached_open_orders().await {
            log::warn!("初始化订单缓存失败: {}", e);
        }

        // 立即获取当前持仓状态
        log::info!("📦 获取初始持仓状态...");
        if let Err(e) = self.update_position_immediately().await {
            log::warn!("获取初始持仓失败: {}", e);
        }

        // 连接WebSocket
        self.connect_websocket().await?;

        // 启动市场数据处理任务
        let data_processor = self.clone_for_task();
        tokio::spawn(async move {
            if let Err(e) = data_processor.process_market_data().await {
                log::error!("市场数据处理失败: {}", e);
            }
        });

        // 启动技术指标计算任务
        let indicator_calculator = self.clone_for_task();
        tokio::spawn(async move {
            if let Err(e) = indicator_calculator.calculate_indicators().await {
                log::error!("技术指标计算失败: {}", e);
            }
        });

        // 启动主交易循环
        self.run_trading_loop().await?;

        Ok(())
    }

    /// 停止策略
    pub async fn stop(&self) -> Result<()> {
        log::info!("⏹️ 停止AS策略");

        *self.running.write().await = false;

        // 取消所有订单
        self.cancel_all_orders().await?;

        // 平掉所有持仓（如果需要）
        if self.config.trading.market_type == "futures" {
            self.close_all_positions().await?;
        }

        // 断开WebSocket
        if let Some(_ws) = self.ws_client.write().await.take() {
            log::info!("已断开WebSocket连接");
        }

        // 输出统计
        self.print_strategy_statistics().await;

        Ok(())
    }

    /// 获取基础资产名称
    async fn get_base_asset(&self) -> String {
        self.symbol_info
            .read()
            .await
            .as_ref()
            .map(|info| info.base_asset.clone())
            .unwrap_or_else(|| {
                self.config
                    .trading
                    .symbol
                    .split('/')
                    .next()
                    .unwrap_or("TOKEN")
                    .to_string()
            })
    }

    /// 获取交易对信息
    async fn fetch_symbol_info(&self) -> Result<()> {
        log::info!("📋 获取交易对信息...");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| {
                ExchangeError::Other(format!("账户不存在: {}", self.config.account.account_id))
            })?;

        let market_type = match self.config.trading.market_type.as_str() {
            "futures" => MarketType::Futures,
            "spot" => MarketType::Spot,
            _ => MarketType::Futures,
        };

        match account
            .exchange
            .get_symbol_info(&self.config.trading.symbol, market_type)
            .await
        {
            Ok(info) => {
                let parts: Vec<&str> = self.config.trading.symbol.split('/').collect();
                let base_asset = parts.get(0).unwrap_or(&"").to_string();
                let quote_asset = parts.get(1).unwrap_or(&"USDC").to_string();

                let price_precision = engine::calculate_precision(info.tick_size);
                let quantity_precision = engine::calculate_precision(info.step_size);

                let symbol_info = SymbolInfo {
                    base_asset: base_asset.clone(),
                    quote_asset,
                    tick_size: info.tick_size,
                    step_size: info.step_size,
                    min_notional: info.min_notional.unwrap_or(5.0),
                    price_precision,
                    quantity_precision,
                };

                log::info!("✅ 交易对信息:");
                log::info!("  - 基础货币: {}", symbol_info.base_asset);
                log::info!("  - 报价货币: {}", symbol_info.quote_asset);
                log::info!(
                    "  - 价格精度: {} 位小数 (tick_size: {})",
                    symbol_info.price_precision,
                    info.tick_size
                );
                log::info!(
                    "  - 数量精度: {} 位小数 (step_size: {})",
                    symbol_info.quantity_precision,
                    info.step_size
                );
                log::info!(
                    "  - 最小名义价值: {} {}",
                    symbol_info.min_notional,
                    symbol_info.quote_asset
                );

                *self.symbol_info.write().await = Some(symbol_info);
                Ok(())
            }
            Err(e) => {
                log::warn!("⚠️ 无法获取交易对信息: {}，使用配置文件设置", e);

                let parts: Vec<&str> = self.config.trading.symbol.split('/').collect();
                let base_asset = parts.get(0).unwrap_or(&"TOKEN").to_string();
                let quote_asset = parts.get(1).unwrap_or(&"USDC").to_string();

                let symbol_info = SymbolInfo {
                    base_asset,
                    quote_asset,
                    tick_size: 1.0 / 10_f64.powi(self.config.trading.price_precision as i32),
                    step_size: 1.0 / 10_f64.powi(self.config.trading.quantity_precision as i32),
                    min_notional: 5.0,
                    price_precision: self.config.trading.price_precision,
                    quantity_precision: self.config.trading.quantity_precision,
                };

                *self.symbol_info.write().await = Some(symbol_info);
                Ok(())
            }
        }
    }

    /// 计算精度（小数位数）

    /// 检查持仓模式
    async fn check_position_mode(&self) -> Result<()> {
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| {
                ExchangeError::Other(format!("账户不存在: {}", self.config.account.account_id))
            })?;

        if self.config.account.exchange.to_lowercase() == "binance" {
            use crate::exchanges::binance::BinanceExchange;

            if let Some(binance) = account.exchange.as_any().downcast_ref::<BinanceExchange>() {
                let is_dual = binance.get_position_mode().await?;
                *self.is_dual_mode.write().await = is_dual;

                log::info!(
                    "✅ Binance账户 {} 持仓模式: {}",
                    self.config.account.account_id,
                    if is_dual {
                        "双向持仓"
                    } else {
                        "单向持仓"
                    }
                );
            }
        }

        Ok(())
    }

    /// 连接WebSocket
    async fn connect_websocket(&self) -> Result<()> {
        log::info!("📡 连接WebSocket获取实时数据...");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| {
                ExchangeError::Other(format!("账户不存在: {}", self.config.account.account_id))
            })?;

        let ws_url = match self.config.account.exchange.as_str() {
            "binance" => {
                // 首先获取用户数据流的ListenKey
                let listen_key = if self.config.trading.market_type == "futures" {
                    self.create_futures_listen_key().await?
                } else {
                    self.create_spot_listen_key().await?
                };

                *self.listen_key.write().await = Some(listen_key.clone());

                // 启动ListenKey保活任务
                let keeper = self.clone_for_task();
                tokio::spawn(async move {
                    keeper.keep_listen_key_alive().await;
                });

                let symbol = self.config.trading.symbol.to_lowercase().replace("/", "");

                // 组合市场数据流和用户数据流
                if self.config.trading.market_type == "futures" {
                    format!(
                        "wss://fstream.binance.com/stream?streams={}@depth20@100ms/{}@trade/{}",
                        symbol, symbol, listen_key
                    )
                } else {
                    format!(
                        "wss://stream.binance.com:9443/stream?streams={}@depth20@100ms/{}@trade/{}",
                        symbol, symbol, listen_key
                    )
                }
            }
            _ => {
                return Err(ExchangeError::Other(format!(
                    "不支持的交易所: {}",
                    self.config.account.exchange
                )));
            }
        };

        log::info!(
            "📡 WebSocket URL: {}",
            ws_url.replace(&ws_url[ws_url.rfind('/').unwrap_or(0) + 1..], "***")
        );

        let mut ws_client = BaseWebSocketClient::new(ws_url, self.config.account.exchange.clone());

        ws_client.connect().await?;

        let ws_client = Arc::new(Mutex::new(ws_client));
        *self.ws_client.write().await = Some(ws_client);

        log::info!("✅ WebSocket连接成功，已订阅市场数据和用户数据流");
        Ok(())
    }

    /// 创建期货ListenKey
    async fn create_futures_listen_key(&self) -> Result<String> {
        log::info!("🔑 创建期货用户数据流ListenKey...");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 使用trait方法
        let listen_key = account
            .exchange
            .create_user_data_stream(crate::core::types::MarketType::Futures)
            .await?;
        log::info!("✅ ListenKey创建成功");
        Ok(listen_key)
    }

    /// 创建现货ListenKey
    async fn create_spot_listen_key(&self) -> Result<String> {
        log::info!("🔑 创建现货用户数据流ListenKey...");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 使用trait方法
        let listen_key = account
            .exchange
            .create_user_data_stream(crate::core::types::MarketType::Spot)
            .await?;
        log::info!("✅ ListenKey创建成功");
        Ok(listen_key)
    }

    /// 初始化用户数据流
    async fn init_user_stream(&self) -> Result<()> {
        log::info!("🔌 初始化用户数据流WebSocket...");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 创建用户数据流
        let market_type = if self.config.trading.market_type == "futures" {
            MarketType::Futures
        } else {
            MarketType::Spot
        };

        let listen_key = account
            .exchange
            .create_user_data_stream(market_type)
            .await?;
        log::info!("✅ 获得ListenKey: {}...", &listen_key[..8]);

        // 构建WebSocket URL
        let ws_url = match self.config.account.exchange.as_str() {
            "binance" => {
                if market_type == MarketType::Futures {
                    format!("wss://fstream.binance.com/ws/{}", listen_key)
                } else {
                    format!("wss://stream.binance.com:9443/ws/{}", listen_key)
                }
            }
            _ => {
                return Err(ExchangeError::Other(format!(
                    "交易所 {} 不支持用户数据流",
                    self.config.account.exchange
                )));
            }
        };

        // 创建WebSocket连接
        let mut user_stream_client =
            BaseWebSocketClient::new(ws_url.clone(), self.config.account.exchange.clone());

        user_stream_client.connect().await?;
        *self.user_stream_client.write().await = Some(Arc::new(Mutex::new(user_stream_client)));

        // 启动消息处理任务
        let processor = self.clone_for_task();
        let listen_key_clone = listen_key.clone();
        tokio::spawn(async move {
            processor
                .process_user_stream_messages(listen_key_clone)
                .await;
        });

        // 启动ListenKey保活任务
        let keeper = self.clone_for_task();
        let listen_key_clone = listen_key.clone();
        tokio::spawn(async move {
            keeper
                .keep_user_stream_alive(listen_key_clone, market_type)
                .await;
        });

        log::info!("✅ 用户数据流WebSocket连接成功");
        Ok(())
    }

    /// 处理用户数据流消息
    async fn process_user_stream_messages(&self, listen_key: String) {
        log::info!("📨 开始处理用户数据流消息");

        while *self.running.read().await {
            if let Some(client) = &*self.user_stream_client.read().await {
                let mut client_guard = client.lock().await;
                match client_guard.receive().await {
                    Ok(Some(message)) => {
                        if let Err(e) = self.handle_user_stream_message(&message).await {
                            log::error!("处理用户数据流消息失败: {}", e);
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                    Err(e) => {
                        log::error!("接收用户数据流消息失败: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            } else {
                break;
            }
        }

        log::info!("📨 用户数据流消息处理结束");
    }

    /// 处理用户数据流消息
    async fn handle_user_stream_message(&self, message: &str) -> Result<()> {
        let json: serde_json::Value = serde_json::from_str(message)?;

        if let Some(event_type) = json.get("e").and_then(|v| v.as_str()) {
            match event_type {
                "ORDER_TRADE_UPDATE" => {
                    // 订单更新 (期货)
                    self.handle_order_update(&json).await?;
                }
                "executionReport" => {
                    // 订单更新 (现货)
                    self.handle_order_update(&json).await?;
                }
                "ACCOUNT_UPDATE" => {
                    // 账户更新
                    log::debug!("收到账户更新事件");
                }
                _ => {
                    log::debug!("收到未处理的事件类型: {}", event_type);
                }
            }
        }

        Ok(())
    }

    /// 处理订单更新
    async fn handle_order_update(&self, json: &serde_json::Value) -> Result<()> {
        // 解析订单更新数据
        if let Some(order_id) = json.get("c").and_then(|v| v.as_str()) {
            let status = if self.config.trading.market_type == "futures" {
                json.get("o")
                    .and_then(|o| o.get("X"))
                    .and_then(|v| v.as_str())
            } else {
                json.get("X").and_then(|v| v.as_str())
            };

            if let Some(status_str) = status {
                match status_str {
                    "FILLED" => {
                        // 订单成交，从活跃订单中移除
                        let mut state = self.state.lock().await;
                        state.active_buy_orders.remove(order_id);
                        state.active_sell_orders.remove(order_id);
                        state.trade_count += 1;
                        log::info!("📦 订单 {} 已成交", order_id);

                        // 使缓存的活跃订单失效，因为订单已完成
                        self.order_cache
                            .invalidate_open_orders(&self.config.trading.symbol)
                            .await;
                    }
                    "CANCELED" | "EXPIRED" | "REJECTED" => {
                        // 订单取消/过期/拒绝，从活跃订单中移除
                        let mut state = self.state.lock().await;
                        state.active_buy_orders.remove(order_id);
                        state.active_sell_orders.remove(order_id);
                        log::debug!("订单 {} 状态: {}", order_id, status_str);

                        // 使缓存的活跃订单失效，因为订单状态已改变
                        self.order_cache
                            .invalidate_open_orders(&self.config.trading.symbol)
                            .await;
                    }
                    "NEW" => {
                        // 新订单，使缓存失效以便下次获取时包含新订单
                        self.order_cache
                            .invalidate_open_orders(&self.config.trading.symbol)
                            .await;
                    }
                    _ => {}
                }

                // 清除缓存中的该订单，使得下次查询时获取最新状态
                self.order_cache.invalidate_order(order_id).await;
            }
        }

        Ok(())
    }

    /// 保持用户数据流活跃
    async fn keep_user_stream_alive(&self, listen_key: String, market_type: MarketType) {
        log::info!("💓 启动用户数据流保活任务");

        while *self.running.read().await {
            tokio::time::sleep(tokio::time::Duration::from_secs(1800)).await; // 每30分钟

            let account = match self
                .account_manager
                .get_account(&self.config.account.account_id)
            {
                Some(acc) => acc,
                None => continue,
            };

            let result = account
                .exchange
                .keepalive_user_data_stream(&listen_key, market_type)
                .await;

            match result {
                Ok(_) => log::debug!("✅ 用户数据流保活成功"),
                Err(e) => log::error!("❌ 用户数据流保活失败: {}", e),
            }
        }

        log::info!("💔 用户数据流保活任务结束");
    }

    /// 保持ListenKey活跃
    async fn keep_listen_key_alive(&self) {
        log::info!("💓 启动ListenKey保活任务");

        while *self.running.read().await {
            tokio::time::sleep(tokio::time::Duration::from_secs(1800)).await; // 每30分钟

            if let Some(listen_key) = &*self.listen_key.read().await {
                let account = match self
                    .account_manager
                    .get_account(&self.config.account.account_id)
                {
                    Some(acc) => acc,
                    None => continue,
                };

                let market_type = if self.config.trading.market_type == "futures" {
                    crate::core::types::MarketType::Futures
                } else {
                    crate::core::types::MarketType::Spot
                };

                // 使用trait方法
                let result = account
                    .exchange
                    .keepalive_user_data_stream(listen_key, market_type)
                    .await;

                match result {
                    Ok(_) => log::debug!("✅ ListenKey保活成功"),
                    Err(e) => log::error!("❌ ListenKey保活失败: {}", e),
                }
            }
        }

        log::info!("💔 ListenKey保活任务结束");
    }

    /// 处理市场数据
    async fn process_market_data(&self) -> Result<()> {
        log::info!("📊 开始处理市场数据...");

        while *self.running.read().await {
            if let Some(ws) = &*self.ws_client.read().await {
                let mut ws_guard = ws.lock().await;
                match ws_guard.receive().await {
                    Ok(Some(message)) => {
                        if let Ok(ws_msg) = self.parse_websocket_message(&message).await {
                            self.process_ws_message(ws_msg).await?;
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                    Err(e) => {
                        log::error!("WebSocket接收错误: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            } else {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }

        Ok(())
    }

    /// 解析WebSocket消息
    async fn parse_websocket_message(&self, message: &str) -> Result<WsMessage> {
        // 与泊松策略类似的解析逻辑
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(message) {
            // 处理订单更新事件
            if let Some(event_type) = json.get("e").and_then(|e| e.as_str()) {
                if event_type == "ORDER_TRADE_UPDATE" {
                    return self.handle_ws_order_update(&json).await;
                }
            }

            // 处理深度和成交数据
            if let Some(stream) = json.get("stream").and_then(|s| s.as_str()) {
                if let Some(data) = json.get("data") {
                    if stream.contains("trade") {
                        return self.parse_trade_data(data).await;
                    } else if stream.contains("depth") {
                        return self.parse_depth_data(data).await;
                    }
                }
            }
        }

        Ok(WsMessage::Text(message.to_string()))
    }

    /// 处理WebSocket订单更新
    async fn handle_ws_order_update(&self, json: &serde_json::Value) -> Result<WsMessage> {
        let order_data = &json["o"];
        let order_status = order_data["X"].as_str().unwrap_or("");
        let order_id = order_data["c"].as_str().unwrap_or("");
        let side = order_data["S"].as_str().unwrap_or("");
        let price = order_data["p"]
            .as_str()
            .and_then(|p| p.parse::<f64>().ok())
            .unwrap_or(0.0);
        let executed_qty = order_data["z"]
            .as_str()
            .and_then(|q| q.parse::<f64>().ok())
            .unwrap_or(0.0);
        let avg_price = order_data["ap"]
            .as_str()
            .and_then(|p| p.parse::<f64>().ok())
            .unwrap_or(price);

        // 根据订单状态更新本地状态和缓存
        let should_invalidate_cache = match order_status {
            "NEW" => {
                log::debug!("📝 AS策略新订单: {} {} @ {}", order_id, side, price);

                // 创建订单对象
                let order = Order {
                    id: order_id.to_string(),
                    symbol: self.config.trading.symbol.clone(),
                    side: if side == "BUY" {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    },
                    order_type: OrderType::Limit,
                    status: OrderStatus::Open,
                    price: Some(price),
                    amount: executed_qty,
                    filled: 0.0,
                    remaining: executed_qty,
                    timestamp: Utc::now(),
                    market_type: if self.config.trading.market_type == "futures" {
                        MarketType::Futures
                    } else {
                        MarketType::Spot
                    },
                    last_trade_timestamp: None,
                    info: serde_json::json!({}),
                };

                // 添加到活跃订单列表
                let mut state = self.state.lock().await;
                if side == "BUY" {
                    state
                        .active_buy_orders
                        .insert(order_id.to_string(), order.clone());
                } else {
                    state
                        .active_sell_orders
                        .insert(order_id.to_string(), order.clone());
                }
                drop(state);

                true // 需要使缓存失效
            }
            "CANCELED" | "EXPIRED" | "REJECTED" => {
                log::debug!("❌ AS策略订单取消/过期: {} {}", order_id, order_status);

                // 从活跃订单中移除
                let mut state = self.state.lock().await;
                state.active_buy_orders.remove(order_id);
                state.active_sell_orders.remove(order_id);
                drop(state);

                true // 需要使缓存失效
            }
            "FILLED" => {
                log::info!(
                    "✅ AS策略订单完全成交: {} {} {} @ {} x {}",
                    order_id,
                    side,
                    self.config.trading.symbol,
                    avg_price,
                    executed_qty
                );

                let mut state = self.state.lock().await;

                // 更新库存和成本
                if side == "BUY" {
                    let new_inventory = state.inventory + executed_qty;
                    state.avg_cost = if state.inventory > 0.0 {
                        (state.avg_cost * state.inventory + avg_price * executed_qty)
                            / new_inventory
                    } else {
                        avg_price
                    };
                    state.inventory = new_inventory;
                    state.active_buy_orders.remove(order_id);
                } else {
                    state.inventory -= executed_qty;
                    state.active_sell_orders.remove(order_id);

                    // 计算盈亏
                    if state.avg_cost > 0.0 {
                        let pnl = (avg_price - state.avg_cost) * executed_qty;
                        state.total_pnl += pnl;
                        state.daily_pnl += pnl;

                        if pnl < 0.0 {
                            state.consecutive_losses += 1;
                        } else {
                            state.consecutive_losses = 0;
                        }
                    }
                }

                state.trade_count += 1;
                drop(state);

                // 记录成交到数据库
                if let Some(ref collector) = self.collector {
                    let trade_data = TradeData {
                        trade_time: Utc::now(),
                        strategy_name: self.config.strategy.name.clone(),
                        account_id: self.config.account.account_id.clone(),
                        exchange: self.config.account.exchange.clone(),
                        symbol: self.config.trading.symbol.clone(),
                        side: side.to_string(),
                        order_type: Some("Limit".to_string()),
                        price: Decimal::from_str(&avg_price.to_string()).unwrap_or_default(),
                        amount: Decimal::from_str(&executed_qty.to_string()).unwrap_or_default(),
                        value: Some(
                            Decimal::from_str(&(avg_price * executed_qty).to_string())
                                .unwrap_or_default(),
                        ),
                        fee: None,
                        fee_currency: Some("USDC".to_string()),
                        realized_pnl: None,
                        pnl_percentage: None,
                        order_id: order_id.to_string(),
                        parent_order_id: None,
                        position_side: None,
                        metadata: None,
                    };

                    if let Err(e) = collector.record_trade(trade_data).await {
                        log::error!("记录AS策略交易失败: {}", e);
                    }
                }

                true // 需要使缓存失效
            }
            "PARTIALLY_FILLED" => {
                log::debug!(
                    "⚡ AS策略订单部分成交: {} {} @ {} x {}",
                    order_id,
                    side,
                    avg_price,
                    executed_qty
                );

                // 更新订单成交量
                let mut state = self.state.lock().await;
                if side == "BUY" {
                    if let Some(order) = state.active_buy_orders.get_mut(order_id) {
                        order.filled = executed_qty;
                        order.remaining = order.amount - executed_qty;
                    }
                } else {
                    if let Some(order) = state.active_sell_orders.get_mut(order_id) {
                        order.filled = executed_qty;
                        order.remaining = order.amount - executed_qty;
                    }
                }
                drop(state);

                false // 不需要使缓存失效，订单仍然活跃
            }
            _ => false,
        };

        // 如果需要，使订单缓存失效
        if should_invalidate_cache {
            self.order_cache
                .invalidate_open_orders(&self.config.trading.symbol)
                .await;
        }

        Ok(WsMessage::Text(json.to_string()))
    }

    /// 解析成交数据
    async fn parse_trade_data(&self, data: &serde_json::Value) -> Result<WsMessage> {
        let price = data["p"]
            .as_str()
            .and_then(|p| p.parse::<f64>().ok())
            .unwrap_or(0.0);
        let amount = data["q"]
            .as_str()
            .and_then(|q| q.parse::<f64>().ok())
            .unwrap_or(0.0);
        let is_buyer_maker = data["m"].as_bool().unwrap_or(false);

        Ok(WsMessage::Trade(Trade {
            id: data["t"].to_string(),
            symbol: self.config.trading.symbol.clone(),
            price,
            amount,
            side: if is_buyer_maker {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            },
            timestamp: Utc::now(),
            fee: None,
            order_id: None,
        }))
    }

    /// 解析深度数据
    async fn parse_depth_data(&self, data: &serde_json::Value) -> Result<WsMessage> {
        let mut bids = Vec::new();
        let mut asks = Vec::new();

        if let Some(bid_array) = data["b"].as_array() {
            for bid in bid_array
                .iter()
                .take(self.config.as_params.microstructure.orderbook_depth_levels)
            {
                if let Some(arr) = bid.as_array() {
                    if arr.len() >= 2 {
                        let price = arr[0]
                            .as_str()
                            .and_then(|p| p.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let qty = arr[1]
                            .as_str()
                            .and_then(|q| q.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        bids.push([price, qty]);
                    }
                }
            }
        }

        if let Some(ask_array) = data["a"].as_array() {
            for ask in ask_array
                .iter()
                .take(self.config.as_params.microstructure.orderbook_depth_levels)
            {
                if let Some(arr) = ask.as_array() {
                    if arr.len() >= 2 {
                        let price = arr[0]
                            .as_str()
                            .and_then(|p| p.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let qty = arr[1]
                            .as_str()
                            .and_then(|q| q.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        asks.push([price, qty]);
                    }
                }
            }
        }

        Ok(WsMessage::OrderBook(OrderBook {
            symbol: self.config.trading.symbol.clone(),
            bids,
            asks,
            timestamp: Utc::now(),
            info: serde_json::Value::Null,
        }))
    }

    /// 处理WebSocket消息
    async fn process_ws_message(&self, message: WsMessage) -> Result<()> {
        match message {
            WsMessage::Trade(trade) => {
                // 更新当前价格
                *self.current_price.write().await = trade.price;

                // 创建市场快照
                let snapshot = MarketSnapshot {
                    timestamp: Utc::now(),
                    best_bid: 0.0, // 将在订单簿更新时设置
                    best_ask: 0.0,
                    mid_price: trade.price,
                    bid_volume: 0.0,
                    ask_volume: 0.0,
                    last_trade_price: trade.price,
                    last_trade_volume: trade.amount,
                };

                // 添加到市场数据缓冲
                let mut buffer = self.market_buffer.write().await;
                buffer.push_back(snapshot);

                // 限制缓冲区大小
                while buffer.len() > 1000 {
                    buffer.pop_front();
                }
            }
            WsMessage::OrderBook(depth) => {
                // 更新订单簿
                let mut orderbook = self.orderbook.write().await;
                orderbook.bids = depth.bids.iter().map(|b| (b[0], b[1])).collect();
                orderbook.asks = depth.asks.iter().map(|a| (a[0], a[1])).collect();
                orderbook.last_update = Utc::now();

                // 更新当前价格
                if !depth.bids.is_empty() && !depth.asks.is_empty() {
                    let best_bid = depth.bids[0][0];
                    let best_ask = depth.asks[0][0];
                    let mid_price = (best_bid + best_ask) / 2.0;
                    if mid_price > 0.0 {
                        *self.current_price.write().await = mid_price;
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// 计算技术指标
    async fn calculate_indicators(&self) -> Result<()> {
        log::info!("📈 开始计算技术指标...");

        while *self.running.read().await {
            let current_price = *self.current_price.read().await;
            if current_price > 0.0 {
                let mut indicators = self.indicators.write().await;

                // 添加新价格到历史记录
                indicators.price_history.push_back(current_price);
                if indicators.price_history.len() > 200 {
                    indicators.price_history.pop_front();
                }

                // 计算EMA
                engine::update_ema(&mut indicators, current_price, &self.config.indicators.ema);

                // 计算RSI
                engine::update_rsi(&mut indicators, &self.config.indicators.rsi);

                // 计算VWAP
                engine::update_vwap(&mut indicators, &self.config.indicators.vwap);

                // 计算ATR
                engine::update_atr(&mut indicators, &self.config.indicators.atr);
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        Ok(())
    }

    /// 计算EMA

    /// 计算RSI - 使用标准的14期RSI计算方法

    /// 计算VWAP

    /// 计算ATR

    /// 主交易循环
    async fn run_trading_loop(&self) -> Result<()> {
        log::info!("💹 开始AS交易循环...");

        // 等待初始化
        log::info!("⏳ 等待市场数据初始化（5秒）...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        while *self.running.read().await {
            let current_price = *self.current_price.read().await;
            if current_price <= 0.0 {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }

            // 定期同步持仓状态（主要依赖WebSocket更新）
            self.update_position_status().await?;

            // 检查交易条件
            if self.should_trade().await? {
                // 计算最优价差
                let (bid_spread, ask_spread) = self.calculate_optimal_spread().await?;

                // 取消过期订单
                self.cancel_stale_orders().await?;

                // 下新订单
                self.place_scalping_orders(bid_spread, ask_spread).await?;
            }

            // 风险检查
            self.check_risk_limits().await?;

            // 定期检查并执行库存再平衡（每5分钟）
            static mut LAST_REBALANCE: Option<i64> = None;
            let now = Utc::now().timestamp();
            let should_rebalance = unsafe {
                if let Some(last) = LAST_REBALANCE {
                    now - last > 300 // 5分钟
                } else {
                    LAST_REBALANCE = Some(now);
                    false
                }
            };

            if should_rebalance {
                self.check_and_rebalance_inventory().await?;
                unsafe {
                    LAST_REBALANCE = Some(now);
                }
            }

            // 输出状态信息
            self.log_strategy_status().await;

            // 等待下次循环
            tokio::time::sleep(tokio::time::Duration::from_secs(
                self.config.trading.refresh_interval_secs,
            ))
            .await;
        }

        Ok(())
    }

    /// 检查并执行库存再平衡
    async fn check_and_rebalance_inventory(&self) -> Result<()> {
        let state = self.state.lock().await;
        let inventory_ratio = state.inventory / self.config.trading.max_inventory;
        let abs_inventory_ratio = inventory_ratio.abs();

        // 只在库存超过80%时执行再平衡
        if abs_inventory_ratio < 0.8 {
            return Ok(());
        }

        let current_price = *self.current_price.read().await;
        if current_price <= 0.0 {
            return Ok(());
        }

        log::warn!(
            "⚠️ AS策略: 库存过高 ({:.1}%)，执行强制再平衡",
            abs_inventory_ratio * 100.0
        );

        // 取消所有现有订单
        drop(state);
        self.cancel_all_orders().await?;

        // 计算再平衡订单大小（至少6 USDC以确保满足最小订单要求，交易所会向上取整）
        let rebalance_size = (self.config.trading.order_size_usdc * 2.0).max(6.0);
        let rebalance_quantity = rebalance_size / current_price;
        let rebalance_quantity = self.round_quantity(rebalance_quantity);

        let state = self.state.lock().await;
        let inventory = state.inventory;
        drop(state);

        // 根据库存方向下反向市价订单
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        if inventory > 0.0 {
            // 多头过多：卖出减仓
            log::info!(
                "🔄 AS再平衡: 卖出 {} {} 减少多头仓位",
                rebalance_quantity,
                self.get_base_asset().await
            );

            let mut sell_params = HashMap::new();
            if *self.is_dual_mode.read().await {
                sell_params.insert("positionSide".to_string(), "LONG".to_string());
            }

            let sell_order = OrderRequest {
                symbol: self.config.trading.symbol.clone(),
                side: OrderSide::Sell,
                order_type: OrderType::Market,
                amount: rebalance_quantity,
                price: None,
                market_type: if self.config.trading.market_type == "futures" {
                    MarketType::Futures
                } else {
                    MarketType::Spot
                },
                params: Some(sell_params),
                client_order_id: None,
                time_in_force: None,
                reduce_only: Some(true), // 只减仓
                post_only: Some(false),
            };

            match account.exchange.create_order(sell_order).await {
                Ok(order) => log::info!("✅ AS再平衡卖单成功: ID={}", order.id),
                Err(e) => log::error!("❌ AS再平衡卖单失败: {}", e),
            }
        } else {
            // 空头过多：买入减仓
            log::info!(
                "🔄 AS再平衡: 买入 {} {} 减少空头仓位",
                rebalance_quantity,
                self.get_base_asset().await
            );

            let mut buy_params = HashMap::new();
            if *self.is_dual_mode.read().await {
                buy_params.insert("positionSide".to_string(), "SHORT".to_string());
            }

            let buy_order = OrderRequest {
                symbol: self.config.trading.symbol.clone(),
                side: OrderSide::Buy,
                order_type: OrderType::Market,
                amount: rebalance_quantity,
                price: None,
                market_type: if self.config.trading.market_type == "futures" {
                    MarketType::Futures
                } else {
                    MarketType::Spot
                },
                params: Some(buy_params),
                client_order_id: None,
                time_in_force: None,
                reduce_only: Some(true), // 只减仓
                post_only: Some(false),
            };

            match account.exchange.create_order(buy_order).await {
                Ok(order) => log::info!("✅ AS再平衡买单成功: ID={}", order.id),
                Err(e) => log::error!("❌ AS再平衡买单失败: {}", e),
            }
        }

        Ok(())
    }

    /// 检查是否应该交易
    async fn should_trade(&self) -> Result<bool> {
        let state = self.state.lock().await;
        let now = Utc::now();

        // 检查最小下单间隔
        let min_interval =
            Duration::milliseconds(self.config.as_params.scalping_frequency.min_interval_ms as i64);
        if now - state.last_order_time < min_interval {
            log::debug!(
                "⏱️ AS策略未到最小下单间隔，等待 {} ms",
                (min_interval - (now - state.last_order_time)).num_milliseconds()
            );
            return Ok(false);
        }

        // 检查每分钟订单限制
        let current_minute = now.timestamp() / 60;
        let mut orders_this_minute = state.orders_this_minute;

        if current_minute != state.current_minute {
            orders_this_minute = 0;
        }

        if orders_this_minute
            >= self
                .config
                .as_params
                .scalping_frequency
                .max_orders_per_minute
        {
            return Ok(false);
        }

        // 检查市场条件
        let indicators = self.indicators.read().await;

        // RSI极端值检查 - 只在极端情况下暂停
        // RSI < 10 或 > 90 表示市场极端超卖或超买，此时风险较高
        if indicators.rsi < 10.0 || indicators.rsi > 90.0 {
            log::debug!("RSI极端值，暂停交易: {:.2}", indicators.rsi);
            return Ok(false);
        }

        // 检查订单簿失衡
        if let Ok(imbalance) = self.calculate_orderbook_imbalance().await {
            if imbalance.abs()
                > self
                    .config
                    .as_params
                    .microstructure
                    .bid_ask_imbalance_threshold
            {
                log::debug!("订单簿失衡过大，暂停交易: {:.3}", imbalance);
                return Ok(false);
            }
        }

        log::debug!("✅ AS策略交易条件满足，准备下单");
        Ok(true)
    }

    /// 计算订单簿失衡
    async fn calculate_orderbook_imbalance(&self) -> Result<f64> {
        let orderbook = self.orderbook.read().await;

        if orderbook.bids.is_empty() || orderbook.asks.is_empty() {
            return Ok(0.0);
        }

        let bid_volume: f64 = orderbook.bids.iter().take(5).map(|(_, vol)| vol).sum();
        let ask_volume: f64 = orderbook.asks.iter().take(5).map(|(_, vol)| vol).sum();

        let total_volume = bid_volume + ask_volume;
        if total_volume > 0.0 {
            Ok((bid_volume - ask_volume) / total_volume)
        } else {
            Ok(0.0)
        }
    }

    /// 计算最优价差 - 改进版动态调整
    async fn calculate_optimal_spread(&self) -> Result<(f64, f64)> {
        let state = self.state.lock().await;
        let indicators = self.indicators.read().await;
        let current_price = *self.current_price.read().await;

        // 基础价差
        let base_spread = self.config.trading.min_spread_bp / 10000.0;

        // 1. 波动率调整 - 使用更精确的计算
        let volatility_ratio = indicators.atr / current_price;
        let volatility_adjustment = if volatility_ratio > 0.01 {
            // 高波动：增加价差
            volatility_ratio
                * self
                    .config
                    .as_params
                    .spread_adjustment
                    .volatility_multiplier
        } else {
            // 低波动：保持基础价差
            0.0
        };

        // 2. 库存风险调整 - 更激进的调整策略
        let inventory_ratio = state.inventory / self.config.trading.max_inventory;
        let inventory_risk = if inventory_ratio.abs() > 0.8 {
            // 极高库存：激进增加价差
            inventory_ratio.abs() * 0.01 // 每10%库存增加1bp (5倍原值)
        } else if inventory_ratio.abs() > 0.6 {
            // 高库存：大幅增加价差
            inventory_ratio.abs() * 0.005 // 每10%库存增加0.5bp
        } else if inventory_ratio.abs() > 0.3 {
            // 中等库存：适度增加
            inventory_ratio.abs() * 0.002
        } else {
            // 低库存：小幅调整
            inventory_ratio.abs() * 0.0005
        };

        // 3. 订单簿压力调整
        let book_imbalance = self.calculate_orderbook_imbalance().await.unwrap_or(0.0);
        let book_adjustment = book_imbalance.abs() * 0.0005; // 订单簿不平衡增加价差

        // 4. 时间因子（交易活跃度）
        use chrono::Timelike;
        let hour = Utc::now().hour();
        let time_factor = if hour >= 14 && hour <= 22 {
            // 美国交易时段：降低价差
            0.9
        } else if hour >= 1 && hour <= 9 {
            // 亚洲交易时段：正常价差
            1.0
        } else {
            // 其他时段：增加价差
            1.1
        };

        // 5. 近期成交率调整（基于最近成交次数）
        let recent_trades = state.trade_count.min(10) as f64;
        let fill_rate_adjustment = if recent_trades > 7.0 {
            // 成交率高：可以收紧价差
            0.95
        } else if recent_trades < 3.0 {
            // 成交率低：需要放宽价差
            1.05
        } else {
            1.0
        };

        // 计算动态价差
        let dynamic_spread = base_spread + volatility_adjustment + inventory_risk + book_adjustment;

        // 应用时间和成交率因子
        let adjusted_spread = dynamic_spread * time_factor * fill_rate_adjustment;

        // 根据库存方向调整买卖价差 - 更激进的不对称调整
        let mut bid_spread = adjusted_spread;
        let mut ask_spread = adjusted_spread;

        if inventory_ratio > 0.7 {
            // 极高多头：激进调整（强力鼓励卖出）
            bid_spread *= 3.0; // 大幅提高买价差，减少买入
            ask_spread *= 0.3; // 大幅降低卖价差，鼓励卖出
        } else if inventory_ratio > 0.5 {
            // 高多头：大幅调整
            bid_spread *= 2.0;
            ask_spread *= 0.5;
        } else if inventory_ratio > 0.2 {
            // 中等多头：适度调整
            bid_spread *= 1.5;
            ask_spread *= 0.7;
        } else if inventory_ratio < -0.7 {
            // 极高空头：激进调整（强力鼓励买入）
            bid_spread *= 0.3; // 大幅降低买价差，鼓励买入
            ask_spread *= 3.0; // 大幅提高卖价差，减少卖出
        } else if inventory_ratio < -0.5 {
            // 高空头：大幅调整
            bid_spread *= 0.5;
            ask_spread *= 2.0;
        } else if inventory_ratio < -0.2 {
            // 中等空头：适度调整
            bid_spread *= 0.7;
            ask_spread *= 1.5;
        }

        // 限制在配置范围内
        let min_spread = self.config.trading.min_spread_bp / 10000.0;
        let max_spread = self.config.trading.max_spread_bp / 10000.0;

        bid_spread = bid_spread.max(min_spread).min(max_spread);
        ask_spread = ask_spread.max(min_spread).min(max_spread);

        log::debug!(
            "AS动态价差: 基础={:.5}, 波动={:.5}, 库存={:.5}, 最终买={:.5}/卖={:.5}",
            base_spread,
            volatility_adjustment,
            inventory_risk,
            bid_spread,
            ask_spread
        );

        Ok((bid_spread, ask_spread))
    }

    /// 下剥头皮订单
    async fn place_scalping_orders(&self, bid_spread: f64, ask_spread: f64) -> Result<()> {
        let current_price = *self.current_price.read().await;
        if current_price <= 0.0 {
            log::debug!("AS策略: 当前价格无效 ({}), 跳过下单", current_price);
            return Ok(());
        }

        log::debug!(
            "📊 AS策略准备下单 - 当前价格: {:.5}, 买价差: {:.5}, 卖价差: {:.5}",
            current_price,
            bid_spread,
            ask_spread
        );

        let mut state = self.state.lock().await;

        // 添加调试日志
        let net_pos = state.long_position - state.short_position;
        log::debug!(
            "📊 AS策略持仓状态 - 多头: {:.4}, 空头: {:.4}, 净持仓: {:.4}, 库存: {:.4}",
            state.long_position,
            state.short_position,
            net_pos,
            state.inventory
        );

        // 检查库存限制 - 更严格的限制
        // 对于期货：允许建立多头和空头仓位，但设置更严格的阈值
        // 对于现货：只能买入和卖出现有库存
        let inventory_ratio = state.inventory / self.config.trading.max_inventory;
        let abs_inventory_ratio = inventory_ratio.abs();

        // 根据库存水平动态调整允许交易的条件
        let can_buy;
        let can_sell;

        if self.config.trading.market_type == "futures" {
            // 期货市场：检查是否可以增加多头或空头仓位
            if *self.is_dual_mode.read().await {
                // 双向持仓模式：分别限制多头和空头
                // 使用更严格的阈值：70%时停止增加仓位
                can_buy = state.long_position < self.config.trading.max_inventory * 0.7;
                can_sell = state.short_position < self.config.trading.max_inventory * 0.7;

                // 极端库存时完全禁止增加仓位方向的交易
                if state.long_position > self.config.trading.max_inventory * 0.8 {
                    log::warn!(
                        "⚠️ AS策略: 多头仓位过高 ({:.2} USDC)，禁止买入",
                        state.long_position
                    );
                }
                if state.short_position > self.config.trading.max_inventory * 0.8 {
                    log::warn!(
                        "⚠️ AS策略: 空头仓位过高 ({:.2} USDC)，禁止卖出",
                        state.short_position
                    );
                }

                log::debug!(
                    "双向持仓检查 - 多头: {:.4}/{:.4}, 空头: {:.4}/{:.4}, 可买: {}, 可卖: {}",
                    state.long_position,
                    self.config.trading.max_inventory,
                    state.short_position,
                    self.config.trading.max_inventory,
                    can_buy,
                    can_sell
                );
            } else {
                // 单向持仓模式：净仓位限制
                // 根据库存比率动态调整限制
                if abs_inventory_ratio > 0.8 {
                    // 库存超过80%：只允许减仓方向的交易
                    can_buy = inventory_ratio < 0.0; // 只有空头时才能买
                    can_sell = inventory_ratio > 0.0; // 只有多头时才能卖
                    log::warn!(
                        "⚠️ AS策略: 库存过高 ({:.2}%)，只允许减仓交易",
                        abs_inventory_ratio * 100.0
                    );
                } else if abs_inventory_ratio > 0.6 {
                    // 库存60-80%：限制增仓
                    can_buy = state.inventory < self.config.trading.max_inventory * 0.7;
                    can_sell = state.inventory > -self.config.trading.max_inventory * 0.7;
                } else {
                    // 库存小于60%：正常交易
                    can_buy = state.inventory < self.config.trading.max_inventory * 0.8;
                    can_sell = state.inventory > -self.config.trading.max_inventory * 0.8;
                }

                log::debug!(
                    "单向持仓检查 - 净仓位: {:.4}/{:.4} ({:.1}%), 可买: {}, 可卖: {}",
                    state.inventory,
                    self.config.trading.max_inventory,
                    abs_inventory_ratio * 100.0,
                    can_buy,
                    can_sell
                );
            }
        } else {
            // 现货市场：只能买入或卖出现有库存
            can_buy = state.inventory < self.config.trading.max_inventory * 0.7;
            can_sell = state.inventory > 0.0; // 只有持有库存时才能卖出
        }

        log::debug!(
            "📊 AS策略交易条件 - can_buy: {}, can_sell: {}, 市场类型: {}, 双向模式: {},",
            can_buy,
            can_sell,
            self.config.trading.market_type,
            *self.is_dual_mode.read().await
        );

        // 计算订单数量 - 保持订单大小不变，通过价差和交易频率控制库存
        let order_size = self.config.trading.order_size_usdc;
        let order_quantity = order_size / current_price;
        let order_quantity = self.round_quantity(order_quantity);

        // 高库存时记录警告但不调整订单大小
        if abs_inventory_ratio > 0.7 {
            log::warn!(
                "⚠️ AS策略: 库存较高 ({:.1}%)，通过价差调整控制",
                abs_inventory_ratio * 100.0
            );
        }

        // 更新下单统计
        let now = Utc::now();
        let current_minute = now.timestamp() / 60;

        if current_minute != state.current_minute {
            state.current_minute = current_minute;
            state.orders_this_minute = 0;
        }

        if state.orders_this_minute
            >= self
                .config
                .as_params
                .scalping_frequency
                .max_orders_per_minute
        {
            log::debug!(
                "AS策略: 达到每分钟订单限制 {}/{}",
                state.orders_this_minute,
                self.config
                    .as_params
                    .scalping_frequency
                    .max_orders_per_minute
            );
            return Ok(());
        }

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 检查当前订单数量限制
        let max_orders_per_side = self
            .config
            .as_params
            .order_management
            .max_open_orders_per_side;

        drop(state); // 释放锁

        // 下买单
        if can_buy {
            let state = self.state.lock().await;
            log::info!(
                "📊 AS买单检查 - 当前买单数: {}, 最大限制: {}",
                state.active_buy_orders.len(),
                max_orders_per_side
            );
            if (state.active_buy_orders.len() as u32) < max_orders_per_side {
                drop(state);

                let buy_price = current_price * (1.0 - bid_spread);
                let buy_price = self.round_price(buy_price);

                log::debug!(
                    "📗 AS策略下买单: {} {} @ {:.5} USDC (价差: -{:.2}%)",
                    order_quantity,
                    self.get_base_asset().await,
                    buy_price,
                    bid_spread * 100.0
                );

                let mut buy_params = HashMap::from([
                    ("postOnly".to_string(), "true".to_string()),
                    (
                        "timeInForce".to_string(),
                        self.config.trading.order_config.time_in_force.clone(),
                    ),
                ]);

                if *self.is_dual_mode.read().await {
                    buy_params.insert("positionSide".to_string(), "LONG".to_string());
                }

                let buy_order = OrderRequest {
                    symbol: self.config.trading.symbol.clone(),
                    side: OrderSide::Buy,
                    order_type: OrderType::Limit,
                    amount: order_quantity,
                    price: Some(buy_price),
                    market_type: if self.config.trading.market_type == "futures" {
                        MarketType::Futures
                    } else {
                        MarketType::Spot
                    },
                    params: Some(buy_params),
                    client_order_id: None,
                    time_in_force: Some(self.config.trading.order_config.time_in_force.clone()),
                    reduce_only: Some(self.config.trading.order_config.reduce_only),
                    post_only: Some(self.config.trading.order_config.post_only),
                };

                self.track_request_weight(1).await; // 下单请求权重

                match account.exchange.create_order(buy_order).await {
                    Ok(order) => {
                        log::debug!("✅ AS买单成功: ID={}, 状态={:?}", order.id, order.status);

                        let mut state = self.state.lock().await;
                        // 立即添加到活跃订单列表，防止WebSocket延迟导致订单失踪
                        state.active_buy_orders.insert(order.id.clone(), order);
                        state.last_order_time = now;
                        state.orders_this_minute += 1;
                        drop(state);

                        // 使订单缓存失效，以便包含新订单
                        self.order_cache
                            .invalidate_open_orders(&self.config.trading.symbol)
                            .await;
                    }
                    Err(e) => {
                        log::error!("❌ AS买单失败: {}", e);
                    }
                }
            }
        }

        // 下卖单
        if can_sell {
            let state = self.state.lock().await;
            log::info!(
                "📊 AS卖单检查 - 当前卖单数: {}, 最大限制: {}",
                state.active_sell_orders.len(),
                max_orders_per_side
            );
            if (state.active_sell_orders.len() as u32) < max_orders_per_side {
                drop(state);

                let sell_price = current_price * (1.0 + ask_spread);
                let sell_price = self.round_price(sell_price);

                log::debug!(
                    "📕 AS策略下卖单: {} {} @ {:.5} USDC (价差: +{:.2}%)",
                    order_quantity,
                    self.get_base_asset().await,
                    sell_price,
                    ask_spread * 100.0
                );

                let mut sell_params = HashMap::from([
                    ("postOnly".to_string(), "true".to_string()),
                    (
                        "timeInForce".to_string(),
                        self.config.trading.order_config.time_in_force.clone(),
                    ),
                ]);

                if *self.is_dual_mode.read().await {
                    sell_params.insert("positionSide".to_string(), "SHORT".to_string());
                }

                let sell_order = OrderRequest {
                    symbol: self.config.trading.symbol.clone(),
                    side: OrderSide::Sell,
                    order_type: OrderType::Limit,
                    amount: order_quantity,
                    price: Some(sell_price),
                    market_type: if self.config.trading.market_type == "futures" {
                        MarketType::Futures
                    } else {
                        MarketType::Spot
                    },
                    params: Some(sell_params),
                    client_order_id: None,
                    time_in_force: Some(self.config.trading.order_config.time_in_force.clone()),
                    reduce_only: Some(self.config.trading.order_config.reduce_only),
                    post_only: Some(self.config.trading.order_config.post_only),
                };

                self.track_request_weight(1).await; // 下单请求权重

                match account.exchange.create_order(sell_order).await {
                    Ok(order) => {
                        log::debug!("✅ AS卖单成功: ID={}, 状态={:?}", order.id, order.status);

                        let mut state = self.state.lock().await;
                        // 立即添加到活跃订单列表，防止WebSocket延迟导致订单失踪
                        state.active_sell_orders.insert(order.id.clone(), order);
                        state.last_order_time = now;
                        state.orders_this_minute += 1;
                        drop(state);

                        // 使订单缓存失效，以便包含新订单
                        self.order_cache
                            .invalidate_open_orders(&self.config.trading.symbol)
                            .await;
                    }
                    Err(e) => {
                        log::error!("❌ AS卖单失败: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    /// 取消过期订单
    async fn cancel_stale_orders(&self) -> Result<()> {
        let now = Utc::now();
        let max_lifetime = Duration::seconds(
            self.config
                .as_params
                .order_management
                .order_lifetime_seconds as i64,
        );

        let state = self.state.lock().await;
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 收集需要取消的订单
        let mut orders_to_cancel = Vec::new();

        for (order_id, order) in &state.active_buy_orders {
            if now - order.timestamp > max_lifetime {
                orders_to_cancel.push(order_id.clone());
            }
        }

        for (order_id, order) in &state.active_sell_orders {
            if now - order.timestamp > max_lifetime {
                orders_to_cancel.push(order_id.clone());
            }
        }

        drop(state); // 释放锁

        // 批量取消过期订单
        for order_id in orders_to_cancel {
            log::debug!("取消过期AS订单: {}", order_id);
            self.track_request_weight(1).await; // 取消订单权重

            let result = account
                .exchange
                .cancel_order(
                    &order_id,
                    &self.config.trading.symbol,
                    if self.config.trading.market_type == "futures" {
                        MarketType::Futures
                    } else {
                        MarketType::Spot
                    },
                )
                .await;

            // 手动从活跃订单列表中移除，不依赖WebSocket更新
            if result.is_ok() {
                let mut state = self.state.lock().await;
                if state.active_buy_orders.remove(&order_id).is_some() {
                    log::debug!("✅ 从买单列表移除订单: {}", order_id);
                } else if state.active_sell_orders.remove(&order_id).is_some() {
                    log::debug!("✅ 从卖单列表移除订单: {}", order_id);
                }
            }
        }

        Ok(())
    }

    /// 获取缓存的活跃订单，如果缓存过期则从API获取
    async fn get_cached_open_orders(&self) -> Result<Vec<Order>> {
        let now = Utc::now();
        let last_fetch = *self.last_order_fetch.read().await;

        // 检查是否超过10分钟需要刷新
        let should_refresh = now - last_fetch > Duration::minutes(10);

        if !should_refresh {
            // 尝试从缓存获取
            if let Some(cached_orders) = self
                .order_cache
                .get_open_orders(&self.config.trading.symbol)
                .await
            {
                log::debug!("📋 使用缓存的活跃订单，数量: {}", cached_orders.len());
                return Ok(cached_orders);
            }
        }

        // 缓存过期或不存在，从API获取新订单
        log::info!("🔄 从交易所获取新的活跃订单...");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        let market_type = if self.config.trading.market_type == "futures" {
            MarketType::Futures
        } else {
            MarketType::Spot
        };

        // 跟踪API请求权重
        self.track_request_weight(10).await;

        // 从交易所获取活跃订单
        let fresh_orders = account
            .exchange
            .get_open_orders(Some(&self.config.trading.symbol), market_type)
            .await?;

        // 更新缓存
        self.order_cache
            .set_open_orders(self.config.trading.symbol.clone(), fresh_orders.clone())
            .await;

        // 更新最后获取时间
        *self.last_order_fetch.write().await = now;

        log::info!("✅ 成功获取 {} 个活跃订单并更新缓存", fresh_orders.len());

        Ok(fresh_orders)
    }

    /// 更新本地订单状态，使用缓存的订单数据
    async fn sync_local_orders_with_cache(&self) -> Result<()> {
        let cached_orders = self.get_cached_open_orders().await?;
        let mut state = self.state.lock().await;

        // 清空现有的活跃订单列表
        state.active_buy_orders.clear();
        state.active_sell_orders.clear();

        // 从缓存的订单重建本地状态
        for order in cached_orders {
            match order.side {
                OrderSide::Buy => {
                    state.active_buy_orders.insert(order.id.clone(), order);
                }
                OrderSide::Sell => {
                    state.active_sell_orders.insert(order.id.clone(), order);
                }
            }
        }

        log::debug!(
            "📋 本地订单状态已与缓存同步 - 买单: {}, 卖单: {}",
            state.active_buy_orders.len(),
            state.active_sell_orders.len()
        );

        Ok(())
    }

    /// 立即更新持仓状态（启动时调用）
    async fn update_position_immediately(&self) -> Result<()> {
        // 仅期货需要查询持仓
        if self.config.trading.market_type != "futures" {
            return Ok(());
        }

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        let positions = account
            .exchange
            .get_positions(Some(&self.config.trading.symbol))
            .await?;

        let mut state = self.state.lock().await;

        // 重置仓位
        state.long_position = 0.0;
        state.short_position = 0.0;
        state.inventory = 0.0;

        // 处理双向持仓
        if *self.is_dual_mode.read().await {
            // 双向持仓模式：可能有多个仓位记录
            for position in &positions {
                // 使用amount字段，它包含了正负号
                if position.side == "LONG" || position.side == "Buy" {
                    state.long_position = position.amount.abs();
                    state.long_avg_cost = position.entry_price;
                } else if position.side == "SHORT" || position.side == "Sell" {
                    state.short_position = position.amount.abs();
                    state.short_avg_cost = position.entry_price;
                }
            }
            state.inventory = state.long_position - state.short_position;

            log::info!(
                "📦 AS初始双向持仓 - 多头: {:.4} @ {:.2}, 空头: {:.4} @ {:.2}, 净库存: {:.4}",
                state.long_position,
                state.long_avg_cost,
                state.short_position,
                state.short_avg_cost,
                state.inventory
            );
        } else {
            // 单向持仓模式
            if let Some(position) = positions.first() {
                // 使用amount字段，它包含了正负号
                state.inventory = position.amount;
                state.avg_cost = position.entry_price;

                log::info!(
                    "📦 AS初始持仓: {} {} @ {:.5}",
                    state.inventory,
                    self.get_base_asset().await,
                    position.entry_price
                );
            } else {
                log::info!("📦 AS初始持仓: 0 {}", self.get_base_asset().await);
            }
        }

        Ok(())
    }

    /// 更新持仓状态 - 仅在需要时查询持仓
    async fn update_position_status(&self) -> Result<()> {
        // 定期同步订单状态和持仓状态
        // 1. 使用缓存的订单数据同步本地状态
        // 2. 查询持仓状态（仅期货）

        static LAST_POSITION_CHECK: tokio::sync::Mutex<Option<DateTime<Utc>>> =
            tokio::sync::Mutex::const_new(None);

        let mut last_check = LAST_POSITION_CHECK.lock().await;
        let now = Utc::now();

        // 每5分钟检查一次持仓和订单，避免数据不同步
        if let Some(last) = *last_check {
            if now - last < Duration::minutes(5) {
                return Ok(());
            }
        }

        *last_check = Some(now);
        drop(last_check);

        // 同步订单状态（使用缓存，仅在必要时从API获取）
        if let Err(e) = self.sync_local_orders_with_cache().await {
            log::warn!("同步订单状态失败: {}", e);
        }

        // 仅期货需要查询持仓
        if self.config.trading.market_type == "futures" {
            self.track_request_weight(5).await; // 记录请求权重

            let account = self
                .account_manager
                .get_account(&self.config.account.account_id)
                .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

            let positions = account
                .exchange
                .get_positions(Some(&self.config.trading.symbol))
                .await?;

            let mut state = self.state.lock().await;

            // 重置仓位
            state.long_position = 0.0;
            state.short_position = 0.0;
            state.inventory = 0.0;

            // 处理双向持仓
            if *self.is_dual_mode.read().await {
                // 双向持仓模式：可能有多个仓位记录
                for position in &positions {
                    // 使用amount字段，它包含了正负号
                    if position.side == "LONG" || position.side == "Buy" {
                        state.long_position = position.amount.abs();
                        state.long_avg_cost = position.entry_price;
                    } else if position.side == "SHORT" || position.side == "Sell" {
                        state.short_position = position.amount.abs();
                        state.short_avg_cost = position.entry_price;
                    }
                }
                state.inventory = state.long_position - state.short_position;

                log::info!(
                    "📦 AS双向持仓同步 - 多头: {:.4} @ {:.2}, 空头: {:.4} @ {:.2}",
                    state.long_position,
                    state.long_avg_cost,
                    state.short_position,
                    state.short_avg_cost
                );
            } else {
                // 单向持仓模式
                if let Some(position) = positions.first() {
                    // 使用amount字段，它包含了正负号
                    let new_inventory = position.amount;

                    // 仅在持仓变化时记录
                    if (new_inventory - state.inventory).abs() > 0.001 {
                        log::info!(
                            "📦 AS持仓同步: {} {} @ {:.5} (原: {} {})",
                            new_inventory,
                            self.get_base_asset().await,
                            position.entry_price,
                            state.inventory,
                            state.avg_cost
                        );

                        state.inventory = new_inventory;
                        state.avg_cost = position.entry_price;
                    }
                } else {
                    // 没有持仓，重置为0
                    if state.inventory.abs() > 0.001 {
                        log::info!(
                            "📦 AS持仓同步: 0 {} (原: {} {})",
                            self.get_base_asset().await,
                            state.inventory,
                            state.avg_cost
                        );

                        state.inventory = 0.0;
                        state.avg_cost = 0.0;
                    }
                }
            }
        }

        Ok(())
    }

    /// 跟踪请求权重
    async fn track_request_weight(&self, weight: u32) {
        let now = Utc::now();
        let mut last_reset = self.last_weight_reset.write().await;
        let mut current_weight = self.request_weight.write().await;

        // 每分钟重置权重计数
        if now - *last_reset > Duration::minutes(1) {
            *last_reset = now;
            *current_weight = weight;
            log::debug!("🔄 请求权重重置: {}", weight);
        } else {
            *current_weight += weight;

            // 警告阈值
            if *current_weight > 800 {
                log::warn!("⚠️ AS策略请求权重较高: {}/1200", *current_weight);
            }

            // 严重警告
            if *current_weight > 1000 {
                log::error!(
                    "🔴 AS策略请求权重接近上限: {}/1200 - 降低请求频率！",
                    *current_weight
                );

                // 主动等待一段时间
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            }
        }
    }
    fn build_risk_limits(config: &ASConfig) -> StrategyRiskLimits {
        StrategyRiskLimits {
            warning_scale_factor: Some(0.8),
            danger_scale_factor: Some(0.5),
            stop_loss_pct: Some(config.risk.stop_loss_pct),
            max_inventory_notional: None,
            max_daily_loss: Some(config.risk.max_daily_loss),
            max_consecutive_losses: Some(config.risk.emergency_stop.consecutive_losses),
            inventory_skew_limit: Some(config.risk.inventory_skew_limit),
            max_unrealized_loss: Some(config.risk.max_unrealized_loss),
        }
    }

    fn build_risk_snapshot(&self, state: &ASStrategyState, current_price: f64) -> StrategySnapshot {
        let mut snapshot = StrategySnapshot::new(self.config.strategy.name.clone());
        snapshot.exposure.notional = current_price * state.inventory;
        snapshot.exposure.net_inventory = state.inventory;
        snapshot.exposure.long_position = state.long_position;
        snapshot.exposure.short_position = state.short_position;
        if self.config.trading.max_inventory > 0.0 {
            snapshot.exposure.inventory_ratio =
                Some((state.inventory.abs() / self.config.trading.max_inventory).min(10.0));
        }

        snapshot.performance.realized_pnl = state.total_pnl;
        snapshot.performance.unrealized_pnl = state.inventory * (current_price - state.avg_cost);
        snapshot.performance.daily_pnl = Some(state.daily_pnl);
        snapshot.performance.consecutive_losses = Some(state.consecutive_losses);
        snapshot.performance.timestamp = Utc::now();
        snapshot.risk_limits = Some(self.risk_limits.clone());
        snapshot
    }

    async fn apply_risk_decision(&self, decision: RiskDecision) -> Result<()> {
        match decision.action {
            RiskAction::None => Ok(()),
            RiskAction::ScaleDown {
                scale_factor,
                reason,
            } => {
                log::warn!(
                    "⚠️ AS策略触发风险缩减: {} (scale={:.2})",
                    reason,
                    scale_factor
                );
                self.cancel_all_orders().await?;
                if scale_factor <= 0.0 {
                    self.close_all_positions().await?;
                    *self.running.write().await = false;
                }
                Ok(())
            }
            RiskAction::Halt { reason } => {
                log::error!("❌ AS策略触发风险停机: {}", reason);
                self.cancel_all_orders().await?;
                self.close_all_positions().await?;
                *self.running.write().await = false;
                Ok(())
            }
            RiskAction::Notify { level, message } => {
                match level {
                    RiskNotifyLevel::Info => log::info!("ℹ️ 风险提示: {}", message),
                    RiskNotifyLevel::Warning => log::warn!("⚠️ 风险警告: {}", message),
                    RiskNotifyLevel::Danger => log::error!("❗ 风险危险: {}", message),
                }
                Ok(())
            }
        }
    }

    /// 风险检查
    async fn check_risk_limits(&self) -> Result<()> {
        let state = self.state.lock().await;
        let current_price = *self.current_price.read().await;

        if current_price <= 0.0 {
            return Ok(());
        }

        let snapshot = self.build_risk_snapshot(&state, current_price);
        drop(state);

        let decision = self.risk_evaluator.evaluate(&snapshot).await;
        self.apply_risk_decision(decision).await
    }

    /// 取消所有订单
    async fn cancel_all_orders(&self) -> Result<()> {
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 使用批量取消API
        let market_type = if self.config.trading.market_type == "futures" {
            MarketType::Futures
        } else {
            MarketType::Spot
        };

        let _ = account
            .exchange
            .cancel_all_orders(Some(&self.config.trading.symbol), market_type)
            .await?;

        log::info!("✅ 已取消AS策略所有订单");
        Ok(())
    }

    /// 平掉所有持仓
    async fn close_all_positions(&self) -> Result<()> {
        if self.config.trading.market_type != "futures" {
            return Ok(());
        }

        let state = self.state.lock().await;
        if state.inventory.abs() < 0.001 {
            return Ok(());
        }

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        let side = if state.inventory > 0.0 {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        };

        let close_order = OrderRequest {
            symbol: self.config.trading.symbol.clone(),
            side,
            order_type: OrderType::Market,
            amount: state.inventory.abs(),
            price: None,
            market_type: MarketType::Futures,
            params: Some(HashMap::from([(
                "reduceOnly".to_string(),
                "true".to_string(),
            )])),
            client_order_id: None,
            time_in_force: None,
            reduce_only: Some(true),
            post_only: None,
        };

        account.exchange.create_order(close_order).await?;

        log::info!("✅ AS策略已平掉所有持仓");
        Ok(())
    }

    /// 输出策略状态
    async fn log_strategy_status(&self) {
        let state = self.state.lock().await;
        let indicators = self.indicators.read().await;
        let current_price = *self.current_price.read().await;

        if state.trade_count % 10 == 0 && state.trade_count > 0 {
            log::info!("📊 AS策略状态 | 价格: {:.5} | 库存: {:.1} | 成交: {} | RSI: {:.1} | EMA: {:.5}/{:.5}",
                current_price, state.inventory, state.trade_count,
                indicators.rsi, indicators.ema_fast, indicators.ema_slow);
        }
    }

    /// 输出策略统计
    async fn print_strategy_statistics(&self) {
        let state = self.state.lock().await;
        let indicators = self.indicators.read().await;
        let runtime = Utc::now().signed_duration_since(state.start_time);

        log::info!("========== AS策略统计 ==========");
        log::info!("策略名称: {}", self.config.strategy.name);
        log::info!("交易对: {}", self.config.trading.symbol);
        log::info!(
            "运行时间: {}小时{}分钟",
            runtime.num_hours(),
            runtime.num_minutes() % 60
        );
        log::info!("成交次数: {}", state.trade_count);
        log::info!("总盈亏: {:.2} USDC", state.total_pnl);
        log::info!("今日盈亏: {:.2} USDC", state.daily_pnl);
        log::info!(
            "最终库存: {:.2} {}",
            state.inventory,
            self.get_base_asset().await
        );
        log::info!("连续亏损: {}", state.consecutive_losses);
        log::info!("最终指标:");
        log::info!("  - RSI: {:.2}", indicators.rsi);
        log::info!("  - EMA快: {:.5}", indicators.ema_fast);
        log::info!("  - EMA慢: {:.5}", indicators.ema_slow);
        log::info!("  - VWAP: {:.5}", indicators.vwap);
        log::info!("  - ATR: {:.5}", indicators.atr);
        log::info!("===============================");
    }

    /// 价格精度处理
    fn round_price(&self, price: f64) -> f64 {
        let precision = if let Ok(guard) = self.symbol_info.try_read() {
            guard
                .as_ref()
                .map(|info| info.price_precision)
                .unwrap_or(self.config.trading.price_precision)
        } else {
            self.config.trading.price_precision
        };

        let multiplier = 10_f64.powi(precision as i32);
        (price * multiplier).round() / multiplier
    }

    /// 数量精度处理
    fn round_quantity(&self, quantity: f64) -> f64 {
        let precision = if let Ok(guard) = self.symbol_info.try_read() {
            guard
                .as_ref()
                .map(|info| info.quantity_precision)
                .unwrap_or(self.config.trading.quantity_precision)
        } else {
            self.config.trading.quantity_precision
        };

        let multiplier = 10_f64.powi(precision as i32);
        (quantity * multiplier).round() / multiplier
    }

    /// 克隆用于任务
    fn clone_for_task(&self) -> Self {
        Self {
            config: self.config.clone(),
            account_manager: self.account_manager.clone(),
            state: self.state.clone(),
            market_buffer: self.market_buffer.clone(),
            indicators: self.indicators.clone(),
            ws_client: self.ws_client.clone(),
            running: self.running.clone(),
            current_price: self.current_price.clone(),
            orderbook: self.orderbook.clone(),
            collector: self.collector.clone(),
            symbol_info: self.symbol_info.clone(),
            is_dual_mode: self.is_dual_mode.clone(),
            listen_key: self.listen_key.clone(),
            request_weight: self.request_weight.clone(),
            last_weight_reset: self.last_weight_reset.clone(),
            order_cache: self.order_cache.clone(),
            user_stream_client: self.user_stream_client.clone(),
            last_order_fetch: self.last_order_fetch.clone(),
            risk_evaluator: self.risk_evaluator.clone(),
            risk_limits: self.risk_limits.clone(),
        }
    }
}

// 类型别名
type Result<T> = std::result::Result<T, ExchangeError>;
