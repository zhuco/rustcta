use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Duration;

use super::{
    config::{BatchSettings, GridManagement, SpacingType, TradingConfig, TrendAdjustment},
    operations,
    state::{ConfigState, TrendStrength},
};
use crate::analysis::{TradeCollector, TradeData};
use crate::core::{
    error::ExchangeError,
    types::{Fee, MarketType, OrderRequest, OrderSide, OrderStatus, OrderType, Trade, WsMessage},
    websocket::MessageHandler,
};
use crate::cta::account_manager::AccountManager;

use rust_decimal::Decimal;

/// 成交处理器
pub(super) struct TradeHandler {
    config_id: String,
    config: TradingConfig,
    state: Arc<Mutex<ConfigState>>,
    account_manager: Arc<AccountManager>,
    config_states: Arc<RwLock<HashMap<String, Arc<Mutex<ConfigState>>>>>,
    grid_management: GridManagement,
    trend_adjustment: TrendAdjustment,
    batch_settings: BatchSettings,
    log_all_trades: bool,
    processed_trades: Arc<Mutex<HashSet<String>>>, // 记录已处理的成交ID，避免重复
    collector: Option<Arc<TradeCollector>>,        // 数据收集器
}

impl TradeHandler {
    pub(super) fn new(
        config_id: String,
        config: TradingConfig,
        state: Arc<Mutex<ConfigState>>,
        account_manager: Arc<AccountManager>,
        config_states: Arc<RwLock<HashMap<String, Arc<Mutex<ConfigState>>>>>,
        grid_management: GridManagement,
        trend_adjustment: TrendAdjustment,
        batch_settings: BatchSettings,
        log_all_trades: bool,
        collector: Option<Arc<TradeCollector>>,
    ) -> Self {
        Self {
            config_id,
            config,
            state,
            account_manager,
            config_states,
            grid_management,
            trend_adjustment,
            batch_settings,
            log_all_trades,
            processed_trades: Arc::new(Mutex::new(HashSet::new())),
            collector,
        }
    }
}

#[async_trait]
impl MessageHandler for TradeHandler {
    async fn handle_message(&self, message: WsMessage) -> Result<()> {
        // 记录消息类型（DEBUG级别）
        log::debug!(
            "📨 {} TradeHandler处理消息类型: {:?}",
            self.config_id,
            std::mem::discriminant(&message)
        );

        match message {
            WsMessage::Trade(trade) => {
                // TRADE_LITE 事件已在binance.rs中记录，这里不重复记录
                // 不处理 TRADE_LITE，等待更完整的 ORDER_TRADE_UPDATE
            }
            WsMessage::Ticker(ticker) => {
                // 更新价格
                let mut state_guard = self.state.lock().await;
                state_guard.current_price = ticker.last;

                // 更新趋势
                if let Some(trend_value) = state_guard.trend_calculator.update(ticker.last) {
                    let new_strength =
                        crate::utils::indicators::trend_strength_to_enum(trend_value);
                    let old_strength = state_guard.trend_strength;
                    state_guard.trend_strength = new_strength;
                    if !matches!(old_strength, new_strength) {
                        log::info!("🔄 {} 趋势更新: {:?}", self.config_id, new_strength);
                    }
                }
            }
            WsMessage::ExecutionReport(report) => {
                // 比较时转换格式（Binance返回ENAUSDC，配置中是ENA/USDC）
                let normalized_symbol = report.symbol.replace("/", "");
                let config_symbol = self.config.symbol.replace("/", "");

                // 检查是否是当前策略的订单
                let state_guard = self.state.lock().await;
                let is_my_order = state_guard.active_orders.contains_key(&report.order_id)
                    || state_guard.grid_orders.contains_key(&report.order_id);
                drop(state_guard);

                // 只有属于当前交易对且是当前策略的订单才输出日志
                if normalized_symbol == config_symbol && is_my_order {
                    log::info!(
                        "📬 {} 收到订单执行报告: 订单ID={}, 状态={:?}, 价格={:.4}, 数量={:.2}",
                        self.config_id,
                        report.order_id,
                        report.status,
                        report.executed_price,
                        report.executed_amount
                    );
                }

                log::debug!(
                    "🔍 {} 符号匹配: report='{}' config='{}' 是否为我的订单={} 状态={:?}",
                    self.config_id,
                    normalized_symbol,
                    config_symbol,
                    is_my_order,
                    report.status
                );

                // 只处理属于当前策略的订单
                if normalized_symbol == config_symbol
                    && is_my_order
                    && report.status == OrderStatus::Closed
                {
                    // 检查是否已处理过这笔成交
                    // 使用订单ID+价格+数量+时间戳生成唯一ID，避免重复处理
                    let trade_id = format!(
                        "{}_{}_{:.4}_{:.2}_{:?}",
                        report.order_id,
                        report.timestamp.timestamp_millis(),
                        report.executed_price,
                        report.executed_amount,
                        report.side
                    );

                    let mut processed = self.processed_trades.lock().await;
                    if processed.contains(&trade_id) {
                        log::warn!(
                            "⚠️ {} 检测到重复成交事件，跳过处理: 订单{} 价格{:.4} 数量{:.2}",
                            self.config_id,
                            report.order_id,
                            report.executed_price,
                            report.executed_amount
                        );
                        return Ok(());
                    }
                    processed.insert(trade_id.clone());

                    // 清理旧的记录（保留最近2000条，增加缓存大小）
                    if processed.len() > 2000 {
                        // 保留最近的1500条
                        let to_remove: Vec<String> = processed.iter().take(500).cloned().collect();
                        for id in to_remove {
                            processed.remove(&id);
                        }
                    }
                    drop(processed);

                    // 使用WebSocket消息中的is_maker字段判断
                    let is_maker = report.is_maker;

                    // 如果是吃单成交，立即重置网格
                    if !is_maker {
                        let now = chrono::Local::now();
                        log::warn!(
                            "[市价单] {} 成交市价单，立即重置网格",
                            now.format("%H:%M:%S")
                        );

                        // 先更新状态
                        let mut state_guard = self.state.lock().await;
                        if report.status == OrderStatus::Closed {
                            state_guard.active_orders.remove(&report.order_id);
                            log::debug!(
                                "🗑️ {} 移除已完全成交订单: {}",
                                self.config_id,
                                report.order_id
                            );
                        }

                        log::info!(
                            "🎯 {} 订单成交: {} {:?} @ {:.4} [吃单方-立即重置]",
                            self.config_id,
                            report.executed_amount,
                            report.side,
                            report.executed_price
                        );

                        // 更新成交统计
                        state_guard.last_trade_price = report.executed_price;
                        state_guard.last_trade_time = report.timestamp;
                        state_guard.trades_count += 1;
                        state_guard.total_fee += report.commission;

                        // 释放锁并立即执行网格重置
                        drop(state_guard);

                        // 吃单后等待 30 秒再重置网格，重置时重新获取最新价格
                        log::info!("⏳ {} 吃单后等待 30 秒再重置网格", self.config_id);
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        log::info!(
                            "🔄 {} 开始重置网格（吃单触发，使用最新行情）",
                            self.config_id
                        );
                        if let Err(e) = operations::reset_grid_for_config(
                            &self.config,
                            &self.state,
                            self.account_manager.as_ref(),
                            &self.batch_settings,
                            &self.trend_adjustment,
                            &self.grid_management,
                            None, // 强制重置时再获取最新价格
                        )
                        .await
                        {
                            log::error!("❌ {} 吃单触发的网格重置失败: {}", self.config_id, e);
                        } else {
                            log::info!("✅ {} 吃单触发的网格重置成功", self.config_id);
                        }

                        // 重要：吃单成交后已经重置网格，不需要再调用handle_grid_adjustment
                        return Ok(());
                    }

                    // 挂单成交，正常处理
                    let mut state_guard = self.state.lock().await;

                    // 如果订单完全成交，从活动订单中移除
                    if report.status == OrderStatus::Closed {
                        state_guard.active_orders.remove(&report.order_id);
                        log::debug!(
                            "🗑️ {} 移除已完全成交订单: {}",
                            self.config_id,
                            report.order_id
                        );
                    }

                    // 订单成交信息已在binance.rs中输出
                    state_guard.last_trade_price = report.executed_price;
                    state_guard.last_trade_time = report.timestamp;
                    state_guard.trades_count += 1;

                    // 计算手续费
                    let fee_amount = report.commission;
                    state_guard.total_fee += fee_amount;

                    // 更新持仓和盈亏统计
                    match report.side {
                        OrderSide::Buy => {
                            let volume = report.executed_amount * report.executed_price;
                            state_guard.position += volume;
                            state_guard.total_buy_volume += volume;
                            state_guard.total_buy_amount += report.executed_amount;
                            state_guard.net_position += report.executed_amount;

                            // 更新平均买入价格
                            if state_guard.total_buy_amount > 0.0 {
                                state_guard.avg_buy_price =
                                    state_guard.total_buy_volume / state_guard.total_buy_amount;
                            }
                        }
                        OrderSide::Sell => {
                            let volume = report.executed_amount * report.executed_price;
                            state_guard.position -= volume;
                            state_guard.total_sell_volume += volume;
                            state_guard.total_sell_amount += report.executed_amount;
                            state_guard.net_position -= report.executed_amount;

                            // 更新平均卖出价格
                            if state_guard.total_sell_amount > 0.0 {
                                state_guard.avg_sell_price =
                                    state_guard.total_sell_volume / state_guard.total_sell_amount;
                            }

                            // 计算已实现盈亏（卖出时实现）
                            if state_guard.avg_buy_price > 0.0 {
                                let profit = (report.executed_price - state_guard.avg_buy_price)
                                    * report.executed_amount;
                                state_guard.realized_pnl += profit;
                            }
                        }
                    }

                    // 计算未实现盈亏
                    if state_guard.net_position != 0.0 && state_guard.avg_buy_price > 0.0 {
                        state_guard.unrealized_pnl = (state_guard.current_price
                            - state_guard.avg_buy_price)
                            * state_guard.net_position;
                    }

                    // 总盈亏 = 已实现 + 未实现 - 手续费
                    state_guard.pnl = state_guard.realized_pnl + state_guard.unrealized_pnl
                        - state_guard.total_fee;

                    // 保存交易记录到数据库
                    if let Some(ref collector) = self.collector {
                        let trade_data = TradeData {
                            trade_time: report.timestamp,
                            strategy_name: format!("trend_grid_v2_{}", self.config_id),
                            account_id: self.config.account.id.clone(),
                            exchange: self.config.account.exchange.clone(),
                            symbol: report.symbol.clone(),
                            side: format!("{:?}", report.side),
                            order_type: Some("Limit".to_string()),
                            price: Decimal::from_f64_retain(report.executed_price)
                                .unwrap_or_default(),
                            amount: Decimal::from_f64_retain(report.executed_amount)
                                .unwrap_or_default(),
                            value: Some(
                                Decimal::from_f64_retain(
                                    report.executed_price * report.executed_amount,
                                )
                                .unwrap_or_default(),
                            ),
                            fee: Some(
                                Decimal::from_f64_retain(report.commission).unwrap_or_default(),
                            ),
                            fee_currency: Some(report.commission_asset.clone()),
                            position_side: None,
                            realized_pnl: if report.side == OrderSide::Sell
                                && state_guard.avg_buy_price > 0.0
                            {
                                Some(
                                    Decimal::from_f64_retain(
                                        (report.executed_price - state_guard.avg_buy_price)
                                            * report.executed_amount,
                                    )
                                    .unwrap_or_default(),
                                )
                            } else {
                                None
                            },
                            pnl_percentage: None,
                            order_id: report.order_id.clone(),
                            parent_order_id: None,
                            metadata: None,
                        };

                        let collector_clone = collector.clone();
                        tokio::spawn(async move {
                            if let Err(e) = collector_clone.record_trade(trade_data).await {
                                log::error!("❌ 保存交易记录失败: {}", e);
                            } else {
                                log::debug!("💾 交易记录已保存到数据库");
                            }
                        });
                    }

                    drop(state_guard); // 释放锁

                    // 创建Trade对象用于网格调整
                    let trade = Trade {
                        id: report.order_id.clone(),
                        order_id: Some(report.order_id.clone()),
                        symbol: report.symbol.clone(),
                        price: report.executed_price,
                        amount: report.executed_amount,
                        timestamp: report.timestamp,
                        side: report.side,
                        fee: Some(Fee {
                            currency: report.commission_asset.clone(),
                            cost: report.commission,
                            rate: None,
                        }),
                    };

                    // 处理网格调整逻辑
                    if let Err(e) = self.handle_grid_adjustment(&trade).await {
                        log::error!("❌ {} 网格调整失败: {}", self.config_id, e);
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    // handle_state_change方法已从 trait 中移除

    async fn handle_error(&self, error: ExchangeError) -> Result<()> {
        log::error!("❌ {} WebSocket错误: {}", self.config_id, error);
        Ok(())
    }
}

impl TradeHandler {
    /// 处理网格调整
    async fn handle_grid_adjustment(&self, trade: &Trade) -> Result<()> {
        // 使用实时计算处理成交
        log::debug!("📝 {} 实时计算处理成交", self.config_id);

        // 获取账户和现有订单
        let account = match self.account_manager.get_account(&self.config.account.id) {
            Some(acc) => acc,
            None => {
                log::error!("❌ 账户 {} 不存在", self.config.account.id);
                return Ok(());
            }
        };

        // 获取现有订单
        let open_orders = match account
            .exchange
            .get_open_orders(Some(&self.config.symbol), MarketType::Futures)
            .await
        {
            Ok(orders) => orders,
            Err(e) => {
                log::error!("❌ 获取挂单失败: {}，触发网格重置", e);
                // 触发网格重置而不是退出
                {
                    let mut state = self.state.lock().await;
                    state.need_grid_reset = true;
                }
                Vec::new() // 返回空订单列表，让后续逻辑处理网格重置
            }
        };

        let state_guard = self.state.lock().await;
        let spacing = self.config.grid.spacing;
        let spacing_type = self.config.grid.spacing_type.clone();
        let orders_per_side = self.config.grid.orders_per_side;
        let order_amount = self.config.grid.order_amount;

        // 网格调整规则：
        // 成交一个订单后：
        // 1. 在对侧最近的位置补充一个新订单（成交价+/-1个间距）
        // 2. 找到现有订单中最远的价格，在更远处补充一个新订单
        // 3. 取消边缘订单以保持固定数量

        // 获取当前买卖订单
        let mut buy_orders: Vec<f64> = open_orders
            .iter()
            .filter(|o| o.side == OrderSide::Buy && o.price.is_some())
            .map(|o| o.price.unwrap())
            .collect();
        let mut sell_orders: Vec<f64> = open_orders
            .iter()
            .filter(|o| o.side == OrderSide::Sell && o.price.is_some())
            .map(|o| o.price.unwrap())
            .collect();

        buy_orders.sort_by(|a, b| b.partial_cmp(a).unwrap()); // 从高到低
        sell_orders.sort_by(|a, b| a.partial_cmp(b).unwrap()); // 从低到高

        let new_orders = match trade.side {
            OrderSide::Buy => {
                // 成交一个买单后（滚动网格）：
                // 1. 在成交价格+网格间距位置挂1个卖单（近端）
                // 2. 在最低买单价格-网格间距位置挂1个新买单（远端）

                let (sell_price, buy_price) = match spacing_type {
                    SpacingType::Arithmetic => {
                        // 等差网格
                        // 新卖单：成交价 + 间距（近端）
                        let sell_price = operations::round_price(
                            trade.price + spacing,
                            state_guard.price_precision,
                        );

                        // 新买单：在网格远端（最低买单 - 间距）
                        let buy_price = if !buy_orders.is_empty() {
                            // 获取最低的买单价格
                            let lowest_buy = buy_orders
                                .iter()
                                .min_by(|a, b| a.partial_cmp(b).unwrap())
                                .unwrap();
                            operations::round_price(
                                lowest_buy - spacing,
                                state_guard.price_precision,
                            )
                        } else {
                            // 如果没有买单，则基于成交价 - 间距*网格数量
                            operations::round_price(
                                trade.price - spacing * orders_per_side as f64,
                                state_guard.price_precision,
                            )
                        };

                        (sell_price, buy_price)
                    }
                    SpacingType::Geometric => {
                        // 等比网格
                        // 新卖单：成交价 * (1+间距)（近端）
                        let sell_price = operations::round_price(
                            trade.price * (1.0 + spacing),
                            state_guard.price_precision,
                        );

                        // 新买单：在网格远端
                        let buy_price = if !buy_orders.is_empty() {
                            let lowest_buy = buy_orders
                                .iter()
                                .min_by(|a, b| a.partial_cmp(b).unwrap())
                                .unwrap();
                            operations::round_price(
                                lowest_buy / (1.0 + spacing),
                                state_guard.price_precision,
                            )
                        } else {
                            operations::round_price(
                                trade.price / f64::powi(1.0 + spacing, orders_per_side as i32),
                                state_guard.price_precision,
                            )
                        };

                        (sell_price, buy_price)
                    }
                };

                // 根据趋势动态调整订单金额（使用配置文件参数）
                let (buy_multiplier, sell_multiplier) = match state_guard.trend_strength {
                    TrendStrength::StrongBull => {
                        (self.trend_adjustment.strong_bull_buy_multiplier, 1.0)
                    } // 强上涨：买单倍数
                    TrendStrength::Bull => (self.trend_adjustment.bull_buy_multiplier, 1.0), // 弱上涨：买单倍数
                    TrendStrength::Neutral => (1.0, 1.0), // 中性：均衡
                    TrendStrength::Bear => (1.0, self.trend_adjustment.bear_sell_multiplier), // 弱下跌：卖单倍数
                    TrendStrength::StrongBear => {
                        (1.0, self.trend_adjustment.strong_bear_sell_multiplier)
                    } // 强下跌：卖单倍数
                };

                // 计算订单金额，确保不低于最小订单金额
                let min_order_amount = 5.0; // 最小订单金额 5 USDT
                let sell_order_amount = (order_amount * sell_multiplier).max(min_order_amount);
                let buy_order_amount = (order_amount * buy_multiplier).max(min_order_amount);

                let new_orders = vec![
                    OrderRequest::new(
                        self.config.symbol.clone(),
                        OrderSide::Sell,
                        OrderType::Limit,
                        operations::round_amount(
                            sell_order_amount / sell_price,
                            state_guard.amount_precision,
                        ),
                        Some(sell_price),
                        MarketType::Futures,
                    ),
                    OrderRequest::new(
                        self.config.symbol.clone(),
                        OrderSide::Buy,
                        OrderType::Limit,
                        operations::round_amount(
                            buy_order_amount / buy_price,
                            state_guard.amount_precision,
                        ),
                        Some(buy_price),
                        MarketType::Futures,
                    ),
                ];

                new_orders
            }
            OrderSide::Sell => {
                // 成交一个卖单后（滚动网格）：
                // 1. 在成交价格-网格间距位置挂1个买单（近端）
                // 2. 在最高卖单价格+网格间距位置挂1个新卖单（远端）

                let (buy_price, sell_price) = match spacing_type {
                    SpacingType::Arithmetic => {
                        // 等差网格
                        // 新买单：成交价 - 间距（近端）
                        let buy_price = operations::round_price(
                            trade.price - spacing,
                            state_guard.price_precision,
                        );

                        // 新卖单：在网格远端（最高卖单 + 间距）
                        let sell_price = if !sell_orders.is_empty() {
                            // 获取最高的卖单价格
                            let highest_sell = sell_orders
                                .iter()
                                .max_by(|a, b| a.partial_cmp(b).unwrap())
                                .unwrap();
                            operations::round_price(
                                highest_sell + spacing,
                                state_guard.price_precision,
                            )
                        } else {
                            // 如果没有卖单，则基于成交价 + 间距*网格数量
                            operations::round_price(
                                trade.price + spacing * orders_per_side as f64,
                                state_guard.price_precision,
                            )
                        };

                        (buy_price, sell_price)
                    }
                    SpacingType::Geometric => {
                        // 等比网格
                        // 新买单：成交价 / (1+间距)（近端）
                        let buy_price = operations::round_price(
                            trade.price / (1.0 + spacing),
                            state_guard.price_precision,
                        );

                        // 新卖单：在网格远端
                        let sell_price = if !sell_orders.is_empty() {
                            let highest_sell = sell_orders
                                .iter()
                                .max_by(|a, b| a.partial_cmp(b).unwrap())
                                .unwrap();
                            operations::round_price(
                                highest_sell * (1.0 + spacing),
                                state_guard.price_precision,
                            )
                        } else {
                            operations::round_price(
                                trade.price * f64::powi(1.0 + spacing, orders_per_side as i32),
                                state_guard.price_precision,
                            )
                        };

                        (buy_price, sell_price)
                    }
                };

                // 根据趋势动态调整订单金额（使用配置文件参数）
                let (buy_multiplier, sell_multiplier) = match state_guard.trend_strength {
                    TrendStrength::StrongBull => {
                        (self.trend_adjustment.strong_bull_buy_multiplier, 1.0)
                    } // 强上涨：买单倍数
                    TrendStrength::Bull => (self.trend_adjustment.bull_buy_multiplier, 1.0), // 弱上涨：买单倍数
                    TrendStrength::Neutral => (1.0, 1.0), // 中性：不调整
                    TrendStrength::Bear => (1.0, self.trend_adjustment.bear_sell_multiplier), // 弱下跌：卖单倍数
                    TrendStrength::StrongBear => {
                        (1.0, self.trend_adjustment.strong_bear_sell_multiplier)
                    } // 强下跌：卖单倍数
                };

                // 计算订单金额，确保不低于最小订单金额
                let min_order_amount = 5.0; // 最小订单金额 5 USDT
                let buy_order_amount = (order_amount * buy_multiplier).max(min_order_amount);
                let sell_order_amount = (order_amount * sell_multiplier).max(min_order_amount);

                let new_orders = vec![
                    OrderRequest::new(
                        self.config.symbol.clone(),
                        OrderSide::Buy,
                        OrderType::Limit,
                        operations::round_amount(
                            buy_order_amount / buy_price,
                            state_guard.amount_precision,
                        ),
                        Some(buy_price),
                        MarketType::Futures,
                    ),
                    OrderRequest::new(
                        self.config.symbol.clone(),
                        OrderSide::Sell,
                        OrderType::Limit,
                        operations::round_amount(
                            sell_order_amount / sell_price,
                            state_guard.amount_precision,
                        ),
                        Some(sell_price),
                        MarketType::Futures,
                    ),
                ];

                new_orders
            }
        };

        drop(state_guard); // 释放锁

        // 精简的成交日志：交易对 买/卖 成交价 -> 新买单价 新卖单价
        let (new_buy_price, new_sell_price) = if new_orders[0].side == OrderSide::Buy {
            (
                new_orders[0].price.unwrap_or(0.0),
                new_orders[1].price.unwrap_or(0.0),
            )
        } else {
            (
                new_orders[1].price.unwrap_or(0.0),
                new_orders[0].price.unwrap_or(0.0),
            )
        };

        let symbol_short = self.config.symbol.replace("/", "");
        let side_str = if trade.side == OrderSide::Buy {
            "买"
        } else {
            "卖"
        };

        log::info!(
            "{} {} {:.4} -> 买:{:.4} 卖:{:.4}",
            symbol_short,
            side_str,
            trade.price,
            new_buy_price,
            new_sell_price
        );

        // 显示当前网格状态
        log::debug!(
            "📊 {} 当前网格: 买单{}个 [最高{:.4} - 最低{:.4}], 卖单{}个 [最低{:.4} - 最高{:.4}]",
            self.config_id,
            buy_orders.len(),
            buy_orders.first().copied().unwrap_or(0.0),
            buy_orders.last().copied().unwrap_or(0.0),
            sell_orders.len(),
            sell_orders.first().copied().unwrap_or(0.0),
            sell_orders.last().copied().unwrap_or(0.0)
        );

        // 并发执行订单提交和取消
        // 先提交新订单
        let submit_result = self
            .account_manager
            .create_batch_orders(&self.config.account.id, new_orders)
            .await;

        match submit_result {
            Ok(response) => {
                if response.failed_orders.len() > 0 {
                    log::warn!(
                        "⚠️ {} 网格调整部分成功: {} 成功, {} 失败",
                        self.config_id,
                        response.successful_orders.len(),
                        response.failed_orders.len()
                    );
                }

                // 提交成功后，更新本地缓存
                if response.successful_orders.len() > 0 {
                    // 将新订单添加到本地缓存
                    {
                        let mut state_guard = self.state.lock().await;
                        for order in &response.successful_orders {
                            state_guard
                                .active_orders
                                .insert(order.id.clone(), order.clone());
                            state_guard
                                .grid_orders
                                .insert(order.id.clone(), order.clone());
                            log::debug!(
                                "📝 {} 添加新订单到缓存: {} {:?}@{:.4}",
                                self.config_id,
                                order.id,
                                order.side,
                                order.price.unwrap_or(0.0)
                            );
                        }
                    }

                    // 等待一小段时间确保订单已经生效
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                    // 重新获取最新的挂单列表
                    if let Ok(updated_orders) = account
                        .exchange
                        .get_open_orders(Some(&self.config.symbol), MarketType::Futures)
                        .await
                    {
                        // 滚动网格取消逻辑：
                        // 买单成交：新增了近端卖单+远端买单，需要取消最远的卖单以保持平衡
                        // 卖单成交：新增了近端买单+远端卖单，需要取消最远的买单以保持平衡
                        let order_to_cancel = match trade.side {
                            OrderSide::Buy => {
                                // 成交买单后，取消最高价（最远）的卖单
                                updated_orders
                                    .iter()
                                    .filter(|o| {
                                        o.side == OrderSide::Sell && o.status == OrderStatus::Open
                                    })
                                    .max_by(|a, b| {
                                        a.price
                                            .partial_cmp(&b.price)
                                            .unwrap_or(std::cmp::Ordering::Equal)
                                    })
                            }
                            OrderSide::Sell => {
                                // 成交卖单后，取消最低价（最远）的买单
                                updated_orders
                                    .iter()
                                    .filter(|o| {
                                        o.side == OrderSide::Buy && o.status == OrderStatus::Open
                                    })
                                    .min_by(|a, b| {
                                        a.price
                                            .partial_cmp(&b.price)
                                            .unwrap_or(std::cmp::Ordering::Equal)
                                    })
                            }
                        };

                        // 执行取消
                        if let Some(order) = order_to_cancel {
                            log::info!(
                                "📌 {} 取消边缘订单: {:?}@{:.4}",
                                self.config_id,
                                order.side,
                                order.price.unwrap_or(0.0)
                            );

                            // 取消订单，忽略"Unknown order"错误（订单可能已成交或已取消）
                            match account
                                .exchange
                                .cancel_order(&order.id, &self.config.symbol, MarketType::Futures)
                                .await
                            {
                                Ok(_) => {
                                    log::debug!(
                                        "✅ {} 成功取消边缘订单 {}",
                                        self.config_id,
                                        order.id
                                    );
                                    // 从本地缓存中移除已取消的订单
                                    {
                                        let mut state_guard = self.state.lock().await;
                                        state_guard.active_orders.remove(&order.id);
                                        state_guard.grid_orders.remove(&order.id);
                                        log::debug!(
                                            "🗑️ {} 从缓存中移除已取消订单: {}",
                                            self.config_id,
                                            order.id
                                        );
                                    }
                                }
                                Err(e) => {
                                    // 检查是否是"Unknown order"错误
                                    let error_str = e.to_string();
                                    if error_str.contains("Unknown order")
                                        || error_str.contains("-2011")
                                    {
                                        // 订单不存在，可能已成交或已取消，这是正常情况
                                        log::debug!(
                                            "⚠️ {} 边缘订单 {} 已不存在（可能已成交）",
                                            self.config_id,
                                            order.id
                                        );
                                        // 也从缓存中移除
                                        {
                                            let mut state_guard = self.state.lock().await;
                                            state_guard.active_orders.remove(&order.id);
                                            state_guard.grid_orders.remove(&order.id);
                                        }
                                    } else {
                                        // 其他错误才记录为错误
                                        log::error!("❌ {} 取消订单失败: {}", self.config_id, e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                log::error!("❌ {} 提交调整订单失败: {}", self.config_id, e);
            }
        }

        Ok(())
    }
}

type Result<T> = std::result::Result<T, ExchangeError>;
