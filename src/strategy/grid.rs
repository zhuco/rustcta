use crate::config::endpoints::WsConnectionStatus;
use crate::config::strategy_config::{GridConfig, GridStrategyConfig};
use crate::error::AppError;
use crate::exchange::binance_model::{
    AccountInfo, Order, OrderUpdate, Position, SymbolInfo, WsEvent,
};
use crate::exchange::traits::Exchange;
use crate::utils::symbol::{MarketType, Symbol};
use crate::SHUTDOWN;
use crate::{strategy_error, strategy_info, strategy_warn};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// 网格均匀性检查结果
#[derive(Debug, Clone)]
struct GridUniformityResult {
    pub is_uniform: bool,
    pub buy_spacing_consistency: f64, // 做多网格间距一致程度 (百分比)
    pub sell_spacing_consistency: f64, // 做空网格间距一致程度 (百分比)
    pub grid_count_consistency: f64,  // 网格数量一致程度 (百分比)
}

/// 网格状态，保存买卖订单（线程安全版本）
struct GridState {
    buy_orders: Vec<Order>,
    sell_orders: Vec<Order>,
}

/// 网格状态的原子操作包装器
type AtomicGridState = Arc<Mutex<GridState>>;

impl GridState {
    /// 创建一个新的 GridState 实例
    fn new() -> Self {
        Self {
            buy_orders: Vec::new(),
            sell_orders: Vec::new(),
        }
    }

    /// 检查网格均匀性
    fn check_grid_uniformity(&self, config: &GridConfig) -> GridUniformityResult {
        let expected_count = config.grid_num as usize / 2;
        let buy_count = self.buy_orders.len();
        let sell_count = self.sell_orders.len();

        // 计算网格数量一致程度
        let grid_count_consistency = if expected_count == 0 {
            100.0
        } else {
            let buy_consistency = (1.0
                - (buy_count as f64 - expected_count as f64).abs() / expected_count as f64)
                .max(0.0)
                * 100.0;
            let sell_consistency = (1.0
                - (sell_count as f64 - expected_count as f64).abs() / expected_count as f64)
                .max(0.0)
                * 100.0;
            (buy_consistency + sell_consistency) / 2.0
        };

        // 如果订单数量太少，返回低一致性结果
        if buy_count < 2 || sell_count < 2 {
            return GridUniformityResult {
                is_uniform: false,
                buy_spacing_consistency: 0.0,
                sell_spacing_consistency: 0.0,
                grid_count_consistency,
            };
        }

        // 计算买单价格间距一致性
        let mut buy_prices: Vec<_> = self.buy_orders.iter().map(|o| o.price).collect();
        buy_prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let buy_spacing_consistency = if buy_prices.len() < 2 {
            0.0
        } else {
            let mut total_deviation = 0.0;
            let mut spacing_count = 0;

            for i in 0..buy_prices.len() - 1 {
                let actual_spacing = buy_prices[i + 1] - buy_prices[i];
                let deviation = (actual_spacing - config.grid_spacing).abs() / config.grid_spacing;
                total_deviation += deviation;
                spacing_count += 1;
            }

            if spacing_count > 0 {
                let avg_deviation = total_deviation / spacing_count as f64;
                (1.0 - avg_deviation).max(0.0) * 100.0
            } else {
                0.0
            }
        };

        // 计算卖单价格间距一致性
        let mut sell_prices: Vec<_> = self.sell_orders.iter().map(|o| o.price).collect();
        sell_prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let sell_spacing_consistency = if sell_prices.len() < 2 {
            0.0
        } else {
            let mut total_deviation = 0.0;
            let mut spacing_count = 0;

            for i in 0..sell_prices.len() - 1 {
                let actual_spacing = sell_prices[i + 1] - sell_prices[i];
                let deviation = (actual_spacing - config.grid_spacing).abs() / config.grid_spacing;
                total_deviation += deviation;
                spacing_count += 1;
            }

            if spacing_count > 0 {
                let avg_deviation = total_deviation / spacing_count as f64;
                (1.0 - avg_deviation).max(0.0) * 100.0
            } else {
                0.0
            }
        };

        // 判断是否均匀（使用原有的容差逻辑）
        let spacing_tolerance = config.grid_spacing * 0.1; // 10%的容差
        let mut is_uniform = true;

        // 检查数量是否在合理范围内
        if buy_count < expected_count.saturating_sub(2)
            || buy_count > expected_count + 2
            || sell_count < expected_count.saturating_sub(2)
            || sell_count > expected_count + 2
        {
            is_uniform = false;
        }

        // 检查买单间距
        if is_uniform {
            for i in 0..buy_prices.len() - 1 {
                let actual_spacing = buy_prices[i + 1] - buy_prices[i];
                if (actual_spacing - config.grid_spacing).abs() > spacing_tolerance {
                    is_uniform = false;
                    break;
                }
            }
        }

        // 检查卖单间距
        if is_uniform {
            for i in 0..sell_prices.len() - 1 {
                let actual_spacing = sell_prices[i + 1] - sell_prices[i];
                if (actual_spacing - config.grid_spacing).abs() > spacing_tolerance {
                    is_uniform = false;
                    break;
                }
            }
        }

        GridUniformityResult {
            is_uniform,
            buy_spacing_consistency,
            sell_spacing_consistency,
            grid_count_consistency,
        }
    }
}

/// 网格策略
pub struct GridStrategy {
    config: GridStrategyConfig,
    states: HashMap<Symbol, AtomicGridState>,
    strategy_name: String,
}

impl GridStrategy {
    /// 创建一个新的 GridStrategy 实例
    pub fn new(config: GridStrategyConfig, strategy_name: String) -> Self {
        let mut states = HashMap::new();
        // 为每个网格配置创建一个状态
        for grid_config in &config.grid_configs {
            states.insert(
                grid_config.symbol.clone(),
                Arc::new(Mutex::new(GridState::new())),
            );
        }
        Self { config, states, strategy_name }
    }

    /// 运行策略
    pub async fn run(&mut self, exchange: Arc<dyn Exchange + Send + Sync>) -> Result<(), AppError> {
        // 为每个网格配置启动一个 runner
        let mut handles = Vec::new();
        let strategy_name = self.strategy_name.clone();

        for grid_config in &self.config.grid_configs {
            let exchange_clone = exchange.clone();
            let grid_config_clone = grid_config.clone();
            let state = self.states.get(&grid_config.symbol).unwrap().clone(); // 获取状态
            let strategy_name_clone = strategy_name.clone();

            let handle = tokio::spawn(async move {
                // 创建并运行 GridRunner
                match GridRunner::new(grid_config_clone, exchange_clone, state, strategy_name_clone).await {
                    Ok(mut runner) => runner.run().await,
                    Err(e) => {
                        eprintln!("创建网格 runner 失败: {e}");
                        Err(e)
                    }
                }
            });
            handles.push(handle);
        }

        // 等待所有任务完成，如果任何一个失败就返回错误
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}             // 任务成功完成
                Ok(Err(e)) => return Err(e), // 任务返回错误
                Err(e) => return Err(AppError::Other(format!("任务执行失败: {}", e))), // 任务被取消或panic
            }
        }

        Ok(())
    }
}

/// 预计算的订单操作
#[derive(Debug, Clone)]
struct PreComputedAction {
    new_orders: Vec<(String, String, f64, Option<f64>)>, // 要提交的新订单
    cancel_order_id: Option<i64>,                        // 要取消的订单ID
}

/// 网格策略的执行器
struct GridRunner {
    config: GridConfig,
    exchange: Arc<dyn Exchange + Send + Sync>,
    state: AtomicGridState,
    symbol_info: SymbolInfo,
    ws_status: Arc<tokio::sync::Mutex<WsConnectionStatus>>,
    // 缓存的精度信息，避免重复计算
    price_precision: u32,
    quantity_precision: u32,
    min_notional: f64,
    // 串行化处理订单成交的通道
    adjust_tx: tokio::sync::mpsc::Sender<crate::exchange::binance_model::OrderUpdate>,
    adjust_rx: Option<tokio::sync::mpsc::Receiver<crate::exchange::binance_model::OrderUpdate>>,
    // 预计算的订单操作缓存
    pre_computed_actions: Arc<tokio::sync::Mutex<HashMap<String, PreComputedAction>>>,

    // 策略控制状态
    should_stop: Arc<std::sync::atomic::AtomicBool>, // 是否应该停止策略
    initial_balance: Option<f64>,                    // 初始资金余额，用于计算亏损
    total_realized_pnl: Arc<std::sync::Mutex<f64>>,  // 累计已实现盈亏
    strategy_name: String,                           // 策略名称，用于更新配置文件
}

impl GridRunner {
    /// 快速格式化价格，使用缓存的精度
    fn format_price_fast(&self, price: f64) -> f64 {
        let multiplier = 10_f64.powi(self.price_precision as i32);
        (price * multiplier).round() / multiplier
    }

    /// 快速格式化数量，使用缓存的精度
    fn format_quantity_fast(&self, quantity: f64) -> f64 {
        let multiplier = 10_f64.powi(self.quantity_precision as i32);
        (quantity * multiplier).round() / multiplier
    }

    /// 更新配置文件中的策略enabled状态
    async fn update_strategy_enabled_status(&self, enabled: bool) -> Result<(), AppError> {
        use std::fs;
        use serde_yaml;
        
        strategy_info!("grid", "🔄 开始更新策略{}的enabled状态为: {}", self.strategy_name, enabled);
        
        let config_path = "config/strategy.yml";
        
        // 读取现有配置文件
        let config_content = fs::read_to_string(config_path)
            .map_err(|e| {
                let error_msg = format!("读取配置文件失败: {}", e);
                strategy_error!("grid", "❌ {}", error_msg);
                AppError::Other(error_msg)
            })?;
        
        strategy_info!("grid", "✅ 成功读取配置文件，长度: {} 字符", config_content.len());
        
        // 解析YAML
        let mut config: serde_yaml::Value = serde_yaml::from_str(&config_content)
            .map_err(|e| {
                let error_msg = format!("解析配置文件失败: {}", e);
                strategy_error!("grid", "❌ {}", error_msg);
                AppError::Other(error_msg)
            })?;
        
        strategy_info!("grid", "✅ 成功解析YAML配置文件");
        
        // 查找并更新对应策略的enabled状态
        let mut found = false;
        if let Some(strategies) = config.get_mut("strategies").and_then(|s| s.as_sequence_mut()) {
            strategy_info!("grid", "📋 找到strategies数组，包含{}个策略", strategies.len());
            for strategy in strategies {
                if let Some(name) = strategy.get("name").and_then(|n| n.as_str()) {
                    strategy_info!("grid", "🔍 检查策略: {}", name);
                    if name == self.strategy_name {
                        strategy_info!("grid", "🎯 找到目标策略: {}", name);
                        found = true;
                        
                        // 获取当前enabled状态
                        let current_enabled = strategy.get("enabled").and_then(|e| e.as_bool()).unwrap_or(true);
                        strategy_info!("grid", "📊 当前enabled状态: {} -> 新状态: {}", current_enabled, enabled);
                        
                        strategy.as_mapping_mut()
                            .and_then(|m| m.insert(
                                serde_yaml::Value::String("enabled".to_string()),
                                serde_yaml::Value::Bool(enabled)
                            ));
                        
                        strategy_info!(
                            "grid",
                            "✅ 已将策略 {} 的enabled状态更新为: {}",
                            self.strategy_name,
                            enabled
                        );
                        break;
                    }
                }
            }
        }
        
        if !found {
            let error_msg = format!("未找到策略: {}", self.strategy_name);
            strategy_error!("grid", "❌ {}", error_msg);
            return Err(AppError::Other(error_msg));
        }
        
        // 写回配置文件
        let updated_content = serde_yaml::to_string(&config)
            .map_err(|e| {
                let error_msg = format!("序列化配置文件失败: {}", e);
                strategy_error!("grid", "❌ {}", error_msg);
                AppError::Other(error_msg)
            })?;
        
        strategy_info!("grid", "📝 成功序列化配置，准备写入文件，内容长度: {} 字符", updated_content.len());
        
        fs::write(config_path, updated_content)
            .map_err(|e| {
                let error_msg = format!("写入配置文件失败: {}", e);
                strategy_error!("grid", "❌ {}", error_msg);
                AppError::Other(error_msg)
            })?;
        
        strategy_info!("grid", "🎉 成功更新配置文件！策略{}的enabled状态已设置为: {}", self.strategy_name, enabled);
        
        Ok(())
    }

    /// 检查价格边界，如果触及边界则停止策略
    async fn check_price_boundaries(&self, current_price: f64) -> Result<bool, AppError> {
        // 检查最高价边界
        if let Some(max_price) = self.config.max_price {
            if current_price >= max_price {
                strategy_warn!(
                    "grid",
                    "{}价格{}触及最高价边界{}，停止策略",
                    self.config.symbol.to_binance(),
                    current_price,
                    max_price
                );
                self.should_stop
                    .store(true, std::sync::atomic::Ordering::SeqCst);

                // 如果配置了触及边界时平仓
                if self.config.close_positions_on_boundary.unwrap_or(false) {
                    self.close_all_positions().await?;
                }
                
                // 更新配置文件中的enabled状态为false
                if let Err(e) = self.update_strategy_enabled_status(false).await {
                    strategy_error!("grid", "更新配置文件失败: {}", e);
                }
                
                return Ok(true);
            }
        }

        // 检查最低价边界
        if let Some(min_price) = self.config.min_price {
            if current_price <= min_price {
                strategy_warn!(
                    "grid",
                    "{}价格{}触及最低价边界{}，停止策略",
                    self.config.symbol.to_binance(),
                    current_price,
                    min_price
                );
                self.should_stop
                    .store(true, std::sync::atomic::Ordering::SeqCst);

                // 如果配置了触及边界时平仓
                if self.config.close_positions_on_boundary.unwrap_or(false) {
                    self.close_all_positions().await?;
                }
                
                // 更新配置文件中的enabled状态为false
                if let Err(e) = self.update_strategy_enabled_status(false).await {
                    strategy_error!("grid", "更新配置文件失败: {}", e);
                }
                
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// 检查全局止损，如果亏损超过阈值则停止策略
    async fn check_global_stop_loss(&self) -> Result<bool, AppError> {
        if let Some(max_loss) = self.config.max_loss_usd {
            // 获取当前账户余额
            let account_info = self
                .exchange
                .get_account_info(MarketType::UsdFutures)
                .await?;
            let current_balance: f64 = account_info
                .total_wallet_balance
                .parse()
                .map_err(|_| AppError::Other("Failed to parse balance".to_string()))?;

            // 计算总亏损（包括未实现和已实现）
            let total_loss = if let Some(initial) = self.initial_balance {
                initial - current_balance
            } else {
                // 如果没有初始余额，使用已实现盈亏
                let realized_pnl = *self.total_realized_pnl.lock().unwrap();
                -realized_pnl // 负的盈亏表示亏损
            };

            if total_loss >= max_loss {
                strategy_warn!(
                    "grid",
                    "{}账户亏损{}USD超过止损阈值{}USD，停止策略",
                    self.config.symbol.to_binance(),
                    total_loss,
                    max_loss
                );
                self.should_stop
                    .store(true, std::sync::atomic::Ordering::SeqCst);

                // 如果配置了触发止损时平仓
                if self.config.close_positions_on_stop_loss.unwrap_or(false) {
                    self.close_all_positions().await?;
                }
                
                // 更新配置文件中的enabled状态为false
                if let Err(e) = self.update_strategy_enabled_status(false).await {
                    strategy_error!("grid", "更新配置文件失败: {}", e);
                }
                
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// 检查止盈，如果盈利达到目标则停止策略
    async fn check_take_profit(&self) -> Result<bool, AppError> {
        if let Some(take_profit_target) = self.config.take_profit_usd {
            // 获取当前账户余额
            let account_info = self
                .exchange
                .get_account_info(MarketType::UsdFutures)
                .await?;
            let current_balance: f64 = account_info
                .total_wallet_balance
                .parse()
                .map_err(|_| AppError::Other("Failed to parse balance".to_string()))?;

            // 计算总盈利（包括未实现和已实现）
            let total_profit = if let Some(initial) = self.initial_balance {
                current_balance - initial
            } else {
                // 如果没有初始余额，使用已实现盈亏
                let realized_pnl = *self.total_realized_pnl.lock().unwrap();
                realized_pnl
            };

            if total_profit >= take_profit_target {
                strategy_warn!(
                    "grid",
                    "{}账户盈利{}USD达到止盈目标{}USD，停止策略",
                    self.config.symbol.to_binance(),
                    total_profit,
                    take_profit_target
                );
                self.should_stop
                    .store(true, std::sync::atomic::Ordering::SeqCst);

                // 如果配置了触发止盈时平仓（默认为true）
                if self.config.close_positions_on_take_profit.unwrap_or(true) {
                    self.close_all_positions().await?;
                }
                
                // 更新配置文件中的enabled状态为false
                if let Err(e) = self.update_strategy_enabled_status(false).await {
                    strategy_error!("grid", "更新配置文件失败: {}", e);
                }
                
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// 平掉所有持仓
    async fn close_all_positions(&self) -> Result<(), AppError> {
        strategy_info!(
            "grid",
            "{}开始平掉所有持仓",
            self.config.symbol.to_binance()
        );

        // 取消所有挂单
        self.cancel_all_orders().await?;

        // 获取当前持仓
        let account_info = self
            .exchange
            .get_account_info(MarketType::UsdFutures)
            .await?;
        let positions = account_info.positions;

        for position in positions {
            let position_amt: f64 = position.position_amt.parse().unwrap_or(0.0);
            if position.symbol == self.config.symbol.to_binance() && position_amt.abs() > 0.0 {
                let side = if position_amt > 0.0 { "SELL" } else { "BUY" };
                let quantity = position_amt.abs();

                strategy_info!(
                    "grid",
                    "{}平仓：{} {} 数量{}",
                    self.config.symbol.to_binance(),
                    side,
                    position.symbol,
                    quantity
                );

                // 使用市价单平仓
                match self
                    .exchange
                    .place_order(&self.config.symbol, side, "MARKET", quantity, None)
                    .await
                {
                    Ok(_) => {
                        strategy_info!("grid", "{}平仓成功", self.config.symbol.to_binance());
                    }
                    Err(e) => {
                        strategy_error!(
                            "grid",
                            "{}平仓失败: {}",
                            self.config.symbol.to_binance(),
                            e
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// 取消所有挂单
    async fn cancel_all_orders(&self) -> Result<(), AppError> {
        strategy_info!(
            "grid",
            "{}开始取消所有挂单",
            self.config.symbol.to_binance()
        );

        let orders_to_cancel = {
            let state = self.state.lock().unwrap();
            let mut all_orders = Vec::new();
            all_orders.extend(state.buy_orders.iter().map(|o| o.order_id));
            all_orders.extend(state.sell_orders.iter().map(|o| o.order_id));
            all_orders
        };

        for order_id in orders_to_cancel {
            match self
                .exchange
                .cancel_order(&self.config.symbol, order_id)
                .await
            {
                Ok(_) => {
                    strategy_info!("grid", "取消订单{}成功", order_id);
                }
                Err(e) => {
                    strategy_warn!("grid", "取消订单{}失败: {}", order_id, e);
                }
            }
        }

        // 清空本地状态
        {
            let mut state = self.state.lock().unwrap();
            state.buy_orders.clear();
            state.sell_orders.clear();
        }

        Ok(())
    }

    /// 并行处理多个批次的订单（优化版本）
    async fn place_orders_optimized(
        &self,
        orders: Vec<(String, String, f64, Option<f64>)>,
    ) -> Result<Vec<Order>, AppError> {
        const BATCH_SIZE: usize = 5;
        let mut all_results = Vec::new();

        if orders.is_empty() {
            return Ok(all_results);
        }

        // 将订单分割成多个批次
        let chunks: Vec<_> = orders.chunks(BATCH_SIZE).collect();

        // 并行处理所有批次
        let futures: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                self.exchange
                    .place_batch_orders(&self.config.symbol, chunk.to_vec())
            })
            .collect();

        let results = futures_util::future::join_all(futures).await;

        // 处理结果
        for result in results {
            match result {
                Ok(orders) => all_results.extend(orders),
                Err(e) => return Err(e),
            }
        }

        Ok(all_results)
    }

    /// 预计算订单操作（成交前预先计算）
    async fn pre_compute_actions(&self) -> Result<(), AppError> {
        let mut actions = HashMap::new();

        // 遍历所有买单，预计算如果成交会产生什么操作
        let buy_orders = {
            let state = self.state.lock().unwrap();
            state.buy_orders.clone()
        };
        for buy_order in &buy_orders {
            let action = self.compute_buy_fill_action(buy_order.price).await?;
            actions.insert(buy_order.client_order_id.clone(), action);
        }

        // 遍历所有卖单，预计算如果成交会产生什么操作
        let sell_orders = {
            let state = self.state.lock().unwrap();
            state.sell_orders.clone()
        };
        for sell_order in &sell_orders {
            let action = self.compute_sell_fill_action(sell_order.price).await?;
            actions.insert(sell_order.client_order_id.clone(), action);
        }

        // 更新预计算缓存
        *self.pre_computed_actions.lock().await = actions;

        Ok(())
    }

    /// 计算买单成交时的操作
    async fn compute_buy_fill_action(
        &self,
        filled_price: f64,
    ) -> Result<PreComputedAction, AppError> {
        let mut new_orders = Vec::new();

        // 计算新订单价格
        let new_sell_price = filled_price + self.config.grid_spacing;
        let grid_count = (self.config.grid_num / 2) as f64;
        let new_buy_price = filled_price - (self.config.grid_spacing * grid_count);

        // 使用快速格式化
        let adjusted_sell_price = self.format_price_fast(new_sell_price);
        let adjusted_buy_price = self.format_price_fast(new_buy_price);

        // 计算数量
        let sell_base_quantity = self.config.order_value / adjusted_sell_price;
        let buy_base_quantity = self.config.order_value / adjusted_buy_price;
        let adjusted_sell_quantity = self.format_quantity_fast(sell_base_quantity);
        let adjusted_buy_quantity = self.format_quantity_fast(buy_base_quantity);

        // 验证并添加空单
        if adjusted_sell_price * adjusted_sell_quantity >= self.min_notional {
            new_orders.push((
                "SELL".to_string(),
                "LIMIT".to_string(),
                adjusted_sell_quantity,
                Some(adjusted_sell_price),
            ));
        }

        // 验证并添加多单
        if adjusted_buy_price * adjusted_buy_quantity >= self.min_notional {
            new_orders.push((
                "BUY".to_string(),
                "LIMIT".to_string(),
                adjusted_buy_quantity,
                Some(adjusted_buy_price),
            ));
        }

        // 找到要取消的订单 - 买单成交时取消最高价卖单
        let cancel_order_id = self
            .state
            .lock()
            .unwrap()
            .sell_orders
            .iter()
            .max_by(|a, b| {
                a.price
                    .partial_cmp(&b.price)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|o| o.order_id);

        Ok(PreComputedAction {
            new_orders,
            cancel_order_id,
        })
    }

    /// 计算卖单成交时的操作
    async fn compute_sell_fill_action(
        &self,
        filled_price: f64,
    ) -> Result<PreComputedAction, AppError> {
        let mut new_orders = Vec::new();

        // 计算新订单价格
        let new_buy_price = filled_price - self.config.grid_spacing;
        let grid_count = (self.config.grid_num / 2) as f64;
        let new_sell_price = filled_price + (self.config.grid_spacing * grid_count);

        // 使用快速格式化
        let adjusted_buy_price = self.format_price_fast(new_buy_price);
        let adjusted_sell_price = self.format_price_fast(new_sell_price);

        // 计算数量
        let buy_base_quantity = self.config.order_value / adjusted_buy_price;
        let sell_base_quantity = self.config.order_value / adjusted_sell_price;
        let adjusted_buy_quantity = self.format_quantity_fast(buy_base_quantity);
        let adjusted_sell_quantity = self.format_quantity_fast(sell_base_quantity);

        // 验证并添加多单
        if adjusted_buy_price * adjusted_buy_quantity >= self.min_notional {
            new_orders.push((
                "BUY".to_string(),
                "LIMIT".to_string(),
                adjusted_buy_quantity,
                Some(adjusted_buy_price),
            ));
        }

        // 验证并添加空单
        if adjusted_sell_price * adjusted_sell_quantity >= self.min_notional {
            new_orders.push((
                "SELL".to_string(),
                "LIMIT".to_string(),
                adjusted_sell_quantity,
                Some(adjusted_sell_price),
            ));
        }

        // 找到要取消的订单 - 卖单成交时取消最低价买单
        let cancel_order_id = self
            .state
            .lock()
            .unwrap()
            .buy_orders
            .iter()
            .min_by(|a, b| {
                a.price
                    .partial_cmp(&b.price)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|o| o.order_id);

        Ok(PreComputedAction {
            new_orders,
            cancel_order_id,
        })
    }

    /// 根据成交订单调整网格（优化版本：使用预计算 + 并发执行）
    async fn adjust_grid(&mut self, filled_order: OrderUpdate) -> Result<(), AppError> {
        let filled_price = filled_order.last_filled_price;

        // 注释掉重复的日志输出，因为binance.rs中的WebSocket处理已经有日志了
        // if filled_order.side == "BUY" {
        //     strategy_info!(
        //         &self.config.symbol.to_binance(),
        //         "多单成交 价格:{}",
        //         filled_price
        //     );
        // } else {
        //     strategy_info!(
        //         &self.config.symbol.to_binance(),
        //         "空单成交 价格:{}",
        //         filled_price
        //     );
        // }

        // 先从状态中移除已成交的订单，并计算盈亏
        {
            let mut state = self.state.lock().unwrap();

            // 计算已实现盈亏（基于实际买卖价差）
            let grid_spacing = self.config.grid_spacing;
            let quantity = filled_order.order_last_filled_quantity;
            let filled_price = filled_order.last_filled_price;

            // 网格策略的盈亏计算：通过低买高卖实现盈利
            let realized_pnl = if filled_order.side == "SELL" {
                // 卖单成交，计算相对于网格中心价格的收益
                // 假设网格中心价格为当前价格减去一半网格间距
                let estimated_buy_price = filled_price - grid_spacing;
                let profit_per_unit = filled_price - estimated_buy_price;
                profit_per_unit * quantity
            } else {
                // 买单成交，计算相对于网格中心价格的潜在收益
                // 这里记录为负值，表示成本，当对应卖单成交时会产生正收益
                let estimated_sell_price = filled_price + grid_spacing;
                let potential_profit = estimated_sell_price - filled_price;
                // 买单成交时先记录成本，实际盈利在卖单成交时体现
                -filled_price * quantity * 0.001 // 记录小额手续费成本
            };

            // 更新累计已实现盈亏
            if let Ok(mut total_pnl) = self.total_realized_pnl.lock() {
                *total_pnl += realized_pnl;
                if realized_pnl != 0.0 {
                    strategy_info!(
                        "grid",
                        "{} 订单成交，已实现盈亏: {:.4}, 累计盈亏: {:.4}",
                        self.config.symbol.to_binance(),
                        realized_pnl,
                        *total_pnl
                    );
                }
            }

            state
                .buy_orders
                .retain(|o| o.client_order_id != filled_order.client_order_id);
            state
                .sell_orders
                .retain(|o| o.client_order_id != filled_order.client_order_id);
        }

        // 尝试从预计算缓存中获取操作
        let action = {
            let cached_actions = self.pre_computed_actions.lock().await;
            cached_actions.get(&filled_order.client_order_id).cloned()
        };

        // 如果没有预计算的操作，则动态计算
        let action = if let Some(action) = action {
            action
        } else {
            // 回退到动态计算
            if filled_order.side == "BUY" {
                self.compute_buy_fill_action(filled_price).await?
            } else {
                self.compute_sell_fill_action(filled_price).await?
            }
        };

        // 并发执行下单和取消订单操作
        let place_result = self.place_orders_optimized(action.new_orders).await;

        // 处理取消订单操作
        let cancel_result = if let Some(order_id) = action.cancel_order_id {
            // 再次确认订单仍在本地状态中，避免重复取消
            let order_exists = if filled_order.side == "BUY" {
                self.state
                    .lock()
                    .unwrap()
                    .sell_orders
                    .iter()
                    .any(|o| o.order_id == order_id)
            } else {
                self.state
                    .lock()
                    .unwrap()
                    .buy_orders
                    .iter()
                    .any(|o| o.order_id == order_id)
            };

            if order_exists {
                Some(
                    self.exchange
                        .cancel_order(&self.config.symbol, order_id)
                        .await,
                )
            } else {
                strategy_info!("grid", "订单{}已不在本地状态中，跳过取消", order_id);
                None
            }
        } else {
            None
        };

        // 处理下单结果
        match place_result {
            Ok(orders) => {
                let mut state = self.state.lock().unwrap();
                for order in orders {
                    if order.side == "BUY" {
                        state.buy_orders.push(order);
                    } else {
                        state.sell_orders.push(order);
                    }
                }
            }
            Err(e) => return Err(e),
        }

        // 处理取消结果
        if let Some(cancel_result) = cancel_result {
            match (cancel_result, action.cancel_order_id) {
                (Ok(_), Some(order_id)) => {
                    let mut state = self.state.lock().unwrap();
                    if filled_order.side == "BUY" {
                        state.sell_orders.retain(|o| o.order_id != order_id);
                    } else {
                        state.buy_orders.retain(|o| o.order_id != order_id);
                    }
                }
                (Err(crate::error::AppError::BinanceError { code: -2011, .. }), Some(order_id)) => {
                    // 订单不存在或已取消，从本地状态移除
                    strategy_info!("grid", "订单{}已不存在，从本地状态移除", order_id);
                    let mut state = self.state.lock().unwrap();
                    if filled_order.side == "BUY" {
                        state.sell_orders.retain(|o| o.order_id != order_id);
                    } else {
                        state.buy_orders.retain(|o| o.order_id != order_id);
                    }
                }
                (Err(e), Some(order_id)) => {
                    let side_desc = if filled_order.side == "BUY" {
                        "最高价卖单"
                    } else {
                        "最低价买单"
                    };
                    strategy_warn!("grid", "取消{}失败: {}, 订单ID: {}", side_desc, e, order_id);
                }
                _ => {}
            }
        }

        // 从预计算缓存中移除已处理的订单
        {
            let mut cached_actions = self.pre_computed_actions.lock().await;
            cached_actions.remove(&filled_order.client_order_id);
        }

        // 网格调整完成后，重新预计算操作
        if let Err(e) = self.pre_compute_actions().await {
            strategy_warn!("grid", "重新预计算操作失败: {}", e);
        }

        Ok(())
    }

    /// 静态版本的网格调整函数，用于通道处理
    async fn adjust_grid_static(
        state: AtomicGridState,
        exchange: Arc<dyn Exchange + Send + Sync>,
        config: GridConfig,
        filled_order: OrderUpdate,
    ) -> Result<(), AppError> {
        // 创建一个临时的GridRunner来调用现有的adjust_grid方法
        // 这样可以避免重复实现复杂的逻辑
        let exchange_info = exchange
            .get_exchange_info(crate::utils::symbol::MarketType::UsdFutures)
            .await?;
        let symbol_info = exchange_info
            .symbols
            .into_iter()
            .find(|s| s.symbol == config.symbol.to_binance())
            .ok_or_else(|| {
                AppError::Other(format!("交易对未找到: {}", config.symbol.to_binance()))
            })?;

        let (adjust_tx, adjust_rx) = tokio::sync::mpsc::channel(1);
        let ws_status = Arc::new(tokio::sync::Mutex::new(WsConnectionStatus::Disconnected));
        let pre_computed_actions = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        // 计算精度信息
        let price_precision = symbol_info
            .filters
            .iter()
            .find_map(|f| match f {
                crate::exchange::binance_model::Filter::PriceFilter { tick_size, .. } => {
                    Some(tick_size.split('.').nth(1).map(|s| s.len()).unwrap_or(0) as u32)
                }
                _ => None,
            })
            .unwrap_or(8);

        let quantity_precision = symbol_info
            .filters
            .iter()
            .find_map(|f| match f {
                crate::exchange::binance_model::Filter::LotSize { step_size, .. } => {
                    Some(step_size.split('.').nth(1).map(|s| s.len()).unwrap_or(0) as u32)
                }
                _ => None,
            })
            .unwrap_or(8);

        let min_notional = symbol_info
            .filters
            .iter()
            .find_map(|f| match f {
                crate::exchange::binance_model::Filter::MinNotional { notional, .. } => {
                    Some(notional.parse::<f64>().unwrap_or(10.0))
                }
                _ => None,
            })
            .unwrap_or(10.0);

        let mut runner = GridRunner {
            config,
            exchange,
            state,
            symbol_info,
            ws_status,
            price_precision,
            quantity_precision,
            min_notional,
            adjust_tx,
            adjust_rx: Some(adjust_rx),
            pre_computed_actions,
            should_stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            initial_balance: None, // 静态方法中不获取初始余额
            total_realized_pnl: Arc::new(std::sync::Mutex::new(0.0)),
            strategy_name: "GridStrategy".to_string(), // 静态方法中使用默认名称
        };

        // 调用现有的adjust_grid方法
        runner.adjust_grid(filled_order).await
    }

    /// 创建一个新的 GridRunner 实例
    async fn new(
        config: GridConfig,
        exchange: Arc<dyn Exchange + Send + Sync>,
        state: AtomicGridState,
        strategy_name: String,
    ) -> Result<Self, AppError> {
        // 获取交易所信息
        let exchange_info = exchange
            .get_exchange_info(config.symbol.market_type)
            .await?;
        
        // 查找交易对信息
        let symbol_info = exchange_info
            .symbols
            .into_iter()
            .find(|s| s.symbol == config.symbol.to_binance())
            .ok_or_else(|| {
                AppError::Other(format!("交易对未找到: {}", config.symbol.to_binance()))
            })?;

        // 缓存精度信息
        let price_precision = symbol_info
            .filters
            .iter()
            .find_map(|f| match f {
                crate::exchange::binance_model::Filter::PriceFilter { tick_size, .. } => {
                    Some(tick_size.split('.').nth(1).map(|s| s.len()).unwrap_or(0) as u32)
                }
                _ => None,
            })
            .unwrap_or(8);

        let quantity_precision = symbol_info
            .filters
            .iter()
            .find_map(|f| match f {
                crate::exchange::binance_model::Filter::LotSize { step_size, .. } => {
                    Some(step_size.split('.').nth(1).map(|s| s.len()).unwrap_or(0) as u32)
                }
                _ => None,
            })
            .unwrap_or(8);

        let min_notional = symbol_info
            .filters
            .iter()
            .find_map(|f| match f {
                crate::exchange::binance_model::Filter::MinNotional { notional } => {
                    Some(notional.parse::<f64>().unwrap_or(10.0))
                }
                _ => None,
            })
            .unwrap_or(10.0);

        // 创建快速处理通道
        let (adjust_tx, adjust_rx) = tokio::sync::mpsc::channel(100);

        // 设置杠杆倍数（如果配置了杠杆）
        if let Some(leverage) = config.leverage {
            if config.symbol.market_type == MarketType::UsdFutures {
                match exchange.set_leverage(&config.symbol, leverage).await {
                    Ok(_) => {
                        strategy_info!(
                            "grid",
                            "{}成功设置杠杆为{}倍",
                            config.symbol.to_binance(),
                            leverage
                        );
                    }
                    Err(e) => {
                        strategy_warn!(
                            "grid",
                            "{}设置杠杆失败: {}，将使用默认杠杆",
                            config.symbol.to_binance(),
                            e
                        );
                    }
                }
            } else {
                strategy_warn!(
                    "grid",
                    "{}现货市场不支持设置杠杆，忽略杠杆配置",
                    config.symbol.to_binance()
                );
            }
        }

        // 获取初始账户余额用于止损计算
        let initial_balance = match exchange.get_account_info(MarketType::UsdFutures).await {
            Ok(account_info) => {
                strategy_info!(
                    "grid",
                    "{}策略启动，初始账户余额: {}USD",
                    config.symbol.to_binance(),
                    account_info.total_wallet_balance
                );
                account_info.total_wallet_balance.parse::<f64>().ok()
            }
            Err(e) => {
                strategy_warn!(
                    "grid",
                    "{}获取初始账户余额失败: {}，将使用已实现盈亏计算止损",
                    config.symbol.to_binance(),
                    e
                );
                None
            }
        };

        Ok(Self {
            config,
            exchange,
            state,
            symbol_info,
            ws_status: Arc::new(tokio::sync::Mutex::new(WsConnectionStatus::Disconnected)),
            price_precision,
            quantity_precision,
            min_notional,
            adjust_tx,
            adjust_rx: Some(adjust_rx),
            pre_computed_actions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            should_stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            initial_balance,
            total_realized_pnl: Arc::new(std::sync::Mutex::new(0.0)),
            strategy_name,
        })
    }

    /// 运行网格策略的主循环
    async fn run(&mut self) -> Result<(), AppError> {
        // 策略主循环
        // 事件循环
        loop {
            // 检查全局关闭信号
            if SHUTDOWN.load(Ordering::SeqCst) {
                strategy_info!(
                    "grid",
                    "收到关闭信号，正在停止网格策略 {}",
                    self.config.symbol.to_binance()
                );
                return Ok(());
            }

            // 初始化网格
            if let Err(e) = self.initialize_grid().await {
                strategy_error!(
                    "grid",
                    "初始化网格失败 {}: {}. 60秒后重试.",
                    self.config.symbol.to_binance(),
                    e
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                continue;
            }

            // 创建一个通道用于 websocket 消息
            let (tx, mut rx) = tokio::sync::mpsc::channel(100);

            // 克隆 self 用于 websocket 任务
            // 启动 websocket 消息处理任务
            let exchange_ws = self.exchange.clone();
            let symbol_ws = self.config.symbol.clone();
            let ws_status_ws = self.ws_status.clone();
            let mut ws_handle = tokio::spawn(async move {
                Self::handle_websocket_messages_static(exchange_ws, symbol_ws, ws_status_ws, tx)
                    .await
            });

            // 创建网格健康检查定时器
            let health_check_interval = self.config.health_check_interval_seconds.unwrap_or(180);
            let mut health_check_timer =
                tokio::time::interval(tokio::time::Duration::from_secs(health_check_interval));
            health_check_timer.tick().await; // 跳过第一次立即触发

            // 创建关闭信号检查定时器
            let mut shutdown_check_timer =
                tokio::time::interval(tokio::time::Duration::from_secs(1));

            // 暂时禁用通道接收器处理任务，直接处理消息
            // let mut adjust_rx = self.adjust_rx.take().unwrap();
            // let adjust_state = self.state.clone();
            // let adjust_exchange = self.exchange.clone();
            // let adjust_config = self.config.clone();
            // let adjust_handle = tokio::spawn(async move {
            //     while let Some(order_update) = adjust_rx.recv().await {
            //         strategy_info!("grid", "{} 开始处理挂单方成交订单: {}",
            //             adjust_config.symbol.to_binance(), order_update.order_id);
            //
            //         // 简化处理：直接调用adjust_grid_static，不处理复杂的Future
            //         match Self::adjust_grid_static(adjust_state.clone(), adjust_exchange.clone(), adjust_config.clone(), order_update).await {
            //             Ok(_) => {
            //                 strategy_info!("grid", "{} 网格调整完成", adjust_config.symbol.to_binance());
            //             }
            //             Err(e) => {
            //                 strategy_error!("grid", "{} 网格调整失败: {}", adjust_config.symbol.to_binance(), e);
            //             }
            //         }
            //     }
            // });

            loop {
                tokio::select! {
                    // 检查关闭信号
                    _ = shutdown_check_timer.tick() => {
                        if SHUTDOWN.load(Ordering::SeqCst) {
                            strategy_info!("grid", "收到关闭信号，正在停止网格策略 {}", self.config.symbol.to_binance());
                            ws_handle.abort();
                            // adjust_handle.abort();
                            return Ok(());
                        }
                    }
                    // 从 websocket 通道接收消息 - 快速响应
                    Some(msg) = rx.recv() => {
                        // 如果是订单更新事件
                        if let WsEvent::OrderTradeUpdate { event_time, transaction_time, order } = msg {
                            // 检查订单是否是当前策略的，并且状态是已成交
                            if order.symbol == self.config.symbol.to_binance() && order.order_status == "FILLED" {
                                // 检查订单类型和成交方式
                                let is_market_order = order.order_type == "MARKET";
                                let is_taker = !order.is_maker; // is_maker为false表示是吃单方

                                if is_market_order || is_taker {
                                    // 市价单成交或吃单方成交，重置网格
                                    if is_market_order {
                                        strategy_warn!("grid", "{} 检测到市价单成交，重置网格。订单ID: {}, 类型: {}",
                                            self.config.symbol.to_binance(), order.order_id, order.order_type);
                                    } else {
                                        strategy_warn!("grid", "{} 检测到吃单方成交，重置网格。订单ID: {}, 是否挂单方: {}",
                                            self.config.symbol.to_binance(), order.order_id, order.is_maker);
                                    }
                                    ws_handle.abort();
                                    // adjust_handle.abort();
                                    break; // 跳出内层循环以重新初始化网格
                                } else {
                                    // 限价单且为挂单方成交，打印成交日志并调用adjust_grid
                                    if order.order_status == "FILLED" && order.last_filled_price > 0.0 && order.execution_type == "TRADE" {
                                        let now = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap()
                                            .as_millis();
                                        let side_text = match order.side.as_str() {
                                            "BUY" => "多单",
                                            "SELL" => "空单",
                                            _ => &order.side,
                                        };
                                        println!(
                                            "[{}] {}  {}成交 价格:{}",
                                            now, order.symbol, side_text, order.last_filled_price
                                        );

                                        if let Err(e) = self.adjust_grid(order.clone()).await {
                                            strategy_error!("grid", "{} 网格调整失败: {}", self.config.symbol.to_binance(), e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // 网格健康检查定时器触发
                    _ = health_check_timer.tick() => {
                        // 检查是否应该停止策略
                        if self.should_stop.load(Ordering::SeqCst) {
                            strategy_info!("grid", "策略已标记为停止，正在退出 {}", self.config.symbol.to_binance());
                            ws_handle.abort();
                            return Ok(());
                        }

                        // 获取当前价格进行边界检查
                        if let Ok(current_price) = self.exchange.get_price(&self.config.symbol).await {

                            // 检查价格边界
                            if let Ok(should_stop) = self.check_price_boundaries(current_price).await {
                                if should_stop {
                                    strategy_warn!("grid", "触及价格边界，停止策略 {}", self.config.symbol.to_binance());
                                    ws_handle.abort();
                                    return Ok(());
                                }
                            }

                            // 检查全局止损
                            if let Ok(should_stop) = self.check_global_stop_loss().await {
                                if should_stop {
                                    strategy_warn!("grid", "触发全局止损，停止策略 {}", self.config.symbol.to_binance());
                                    ws_handle.abort();
                                    return Ok(());
                                }
                            }

                            // 检查止盈
                            if let Ok(should_stop) = self.check_take_profit().await {
                                if should_stop {
                                    strategy_warn!("grid", "触发止盈，停止策略 {}", self.config.symbol.to_binance());
                                    ws_handle.abort();
                                    return Ok(());
                                }
                            }
                        }

                        // 从交易所API获取实时的开放订单
                        match self.exchange.get_open_orders(&self.config.symbol).await {
                            Ok(open_orders) => {
                                // 更新本地状态以保持同步
                                let (uniformity_result, expected_count, buy_count, sell_count) = {
                                    let mut state = self.state.lock().unwrap();
                                    state.buy_orders = open_orders.iter().filter(|o| o.side == "BUY").cloned().collect();
                                    state.sell_orders = open_orders.iter().filter(|o| o.side == "SELL").cloned().collect();

                                    let uniformity_result = state.check_grid_uniformity(&self.config);
                                    let expected_count = self.config.grid_num / 2;
                                    let buy_count = state.buy_orders.len();
                                    let sell_count = state.sell_orders.len();

                                    (uniformity_result, expected_count, buy_count, sell_count)
                                };

                                // 计算总体均匀性（类似Python代码中的uniformity计算）
                                let overall_uniformity = if expected_count == 0 {
                                    1.0
                                } else {
                                    (buy_count.min(sell_count) as f64) / (expected_count as f64)
                                };

                                // strategy_info!(
                                //     "grid",
                                //     "{} 网格健康检查。买单: {}, 卖单: {}, 期望: {}, 均匀性: {:.2}%, 买单间距一致性: {:.1}%, 卖单间距一致性: {:.1}%, 数量一致性: {:.1}% [实时API数据]",
                                //     self.config.symbol.to_binance(),
                                //     buy_count,
                                //     sell_count,
                                //     expected_count,
                                //     overall_uniformity * 100.0,
                                //     uniformity_result.buy_spacing_consistency,
                                //     uniformity_result.sell_spacing_consistency,
                                //     uniformity_result.grid_count_consistency
                                // );

                                // 检查是否需要重置网格
                                let uniformity_threshold = self.config.uniformity_threshold.unwrap_or(0.95);
                                let needs_reset = !uniformity_result.is_uniform ||
                                                overall_uniformity < uniformity_threshold ||
                                                buy_count == 0 || sell_count == 0;

                                if needs_reset {
                                    if buy_count == 0 || sell_count == 0 {
                                        strategy_warn!("grid", "{} 网格订单不完整，买单/卖单为空。", self.config.symbol.to_binance());
                                    } else {
                                        strategy_warn!("grid", "{} 网格均匀性检查不通过，重置网格", self.config.symbol.to_binance());
                                    }
                                    ws_handle.abort();
                                    break; // 跳出内层循环以重新初始化
                                }
                            }
                            Err(e) => {
                                strategy_error!("grid", "{} 获取开放订单失败: {}，使用本地状态进行健康检查", self.config.symbol.to_binance(), e);

                                // 降级到使用本地状态
                                let state = self.state.lock().unwrap();
                                let uniformity_result = state.check_grid_uniformity(&self.config);
                                let expected_count = self.config.grid_num / 2;
                                let buy_count = state.buy_orders.len();
                                let sell_count = state.sell_orders.len();

                                let overall_uniformity = if expected_count == 0 {
                                    1.0
                                } else {
                                    (buy_count.min(sell_count) as f64) / (expected_count as f64)
                                };

                                // strategy_info!(
                                //     "grid",
                                //     "{} 网格健康检查。买单: {}, 卖单: {}, 期望: {}, 均匀性: {:.2}% [本地状态数据]",
                                //     self.config.symbol.to_binance(),
                                //     buy_count,
                                //     sell_count,
                                //     expected_count,
                                //     overall_uniformity * 100.0
                                // );

                                // 如果API调用失败且本地状态显示订单为空，则重置网格
                                if buy_count == 0 || sell_count == 0 {
                                    strategy_warn!("grid", "{} API调用失败且本地状态显示订单为空，重置网格", self.config.symbol.to_binance());
                                    ws_handle.abort();
                                    break;
                                }
                            }
                        }
                    }
                    // websocket 任务结束
                    result = &mut ws_handle => {
                        match result {
                            Ok(Ok(())) => {
                                // WebSocket正常结束
                                strategy_info!("grid", "WebSocket连接正常结束 {}", self.config.symbol.to_binance());
                                break; // 跳出内层循环以重新初始化
                            }
                            Ok(Err(e)) => {
                                // WebSocket连接失败
                                let error_msg = format!("WebSocket连接失败 {}: {}", self.config.symbol.to_binance(), e);
                                strategy_error!("grid", "❌ {}", error_msg);
                                return Err(e); // 返回错误，停止策略
                            }
                            Err(e) => {
                                // 任务被取消或panic
                                let error_msg = format!("WebSocket任务异常结束 {}: {}", self.config.symbol.to_binance(), e);
                                strategy_error!("grid", "❌ {}", error_msg);
                                return Err(AppError::Other(error_msg)); // 返回错误，停止策略
                            }
                        }
                    }
                }
            }
        }
    }

    /// 初始化网格，包括取消现有订单、获取最新价格和下新订单
    async fn initialize_grid(&mut self) -> Result<(), AppError> {
        strategy_info!(
            "grid",
            "正在为 {} 初始化网格",
            self.config.symbol.to_binance()
        );

        // 1. Cancel all existing orders for the symbol
        // 1. 取消该交易对的所有现有订单
        self.exchange.cancel_all_orders(&self.config.symbol).await?;

        // 2. Clear state
        // 2. 清空状态
        {
            let mut state = self.state.lock().unwrap();
            state.buy_orders.clear();
            state.sell_orders.clear();
        }

        // 3. Get the latest price
        // 3. 获取最新价格
        let latest_price = self.exchange.get_price(&self.config.symbol).await?;

        // 4. Calculate grid levels
        // 4. 计算网格价格水平
        let (buy_prices, sell_prices) = self.calculate_grid_prices(latest_price);

        // 5. Prepare all orders using fast formatting
        // 5. 使用快速格式化准备所有订单
        let mut all_orders = Vec::new();

        // 准备买单 - 使用快速格式化
        for price in buy_prices {
            let adjusted_price = self.format_price_fast(price);
            let base_quantity = self.config.order_value / adjusted_price;
            let adjusted_quantity = self.format_quantity_fast(base_quantity);

            if adjusted_price * adjusted_quantity >= self.min_notional {
                all_orders.push((
                    "BUY".to_string(),
                    "LIMIT".to_string(),
                    adjusted_quantity,
                    Some(adjusted_price),
                ));
            }
        }

        // 准备卖单 - 使用快速格式化
        for price in sell_prices {
            let adjusted_price = self.format_price_fast(price);
            let base_quantity = self.config.order_value / adjusted_price;
            let adjusted_quantity = self.format_quantity_fast(base_quantity);

            if adjusted_price * adjusted_quantity >= self.min_notional {
                all_orders.push((
                    "SELL".to_string(),
                    "LIMIT".to_string(),
                    adjusted_quantity,
                    Some(adjusted_price),
                ));
            }
        }

        // 6. Use optimized parallel batch processing
        // 6. 使用优化的并行批次处理
        match self.place_orders_optimized(all_orders).await {
            Ok(orders) => {
                let mut state = self.state.lock().unwrap();
                for order in orders {
                    if order.side == "BUY" {
                        state.buy_orders.push(order);
                    } else {
                        state.sell_orders.push(order);
                    }
                }
            }
            Err(e) => {
                return Err(AppError::Other(format!("网格初始化失败: {}", e)));
            }
        }

        let (buy_count, sell_count) = {
            let state = self.state.lock().unwrap();
            (state.buy_orders.len(), state.sell_orders.len())
        };

        strategy_info!(
            "grid",
            "网格初始化完成，买单数量: {}, 卖单数量: {}",
            buy_count,
            sell_count
        );

        // 初始化完成后，预计算所有可能的订单操作
        if let Err(e) = self.pre_compute_actions().await {
            strategy_warn!("grid", "预计算订单操作失败: {}", e);
        }

        Ok(())
    }

    /// 根据基础价格计算网格的买卖价格
    fn calculate_grid_prices(&self, base_price: f64) -> (Vec<f64>, Vec<f64>) {
        let mut buy_prices = Vec::new();
        let mut sell_prices = Vec::new();
        let grid_count = self.config.grid_num / 2;

        // 计算每个网格水平的价格
        for i in 1..=grid_count {
            let price_diff = self.config.grid_spacing * i as f64;
            buy_prices.push(base_price - price_diff);
            sell_prices.push(base_price + price_diff);
        }

        (buy_prices, sell_prices)
    }

    /// 处理 WebSocket 消息（静态版本）
    async fn handle_websocket_messages_static(
        exchange: Arc<dyn Exchange + Send + Sync>,
        symbol: crate::utils::symbol::Symbol,
        ws_status: Arc<tokio::sync::Mutex<WsConnectionStatus>>,
        tx: tokio::sync::mpsc::Sender<WsEvent>,
    ) -> Result<(), AppError> {
        let ws_status_for_closure = ws_status.clone();
        let on_message: Box<
            dyn FnMut(WsEvent) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
        > = Box::new(move |msg: WsEvent| {
            let tx_clone = tx.clone();
            let ws_status_clone = ws_status_for_closure.clone();
            Box::pin(async move {
                // 更新WebSocket连接状态为已连接
                {
                    let mut status_guard = ws_status_clone.lock().await;
                    *status_guard = WsConnectionStatus::Connected;
                }

                if let Err(e) = tx_clone.send(msg).await {
                    strategy_error!("grid", "无法将ws消息发送到通道: {e}");
                }
            }) as Pin<Box<dyn Future<Output = ()> + Send>>
        });

        // 设置连接状态为连接中
        {
            let mut status_guard = ws_status.lock().await;
            *status_guard = WsConnectionStatus::Connecting;
        }

        match exchange.connect_ws(symbol.market_type, on_message).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // 设置连接状态为错误
                let error_msg = format!("WebSocket连接失败: {}", e);
                {
                    let mut status_guard = ws_status.lock().await;
                    *status_guard = WsConnectionStatus::Error(error_msg.clone());
                }
                Err(AppError::Other(error_msg))
            }
        }
    }
}
