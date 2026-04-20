use super::{PoissonMarketMaker, Result};
use crate::analysis::TradeData;
use crate::core::{error::ExchangeError, types::*};
use crate::strategies::poisson_market_maker::domain::PoissonParameters;
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;

impl PoissonMarketMaker {
    /// 运行做市主循环
    pub(crate) async fn run_market_making(&self) -> Result<()> {
        log::info!("💹 开始做市交易...");

        // 等待数据初始化
        log::info!("⏳ 等待市场数据和参数初始化（10秒）...");
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        let mut no_price_warning_count = 0;

        while *self.running.read().await {
            // 检查是否有价格数据
            let current_price = *self.current_price.read().await;
            if current_price <= 0.0 {
                no_price_warning_count += 1;
                if no_price_warning_count % 10 == 1 {
                    // 每10次警告一次
                    log::warn!("⚠️ 等待价格数据... 当前价格: {}", current_price);
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }

            // 显示当前价格
            if no_price_warning_count > 0 {
                log::info!("✅ 收到价格数据: {} USDC", current_price);
                no_price_warning_count = 0;
            }

            // 1. 更新当前持仓状态（每5-10分钟一次）
            let now = Utc::now();
            let last_pos_update = *self.last_position_update.read().await;
            if now.signed_duration_since(last_pos_update).num_seconds() > 600 {
                // 10分钟
                match self.update_position_status().await {
                    Ok(()) => {
                        *self.last_position_update.write().await = now;
                    }
                    Err(e) => {
                        log::warn!("❗ 更新持仓状态失败: {} (将跳过本次刷新)", e);
                    }
                }
            }

            // 2. 计算最优价差
            let (bid_spread, ask_spread) = match self.calculate_optimal_spread().await {
                Ok(spreads) => spreads,
                Err(e) => {
                    log::warn!("⚠️ 计算最优价差失败: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
            };

            // 3. 检查是否需要刷新订单（价格变化超过0.1%）
            let should_refresh = self.should_refresh_orders(current_price).await;

            if should_refresh {
                // 3a. 取消旧订单
                if let Err(e) = self.cancel_stale_orders().await {
                    log::warn!("⚠️ 取消过期订单失败: {}", e);
                }
            }

            // 4. 确保当前报价齐全
            if let Err(e) = self.place_orders(bid_spread, ask_spread).await {
                log::error!("❌ 下新订单失败: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }

            // 5. 风险检查
            if let Err(e) = self.check_risk_limits().await {
                log::error!("⚠️ 风险检查失败: {}", e);
            }

            // 显示状态
            let params = self.poisson_params.read().await;
            let state = self.state.lock().await;
            log::info!(
                "📊 状态 | 价格: {:.5} | λ: {:.2}/{:.2} | 挂单: {}/{} | 库存: {:.1}",
                current_price,
                params.lambda_bid,
                params.lambda_ask,
                state.active_buy_orders.len(),
                state.active_sell_orders.len(),
                state.inventory
            );
            drop(state);

            // 等待下次刷新
            tokio::time::sleep(tokio::time::Duration::from_secs(
                self.config.trading.refresh_interval_secs,
            ))
            .await;
        }

        self.log_market_making_exit("运行标志置为false").await;
        Ok(())
    }

    /// 记录市场做市循环结束原因
    pub(crate) async fn log_market_making_exit(&self, reason: &str) {
        log::warn!(
            "🛑 市场做市循环退出: {} | running={} | last_error={:?}",
            reason,
            *self.running.read().await,
            self.last_error.read().await.clone()
        );
    }

    /// 检查是否需要刷新订单
    pub(crate) async fn should_refresh_orders(&self, current_price: f64) -> bool {
        let state = self.state.lock().await;

        // 1. 没有订单时需要挂单
        if state.active_buy_orders.is_empty() && state.active_sell_orders.is_empty() {
            return true;
        }

        // 2. 检查30秒超时
        let now = Utc::now();
        for order in state.active_buy_orders.values() {
            if now.signed_duration_since(order.timestamp).num_seconds() > 30 {
                log::debug!("买单超过30秒未成交，需要刷新");
                return true;
            }
        }

        for order in state.active_sell_orders.values() {
            if now.signed_duration_since(order.timestamp).num_seconds() > 30 {
                log::debug!("卖单超过30秒未成交，需要刷新");
                return true;
            }
        }

        // 3. 检查价格变化
        let last_bid = *self.last_bid_price.read().await;
        let last_ask = *self.last_ask_price.read().await;

        if last_bid > 0.0 && last_ask > 0.0 {
            let mid_price = (last_bid + last_ask) / 2.0;
            let price_change_pct = ((current_price - mid_price) / mid_price).abs();

            if price_change_pct > 0.0005 {
                // 0.05%
                log::debug!("价格变化 {:.3}%，需要刷新订单", price_change_pct * 100.0);
                return true;
            }
        }

        false
    }

    /// 计算最优价差（动态调整版）
    pub(crate) async fn calculate_optimal_spread(&self) -> Result<(f64, f64)> {
        let params = self.poisson_params.read().await;
        let state = self.state.lock().await;

        // 基础价差(基点转换为小数)
        let base_spread = self.config.trading.min_spread_bp / 10000.0;

        // 根据成交频率动态调整价差
        let activity_factor = self.calculate_market_activity_factor(&params).await;
        let dynamic_spread = base_spread * activity_factor;

        // 根据队列长度调整价差
        // 队列越长，说明流动性越差，需要更大的价差
        let queue_adjustment = (params.avg_queue_bid + params.avg_queue_ask) / 20.0;

        // 根据最近成交时间调整
        let time_since_last_trade = if let Some(last_trade) = params.last_trade_time {
            (Utc::now() - last_trade).num_seconds() as f64
        } else {
            300.0 // 默认5分钟
        };

        // 超过30秒未成交，逐步缩小价差以增加成交机会
        let urgency_factor = if time_since_last_trade > 30.0 {
            (0.9_f64).max(1.0 - (time_since_last_trade - 30.0) / 300.0)
        } else {
            1.0
        };

        // 根据库存调整价差（库存偏斜惩罚）
        // max_inventory是USDT价值，计算当前库存的USDT价值比例
        let current_inventory_value = state.inventory * *self.current_price.read().await;
        let inventory_ratio = current_inventory_value / self.config.trading.max_inventory;
        let inventory_penalty = inventory_ratio.abs() * 0.001; // 每10%库存增加1bp

        // 计算买卖价差（结合所有动态因子）
        let mut bid_spread =
            (dynamic_spread + queue_adjustment * base_spread + inventory_penalty) * urgency_factor;
        let mut ask_spread =
            (dynamic_spread + queue_adjustment * base_spread + inventory_penalty) * urgency_factor;

        // 库存偏斜调整：持有多头时，降低买价提高卖价
        if inventory_ratio > 0.0 {
            bid_spread *= 1.0 + inventory_ratio * 0.5; // 多头时买价更保守
            ask_spread *= 1.0 - inventory_ratio * 0.3; // 卖价更激进
        } else if inventory_ratio < 0.0 {
            bid_spread *= 1.0 + inventory_ratio.abs() * 0.3; // 空头时买价更激进
            ask_spread *= 1.0 - inventory_ratio.abs() * 0.5; // 卖价更保守
        }

        // 限制在配置范围内
        let max_spread = self.config.trading.max_spread_bp / 10000.0;
        bid_spread = bid_spread.min(max_spread);
        ask_spread = ask_spread.min(max_spread);

        log::debug!(
            "计算价差: bid_spread={:.4}%, ask_spread={:.4}%",
            bid_spread * 100.0,
            ask_spread * 100.0
        );

        Ok((bid_spread, ask_spread))
    }

    /// 下单
    pub(crate) async fn place_orders(&self, bid_spread: f64, ask_spread: f64) -> Result<()> {
        let current_price = *self.current_price.read().await;
        if current_price <= 0.0 {
            log::debug!("等待价格数据...");
            return Ok(());
        }

        // 获取盘口最优价格
        let (best_bid, best_ask) = {
            let orderbook = self.orderbook.read().await;
            if orderbook.bids.is_empty() || orderbook.asks.is_empty() {
                log::debug!("等待订单簿数据...");
                return Ok(());
            }
            (orderbook.bids[0].0, orderbook.asks[0].0)
        };

        let is_dual_mode = self.is_dual_position_mode().await;
        let (inventory, long_inventory, short_inventory, initial_buy_count, initial_sell_count) = {
            let state = self.state.lock().await;
            (
                state.inventory,
                state.long_inventory,
                state.short_inventory,
                state.active_buy_orders.len(),
                state.active_sell_orders.len(),
            )
        };

        // 检查库存限制
        let inventory_cap = self.config.trading.max_inventory * 0.9;
        let (can_buy, can_sell) = if is_dual_mode {
            (
                long_inventory < inventory_cap,
                short_inventory < inventory_cap,
            )
        } else {
            (
                inventory < inventory_cap,
                inventory > -inventory_cap,
            )
        };

        // 计算订单数量（固定6 USDT）
        let order_quantity = self.config.trading.order_size_usdc / current_price;
        let order_quantity = self.round_quantity(order_quantity);

        log::debug!(
            "准备下单 - 价格: {:.5}, 数量: {}, 买价差: {:.2}%, 卖价差: {:.2}%",
            current_price,
            order_quantity,
            bid_spread * 100.0,
            ask_spread * 100.0
        );
        log::debug!(
            "订单状态 - 买单: {}, 卖单: {}, 净库存: {:.3}, 多头: {:.3}, 空头: {:.3}, can_buy: {}, can_sell: {}",
            initial_buy_count,
            initial_sell_count,
            inventory,
            long_inventory,
            short_inventory,
            can_buy,
            can_sell
        );

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        let tick_size = self.tick_size();
        let target_buy_price = self.round_price(best_bid * (1.0 - bid_spread));
        let target_sell_price = self.round_price(best_ask * (1.0 + ask_spread));
        let reduce_threshold = (order_quantity * 0.5).max(0.0001);
        let reduce_buy = short_inventory > reduce_threshold;
        let reduce_sell = long_inventory > reduce_threshold;

        let mut replace_buy: Option<(String, Option<String>)> = None;
        let mut replace_sell: Option<(String, Option<String>)> = None;

        {
            let state = self.state.lock().await;
            if let Some((id, order)) = state.active_buy_orders.iter().next() {
                if let Some(price) = order.price {
                    if (price - target_buy_price).abs() >= tick_size * 0.5 || reduce_buy {
                        let client = state.buy_exchange_to_client.get(id).cloned();
                        replace_buy = Some((id.clone(), client));
                    }
                } else {
                    let client = state.buy_exchange_to_client.get(id).cloned();
                    replace_buy = Some((id.clone(), client));
                }
            }
            if let Some((id, order)) = state.active_sell_orders.iter().next() {
                if let Some(price) = order.price {
                    if (price - target_sell_price).abs() >= tick_size * 0.5 || reduce_sell {
                        let client = state.sell_exchange_to_client.get(id).cloned();
                        replace_sell = Some((id.clone(), client));
                    }
                } else {
                    let client = state.sell_exchange_to_client.get(id).cloned();
                    replace_sell = Some((id.clone(), client));
                }
            }
        }

        if let Some((exchange_id, client_id)) = replace_buy.clone() {
            if let Err(err) = account
                .exchange
                .cancel_order(&exchange_id, &self.config.trading.symbol, MarketType::Futures)
                .await
            {
                log::warn!("取消买单 {} 失败: {}", exchange_id, err);
            }
            let mut state = self.state.lock().await;
            state.active_buy_orders.remove(&exchange_id);
            if let Some(client) = client_id {
                state.buy_client_to_exchange.remove(&client);
            }
            state.buy_exchange_to_client.remove(&exchange_id);
        }

        if let Some((exchange_id, client_id)) = replace_sell.clone() {
            if let Err(err) = account
                .exchange
                .cancel_order(&exchange_id, &self.config.trading.symbol, MarketType::Futures)
                .await
            {
                log::warn!("取消卖单 {} 失败: {}", exchange_id, err);
            }
            let mut state = self.state.lock().await;
            state.active_sell_orders.remove(&exchange_id);
            if let Some(client) = client_id {
                state.sell_client_to_exchange.remove(&client);
            }
            state.sell_exchange_to_client.remove(&exchange_id);
        }

        // 检查订单平衡
        let (mut buy_count, mut sell_count) = {
            let state = self.state.lock().await;
            (
                state.active_buy_orders.len(),
                state.active_sell_orders.len(),
            )
        };

        // 处理订单不平衡情况
        // 正确逻辑：如果有超过一个订单，先取消多余的
        if buy_count > 1 || sell_count > 1 {
            log::warn!(
                "⚠️ 订单过多: 买单={}, 卖单={}，取消多余订单",
                buy_count,
                sell_count
            );
            if let Err(e) = self.cancel_all_orders().await {
                log::error!("取消订单失败: {}", e);
            }
            return Ok(());
        }

        // 取消之后重新统计
        {
            let state = self.state.lock().await;
            buy_count = state.active_buy_orders.len();
            sell_count = state.active_sell_orders.len();
        }
        let mut need_buy_order = buy_count == 0 || replace_buy.is_some();
        let mut need_sell_order = sell_count == 0 || replace_sell.is_some();

        if reduce_sell && !reduce_buy {
            need_buy_order = false;
        }
        if reduce_buy && !reduce_sell {
            need_sell_order = false;
        }

        if buy_count == 1 && sell_count == 1 {
            log::debug!("✅ 订单平衡: 买单=1, 卖单=1");
        } else if need_buy_order || need_sell_order {
            log::debug!("🔄 需要补充订单: 买单={}, 卖单={}", buy_count, sell_count);
        }

        // 下买单
        if can_buy && need_buy_order {
            let state = self.state.lock().await;
            if state.active_buy_orders.is_empty() {
                drop(state); // 释放锁

                let base_asset = self
                    .symbol_info
                    .read()
                    .await
                    .as_ref()
                    .map(|info| info.base_asset.clone())
                    .unwrap_or_else(|| "TOKEN".to_string());

                let timestamp = chrono::Utc::now();
                log::info!(
                    "[{}] 📗 准备下买单: {} {} @ {:.5} {} (价差: -{:.2}%)",
                    timestamp.format("%H:%M:%S%.3f"),
                    order_quantity,
                    base_asset,
                    target_buy_price,
                    self.get_quote_asset().await,
                    bid_spread * 100.0
                );

                let is_dual_mode = self.is_dual_position_mode().await;
                let time_in_force = if reduce_buy { "GTC" } else { "GTX" };
                let mut buy_params = HashMap::new();

                if !reduce_buy {
                    buy_params.insert("postOnly".to_string(), "true".to_string());
                }
                buy_params.insert("timeInForce".to_string(), time_in_force.to_string());

                if is_dual_mode {
                    let position_side = if reduce_buy { "SHORT" } else { "LONG" };
                    buy_params.insert("positionSide".to_string(), position_side.to_string());
                }

                let reduce_only_flag = if !is_dual_mode && reduce_buy {
                    Some(true)
                } else {
                    None
                };

                let strategy_name = format!(
                    "poisson_{}",
                    self.config
                        .trading
                        .symbol
                        .split('/')
                        .next()
                        .unwrap_or("")
                        .to_lowercase()
                );
                let client_order_id = crate::utils::generate_order_id_with_tag(
                    &strategy_name,
                    &account.exchange_name,
                    "B",
                );

                let mut buy_order = OrderRequest {
                    symbol: self.config.trading.symbol.clone(),
                    side: OrderSide::Buy,
                    order_type: OrderType::Limit,
                    amount: order_quantity,
                    price: Some(target_buy_price),
                    market_type: MarketType::Futures,
                    params: Some(buy_params),
                    client_order_id: Some(client_order_id.clone()),
                    time_in_force: Some(time_in_force.to_string()),
                    reduce_only: reduce_only_flag,
                    post_only: Some(!reduce_buy),
                };

                let mut attempt_price = target_buy_price;
                let mut attempts = 0;

                loop {
                    match account.exchange.create_order(buy_order.clone()).await {
                        Ok(order) => {
                            log::info!("✅ 买单成功: ID={}, 状态={:?}", order.id, order.status);
                            *self.last_bid_price.write().await = attempt_price;

                            let mut state = self.state.lock().await;
                            let exchange_order_id = order.id.clone();
                            state
                                .active_buy_orders
                                .insert(exchange_order_id.clone(), order);
                            state
                                .buy_client_to_exchange
                                .insert(client_order_id.clone(), exchange_order_id.clone());
                            state
                                .buy_exchange_to_client
                                .insert(exchange_order_id, client_order_id.clone());
                            break;
                        }
                        Err(e) => {
                            let err_msg = e.to_string();
                            if attempts == 0
                                && !reduce_buy
                                && Self::is_post_only_reject(&err_msg)
                            {
                                if let Some(adjusted) =
                                    self.adjust_post_only_price(OrderSide::Buy, attempt_price)
                                {
                                    if (adjusted - attempt_price).abs() > f64::EPSILON {
                                        log::warn!(
                                            "⚠️ Post-only买单被拒，调整报价: {:.5} -> {:.5}",
                                            attempt_price,
                                            adjusted
                                        );
                                        attempt_price = adjusted;
                                        buy_order.price = Some(attempt_price);
                                        attempts += 1;
                                        continue;
                                    }
                                }
                            }

                            log::error!("❌ 买单失败: {}", err_msg);
                            break;
                        }
                    }
                }
            }
        }

        // 下卖单
        if can_sell && need_sell_order {
            let state = self.state.lock().await;
            if state.active_sell_orders.is_empty() {
                drop(state); // 释放锁

                let base_asset = self
                    .symbol_info
                    .read()
                    .await
                    .as_ref()
                    .map(|info| info.base_asset.clone())
                    .unwrap_or_else(|| "TOKEN".to_string());

                log::info!(
                    "📕 准备下卖单: {} {} @ {:.5} {} (价差: +{:.2}%)",
                    order_quantity,
                    base_asset,
                    target_sell_price,
                    self.get_quote_asset().await,
                    ask_spread * 100.0
                );

                let is_dual_mode = self.is_dual_position_mode().await;
                let time_in_force = if reduce_sell { "GTC" } else { "GTX" };
                let mut sell_params = HashMap::new();

                if !reduce_sell {
                    sell_params.insert("postOnly".to_string(), "true".to_string());
                }
                sell_params.insert("timeInForce".to_string(), time_in_force.to_string());

                if is_dual_mode {
                    let position_side = if reduce_sell { "LONG" } else { "SHORT" };
                    sell_params.insert("positionSide".to_string(), position_side.to_string());
                }

                let reduce_only_flag = if !is_dual_mode && reduce_sell {
                    Some(true)
                } else {
                    None
                };

                let strategy_name = format!(
                    "poisson_{}",
                    self.config
                        .trading
                        .symbol
                        .split('/')
                        .next()
                        .unwrap_or("")
                        .to_lowercase()
                );
                let client_order_id = crate::utils::generate_order_id_with_tag(
                    &strategy_name,
                    &account.exchange_name,
                    "S",
                );

                let mut sell_order = OrderRequest {
                    symbol: self.config.trading.symbol.clone(),
                    side: OrderSide::Sell,
                    order_type: OrderType::Limit,
                    amount: order_quantity,
                    price: Some(target_sell_price),
                    market_type: MarketType::Futures,
                    params: Some(sell_params),
                    client_order_id: Some(client_order_id.clone()),
                    time_in_force: Some(time_in_force.to_string()),
                    reduce_only: reduce_only_flag,
                    post_only: Some(!reduce_sell),
                };

                let mut attempt_price = target_sell_price;
                let mut attempts = 0;

                loop {
                    match account.exchange.create_order(sell_order.clone()).await {
                        Ok(order) => {
                            log::info!("✅ 卖单成功: ID={}, 状态={:?}", order.id, order.status);
                            *self.last_ask_price.write().await = attempt_price;

                            let mut state = self.state.lock().await;
                            let exchange_order_id = order.id.clone();
                            state
                                .active_sell_orders
                                .insert(exchange_order_id.clone(), order);
                            state
                                .sell_client_to_exchange
                                .insert(client_order_id.clone(), exchange_order_id.clone());
                            state
                                .sell_exchange_to_client
                                .insert(exchange_order_id, client_order_id.clone());
                            break;
                        }
                        Err(e) => {
                            let err_msg = e.to_string();
                            if attempts == 0
                                && !reduce_sell
                                && Self::is_post_only_reject(&err_msg)
                            {
                                if let Some(adjusted) =
                                    self.adjust_post_only_price(OrderSide::Sell, attempt_price)
                                {
                                    if (adjusted - attempt_price).abs() > f64::EPSILON {
                                        log::warn!(
                                            "⚠️ Post-only卖单被拒，调整报价: {:.5} -> {:.5}",
                                            attempt_price,
                                            adjusted
                                        );
                                        attempt_price = adjusted;
                                        sell_order.price = Some(attempt_price);
                                        attempts += 1;
                                        continue;
                                    }
                                }
                            }

                            log::error!("❌ 卖单失败: {}", err_msg);
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 取消过期订单
    pub(crate) async fn cancel_stale_orders(&self) -> Result<()> {
        let current_price = *self.current_price.read().await;
        if current_price <= 0.0 {
            return Ok(());
        }

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 先确定需要取消的订单ID，避免持有锁时调用网络接口
        let (buy_to_cancel, sell_to_cancel) = {
            let state = self.state.lock().await;
            let mut buys = Vec::new();
            let mut sells = Vec::new();

            for (order_id, order) in state.active_buy_orders.iter() {
                if let Some(price) = order.price {
                    if price < current_price * 0.995 {
                        buys.push(order_id.clone());
                    }
                }
            }

            for (order_id, order) in state.active_sell_orders.iter() {
                if let Some(price) = order.price {
                    if price > current_price * 1.005 {
                        sells.push(order_id.clone());
                    }
                }
            }

            (buys, sells)
        };

        // 执行取消并更新本地状态
        for order_id in buy_to_cancel {
            log::debug!("取消过期买单: {}", order_id);
            match account
                .exchange
                .cancel_order(&order_id, &self.config.trading.symbol, MarketType::Futures)
                .await
            {
                Ok(_) => {
                    let mut state = self.state.lock().await;
                    state.active_buy_orders.remove(&order_id);
                    if let Some(client_id) = state.buy_exchange_to_client.remove(&order_id) {
                        state.buy_client_to_exchange.remove(&client_id);
                    }
                    self.order_cache.invalidate_order(&order_id).await;
                }
                Err(e) => {
                    log::warn!("取消买单失败 ({}): {}", order_id, e);
                }
            }
        }

        for order_id in sell_to_cancel {
            log::debug!("取消过期卖单: {}", order_id);
            match account
                .exchange
                .cancel_order(&order_id, &self.config.trading.symbol, MarketType::Futures)
                .await
            {
                Ok(_) => {
                    let mut state = self.state.lock().await;
                    state.active_sell_orders.remove(&order_id);
                    if let Some(client_id) = state.sell_exchange_to_client.remove(&order_id) {
                        state.sell_client_to_exchange.remove(&client_id);
                    }
                    self.order_cache.invalidate_order(&order_id).await;
                }
                Err(e) => {
                    log::warn!("取消卖单失败 ({}): {}", order_id, e);
                }
            }
        }

        Ok(())
    }

    /// 更新持仓状态
    pub(crate) async fn update_position_status(&self) -> Result<()> {
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 获取当前持仓
        let positions = account
            .exchange
            .get_positions(Some(&self.config.trading.symbol))
            .await?;

        let mut long_position = 0.0;
        let mut short_position = 0.0;
        let mut long_avg_price = 0.0;
        let mut short_avg_price = 0.0;
        let mut long_pnl = 0.0;
        let mut short_pnl = 0.0;

        for position in &positions {
            let side = position.side.to_ascii_uppercase();
            if side == "LONG" || position.amount > 0.0 {
                long_position = position.amount.abs();
                long_avg_price = position.entry_price;
                long_pnl = position.unrealized_pnl;
            } else if side == "SHORT" || position.amount < 0.0 {
                short_position = position.amount.abs();
                short_avg_price = position.entry_price;
                short_pnl = position.unrealized_pnl;
            }
        }

        let net_inventory = long_position - short_position;

        {
            let mut state = self.state.lock().await;
            state.long_inventory = long_position;
            state.short_inventory = short_position;
            state.inventory = net_inventory;
            state.long_avg_price = long_avg_price;
            state.short_avg_price = short_avg_price;
            state.avg_price = if net_inventory > 0.0 {
                long_avg_price
            } else if net_inventory < 0.0 {
                short_avg_price
            } else {
                0.0
            };

            if long_pnl != 0.0 {
                let base_asset = self
                    .symbol_info
                    .read()
                    .await
                    .as_ref()
                    .map(|info| info.base_asset.clone())
                    .unwrap_or_else(|| "TOKEN".to_string());
                log::debug!(
                    "多头持仓: {:.3} {} @ {:.5}, 未实现盈亏: {:.2} USDC",
                    state.long_inventory,
                    base_asset,
                    state.long_avg_price,
                    long_pnl
                );
            }

            if short_pnl != 0.0 {
                let base_asset = self
                    .symbol_info
                    .read()
                    .await
                    .as_ref()
                    .map(|info| info.base_asset.clone())
                    .unwrap_or_else(|| "TOKEN".to_string());
                log::debug!(
                    "空头持仓: {:.3} {} @ {:.5}, 未实现盈亏: {:.2} USDC",
                    state.short_inventory,
                    base_asset,
                    state.short_avg_price,
                    short_pnl
                );
            }
        }

        *self.local_position.write().await = net_inventory;

        let mut state = self.state.lock().await;

        // 更新活跃订单状态（使用缓存）
        let open_orders = self.get_cached_open_orders().await?;

        // 检查并记录成交的订单
        let mut filled_orders = Vec::new();

        // 检查买单成交
        for (order_id, order_info) in state.active_buy_orders.iter() {
            if !open_orders.iter().any(|o| &o.id == order_id) {
                // 订单不在开放订单中，可能已成交
                filled_orders.push((order_id.clone(), OrderSide::Buy, order_info.clone()));
            }
        }

        // 检查卖单成交
        for (order_id, order_info) in state.active_sell_orders.iter() {
            if !open_orders.iter().any(|o| &o.id == order_id) {
                // 订单不在开放订单中，可能已成交
                filled_orders.push((order_id.clone(), OrderSide::Sell, order_info.clone()));
            }
        }

        // 记录成交到数据库
        if !filled_orders.is_empty() && self.collector.is_some() {
            // 获取最近的成交记录
            let trades = account
                .exchange
                .get_my_trades(
                    Some(&self.config.trading.symbol),
                    MarketType::Futures,
                    Some(20),
                )
                .await?;

            for (order_id, side, order_info) in filled_orders {
                // 查找对应的成交记录
                if let Some(trade) = trades.iter().find(|t| t.order_id == Some(order_id.clone())) {
                    // 记录到数据库
                    if let Some(ref collector) = self.collector {
                        let trade_data = TradeData {
                            trade_time: trade.timestamp,
                            strategy_name: self.config.name.clone(),
                            account_id: account.id.clone(),
                            exchange: self.config.account.exchange.clone(),
                            symbol: self.config.trading.symbol.clone(),
                            side: format!("{:?}", side),
                            order_type: Some("Limit".to_string()),
                            price: Decimal::from_str(&trade.price.to_string()).unwrap_or_default(),
                            amount: Decimal::from_str(&trade.amount.to_string())
                                .unwrap_or_default(),
                            value: Some(
                                Decimal::from_str(&(trade.price * trade.amount).to_string())
                                    .unwrap_or_default(),
                            ),
                            fee: trade.fee.as_ref().map(|f| {
                                Decimal::from_str(&f.cost.to_string()).unwrap_or_default()
                            }),
                            fee_currency: trade.fee.as_ref().map(|f| f.currency.clone()),
                            realized_pnl: None, // 做市策略的盈亏需要综合计算
                            pnl_percentage: None,
                            order_id: order_id.clone(),
                            parent_order_id: None,
                            position_side: None,
                            metadata: None,
                        };

                        if let Err(e) = collector.record_trade(trade_data).await {
                            log::error!("记录交易失败: {}", e);
                        } else {
                            state.trade_count += 1;
                            log::info!(
                                "📝 记录成交: {} {:?} @ {:.5} x {} (真实API成交)",
                                self.config.trading.symbol,
                                side,
                                trade.price,
                                trade.amount
                            );
                        }
                    }
                }
            }
        }

        // 清理已成交或取消的订单
        state.active_buy_orders.retain(|id, _| {
            open_orders
                .iter()
                .any(|o| &o.id == id && o.side == OrderSide::Buy)
        });
        let active_buy_ids: Vec<String> = state.active_buy_orders.keys().cloned().collect();
        state
            .buy_exchange_to_client
            .retain(|exchange_id, _| active_buy_ids.contains(exchange_id));
        state
            .buy_client_to_exchange
            .retain(|_, exchange_id| active_buy_ids.contains(exchange_id));

        state.active_sell_orders.retain(|id, _| {
            open_orders
                .iter()
                .any(|o| &o.id == id && o.side == OrderSide::Sell)
        });
        let active_sell_ids: Vec<String> = state.active_sell_orders.keys().cloned().collect();
        state
            .sell_exchange_to_client
            .retain(|exchange_id, _| active_sell_ids.contains(exchange_id));
        state
            .sell_client_to_exchange
            .retain(|_, exchange_id| active_sell_ids.contains(exchange_id));

        state.trim_slot_orders();

        Ok(())
    }

    /// 取消所有订单
    pub(crate) async fn cancel_all_orders(&self) -> Result<()> {
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 使用批量取消API
        let _ = account
            .exchange
            .cancel_all_orders(Some(&self.config.trading.symbol), MarketType::Futures)
            .await?;

        {
            let mut state = self.state.lock().await;
            let buy_cleared = state.active_buy_orders.len();
            let sell_cleared = state.active_sell_orders.len();
            state.active_buy_orders.clear();
            state.active_sell_orders.clear();
            state.buy_client_to_exchange.clear();
            state.sell_client_to_exchange.clear();
            state.buy_exchange_to_client.clear();
            state.sell_exchange_to_client.clear();
            state.order_slots.clear();
            state.client_to_slot.clear();
            state.exchange_to_slot.clear();
            if buy_cleared > 0 || sell_cleared > 0 {
                log::debug!(
                    "🧹 本地挂单缓存清理: 买单{}个, 卖单{}个",
                    buy_cleared,
                    sell_cleared
                );
            }
        }
        self.order_cache
            .invalidate_open_orders(&self.config.trading.symbol)
            .await;

        log::info!("✅ 已取消所有订单");
        Ok(())
    }

    /// 平掉所有持仓
    pub(crate) async fn close_all_positions(&self) -> Result<()> {
        let is_dual_mode = self.is_dual_position_mode().await;
        let (long_qty, short_qty, net_inventory) = {
            let state = self.state.lock().await;
            (
                state.long_inventory,
                state.short_inventory,
                state.inventory,
            )
        };
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        if is_dual_mode {
            let mut closed = false;
            if long_qty > 0.0005 {
                let amount = self.round_quantity(long_qty);
                let close_long = OrderRequest {
                    symbol: self.config.trading.symbol.clone(),
                    side: OrderSide::Sell,
                    order_type: OrderType::Market,
                    amount,
                    price: None,
                    market_type: MarketType::Futures,
                    params: Some(HashMap::from([
                        ("reduceOnly".to_string(), "true".to_string()),
                        ("positionSide".to_string(), "LONG".to_string()),
                    ])),
                    client_order_id: None,
                    time_in_force: None,
                    reduce_only: Some(true),
                    post_only: None,
                };
                account.exchange.create_order(close_long).await?;
                closed = true;
            }
            if short_qty > 0.0005 {
                let amount = self.round_quantity(short_qty);
                let close_short = OrderRequest {
                    symbol: self.config.trading.symbol.clone(),
                    side: OrderSide::Buy,
                    order_type: OrderType::Market,
                    amount,
                    price: None,
                    market_type: MarketType::Futures,
                    params: Some(HashMap::from([
                        ("reduceOnly".to_string(), "true".to_string()),
                        ("positionSide".to_string(), "SHORT".to_string()),
                    ])),
                    client_order_id: None,
                    time_in_force: None,
                    reduce_only: Some(true),
                    post_only: None,
                };
                account.exchange.create_order(close_short).await?;
                closed = true;
            }
            if !closed {
                return Ok(());
            }
        } else {
            if net_inventory.abs() < 0.001 {
                return Ok(());
            }

            let side = if net_inventory > 0.0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };

            let close_order = OrderRequest {
                symbol: self.config.trading.symbol.clone(),
                side,
                order_type: OrderType::Market,
                amount: self.round_quantity(net_inventory.abs()),
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
        }

        log::info!("✅ 已平掉所有持仓");
        Ok(())
    }

    /// 输出统计信息
    pub(crate) async fn print_statistics(&self) {
        let state = self.state.lock().await;
        let params = self.poisson_params.read().await;
        let runtime = Utc::now().signed_duration_since(state.start_time);

        log::info!("========== 泊松做市策略统计 ==========");
        log::info!(
            "运行时间: {}小时{}分钟",
            runtime.num_hours(),
            runtime.num_minutes() % 60
        );
        log::info!("成交次数: {}", state.trade_count);
        log::info!("今日盈亏: {:.2} USDC", state.daily_pnl);
        log::info!("总盈亏: {:.2} USDC", state.total_pnl);
        let base_asset = self
            .symbol_info
            .read()
            .await
            .as_ref()
            .map(|info| info.base_asset.clone())
            .unwrap_or_else(|| "TOKEN".to_string());
        log::info!(
            "库存情况: 净仓 {:.2} {}, 多头 {:.2}, 空头 {:.2}",
            state.inventory,
            base_asset,
            state.long_inventory,
            state.short_inventory
        );
        log::info!("泊松参数:");
        log::info!("  - λ_bid: {:.2} 订单/秒", params.lambda_bid);
        log::info!("  - λ_ask: {:.2} 订单/秒", params.lambda_ask);
        log::info!("  - μ_bid: {:.2} 成交/秒", params.mu_bid);
        log::info!("  - μ_ask: {:.2} 成交/秒", params.mu_ask);
        log::info!("  - 平均队列(买): {:.2}", params.avg_queue_bid);
        log::info!("  - 平均队列(卖): {:.2}", params.avg_queue_ask);
        log::info!("=====================================");
    }

    /// 价格精度处理
    pub(crate) fn round_price(&self, price: f64) -> f64 {
        // 优先使用动态获取的精度，否则使用配置文件中的精度
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

    /// 获取交易对的最小价格步长
    pub(crate) fn tick_size(&self) -> f64 {
        if let Ok(guard) = self.symbol_info.try_read() {
            if let Some(info) = guard.as_ref() {
                if info.tick_size > 0.0 {
                    return info.tick_size;
                }
            }
        }

        1.0 / 10_f64.powi(self.config.trading.price_precision as i32)
    }

    fn adjust_post_only_price(&self, side: OrderSide, price: f64) -> Option<f64> {
        let tick = self.tick_size();
        if tick <= 0.0 {
            return None;
        }

        match side {
            OrderSide::Buy => {
                let adjusted = price - tick;
                if adjusted > 0.0 {
                    Some(self.round_price(adjusted))
                } else {
                    None
                }
            }
            OrderSide::Sell => Some(self.round_price(price + tick)),
        }
    }

    fn is_post_only_reject(err_msg: &str) -> bool {
        err_msg.contains("-5022")
            || err_msg.contains("-5021")
            || err_msg.contains("POST_ONLY_REJECT")
            || err_msg.contains("Post Only order will be rejected")
            || err_msg.to_ascii_lowercase().contains("post only")
    }

    /// 数量精度处理
    pub(crate) fn round_quantity(&self, quantity: f64) -> f64 {
        // 优先使用动态获取的精度，否则使用配置文件中的精度
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

    /// 获取缓存的开放订单（优先使用缓存，减少API调用）
    pub(crate) async fn get_cached_open_orders(&self) -> Result<Vec<Order>> {
        // 首先尝试从缓存获取
        if let Some(cached_orders) = self
            .order_cache
            .get_open_orders(&self.config.trading.symbol)
            .await
        {
            // 检查缓存是否太旧（超过10分钟才同步）
            let now = Utc::now();
            let last_fetch = *self.last_order_fetch.read().await;

            if now.signed_duration_since(last_fetch).num_seconds() > 600 {
                log::info!("📋 定期同步交易所订单状态（10分钟）");

                // 后台同步，但不阻塞当前操作
                let account_id = self.config.account.account_id.clone();
                let symbol = self.config.trading.symbol.clone();
                let cache = self.order_cache.clone();
                let manager = self.account_manager.clone();
                let last_fetch_ref = self.last_order_fetch.clone();

                tokio::spawn(async move {
                    if let Some(account) = manager.get_account(&account_id) {
                        match account
                            .exchange
                            .get_open_orders(Some(&symbol), MarketType::Futures)
                            .await
                        {
                            Ok(orders) => {
                                // 更新缓存
                                cache.set_open_orders(symbol.clone(), orders.clone()).await;

                                // 更新时间戳
                                *last_fetch_ref.write().await = Utc::now();

                                // 检查差异
                                if let Some(old_cached) = cache.get_open_orders(&symbol).await {
                                    if old_cached.len() != orders.len() {
                                        log::debug!(
                                            "订单同步: 缓存 {} -> 实际 {}",
                                            old_cached.len(),
                                            orders.len()
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!("后台订单同步失败: {}", e);
                            }
                        }
                    }
                });
            }

            return Ok(cached_orders);
        }

        // 缓存为空，必须从API获取
        log::info!("📋 缓存为空，从交易所获取订单");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 从API获取新订单
        let orders = account
            .exchange
            .get_open_orders(Some(&self.config.trading.symbol), MarketType::Futures)
            .await?;

        // 更新缓存
        self.order_cache
            .set_open_orders(self.config.trading.symbol.clone(), orders.clone())
            .await;

        // 更新最后获取时间
        *self.last_order_fetch.write().await = Utc::now();

        // 记录订单统计
        let buy_orders = orders.iter().filter(|o| o.side == OrderSide::Buy).count();
        let sell_orders = orders.iter().filter(|o| o.side == OrderSide::Sell).count();
        log::info!(
            "📊 从交易所获取订单: 买单 {} 个, 卖单 {} 个",
            buy_orders,
            sell_orders
        );

        Ok(orders)
    }

    /// 成交后立即补单
    pub(crate) async fn execute_immediate_replenishment(&self) -> Result<()> {
        // 获取当前价差
        let (bid_spread, ask_spread) = self.calculate_optimal_spread().await?;

        // 立即下新订单
        self.place_orders(bid_spread, ask_spread).await?;

        Ok(())
    }

    /// 成交后动态更新泊松参数
    pub(crate) async fn update_poisson_params_on_fill(&self) {
        let mut params = self.poisson_params.write().await;

        // 记录成交时间
        let now = Utc::now();
        params.last_trade_time = Some(now);

        // 更新到达率（基于最近成交频率）
        let time_diff = (now - params.last_update).num_seconds() as f64;
        if time_diff > 0.0 {
            // 使用指数移动平均更新lambda
            let instant_lambda = 1.0 / time_diff * 60.0; // 转换为每分钟到达率
            params.lambda_bid = params.lambda_bid * 0.9 + instant_lambda * 0.1;
            params.lambda_ask = params.lambda_ask * 0.9 + instant_lambda * 0.1;

            log::debug!(
                "📡 更新泊松参数: λ_bid={:.2}, λ_ask={:.2}",
                params.lambda_bid,
                params.lambda_ask
            );
        }
        params.last_update = now;
    }

    /// 计算市场活跃度因子
    pub(crate) async fn calculate_market_activity_factor(&self, params: &PoissonParameters) -> f64 {
        // 基于lambda值评估市场活跃度
        let avg_lambda = (params.lambda_bid + params.lambda_ask) / 2.0;

        // lambda越高，市场越活跃，价差可以更小
        if avg_lambda > 10.0 {
            0.8 // 高活跃度，缩小价差20%
        } else if avg_lambda > 5.0 {
            0.9 // 中等活跃度，缩小价差10%
        } else if avg_lambda < 1.0 {
            1.2 // 低活跃度，增大价差20%
        } else {
            1.0 // 正常价差
        }
    }
}
