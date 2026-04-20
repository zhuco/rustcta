use anyhow::{anyhow, Result};
use chrono::Utc;
use tokio::time::{interval, Duration, MissedTickBehavior};

use crate::core::types::{MarketType, Order, OrderRequest, OrderSide, OrderStatus, OrderType};
use crate::utils::order_id::generate_order_id_with_tag;
use crate::utils::webhook::{notify_event, MessageLevel};

use super::logging;
use super::model::{OrderTracker, PositionState, SignalSnapshot};
use super::planner::OrderPlan;
use super::MeanReversionStrategy;

impl MeanReversionStrategy {
    pub(super) fn spawn_order_manager_loop(&self) -> tokio::task::JoinHandle<()> {
        let strategy = self.clone();
        tokio::spawn(async move {
            strategy.order_manager_task().await;
        })
    }

    async fn order_manager_task(self) {
        let mut ticker = interval(Duration::from_secs(5));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        while self.is_running().await {
            ticker.tick().await;
            if !self.is_running().await {
                break;
            }
            if let Err(err) = self.manage_orders_once().await {
                logging::error(None, format!("订单管理失败: {}", err));
            }
        }
    }

    async fn manage_orders_once(&self) -> Result<()> {
        let pending_snapshot = {
            let states = self.symbol_states.read().await;
            states
                .iter()
                .map(|(symbol, state)| {
                    (
                        symbol.clone(),
                        state.pending_orders.values().cloned().collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>()
        };

        for (symbol, trackers) in pending_snapshot {
            for tracker in trackers {
                self.process_pending_order(&symbol, tracker).await?;
            }
        }

        self.manage_positions().await?;

        Ok(())
    }

    async fn process_pending_order(&self, symbol: &str, tracker: OrderTracker) -> Result<()> {
        let now = Utc::now();
        if now > tracker.expires_at {
            let mut limiter = self.cancel_limiter.lock().await;
            if limiter.allow() {
                if let Some(order_id) = &tracker.order_id {
                    let _ = self
                        .account
                        .exchange
                        .cancel_order(order_id, symbol, self.market_type)
                        .await;
                }
                let mut states = self.symbol_states.write().await;
                if let Some(state) = states.get_mut(symbol) {
                    state.pending_orders.remove(&tracker.client_order_id);
                }
                drop(states);
                self.update_status().await;
                logging::info(
                    Some(symbol),
                    format!("撤单 client_id={} 原因=TTL超时", tracker.client_order_id),
                );
            }
            return Ok(());
        }

        let order_id = match &tracker.order_id {
            Some(id) => id.clone(),
            None => return Ok(()),
        };

        let order = self
            .account
            .exchange
            .get_order(&order_id, symbol, self.market_type)
            .await?;

        match order.status {
            OrderStatus::Closed | OrderStatus::PartiallyFilled => {
                if order.filled >= tracker.quantity * 0.95 {
                    self.handle_order_filled(symbol, tracker, order).await?;
                }
            }
            OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::Rejected => {
                let mut states = self.symbol_states.write().await;
                if let Some(state) = states.get_mut(symbol) {
                    state.pending_orders.remove(&tracker.client_order_id);
                }
                drop(states);
                self.update_status().await;
                logging::info(
                    Some(symbol),
                    format!(
                        "订单 client_id={} 状态 {:?}",
                        tracker.client_order_id, order.status
                    ),
                );
            }
            _ => {}
        }

        Ok(())
    }

    async fn handle_order_filled(
        &self,
        symbol: &str,
        tracker: OrderTracker,
        order: Order,
    ) -> Result<()> {
        let filled_price = order.price.unwrap_or(tracker.price);
        let position = PositionState::new(
            tracker.side,
            order.filled,
            filled_price,
            tracker.stop_price,
            tracker.trailing_distance,
            tracker.take_profit,
        );

        {
            let mut states = self.symbol_states.write().await;
            if let Some(state) = states.get_mut(symbol) {
                state.pending_orders.remove(&tracker.client_order_id);
                state.set_position(tracker.side, position.clone());
                logging::info(
                    Some(symbol),
                    format!(
                        "成交 -> side={:?} qty={:.4} price={:.6} client_id={}",
                        tracker.side, order.filled, filled_price, tracker.client_order_id
                    ),
                );
            }
        }

        self.place_protection_orders(symbol, &tracker, &position)
            .await?;
        self.update_status().await;

        let direction = match tracker.side {
            OrderSide::Buy => "买入",
            OrderSide::Sell => "卖出",
        };
        let body = format!(
            "**交易对**: {}\n**方向**: {}\n**成交数量**: {:.4}\n**成交价格**: {:.6}\n**订单ID**: {}\n**Client ID**: {}\n**时间**: {}",
            symbol,
            direction,
            order.filled,
            filled_price,
            order.id,
            tracker.client_order_id,
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
        );
        let title = format!("{} 开仓成交", symbol);
        notify_event(
            &self.config.strategy.name,
            &title,
            &body,
            MessageLevel::Info,
        )
        .await;

        Ok(())
    }

    async fn place_protection_orders(
        &self,
        symbol: &str,
        tracker: &OrderTracker,
        position: &PositionState,
    ) -> Result<()> {
        let meta = self.get_symbol_meta(symbol).await?;
        let stop_side = match tracker.side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        };

        let mut stop_params = std::collections::HashMap::new();
        let stop_price = super::utils::round_price(
            tracker.stop_price,
            meta.tick_size,
            meta.price_precision,
            stop_side,
        )?;
        stop_params.insert("stopPrice".to_string(), stop_price.to_string());
        self.apply_position_side(&mut stop_params, position.side);

        let exchange_symbol = self.exchange_symbol(symbol);

        let mut stop_amount = match super::utils::round_quantity(
            position.remaining_qty,
            meta.step_size,
            meta.amount_precision,
        ) {
            Ok(q) if q > 0.0 => q,
            _ => super::utils::normalize_to_step(
                position.remaining_qty,
                meta.step_size,
                meta.amount_precision,
            ),
        };

        let normalized_symbol = super::utils::normalize_symbol(symbol);
        let actual_qty = match self
            .account
            .exchange
            .get_positions(Some(&exchange_symbol))
            .await
        {
            Ok(positions) => positions
                .into_iter()
                .find(|pos| {
                    super::utils::normalize_symbol(&pos.symbol) == normalized_symbol
                        && Self::matches_position_side(pos.side.as_str(), position.side)
                })
                .map(|pos| pos.contracts.abs())
                .unwrap_or(0.0),
            Err(err) => {
                logging::warn(
                    Some(symbol),
                    format!("获取持仓信息失败，跳过止损下单: {}", err),
                );
                0.0
            }
        };

        if actual_qty <= 1e-9 {
            logging::warn(Some(symbol), "未检测到实际持仓，跳过止损下单");
            return Ok(());
        }

        if stop_amount > actual_qty {
            logging::debug(
                Some(symbol),
                format!(
                    "调整止损数量: 本地剩余 {:.6} > 实际持仓 {:.6}",
                    stop_amount, actual_qty
                ),
            );
            stop_amount = actual_qty;
        }

        if stop_amount <= 0.0 {
            logging::warn(Some(symbol), "跳过止损单，剩余仓位低于精度要求");
            return Ok(());
        }

        {
            let mut states = self.symbol_states.write().await;
            if let Some(state) = states.get_mut(symbol) {
                if let Some(pos) = state.position_mut(position.side) {
                    pos.remaining_qty = actual_qty;
                }
            }
        }

        let stop_order = OrderRequest {
            symbol: exchange_symbol.clone(),
            side: stop_side,
            order_type: OrderType::StopMarket,
            amount: stop_amount,
            price: None,
            market_type: self.market_type,
            params: Some(stop_params),
            client_order_id: None,
            time_in_force: None,
            reduce_only: self.reduce_only_flag(),
            post_only: Some(false),
        };

        let stop_order_id = match self.account.exchange.create_order(stop_order).await {
            Ok(order) => {
                logging::info(
                    Some(symbol),
                    format!(
                        "已提交止损单 id={} side={:?} stop_price={:.6} qty={:.4}",
                        order.id, stop_side, stop_price, stop_amount
                    ),
                );
                crate::utils::webhook::notify_info(
                    symbol,
                    &format!(
                        "止损单下单成功\n方向: {:?}\n数量: {:.4}\n价格: {:.6}",
                        stop_side, stop_amount, stop_price
                    ),
                )
                .await;
                Some(order.id.clone())
            }
            Err(err) => {
                let msg = format!("提交止损失败: {}", err);
                logging::warn(Some(symbol), &msg);
                crate::utils::webhook::notify_error(symbol, &msg).await;
                return Ok(());
            }
        };

        let tp_side = stop_side;
        let tp_amount =
            match super::utils::round_quantity(actual_qty, meta.step_size, meta.amount_precision) {
                Ok(q) if q > 0.0 => q,
                _ => super::utils::normalize_to_step(
                    actual_qty,
                    meta.step_size,
                    meta.amount_precision,
                ),
            };

        let tp_amount = tp_amount.max(meta.min_order_size);

        if tp_amount <= 0.0 {
            logging::warn(Some(symbol), "跳过止盈单，剩余仓位低于精度要求");
            return Ok(());
        }

        let tp_price = super::utils::round_price(
            position.take_profit,
            meta.tick_size,
            meta.price_precision,
            tp_side,
        )?;

        let mut tp_params = std::collections::HashMap::new();
        self.apply_position_side(&mut tp_params, position.side);
        let tp_order = OrderRequest {
            symbol: exchange_symbol.clone(),
            side: tp_side,
            order_type: OrderType::Limit,
            amount: tp_amount,
            price: Some(tp_price),
            market_type: self.market_type,
            params: Some(tp_params),
            client_order_id: None,
            time_in_force: Some("GTC".to_string()),
            reduce_only: self.reduce_only_flag(),
            post_only: Some(true),
        };

        let tp_order_id = match self.account.exchange.create_order(tp_order).await {
            Ok(order) => {
                logging::info(
                    Some(symbol),
                    format!(
                        "已提交止盈单 id={} side={:?} price={:.6} qty={:.4}",
                        order.id, tp_side, tp_price, tp_amount
                    ),
                );
                crate::utils::webhook::notify_info(
                    symbol,
                    &format!(
                        "止盈单下单成功\n方向: {:?}\n数量: {:.4}\n价格: {:.6}",
                        tp_side, tp_amount, tp_price
                    ),
                )
                .await;
                Some(order.id.clone())
            }
            Err(err) => {
                logging::warn(Some(symbol), format!("提交止盈失败: {}", err));
                None
            }
        };

        {
            let mut states = self.symbol_states.write().await;
            if let Some(state) = states.get_mut(symbol) {
                if let Some(pos) = state.position_mut(position.side) {
                    pos.stop_order_id = stop_order_id.clone();
                    pos.tp_order_id = tp_order_id.clone();
                }
            }
        }

        Ok(())
    }

    async fn manage_positions(&self) -> Result<()> {
        let snapshots = {
            let states = self.symbol_states.read().await;
            states
                .iter()
                .flat_map(|(symbol, state)| {
                    state.iter_positions().map(|(side, pos)| {
                        (
                            symbol.clone(),
                            side,
                            pos.clone(),
                            state.one_minute.back().cloned(),
                            state.five_minute.back().cloned(),
                        )
                    })
                })
                .collect::<Vec<_>>()
        };

        for (symbol, side, position, last_1m, last_5m) in snapshots {
            let meta = match self.get_symbol_meta(&symbol).await {
                Ok(meta) => meta,
                Err(err) => {
                    logging::warn(
                        Some(symbol.as_str()),
                        format!("获取精度信息失败，跳过仓位检查: {}", err),
                    );
                    continue;
                }
            };

            let exchange_symbol = self.exchange_symbol(&symbol);
            let normalized_symbol = super::utils::normalize_symbol(&symbol);
            let actual_qty = match self
                .account
                .exchange
                .get_positions(Some(&exchange_symbol))
                .await
            {
                Ok(positions) => positions
                    .into_iter()
                    .find(|pos| {
                        super::utils::normalize_symbol(&pos.symbol) == normalized_symbol
                            && Self::matches_position_side(pos.side.as_str(), side)
                    })
                    .map(|pos| pos.contracts.abs())
                    .unwrap_or(0.0),
                Err(err) => {
                    logging::warn(
                        Some(symbol.as_str()),
                        format!("获取持仓信息失败，跳过仓位同步: {}", err),
                    );
                    continue;
                }
            };

            let flat_threshold = (meta.step_size * 0.5).max(1e-9);
            if actual_qty <= flat_threshold {
                self.cleanup_position_orders(&symbol, &position, "检测到仓位为0")
                    .await?;
                continue;
            }

            let current_price = last_1m
                .as_ref()
                .or(last_5m.as_ref())
                .map(|k| k.close)
                .unwrap_or(position.entry_price);

            {
                let mut states = self.symbol_states.write().await;
                if let Some(state) = states.get_mut(&symbol) {
                    if let Some(pos) = state.position_mut(side) {
                        pos.remaining_qty = actual_qty;
                        self.lock_profit_if_needed(&symbol, pos, current_price);
                    }
                }
            }

            let should_close = match position.side {
                OrderSide::Buy => current_price <= position.stop_price,
                OrderSide::Sell => current_price >= position.stop_price,
            };

            if should_close {
                logging::info(
                    Some(symbol.as_str()),
                    format!(
                        "触发止损 -> side={:?} price={:.4} stop={:.4}",
                        position.side, current_price, position.stop_price
                    ),
                );
                self.close_position(&symbol, &position).await?;
                continue;
            }

            let trailing = match position.side {
                OrderSide::Buy => current_price - position.entry_price,
                OrderSide::Sell => position.entry_price - current_price,
            };

            if trailing > position.trailing_distance {
                let new_stop = match position.side {
                    OrderSide::Buy => current_price - position.trailing_distance,
                    OrderSide::Sell => current_price + position.trailing_distance,
                };
                let mut states = self.symbol_states.write().await;
                if let Some(state) = states.get_mut(&symbol) {
                    if let Some(pos) = state.position_mut(side) {
                        pos.stop_price = new_stop;
                        pos.last_update = Utc::now();
                    }
                }
                logging::info(
                    Some(symbol.as_str()),
                    format!(
                        "更新移动止损 -> side={:?} new_stop={:.4} 当前价差={:.4}",
                        position.side, new_stop, trailing
                    ),
                );
            }
        }

        Ok(())
    }

    async fn close_position(&self, symbol: &str, position: &PositionState) -> Result<()> {
        let side = match position.side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        };

        let mut params = std::collections::HashMap::new();
        self.apply_position_side(&mut params, position.side);

        let exchange_symbol = self.exchange_symbol(symbol);
        let normalized_symbol = super::utils::normalize_symbol(symbol);
        let actual_qty = match self
            .account
            .exchange
            .get_positions(Some(&exchange_symbol))
            .await
        {
            Ok(positions) => positions
                .into_iter()
                .find(|pos| {
                    super::utils::normalize_symbol(&pos.symbol) == normalized_symbol
                        && (!self.uses_dual_position_mode()
                            || Self::matches_position_side(pos.side.as_str(), position.side))
                })
                .map(|pos| pos.contracts.abs())
                .unwrap_or(0.0),
            Err(err) => {
                let msg = format!("获取持仓信息失败: {}", err);
                logging::warn(Some(symbol), &msg);
                crate::utils::webhook::notify_error(symbol, &msg).await;
                0.0
            }
        };

        if actual_qty <= 1e-9 {
            logging::info(Some(symbol), "已无实际持仓，跳过平仓");
            crate::utils::webhook::notify_info(symbol, "检测到仓位已为0，跳过平仓任务").await;
            let mut states = self.symbol_states.write().await;
            if let Some(state) = states.get_mut(symbol) {
                state.clear_position(position.side);
            }
            drop(states);
            self.update_status().await;
            return Ok(());
        }

        let amount = position.remaining_qty.min(actual_qty);

        let order = OrderRequest {
            symbol: exchange_symbol,
            side,
            order_type: OrderType::Market,
            amount,
            price: None,
            market_type: self.market_type,
            params: Some(params),
            client_order_id: None,
            time_in_force: None,
            reduce_only: self.reduce_only_flag(),
            post_only: Some(false),
        };

        match self.account.exchange.create_order(order).await {
            Ok(_) => {
                logging::info(
                    Some(symbol),
                    format!("市价平仓 -> side={:?} qty={:.4}", side, amount),
                );
                crate::utils::webhook::notify_info(
                    symbol,
                    &format!("市价平仓成功\n方向: {:?}\n数量: {:.4}", side, amount),
                )
                .await;
                self.cleanup_position_orders(symbol, position, "市价平仓完成")
                    .await?;
            }
            Err(err) => {
                let msg = format!("平仓失败: {}", err);
                logging::error(Some(symbol), &msg);
                crate::utils::webhook::notify_error(symbol, &msg).await;
                if msg.contains("-2022") || msg.contains("ReduceOnly") {
                    logging::warn(Some(symbol), "判定为仓位已平，停止重复尝试");
                }
                self.cleanup_position_orders(symbol, position, "平仓失败后尝试清理保护单")
                    .await?;
            }
        }

        Ok(())
    }

    async fn cleanup_position_orders(
        &self,
        symbol: &str,
        position: &PositionState,
        reason: &str,
    ) -> Result<()> {
        if let Some(tp_order_id) = &position.tp_order_id {
            match self
                .account
                .exchange
                .cancel_order(tp_order_id, symbol, self.market_type)
                .await
            {
                Ok(_) => logging::info(Some(symbol), format!("取消止盈单 id={}", tp_order_id)),
                Err(err) => logging::debug(
                    Some(symbol),
                    format!("取消止盈单失败 (可能已成交): {}", err),
                ),
            }
        }

        if let Some(stop_order_id) = &position.stop_order_id {
            match self
                .account
                .exchange
                .cancel_order(stop_order_id, symbol, self.market_type)
                .await
            {
                Ok(_) => logging::info(Some(symbol), format!("取消止损单 id={}", stop_order_id)),
                Err(err) => logging::debug(
                    Some(symbol),
                    format!("取消止损单失败 (可能已成交): {}", err),
                ),
            }
        }

        {
            let mut states = self.symbol_states.write().await;
            if let Some(state) = states.get_mut(symbol) {
                if let Some(pos) = state.position_mut(position.side) {
                    pos.stop_order_id = None;
                    pos.tp_order_id = None;
                    pos.remaining_qty = 0.0;
                }
                state.clear_position(position.side);
            }
        }

        self.update_status().await;
        logging::info(Some(symbol), format!("仓位清理完成: {}", reason));
        Ok(())
    }

    pub(super) async fn submit_order(&self, plan: OrderPlan) -> Result<()> {
        let exchange_name = self.account.exchange_name.clone();
        let client_id =
            generate_order_id_with_tag(&self.config.strategy.name, &exchange_name, "MR");

        let mut params = std::collections::HashMap::new();
        if self.config.execution.post_only {
            params.insert("postOnly".to_string(), "true".to_string());
        }
        self.apply_position_side(&mut params, plan.side);

        let exchange_symbol = self.exchange_symbol(&plan.symbol);

        let order_req = OrderRequest {
            symbol: exchange_symbol.clone(),
            side: plan.side,
            order_type: OrderType::Limit,
            amount: plan.quantity,
            price: Some(plan.limit_price),
            market_type: self.market_type,
            params: Some(params),
            client_order_id: Some(client_id.clone()),
            time_in_force: Some("GTX".to_string()),
            reduce_only: None,
            post_only: Some(self.config.execution.post_only),
        };

        let order = self
            .account
            .exchange
            .create_order(order_req)
            .await
            .map_err(|e| anyhow!("create order failed: {}", e))?;

        crate::utils::webhook::notify_info(
            &plan.symbol,
            &format!(
                "开仓下单成功\n方向: {:?}\n数量: {:.4}\n价格: {:.6}",
                plan.side, plan.quantity, plan.limit_price
            ),
        )
        .await;

        let tracker = OrderTracker::new(
            client_id.clone(),
            plan.side,
            plan.limit_price,
            plan.quantity,
            self.config.execution.ttl_secs,
            plan.stop_price,
            plan.take_profit,
            plan.improve,
            plan.trailing_distance,
        );

        {
            let mut states = self.symbol_states.write().await;
            if let Some(state) = states.get_mut(&plan.symbol) {
                let mut tracker = tracker;
                tracker.order_id = Some(order.id.clone());
                state.pending_orders.insert(client_id.clone(), tracker);
                state.last_signal = Some(SignalSnapshot {
                    timestamp: plan.indicators.timestamp,
                    z_score: plan.indicators.bollinger_5m.z_score,
                    band_percent: plan.indicators.bollinger_5m.band_percent,
                    rsi: plan.indicators.rsi,
                    bollinger_mid: plan.indicators.bollinger_5m.middle,
                    bollinger_sigma: plan.indicators.bollinger_5m.sigma,
                    atr: plan.indicators.atr,
                    adx: plan.indicators.adx,
                    bbw: plan.indicators.bbw,
                    bbw_percentile: plan.indicators.bbw_percentile,
                    slope_metric: plan.indicators.slope_metric,
                    choppiness: plan.indicators.choppiness,
                });
            }
        }

        logging::info(
            Some(plan.symbol.as_str()),
            format!(
                "提交限价单 id={} side={:?} qty={:.4} price={:.6}",
                order.id, plan.side, plan.quantity, plan.limit_price
            ),
        );

        self.update_status().await;

        Ok(())
    }

    fn apply_position_side(
        &self,
        params: &mut std::collections::HashMap<String, String>,
        position_side: OrderSide,
    ) {
        if !self.uses_dual_position_mode() {
            return;
        }
        params.insert(
            "positionSide".to_string(),
            Self::position_side_label(position_side).to_string(),
        );
    }

    fn uses_dual_position_mode(&self) -> bool {
        self.config.account.dual_position_mode && matches!(self.market_type, MarketType::Futures)
    }

    fn position_side_label(side: OrderSide) -> &'static str {
        match side {
            OrderSide::Buy => "LONG",
            OrderSide::Sell => "SHORT",
        }
    }

    fn matches_position_side(value: &str, side: OrderSide) -> bool {
        let target = Self::position_side_label(side);
        if value.eq_ignore_ascii_case(target) {
            return true;
        }
        match side {
            OrderSide::Buy => value.eq_ignore_ascii_case("BUY"),
            OrderSide::Sell => value.eq_ignore_ascii_case("SELL"),
        }
    }

    fn reduce_only_flag(&self) -> Option<bool> {
        if self.uses_dual_position_mode() {
            None
        } else {
            Some(true)
        }
    }

    fn lock_profit_if_needed(
        &self,
        symbol: &str,
        position: &mut PositionState,
        current_price: f64,
    ) {
        let profit_percent = match position.side {
            OrderSide::Buy => (current_price - position.entry_price) / position.entry_price,
            OrderSide::Sell => (position.entry_price - current_price) / position.entry_price,
        };

        if profit_percent < self.config.execution.lock_profit_pct {
            return;
        }

        let lock_buffer = self.config.execution.lock_buffer_pct;
        let target_stop = match position.side {
            OrderSide::Buy => position.entry_price * (1.0 + lock_buffer),
            OrderSide::Sell => position.entry_price * (1.0 - lock_buffer),
        };

        let should_update = match position.side {
            OrderSide::Buy => target_stop > position.stop_price,
            OrderSide::Sell => target_stop < position.stop_price,
        };

        if should_update {
            position.stop_price = target_stop;
            position.last_update = Utc::now();
            logging::info(
                Some(symbol),
                format!(
                    "锁定利润 -> side={:?} stop={:.6} profit={:.2}%",
                    position.side,
                    target_stop,
                    profit_percent * 100.0
                ),
            );
        }
    }
}
