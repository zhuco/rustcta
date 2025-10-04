use anyhow::{anyhow, Result};
use chrono::Utc;
use tokio::time::{interval, Duration, MissedTickBehavior};

use crate::core::types::{Order, OrderRequest, OrderSide, OrderStatus, OrderType};
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
            tracker.take_profit_primary,
            tracker.take_profit_secondary,
        );

        {
            let mut states = self.symbol_states.write().await;
            if let Some(state) = states.get_mut(symbol) {
                state.pending_orders.remove(&tracker.client_order_id);
                state.position = Some(position.clone());
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
        stop_params.insert("reduceOnly".to_string(), "true".to_string());

        let exchange_symbol = self.exchange_symbol(symbol);

        let stop_amount = match super::utils::round_quantity(
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

        if stop_amount <= 0.0 {
            logging::warn(Some(symbol), "跳过止损单，剩余仓位低于精度要求");
            return Ok(());
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
            reduce_only: Some(true),
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
                Some(order.id.clone())
            }
            Err(err) => {
                logging::warn(Some(symbol), format!("提交止损失败: {}", err));
                None
            }
        };

        let tp_side = stop_side;
        let mut tp_orders = Vec::new();
        let half_qty = (position.remaining_qty / 2.0).max(meta.min_order_size);
        let qty1 = super::utils::round_quantity(half_qty, meta.step_size, meta.amount_precision)
            .unwrap_or(half_qty);
        let qty2 = (position.remaining_qty - qty1).max(0.0);

        let tp_prices = vec![tracker.take_profit_primary, tracker.take_profit_secondary];
        let quantities = vec![qty1, qty2];

        for (tp_price, qty) in tp_prices.into_iter().zip(quantities.into_iter()) {
            let order_qty =
                match super::utils::round_quantity(qty, meta.step_size, meta.amount_precision) {
                    Ok(q) if q > 0.0 => q,
                    _ => {
                        super::utils::normalize_to_step(qty, meta.step_size, meta.amount_precision)
                    }
                };

            if order_qty <= 0.0 {
                continue;
            }
            let price =
                super::utils::round_price(tp_price, meta.tick_size, meta.price_precision, tp_side)?;
            let mut params = std::collections::HashMap::new();
            params.insert("reduceOnly".to_string(), "true".to_string());
            let tp_order = OrderRequest {
                symbol: exchange_symbol.clone(),
                side: tp_side,
                order_type: OrderType::Limit,
                amount: order_qty,
                price: Some(price),
                market_type: self.market_type,
                params: Some(params),
                client_order_id: None,
                time_in_force: Some("GTC".to_string()),
                reduce_only: Some(true),
                post_only: Some(true),
            };

            match self.account.exchange.create_order(tp_order).await {
                Ok(order) => {
                    logging::info(
                        Some(symbol),
                        format!(
                            "已提交止盈单 id={} side={:?} price={:.6} qty={:.4}",
                            order.id, tp_side, price, order_qty
                        ),
                    );
                    tp_orders.push(order.id.clone());
                }
                Err(err) => logging::warn(Some(symbol), format!("提交止盈失败: {}", err)),
            }
        }

        {
            let mut states = self.symbol_states.write().await;
            if let Some(state) = states.get_mut(symbol) {
                if let Some(pos) = state.position.as_mut() {
                    pos.stop_order_id = stop_order_id.clone();
                    pos.tp_order_ids.extend(tp_orders.clone());
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
                .filter_map(|(symbol, state)| {
                    state.position.as_ref().map(|pos| {
                        (
                            symbol.clone(),
                            pos.clone(),
                            state.one_minute.back().cloned(),
                            state.five_minute.back().cloned(),
                        )
                    })
                })
                .collect::<Vec<_>>()
        };

        for (symbol, position, last_1m, last_5m) in snapshots {
            let current_price = last_1m
                .as_ref()
                .or(last_5m.as_ref())
                .map(|k| k.close)
                .unwrap_or(position.entry_price);

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
                    if let Some(pos) = state.position.as_mut() {
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
        params.insert("reduceOnly".to_string(), "true".to_string());

        let order = OrderRequest {
            symbol: self.exchange_symbol(symbol),
            side,
            order_type: OrderType::Market,
            amount: position.remaining_qty,
            price: None,
            market_type: self.market_type,
            params: Some(params),
            client_order_id: None,
            time_in_force: None,
            reduce_only: Some(true),
            post_only: Some(false),
        };

        match self.account.exchange.create_order(order).await {
            Ok(_) => {
                logging::info(
                    Some(symbol),
                    format!(
                        "市价平仓 -> side={:?} qty={:.4}",
                        side, position.remaining_qty
                    ),
                );
                let mut states = self.symbol_states.write().await;
                if let Some(state) = states.get_mut(symbol) {
                    state.position = None;
                }
                drop(states);
                self.update_status().await;
            }
            Err(err) => logging::error(Some(symbol), format!("平仓失败: {}", err)),
        }

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
            reduce_only: Some(false),
            post_only: Some(self.config.execution.post_only),
        };

        let order = self
            .account
            .exchange
            .create_order(order_req)
            .await
            .map_err(|e| anyhow!("create order failed: {}", e))?;

        let tracker = OrderTracker::new(
            client_id.clone(),
            plan.side,
            plan.limit_price,
            plan.quantity,
            self.config.execution.ttl_secs,
            plan.stop_price,
            plan.take_profit_primary,
            plan.take_profit_secondary,
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
}
