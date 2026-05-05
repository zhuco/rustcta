use anyhow::{anyhow, Result};
use chrono::Utc;
use serde::Serialize;

use crate::core::types::{MarketType, Order, OrderRequest, OrderSide, OrderType, Position};

use super::config::SymbolConfig;
use super::logging;
use super::market::ShortLadderSignalSnapshot;
use super::model::{
    adopt_short_progress, capped_layer_notional, cumulative_layer_notionals,
    infer_short_ladder_last_price, matches_short_position_side, position_params, precision_format,
    precision_round_down, precision_round_up, LiveShortPosition, PendingExitOrder,
    PendingInitialEntry, SymbolPrecision,
};
use super::ShortLadderLiveStrategy;

impl ShortLadderLiveStrategy {
    pub(super) async fn ensure_symbol_setup(&self, symbol: &str) -> Result<()> {
        let needs_precision = {
            let runtime = self.runtime.read().await;
            let state = runtime
                .symbols
                .get(symbol)
                .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;
            state.precision.is_none()
        };

        if needs_precision {
            let info = self
                .exchange
                .get_symbol_info(symbol, self.market_type)
                .await
                .map_err(|err| anyhow!("get_symbol_info failed for {}: {}", symbol, err))?;

            let precision = SymbolPrecision {
                step_size: info.step_size,
                tick_size: info.tick_size,
                min_notional: info.min_notional.unwrap_or(0.0),
                min_order_size: info.min_order_size,
            };

            let mut runtime = self.runtime.write().await;
            if let Some(state) = runtime.symbols.get_mut(symbol) {
                state.precision = Some(precision);
            }
        }

        Ok(())
    }

    pub(super) fn symbol_config(&self, symbol: &str) -> Option<&SymbolConfig> {
        self.config
            .symbols
            .iter()
            .find(|cfg| cfg.enabled && cfg.symbol == symbol)
    }

    pub(super) async fn has_open_orders(&self, symbol: &str) -> Result<bool> {
        let orders = self
            .exchange
            .get_open_orders(Some(symbol), self.market_type)
            .await
            .map_err(|err| anyhow!("get_open_orders failed for {}: {}", symbol, err))?;
        Ok(orders
            .into_iter()
            .any(|order| !is_short_ladder_exit_order(&order)))
    }

    pub(super) fn build_short_position_params(
        &self,
    ) -> Option<std::collections::HashMap<String, String>> {
        if !matches!(self.market_type, MarketType::Futures) {
            return None;
        }
        position_params(self.config.account.dual_position_mode, OrderSide::Sell)
    }

    fn order_params_with_precision(
        &self,
        precision: &SymbolPrecision,
        quantity: f64,
        price: Option<f64>,
    ) -> Option<std::collections::HashMap<String, String>> {
        let mut params = self.build_short_position_params().unwrap_or_default();
        params.insert(
            "quantity".to_string(),
            precision_format(quantity, precision.step_size),
        );
        if let Some(price) = price {
            params.insert(
                "price".to_string(),
                precision_format(price, precision.tick_size),
            );
        }
        Some(params)
    }

    pub(super) async fn derive_order_quantity(
        &self,
        symbol: &str,
        notional: f64,
        reference_price: f64,
    ) -> Result<f64> {
        if notional <= 0.0 || reference_price <= 0.0 {
            return Err(anyhow!(
                "invalid order sizing for {} notional={} price={}",
                symbol,
                notional,
                reference_price
            ));
        }

        let precision = self.symbol_precision(symbol).await?;
        let mut qty = notional / reference_price;
        qty = precision_round_down(qty, precision.step_size.max(precision.min_order_size));

        if qty < precision.min_order_size {
            qty = precision.min_order_size;
        }
        qty = precision_round_down(qty, precision.step_size);

        if qty <= 0.0 {
            return Err(anyhow!("quantity rounded to zero for {}", symbol));
        }

        let final_notional = qty * reference_price;
        if precision.min_notional > 0.0 && final_notional + 1e-9 < precision.min_notional {
            let min_qty = precision_round_down(
                (precision.min_notional / reference_price).max(precision.min_order_size),
                precision.step_size,
            );
            if min_qty <= 0.0 {
                return Err(anyhow!("min notional adjustment failed for {}", symbol));
            }
            qty = min_qty;
        }

        Ok(qty)
    }

    async fn symbol_precision(&self, symbol: &str) -> Result<SymbolPrecision> {
        let runtime = self.runtime.read().await;
        runtime
            .symbols
            .get(symbol)
            .and_then(|state| state.precision.clone())
            .ok_or_else(|| anyhow!("precision missing for {}", symbol))
    }

    fn maker_sell_price(&self, precision: &SymbolPrecision, reference_price: f64) -> f64 {
        let offset = self.config.execution.maker_price_offset_bps.max(0.0) / 10_000.0;
        let price = reference_price * (1.0 + offset);
        precision_round_up(price, precision.tick_size)
    }

    fn maker_buy_price(&self, precision: &SymbolPrecision, reference_price: f64) -> f64 {
        let offset = self.config.execution.maker_price_offset_bps.max(0.0) / 10_000.0;
        let price = reference_price * (1.0 - offset);
        precision_round_down(price, precision.tick_size)
    }

    pub(super) async fn sync_short_position(
        &self,
        symbol: &str,
        atr_5m: f64,
        current_price: f64,
    ) -> Result<()> {
        let positions = self
            .exchange
            .get_positions(Some(symbol))
            .await
            .map_err(|err| anyhow!("get_positions failed for {}: {}", symbol, err))?;

        let matched = positions
            .into_iter()
            .filter(|pos| pos.symbol == symbol)
            .find(Self::is_short_position);

        let Some(pos) = matched else {
            let mut runtime = self.runtime.write().await;
            if let Some(state) = runtime.symbols.get_mut(symbol) {
                if state.short.is_some() {
                    logging::info(Some(symbol), "交易所无空头持仓，已清空本地接管状态");
                }
                state.short = None;
            }
            return Ok(());
        };

        let quantity = position_quantity(&pos);
        let average_entry_price = pos.entry_price;
        if quantity <= 0.0 || average_entry_price <= 0.0 {
            return Ok(());
        }

        let symbol_config = self
            .symbol_config(symbol)
            .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;
        let progress = adopt_short_progress(
            quantity,
            average_entry_price,
            symbol_config.initial_notional,
            symbol_config.max_notional,
            &self.config.ladder.layer_weights,
            self.config.ladder.adopt_progress_tolerance_pct,
        );
        let layer_notionals = layer_notionals_until(
            symbol_config.initial_notional,
            &self.config.ladder.layer_weights,
            progress.filled_layers,
        );
        let spacing = self.config.ladder.layer_spacing_atr * atr_5m.max(0.0);
        let last_layer_price = if !layer_notionals.is_empty() {
            infer_short_ladder_last_price(average_entry_price, spacing, &layer_notionals)
        } else {
            average_entry_price
        };
        let atr_at_entry = {
            let runtime = self.runtime.read().await;
            runtime
                .symbols
                .get(symbol)
                .and_then(|state| state.short.as_ref())
                .map(|short| short.atr_at_entry)
                .filter(|existing| *existing > 0.0)
                .unwrap_or(atr_5m.max(1e-9))
        };
        let breakeven_armed = {
            let runtime = self.runtime.read().await;
            runtime
                .symbols
                .get(symbol)
                .and_then(|state| state.short.as_ref())
                .map(|short| short.breakeven_armed)
                .unwrap_or(false)
        };
        let (trailing_take_profit_armed, best_favorable_price) = {
            let runtime = self.runtime.read().await;
            runtime
                .symbols
                .get(symbol)
                .and_then(|state| state.short.as_ref())
                .map(|short| (short.trailing_take_profit_armed, short.best_favorable_price))
                .unwrap_or((false, None))
        };
        let opened_at = {
            let runtime = self.runtime.read().await;
            runtime
                .symbols
                .get(symbol)
                .and_then(|state| state.short.as_ref())
                .map(|short| short.opened_at)
                .unwrap_or_else(Utc::now)
        };

        let live_short = LiveShortPosition {
            average_entry_price,
            quantity,
            current_notional: if current_price > 0.0 {
                quantity * current_price
            } else {
                progress.current_notional
            },
            filled_layers: progress.filled_layers,
            next_layer_index: progress.next_layer_index,
            last_layer_price,
            atr_at_entry,
            breakeven_armed,
            trailing_take_profit_armed,
            best_favorable_price,
            opened_at,
            last_sync_at: Utc::now(),
        };

        {
            let mut runtime = self.runtime.write().await;
            if let Some(state) = runtime.symbols.get_mut(symbol) {
                let was_empty = state.short.is_none();
                state.short = Some(live_short.clone());
                state.last_error = None;
                if was_empty {
                    logging::info(
                        Some(symbol),
                        format!(
                            "接管交易所空头: qty={:.6} avg={:.6} notional≈{:.2} layers={}/{} next=L{}",
                            quantity,
                            average_entry_price,
                            progress.current_notional,
                            progress.filled_layers,
                            self.config.ladder.layer_weights.len(),
                            progress.next_layer_index + 1
                        ),
                    );
                }
            }
        }

        Ok(())
    }

    fn is_short_position(position: &Position) -> bool {
        if matches_short_position_side(&position.side) {
            return true;
        }
        position.amount < -1e-9 || position.size < -1e-9
    }

    pub(super) async fn open_initial_short(
        &self,
        symbol: &str,
        snapshot: &ShortLadderSignalSnapshot,
    ) -> Result<()> {
        let symbol_config = self
            .symbol_config(symbol)
            .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;
        let notional = capped_layer_notional(
            symbol_config.initial_notional,
            symbol_config.max_notional,
            0.0,
            &self.config.ladder.layer_weights,
            0,
        )
        .ok_or_else(|| anyhow!("initial notional unavailable for {}", symbol))?;

        self.place_short_entry_order(symbol, 0, notional, snapshot.current_price, "initial short")
            .await
    }

    pub(super) async fn add_short_layer(
        &self,
        symbol: &str,
        layer_index: usize,
        trigger_price: f64,
    ) -> Result<()> {
        let (initial_notional, max_notional, current_notional) = {
            let runtime = self.runtime.read().await;
            let state = runtime
                .symbols
                .get(symbol)
                .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;
            let current_notional = state
                .short
                .as_ref()
                .map(|short| short.average_entry_price * short.quantity)
                .unwrap_or(0.0);
            let symbol_config = self
                .symbol_config(symbol)
                .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;
            (
                symbol_config.initial_notional,
                symbol_config.max_notional,
                current_notional,
            )
        };

        let notional = capped_layer_notional(
            initial_notional,
            max_notional,
            current_notional,
            &self.config.ladder.layer_weights,
            layer_index,
        )
        .ok_or_else(|| {
            anyhow!(
                "layer {} notional unavailable for {}",
                layer_index + 1,
                symbol
            )
        })?;

        self.place_short_entry_order(
            symbol,
            layer_index,
            notional,
            trigger_price,
            "adverse add-on",
        )
        .await
    }

    async fn place_short_entry_order(
        &self,
        symbol: &str,
        layer_index: usize,
        notional: f64,
        reference_price: f64,
        reason: &str,
    ) -> Result<()> {
        let precision = self.symbol_precision(symbol).await?;
        let order_price = if self.config.execution.use_post_only_entry {
            self.maker_sell_price(&precision, reference_price)
        } else {
            reference_price
        };
        let quantity = self
            .derive_order_quantity(symbol, notional, order_price)
            .await?;
        let order_type = if self.config.execution.use_post_only_entry {
            OrderType::Limit
        } else {
            OrderType::Market
        };

        let request = OrderRequest {
            symbol: symbol.to_string(),
            side: OrderSide::Sell,
            order_type,
            amount: quantity,
            price: if matches!(order_type, OrderType::Limit) {
                Some(order_price)
            } else {
                None
            },
            market_type: self.market_type,
            params: self.order_params_with_precision(
                &precision,
                quantity,
                if matches!(order_type, OrderType::Limit) {
                    Some(order_price)
                } else {
                    None
                },
            ),
            client_order_id: Some(build_short_ladder_client_order_id(
                symbol,
                "l",
                layer_index + 1,
            )),
            time_in_force: if self.config.execution.use_post_only_entry {
                Some("GTX".to_string())
            } else {
                None
            },
            reduce_only: None,
            post_only: Some(self.config.execution.use_post_only_entry),
        };

        let created_order = self
            .exchange
            .create_order(request)
            .await
            .map_err(|err| anyhow!("create short order failed for {}: {}", symbol, err))?;

        {
            let mut runtime = self.runtime.write().await;
            if let Some(state) = runtime.symbols.get_mut(symbol) {
                state.last_order_at = Some(Utc::now());
                if self.should_fallback_initial_entry_to_market(layer_index) {
                    state.pending_initial_entry = Some(PendingInitialEntry {
                        order_id: created_order.id.clone(),
                        client_order_id: client_order_id(&created_order).map(str::to_string),
                        notional,
                        order_price,
                        reference_price,
                        submitted_at: created_order.timestamp,
                    });
                }
                state.last_error = None;
            }
        }

        logging::info(
            Some(symbol),
            format!(
                "提交空头{}单: L{} qty={:.6} price={:.6} notional≈{:.2} reason={}",
                if self.config.execution.use_post_only_entry {
                    "挂"
                } else {
                    "市价"
                },
                layer_index + 1,
                quantity,
                order_price,
                quantity * order_price,
                reason
            ),
        );
        self.notify_trade_event(
            "开仓/加仓",
            symbol,
            format!(
                "方向: 开空\n层级: L{}\n数量: {:.6}\n价格: {:.6}\n名义价值: {:.2} USDC\n原因: {}",
                layer_index + 1,
                quantity,
                order_price,
                quantity * order_price,
                reason
            ),
        )
        .await;
        self.update_status().await;
        Ok(())
    }

    fn should_fallback_initial_entry_to_market(&self, layer_index: usize) -> bool {
        layer_index == 0
            && self.config.execution.use_post_only_entry
            && self
                .config
                .execution
                .initial_order_taker_fallback_secs
                .is_some_and(|seconds| seconds > 0)
    }

    pub(super) async fn fallback_stale_initial_entry_to_market(&self, symbol: &str) -> Result<()> {
        let Some(wait_secs) = self.config.execution.initial_order_taker_fallback_secs else {
            return Ok(());
        };
        let pending = {
            let runtime = self.runtime.read().await;
            runtime
                .symbols
                .get(symbol)
                .and_then(|state| state.pending_initial_entry.clone())
        };
        let Some(pending) = pending else {
            return Ok(());
        };

        let elapsed = Utc::now()
            .signed_duration_since(pending.submitted_at)
            .num_seconds()
            .max(0) as u64;
        if elapsed < wait_secs {
            return Ok(());
        }

        let open_orders = self
            .exchange
            .get_open_orders(Some(symbol), self.market_type)
            .await
            .map_err(|err| anyhow!("get_open_orders failed for {}: {}", symbol, err))?;
        let Some(open_order) = open_orders
            .into_iter()
            .find(|order| same_pending_initial_order(order, &pending))
        else {
            self.clear_pending_initial_entry(symbol).await;
            logging::info(Some(symbol), "L1 挂单已成交或已撤销，无需转市价");
            return Ok(());
        };

        self.exchange
            .cancel_order(&open_order.id, symbol, self.market_type)
            .await
            .map_err(|err| anyhow!("cancel stale L1 maker order failed for {}: {}", symbol, err))?;

        let remaining_notional = if open_order.remaining > 0.0 {
            (open_order.remaining * pending.order_price)
                .min(pending.notional)
                .max(0.0)
        } else if open_order.filled > 0.0 && open_order.amount > open_order.filled {
            ((open_order.amount - open_order.filled) * pending.order_price)
                .min(pending.notional)
                .max(0.0)
        } else {
            pending.notional
        };
        if remaining_notional <= 0.0 {
            self.clear_pending_initial_entry(symbol).await;
            logging::info(Some(symbol), "L1 挂单撤销时无剩余数量，无需转市价");
            return Ok(());
        }

        let quantity = self
            .derive_order_quantity(
                symbol,
                remaining_notional,
                pending.reference_price.max(pending.order_price),
            )
            .await?;
        let precision = self.symbol_precision(symbol).await?;
        let request = OrderRequest {
            symbol: symbol.to_string(),
            side: OrderSide::Sell,
            order_type: OrderType::Market,
            amount: quantity,
            price: None,
            market_type: self.market_type,
            params: self.order_params_with_precision(&precision, quantity, None),
            client_order_id: Some(build_short_ladder_client_order_id(symbol, "lm", 1)),
            time_in_force: None,
            reduce_only: None,
            post_only: Some(false),
        };

        self.exchange.create_order(request).await.map_err(|err| {
            anyhow!(
                "create fallback market short failed for {}: {}",
                symbol,
                err
            )
        })?;
        {
            let mut runtime = self.runtime.write().await;
            if let Some(state) = runtime.symbols.get_mut(symbol) {
                state.pending_initial_entry = None;
                state.last_order_at = Some(Utc::now());
            }
        }

        logging::info(
            Some(symbol),
            format!(
                "L1 挂单 {} 秒未成交，已撤单并按市价补开: qty={:.6} notional≈{:.2}",
                wait_secs, quantity, remaining_notional
            ),
        );
        Ok(())
    }

    async fn clear_pending_initial_entry(&self, symbol: &str) {
        let mut runtime = self.runtime.write().await;
        if let Some(state) = runtime.symbols.get_mut(symbol) {
            state.pending_initial_entry = None;
        }
    }

    pub(super) async fn close_short(
        &self,
        symbol: &str,
        reference_price: f64,
        reason: &str,
    ) -> Result<()> {
        let existing = {
            let runtime = self.runtime.read().await;
            runtime
                .symbols
                .get(symbol)
                .and_then(|state| state.short.clone())
        };

        let Some(position) = existing else {
            return Ok(());
        };
        if position.quantity <= 0.0 {
            return Ok(());
        }

        let maker_first_take_profit = matches!(reason, "take_profit" | "trailing_take_profit")
            && self.config.execution.take_profit_maker_first;
        let order_type =
            if self.config.execution.close_with_market_order && !maker_first_take_profit {
                OrderType::Market
            } else {
                OrderType::Limit
            };
        self.submit_short_close_order(symbol, reference_price, reason, order_type)
            .await
    }

    async fn submit_short_close_order(
        &self,
        symbol: &str,
        reference_price: f64,
        reason: &str,
        order_type: OrderType,
    ) -> Result<()> {
        let existing = {
            let runtime = self.runtime.read().await;
            runtime
                .symbols
                .get(symbol)
                .and_then(|state| state.short.clone())
        };

        let Some(position) = existing else {
            return Ok(());
        };
        if position.quantity <= 0.0 {
            return Ok(());
        }

        let precision = self.symbol_precision(symbol).await?;
        let close_price = self.maker_buy_price(&precision, reference_price);
        let client_order_id_value = build_short_ladder_client_order_id(symbol, "c", 0);
        let request = OrderRequest {
            symbol: symbol.to_string(),
            side: OrderSide::Buy,
            order_type,
            amount: position.quantity,
            price: if matches!(order_type, OrderType::Limit) {
                Some(close_price)
            } else {
                None
            },
            market_type: self.market_type,
            params: self.order_params_with_precision(
                &precision,
                position.quantity,
                if matches!(order_type, OrderType::Limit) {
                    Some(close_price)
                } else {
                    None
                },
            ),
            client_order_id: Some(client_order_id_value.clone()),
            time_in_force: if matches!(order_type, OrderType::Limit) {
                Some("GTX".to_string())
            } else {
                None
            },
            reduce_only: if self.config.account.dual_position_mode {
                None
            } else {
                Some(true)
            },
            post_only: Some(matches!(order_type, OrderType::Limit)),
        };

        let created_order = self
            .exchange
            .create_order(request)
            .await
            .map_err(|err| anyhow!("close short failed for {}: {}", symbol, err))?;

        let pnl = position.unrealized_pnl(reference_price);
        {
            let mut runtime = self.runtime.write().await;
            if let Some(state) = runtime.symbols.get_mut(symbol) {
                if matches!(order_type, OrderType::Market) {
                    state.short = None;
                    state.pending_exit = None;
                } else {
                    state.pending_exit = Some(PendingExitOrder {
                        order_id: created_order.id.clone(),
                        client_order_id: client_order_id(&created_order)
                            .map(str::to_string)
                            .or(Some(client_order_id_value)),
                        reason: reason.to_string(),
                        submitted_at: Utc::now(),
                        reference_price,
                    });
                }
                state.last_order_at = Some(Utc::now());
                state.last_error = None;
            }
        }

        logging::info(
            Some(symbol),
            format!(
                "提交空头平仓: qty={:.6} avg={:.6} ref={:.6} pnl≈{:.4} reason={}",
                position.quantity, position.average_entry_price, reference_price, pnl, reason
            ),
        );
        let title = if reason == "take_profit" || reason == "trailing_take_profit" {
            "止盈"
        } else if reason == "stop_loss" {
            "止损"
        } else {
            "平仓"
        };
        self.notify_trade_event(
            title,
            symbol,
            format!(
                "方向: 平空\n数量: {:.6}\n均价: {:.6}\n参考价: {:.6}\n预估PnL: {:.4} USDC\n原因: {}",
                position.quantity,
                position.average_entry_price,
                reference_price,
                pnl,
                reason
            ),
        )
        .await;
        if matches!(order_type, OrderType::Market) {
            let _ = self
                .sync_short_position(symbol, position.atr_at_entry, reference_price)
                .await;
        }
        self.update_status().await;
        Ok(())
    }

    pub(super) async fn fallback_stale_exit_to_market(&self, symbol: &str) -> Result<()> {
        let pending = {
            let runtime = self.runtime.read().await;
            runtime
                .symbols
                .get(symbol)
                .and_then(|state| state.pending_exit.clone())
        };
        let Some(pending) = pending else {
            return Ok(());
        };

        let elapsed = Utc::now()
            .signed_duration_since(pending.submitted_at)
            .num_seconds()
            .max(0) as u64;
        if elapsed < self.config.execution.take_profit_maker_timeout_secs.max(1) {
            return Ok(());
        }

        let open_orders = self
            .exchange
            .get_open_orders(Some(symbol), self.market_type)
            .await
            .map_err(|err| anyhow!("get_open_orders failed for {}: {}", symbol, err))?;
        let Some(open_order) = open_orders.into_iter().find(|order| {
            order.id == pending.order_id
                || client_order_id(order)
                    .zip(pending.client_order_id.as_deref())
                    .is_some_and(|(actual, expected)| actual == expected)
        }) else {
            let mut runtime = self.runtime.write().await;
            if let Some(state) = runtime.symbols.get_mut(symbol) {
                state.pending_exit = None;
            }
            logging::info(Some(symbol), "止盈 maker 平仓单已成交或已撤销");
            return Ok(());
        };

        self.exchange
            .cancel_order(&open_order.id, symbol, self.market_type)
            .await
            .map_err(|err| anyhow!("cancel stale exit order failed for {}: {}", symbol, err))?;

        {
            let mut runtime = self.runtime.write().await;
            if let Some(state) = runtime.symbols.get_mut(symbol) {
                state.pending_exit = None;
            }
        }

        logging::info(
            Some(symbol),
            format!(
                "止盈 maker 平仓单 {} 秒未成交，撤单后转市价: reason={} ref={:.6}",
                elapsed, pending.reason, pending.reference_price
            ),
        );
        self.submit_short_close_order(
            symbol,
            pending.reference_price,
            &pending.reason,
            OrderType::Market,
        )
        .await
    }

    async fn notify_trade_event(&self, title: &str, symbol: &str, body: String) {
        if !self.config.notifications.enabled {
            return;
        }
        let url = self.config.notifications.wechat_webhook_url.trim();
        if url.is_empty() {
            return;
        }

        #[derive(Serialize)]
        struct MarkdownMessage<'a> {
            msgtype: &'a str,
            markdown: MarkdownContent,
        }

        #[derive(Serialize)]
        struct MarkdownContent {
            content: String,
        }

        let content = format!(
            "## Short Ladder {}\n\n**策略**: {}\n**交易对**: {}\n**时间**: {} UTC\n\n{}",
            title,
            self.config.strategy.name,
            symbol,
            Utc::now().format("%Y-%m-%d %H:%M:%S"),
            body
        );
        let payload = MarkdownMessage {
            msgtype: "markdown",
            markdown: MarkdownContent { content },
        };

        match reqwest::Client::new().post(url).json(&payload).send().await {
            Ok(response) if response.status().is_success() => {
                logging::info(Some(symbol), format!("企业微信通知已发送: {}", title));
            }
            Ok(response) => {
                logging::warn(
                    Some(symbol),
                    format!("企业微信通知失败: {} status={}", title, response.status()),
                );
            }
            Err(err) => {
                logging::warn(
                    Some(symbol),
                    format!("企业微信通知异常: {} err={}", title, err),
                );
            }
        }
    }
}

fn position_quantity(position: &Position) -> f64 {
    position
        .contracts
        .abs()
        .max(position.size.abs())
        .max(position.amount.abs())
}

fn same_pending_initial_order(order: &Order, pending: &PendingInitialEntry) -> bool {
    order.id == pending.order_id
        || client_order_id(order)
            .zip(pending.client_order_id.as_deref())
            .is_some_and(|(left_id, right_id)| left_id == right_id)
}

fn is_short_ladder_exit_order(order: &Order) -> bool {
    client_order_id(order)
        .map(|id| id.starts_with("sll_") && (id.contains("_close_") || id.contains("_c_")))
        .unwrap_or(false)
}

fn build_short_ladder_client_order_id(symbol: &str, tag: &str, level: usize) -> String {
    let symbol_slug = symbol
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();
    let timestamp = Utc::now().timestamp_millis().rem_euclid(1_000_000_000);
    let id = format!(
        "sll_{}_{}_{}_{:02}",
        symbol_slug,
        tag,
        timestamp,
        level % 100
    );
    if id.len() <= 36 {
        id
    } else {
        let max_symbol_len = 36usize
            .saturating_sub("sll_".len())
            .saturating_sub(tag.len())
            .saturating_sub(1)
            .saturating_sub(timestamp.to_string().len())
            .saturating_sub(1)
            .saturating_sub(2);
        let truncated_symbol = symbol_slug.chars().take(max_symbol_len).collect::<String>();
        format!(
            "sll_{}_{}_{}_{:02}",
            truncated_symbol,
            tag,
            timestamp,
            level % 100
        )
    }
}

fn client_order_id(order: &Order) -> Option<&str> {
    order
        .info
        .get("clientOrderId")
        .and_then(|value| value.as_str())
}

fn layer_notionals_until(
    initial_notional: f64,
    layer_weights: &[f64],
    filled_layers: usize,
) -> Vec<f64> {
    let cumulative_len = cumulative_layer_notionals(initial_notional, layer_weights).len();
    layer_weights
        .iter()
        .take(filled_layers.min(cumulative_len))
        .map(|weight| initial_notional * *weight)
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    use crate::core::types::{MarketType, OrderStatus};

    use super::*;

    fn sample_order(id: &str, client_order_id: Option<&str>) -> Order {
        Order {
            id: id.to_string(),
            symbol: "ENA/USDC".to_string(),
            side: OrderSide::Sell,
            order_type: OrderType::Limit,
            amount: 2.0,
            price: Some(100.0),
            filled: 0.0,
            remaining: 2.0,
            status: OrderStatus::Open,
            market_type: MarketType::Futures,
            timestamp: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            last_trade_timestamp: None,
            info: json!({ "clientOrderId": client_order_id }),
        }
    }

    #[test]
    fn pending_initial_order_matches_exchange_id_or_client_order_id() {
        let pending = PendingInitialEntry {
            order_id: "12345".to_string(),
            client_order_id: Some("sll_enausdc_L1_1".to_string()),
            notional: 200.0,
            order_price: 1.0,
            reference_price: 1.0,
            submitted_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        };

        assert!(same_pending_initial_order(
            &sample_order("12345", Some("other")),
            &pending
        ));
        assert!(same_pending_initial_order(
            &sample_order("99999", Some("sll_enausdc_L1_1")),
            &pending
        ));
        assert!(!same_pending_initial_order(
            &sample_order("99999", Some("other")),
            &pending
        ));
    }
}
