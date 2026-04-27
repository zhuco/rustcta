use anyhow::{anyhow, Result};
use chrono::Utc;

use crate::core::types::{MarketType, OrderRequest, OrderSide, OrderType, Position};

use super::config::SymbolConfig;
use super::logging;
use super::market::ShortLadderSignalSnapshot;
use super::model::{
    adopt_short_progress, capped_layer_notional, cumulative_layer_notionals,
    infer_short_ladder_last_price, matches_short_position_side, position_params,
    precision_round_down, precision_round_up, LiveShortPosition, SymbolPrecision,
};
use super::ShortLadderLiveStrategy;

impl ShortLadderLiveStrategy {
    pub(super) async fn ensure_symbol_setup(&self, symbol: &str) -> Result<()> {
        let (needs_precision, leverage) = {
            let runtime = self.runtime.read().await;
            let state = runtime
                .symbols
                .get(symbol)
                .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;
            let leverage = self
                .symbol_config(symbol)
                .and_then(|cfg| cfg.leverage)
                .or(self.config.account.default_leverage);
            (state.precision.is_none(), leverage)
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

        if matches!(self.market_type, MarketType::Futures) {
            if let Some(lev) = leverage {
                if let Err(err) = self.exchange.set_leverage(symbol, lev).await {
                    logging::warn(
                        Some(symbol),
                        format!("设置杠杆失败 leverage={} err={}", lev, err),
                    );
                } else {
                    logging::info(Some(symbol), format!("设置杠杆成功 leverage={}", lev));
                }
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
        Ok(!orders.is_empty())
    }

    pub(super) fn build_short_position_params(
        &self,
    ) -> Option<std::collections::HashMap<String, String>> {
        if !matches!(self.market_type, MarketType::Futures) {
            return None;
        }
        position_params(self.config.account.dual_position_mode, OrderSide::Sell)
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
            params: self.build_short_position_params(),
            client_order_id: Some(format!(
                "sll_{}_L{}_{}",
                symbol.to_lowercase(),
                layer_index + 1,
                Utc::now().timestamp_millis()
            )),
            time_in_force: if self.config.execution.use_post_only_entry {
                Some("GTX".to_string())
            } else {
                None
            },
            reduce_only: None,
            post_only: Some(self.config.execution.use_post_only_entry),
        };

        self.exchange
            .create_order(request)
            .await
            .map_err(|err| anyhow!("create short order failed for {}: {}", symbol, err))?;

        {
            let mut runtime = self.runtime.write().await;
            if let Some(state) = runtime.symbols.get_mut(symbol) {
                state.last_order_at = Some(Utc::now());
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
        self.update_status().await;
        Ok(())
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

        let precision = self.symbol_precision(symbol).await?;
        let close_price = self.maker_buy_price(&precision, reference_price);
        let order_type = if self.config.execution.close_with_market_order {
            OrderType::Market
        } else {
            OrderType::Limit
        };
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
            params: self.build_short_position_params(),
            client_order_id: Some(format!(
                "sll_{}_close_{}",
                symbol.to_lowercase(),
                Utc::now().timestamp_millis()
            )),
            time_in_force: if matches!(order_type, OrderType::Limit) {
                Some("GTC".to_string())
            } else {
                None
            },
            reduce_only: if self.config.account.dual_position_mode {
                None
            } else {
                Some(true)
            },
            post_only: Some(false),
        };

        self.exchange
            .create_order(request)
            .await
            .map_err(|err| anyhow!("close short failed for {}: {}", symbol, err))?;

        let pnl = position.unrealized_pnl(reference_price);
        {
            let mut runtime = self.runtime.write().await;
            if let Some(state) = runtime.symbols.get_mut(symbol) {
                state.short = None;
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
        let _ = self
            .sync_short_position(symbol, position.atr_at_entry, reference_price)
            .await;
        self.update_status().await;
        Ok(())
    }
}

fn position_quantity(position: &Position) -> f64 {
    position
        .contracts
        .abs()
        .max(position.size.abs())
        .max(position.amount.abs())
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
