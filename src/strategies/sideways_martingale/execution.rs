use std::collections::HashMap;

use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};

use crate::core::types::{MarketType, OrderRequest, OrderSide, OrderType, Position};

use super::logging;
use super::model::{precision_round_down, SidePositionState, SymbolPrecision};
use super::SidewaysMartingaleStrategy;

impl SidewaysMartingaleStrategy {
    pub(super) async fn ensure_symbol_setup(&self, symbol: &str) -> Result<()> {
        let (needs_precision, leverage) = {
            let runtime = self.runtime.read().await;
            let state = runtime
                .symbols
                .get(symbol)
                .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;
            (state.precision.is_none(), state.config.leverage)
        };

        if needs_precision {
            let info = self
                .exchange
                .get_symbol_info(symbol, self.market_type)
                .await
                .map_err(|e| anyhow!("get_symbol_info failed for {}: {}", symbol, e))?;

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
            if let Some(lev) = leverage.or(self.config.account.default_leverage) {
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

    fn build_position_params(&self, side: OrderSide) -> Option<HashMap<String, String>> {
        if !self.config.account.dual_position_mode
            || !matches!(self.market_type, MarketType::Futures)
        {
            return None;
        }

        let mut params = HashMap::new();
        params.insert(
            "positionSide".to_string(),
            match side {
                OrderSide::Buy => "LONG".to_string(),
                OrderSide::Sell => "SHORT".to_string(),
            },
        );
        Some(params)
    }

    pub(super) async fn derive_order_quantity(
        &self,
        symbol: &str,
        notional: f64,
        reference_price: f64,
    ) -> Result<f64> {
        if reference_price <= 0.0 {
            return Err(anyhow!("invalid reference price for {}", symbol));
        }

        let precision = {
            let runtime = self.runtime.read().await;
            runtime
                .symbols
                .get(symbol)
                .and_then(|state| state.precision.clone())
                .ok_or_else(|| anyhow!("precision missing for {}", symbol))?
        };

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

    pub(super) async fn open_or_add_position(
        &self,
        symbol: &str,
        side: OrderSide,
        layer_index: usize,
        multiplier: f64,
        reference_price: f64,
        reason: &str,
    ) -> Result<()> {
        let base_notional = {
            let runtime = self.runtime.read().await;
            runtime
                .symbols
                .get(symbol)
                .map(|s| s.config.base_notional)
                .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?
        };
        let notional = base_notional * multiplier;
        let quantity = self
            .derive_order_quantity(symbol, notional, reference_price)
            .await?;

        let request = OrderRequest {
            symbol: symbol.to_string(),
            side,
            order_type: OrderType::Market,
            amount: quantity,
            price: None,
            market_type: self.market_type,
            params: self.build_position_params(side),
            client_order_id: None,
            time_in_force: None,
            reduce_only: None,
            post_only: Some(false),
        };

        self.exchange
            .create_order(request)
            .await
            .map_err(|e| anyhow!("create_order failed: {}", e))?;

        {
            let mut runtime = self.runtime.write().await;
            let state = runtime
                .symbols
                .get_mut(symbol)
                .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;
            let now = Utc::now();
            match state.side_mut(side) {
                Some(position) => position.apply_fill(quantity, reference_price, multiplier),
                None => state.set_side(SidePositionState::new(
                    side,
                    quantity,
                    reference_price,
                    multiplier,
                )),
            }
            state.last_signal_at = Some(now);
        }

        logging::info(
            Some(symbol),
            format!(
                "交易记录: 开仓/补仓 side={:?} layer={} qty={:.6} price={:.6} notional={:.2} reason={}",
                side,
                layer_index + 1,
                quantity,
                reference_price,
                quantity * reference_price,
                reason
            ),
        );

        let _ = self.sync_symbol_position(symbol, side).await;
        self.update_status().await;
        Ok(())
    }

    pub(super) async fn close_position(
        &self,
        symbol: &str,
        side: OrderSide,
        reference_price: f64,
        reason: &str,
        is_stopout: bool,
    ) -> Result<()> {
        let existing = {
            let runtime = self.runtime.read().await;
            runtime
                .symbols
                .get(symbol)
                .and_then(|state| state.side(side).cloned())
        };

        let Some(position) = existing else {
            return Ok(());
        };
        if position.total_quantity <= 0.0 {
            return Ok(());
        }

        let close_side = match side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        };

        let request = OrderRequest {
            symbol: symbol.to_string(),
            side: close_side,
            order_type: OrderType::Market,
            amount: position.total_quantity,
            price: None,
            market_type: self.market_type,
            params: self.build_position_params(side),
            client_order_id: None,
            time_in_force: None,
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
            .map_err(|e| anyhow!("close create_order failed: {}", e))?;

        let pnl_pct = position.unrealized_pnl_pct(reference_price);
        let realized = match side {
            OrderSide::Buy => (reference_price - position.average_price) * position.total_quantity,
            OrderSide::Sell => (position.average_price - reference_price) * position.total_quantity,
        };

        {
            let mut runtime = self.runtime.write().await;
            let state = runtime
                .symbols
                .get_mut(symbol)
                .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;
            state.realized_pnl += realized;
            state.last_close_reason = Some(reason.to_string());
            state.clear_side(side);

            if is_stopout {
                runtime.consecutive_stopouts += 1;
                if runtime.consecutive_stopouts >= self.config.risk.max_consecutive_stopouts {
                    let pause_until = Utc::now() + Duration::hours(self.config.risk.cooldown_hours);
                    runtime.trading_paused_until = Some(pause_until);
                    logging::warn(
                        Some(symbol),
                        format!(
                            "触发连续止损熔断，暂停开仓至 {}",
                            pause_until.format("%Y-%m-%d %H:%M:%S UTC")
                        ),
                    );
                }
            } else {
                runtime.consecutive_stopouts = 0;
            }
        }

        logging::info(
            Some(symbol),
            format!(
                "交易记录: 平仓 side={:?} qty={:.6} avg={:.6} exit={:.6} pnl={:.4} pnl_pct={:.2}% reason={}",
                side,
                position.total_quantity,
                position.average_price,
                reference_price,
                realized,
                pnl_pct * 100.0,
                reason
            ),
        );

        let _ = self.sync_symbol_position(symbol, side).await;
        self.update_status().await;
        Ok(())
    }

    pub(super) async fn sync_symbol_position(&self, symbol: &str, side: OrderSide) -> Result<()> {
        let positions = self
            .exchange
            .get_positions(Some(symbol))
            .await
            .map_err(|e| anyhow!("get_positions failed: {}", e))?;
        let matched = positions
            .into_iter()
            .find(|pos| pos.symbol == symbol && Self::matches_position_side(pos, side));

        let mut runtime = self.runtime.write().await;
        let state = runtime
            .symbols
            .get_mut(symbol)
            .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;

        match matched {
            Some(pos) if pos.contracts.abs() > 1e-9 => {
                let qty = pos.contracts.abs();
                let avg = pos.entry_price;
                if let Some(existing) = state.side_mut(side) {
                    existing.total_quantity = qty;
                    existing.total_cost = qty * avg;
                    existing.average_price = avg;
                    existing.updated_at = Utc::now();
                    if pos.mark_price > 0.0 {
                        existing.last_entry_price = existing.last_entry_price.max(1e-9);
                    }
                } else {
                    state.set_side(SidePositionState::new(side, qty, avg, 1.0));
                }
            }
            _ => state.clear_side(side),
        }

        Ok(())
    }

    fn matches_position_side(position: &Position, side: OrderSide) -> bool {
        match side {
            OrderSide::Buy => {
                position.side.eq_ignore_ascii_case("LONG")
                    || position.side.eq_ignore_ascii_case("BUY")
            }
            OrderSide::Sell => {
                position.side.eq_ignore_ascii_case("SHORT")
                    || position.side.eq_ignore_ascii_case("SELL")
            }
        }
    }
}
