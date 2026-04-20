use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};
use tokio::time::{interval, MissedTickBehavior};

use crate::core::types::{Kline, OrderSide};
use crate::utils::indicators::{calculate_adx, calculate_atr, calculate_ema, calculate_rsi};

use super::logging;
use super::model::{
    amplitude_pct, interval_duration_secs, latest_bar_change_pct, parse_interval, IndicatorSnapshot,
};
use super::SidewaysMartingaleStrategy;

impl SidewaysMartingaleStrategy {
    pub(super) async fn bootstrap(&self) -> Result<()> {
        let symbols = {
            let runtime = self.runtime.read().await;
            runtime.symbols.keys().cloned().collect::<Vec<_>>()
        };

        for symbol in symbols {
            self.ensure_symbol_setup(&symbol).await?;
        }

        Ok(())
    }

    pub(super) async fn spawn_tasks(&self) -> Result<()> {
        let loop_strategy = self.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = interval(std::time::Duration::from_secs(
                loop_strategy.config.data.poll_interval_secs,
            ));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let running = *loop_strategy.running.read().await;
                if !running {
                    break;
                }
                if let Err(err) = loop_strategy.run_once().await {
                    loop_strategy.mark_error(err.to_string()).await;
                }
            }
        });

        let mut handles = self.task_handles.lock().await;
        handles.push(handle);
        Ok(())
    }

    async fn run_once(&self) -> Result<()> {
        let symbols = {
            let runtime = self.runtime.read().await;
            runtime.symbols.keys().cloned().collect::<Vec<_>>()
        };

        for symbol in symbols {
            if let Err(err) = self.process_symbol(&symbol).await {
                logging::error(Some(&symbol), format!("处理失败: {}", err));
            }
        }

        self.update_status().await;
        Ok(())
    }

    async fn process_symbol(&self, symbol: &str) -> Result<()> {
        self.ensure_symbol_setup(symbol).await?;
        let klines = self.fetch_klines(symbol).await?;
        let indicators = self.build_indicator_snapshot(symbol, &klines).await?;
        self.update_symbol_market_snapshot(symbol, &klines, &indicators)
            .await?;

        self.apply_single_bar_pause(symbol, &indicators).await;
        self.manage_open_positions(symbol, &indicators).await?;
        self.maybe_open_positions(symbol, &indicators).await?;
        Ok(())
    }

    async fn fetch_klines(&self, symbol: &str) -> Result<Vec<Kline>> {
        let interval = parse_interval(&self.config.data.decision_interval);
        let bars = self
            .exchange
            .get_klines(
                symbol,
                interval,
                self.market_type,
                Some(self.config.data.kline_limit),
            )
            .await
            .map_err(|e| anyhow!("get_klines failed: {}", e))?;

        if bars.len() < 30 {
            return Err(anyhow!("insufficient klines for {}", symbol));
        }

        Ok(bars)
    }

    async fn build_indicator_snapshot(
        &self,
        symbol: &str,
        klines: &[Kline],
    ) -> Result<IndicatorSnapshot> {
        let closes = klines.iter().map(|k| k.close).collect::<Vec<_>>();
        let highs = klines.iter().map(|k| k.high).collect::<Vec<_>>();
        let lows = klines.iter().map(|k| k.low).collect::<Vec<_>>();

        let ema20 = calculate_ema(&closes, self.config.entry.ema_period)
            .ok_or_else(|| anyhow!("EMA unavailable for {}", symbol))?;
        let rsi14 = calculate_rsi(&closes, self.config.entry.rsi_period)
            .ok_or_else(|| anyhow!("RSI unavailable for {}", symbol))?;
        let atr14 = calculate_atr(&highs, &lows, &closes, self.config.filters.atr_period)
            .ok_or_else(|| anyhow!("ATR unavailable for {}", symbol))?;
        let adx14 = calculate_adx(&highs, &lows, &closes, self.config.filters.adx_period)
            .ok_or_else(|| anyhow!("ADX unavailable for {}", symbol))?;
        let last = klines
            .last()
            .ok_or_else(|| anyhow!("empty klines for {}", symbol))?;
        let current_price = last.close;
        let atr_ratio = if current_price.abs() > 1e-9 {
            atr14 / current_price.abs()
        } else {
            0.0
        };

        let lookback = self.config.filters.range_lookback.min(klines.len());
        let amplitude_20 = amplitude_pct(&klines[klines.len() - lookback..]);
        let latest_bar_change = latest_bar_change_pct(last);

        Ok(IndicatorSnapshot {
            current_price,
            ema20,
            rsi14,
            atr14,
            adx14,
            atr_ratio,
            amplitude_20,
            latest_bar_change,
            last_close_time: last.close_time,
        })
    }

    async fn update_symbol_market_snapshot(
        &self,
        symbol: &str,
        klines: &[Kline],
        indicators: &IndicatorSnapshot,
    ) -> Result<()> {
        let mut runtime = self.runtime.write().await;
        let state = runtime
            .symbols
            .get_mut(symbol)
            .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;
        state.last_klines = klines.to_vec();
        state.last_scan_at = Some(Utc::now());
        if let Some(long) = state.long.as_mut() {
            long.updated_at = Utc::now();
            if indicators.current_price > 0.0 {
                long.average_price = long.average_price.max(1e-9);
            }
        }
        if let Some(short) = state.short.as_mut() {
            short.updated_at = Utc::now();
            if indicators.current_price > 0.0 {
                short.average_price = short.average_price.max(1e-9);
            }
        }
        Ok(())
    }

    async fn apply_single_bar_pause(&self, symbol: &str, indicators: &IndicatorSnapshot) {
        if indicators.latest_bar_change <= self.config.filters.single_bar_pause_pct {
            return;
        }

        let interval = parse_interval(&self.config.data.decision_interval);
        let pause_until =
            indicators.last_close_time + Duration::seconds(interval_duration_secs(interval));
        let mut runtime = self.runtime.write().await;
        if let Some(state) = runtime.symbols.get_mut(symbol) {
            state.entry_pause_until = Some(pause_until);
            state.pause_reason = Some(format!(
                "single bar volatility {:.2}% exceeded {:.2}%",
                indicators.latest_bar_change * 100.0,
                self.config.filters.single_bar_pause_pct * 100.0
            ));
        }
        logging::warn(
            Some(symbol),
            format!(
                "单根K线波动过大，暂停开仓至 {}",
                pause_until.format("%Y-%m-%d %H:%M:%S UTC")
            ),
        );
    }

    async fn manage_open_positions(
        &self,
        symbol: &str,
        indicators: &IndicatorSnapshot,
    ) -> Result<()> {
        let current_price = indicators.current_price;
        let (long, short) = {
            let runtime = self.runtime.read().await;
            let state = runtime
                .symbols
                .get(symbol)
                .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;
            (state.long.clone(), state.short.clone())
        };

        if let Some(position) = long {
            let pnl_pct = position.unrealized_pnl_pct(current_price);
            if pnl_pct <= -self.config.risk.max_floating_loss_pct {
                self.close_position(
                    symbol,
                    OrderSide::Buy,
                    current_price,
                    "max floating loss stopout",
                    true,
                )
                .await?;
            } else if current_price
                >= position.take_profit_price(self.config.take_profit.target_pct)
            {
                self.close_position(
                    symbol,
                    OrderSide::Buy,
                    current_price,
                    "basket take profit",
                    false,
                )
                .await?;
            } else if position.should_add_layer(
                current_price,
                &self.config.martingale,
                self.config.martingale.layer_multipliers.len(),
            ) {
                let layer_index = position.next_layer_index();
                let multiplier = self
                    .config
                    .martingale
                    .layer_multipliers
                    .get(layer_index)
                    .copied()
                    .ok_or_else(|| anyhow!("layer multiplier missing"))?;
                self.open_or_add_position(
                    symbol,
                    OrderSide::Buy,
                    layer_index,
                    multiplier,
                    current_price,
                    "martingale add-on",
                )
                .await?;
            }
        }

        if let Some(position) = short {
            let pnl_pct = position.unrealized_pnl_pct(current_price);
            if pnl_pct <= -self.config.risk.max_floating_loss_pct {
                self.close_position(
                    symbol,
                    OrderSide::Sell,
                    current_price,
                    "max floating loss stopout",
                    true,
                )
                .await?;
            } else if current_price
                <= position.take_profit_price(self.config.take_profit.target_pct)
            {
                self.close_position(
                    symbol,
                    OrderSide::Sell,
                    current_price,
                    "basket take profit",
                    false,
                )
                .await?;
            } else if position.should_add_layer(
                current_price,
                &self.config.martingale,
                self.config.martingale.layer_multipliers.len(),
            ) {
                let layer_index = position.next_layer_index();
                let multiplier = self
                    .config
                    .martingale
                    .layer_multipliers
                    .get(layer_index)
                    .copied()
                    .ok_or_else(|| anyhow!("layer multiplier missing"))?;
                self.open_or_add_position(
                    symbol,
                    OrderSide::Sell,
                    layer_index,
                    multiplier,
                    current_price,
                    "martingale add-on",
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn maybe_open_positions(
        &self,
        symbol: &str,
        indicators: &IndicatorSnapshot,
    ) -> Result<()> {
        let now = Utc::now();
        let market_ok = self.passes_sideways_filter(indicators);
        let (global_paused, symbol_paused, has_long, has_short, can_open, allow_short) = {
            let runtime = self.runtime.read().await;
            let state = runtime
                .symbols
                .get(symbol)
                .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;
            (
                runtime
                    .trading_paused_until
                    .map(|until| until > now)
                    .unwrap_or(false),
                state
                    .entry_pause_until
                    .map(|until| until > now)
                    .unwrap_or(false),
                state.long.is_some(),
                state.short.is_some(),
                state.can_open_again(now, self.config.execution.entry_cooldown_secs),
                state
                    .config
                    .allow_short
                    .unwrap_or(self.config.account.allow_short)
                    && self.config.account.allow_short,
            )
        };

        if global_paused || symbol_paused || !can_open || !market_ok {
            return Ok(());
        }

        let current_price = indicators.current_price;

        if !has_long
            && current_price < indicators.ema20
            && indicators.rsi14 < self.config.entry.long_rsi_max
        {
            self.open_or_add_position(
                symbol,
                OrderSide::Buy,
                0,
                self.config.martingale.layer_multipliers[0],
                current_price,
                "initial long entry",
            )
            .await?;
        }

        if allow_short
            && !has_short
            && current_price > indicators.ema20
            && indicators.rsi14 > self.config.entry.short_rsi_min
        {
            self.open_or_add_position(
                symbol,
                OrderSide::Sell,
                0,
                self.config.martingale.layer_multipliers[0],
                current_price,
                "initial short entry",
            )
            .await?;
        }

        Ok(())
    }

    fn passes_sideways_filter(&self, indicators: &IndicatorSnapshot) -> bool {
        indicators.adx14 < self.config.filters.adx_threshold
            && indicators.atr_ratio < self.config.filters.atr_ratio_max
            && indicators.amplitude_20 < self.config.filters.range_amplitude_max
    }
}
