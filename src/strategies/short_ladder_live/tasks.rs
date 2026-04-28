use anyhow::{anyhow, Result};
use chrono::Utc;
use tokio::time::{sleep, Duration as TokioDuration};

use super::config::LiveTakeProfitMode;
use super::logging;
use super::market::{
    build_signal_snapshot, is_in_session, parse_interval, ShortLadderSignalSnapshot,
};
use super::model::{held_bars_since, should_add_short_layer};
use super::ShortLadderLiveStrategy;

impl ShortLadderLiveStrategy {
    pub(super) async fn bootstrap(&self) -> Result<()> {
        for symbol in self.enabled_symbols() {
            if let Err(err) = self.bootstrap_symbol(&symbol).await {
                self.record_symbol_error(&symbol, format!("bootstrap failed: {}", err))
                    .await;
            }
        }
        Ok(())
    }

    async fn bootstrap_symbol(&self, symbol: &str) -> Result<()> {
        self.ensure_symbol_setup(symbol).await?;
        let klines = self.fetch_signal_klines(symbol).await?;
        let snapshot = build_signal_snapshot(symbol, &klines, &self.config)?;
        {
            let mut runtime = self.runtime.write().await;
            if let Some(state) = runtime.symbols.get_mut(symbol) {
                state.last_klines = klines;
            }
        }
        self.sync_short_position(symbol, snapshot.atr_5m, snapshot.current_price)
            .await?;
        logging::info(Some(symbol), "启动接管检查完成");
        Ok(())
    }

    pub(super) async fn spawn_tasks(&self) -> Result<()> {
        let worker = self.clone();
        let handle = tokio::spawn(async move {
            loop {
                if !*worker.running.read().await {
                    break;
                }

                if let Err(err) = worker.run_once().await {
                    let message = format!("short ladder live loop failed: {}", err);
                    worker.mark_error(message.clone()).await;
                    logging::error(None, message);
                }

                sleep(TokioDuration::from_secs(
                    worker.config.data.poll_interval_secs.max(5),
                ))
                .await;
            }
        });

        self.task_handles.lock().await.push(handle);
        Ok(())
    }

    pub(super) async fn run_once(&self) -> Result<()> {
        for symbol in self.enabled_symbols() {
            if let Err(err) = self.process_symbol(&symbol).await {
                self.record_symbol_error(&symbol, err.to_string()).await;
                logging::error(Some(&symbol), format!("处理失败: {}", err));
            }
        }
        self.update_status().await;
        Ok(())
    }

    async fn process_symbol(&self, symbol: &str) -> Result<()> {
        self.ensure_symbol_setup(symbol).await?;
        let klines = self.fetch_signal_klines(symbol).await?;
        let snapshot = build_signal_snapshot(symbol, &klines, &self.config)?;
        {
            let mut runtime = self.runtime.write().await;
            if let Some(state) = runtime.symbols.get_mut(symbol) {
                state.last_klines = klines;
                state.last_error = None;
            }
        }

        self.sync_short_position(symbol, snapshot.atr_5m, snapshot.current_price)
            .await?;

        self.fallback_stale_initial_entry_to_market(symbol).await?;

        if self.has_open_orders(symbol).await? {
            logging::info(Some(symbol), "存在未成交挂单，本轮不新增订单");
            return Ok(());
        }

        if self.manage_existing_short(symbol, &snapshot).await? {
            return Ok(());
        }

        self.maybe_open_initial_short(symbol, &snapshot).await
    }

    async fn manage_existing_short(
        &self,
        symbol: &str,
        snapshot: &ShortLadderSignalSnapshot,
    ) -> Result<bool> {
        let short = {
            let runtime = self.runtime.read().await;
            runtime
                .symbols
                .get(symbol)
                .and_then(|state| state.short.clone())
        };
        let Some(short) = short else {
            return Ok(false);
        };

        if let Some(reason) = self.exit_reason(symbol, &short, snapshot).await? {
            self.close_short(symbol, snapshot.current_price, &reason)
                .await?;
            return Ok(true);
        }

        if !is_in_session(snapshot.close_time, &self.config)
            || !self.can_submit_order(symbol).await?
        {
            return Ok(true);
        }

        self.arm_breakeven_if_needed(symbol, &short, snapshot)
            .await?;

        let allow_final_layer =
            !self.config.signal.final_layer_requires_strong_1h || snapshot.strong_1h_short_regime;
        if let Some((layer_index, trigger_price)) = should_add_short_layer(
            &short,
            snapshot.current_price,
            self.config.ladder.layer_weights.len(),
            self.config.ladder.layer_spacing_atr,
            allow_final_layer,
        ) {
            if short.is_losing(snapshot.current_price) {
                self.add_short_layer(symbol, layer_index, trigger_price)
                    .await?;
            }
        }

        Ok(true)
    }

    async fn maybe_open_initial_short(
        &self,
        symbol: &str,
        snapshot: &ShortLadderSignalSnapshot,
    ) -> Result<()> {
        if !is_in_session(snapshot.close_time, &self.config) {
            return Ok(());
        }
        if !snapshot.short_regime || !snapshot.short_entry_trigger {
            return Ok(());
        }
        if !self.can_submit_order(symbol).await? {
            return Ok(());
        }

        self.open_initial_short(symbol, snapshot).await
    }

    async fn exit_reason(
        &self,
        symbol: &str,
        short: &super::model::LiveShortPosition,
        snapshot: &ShortLadderSignalSnapshot,
    ) -> Result<Option<String>> {
        let stop_price =
            short.average_entry_price + self.config.ladder.stop_loss_atr * short.atr_at_entry;
        if snapshot.current_price >= stop_price {
            return Ok(Some("stop_loss".to_string()));
        }

        if self.config.ladder.breakeven_stop {
            let breakeven_price = short.average_entry_price
                * (1.0 - self.config.ladder.breakeven_buffer_bps.max(0.0) / 10_000.0);
            if short.breakeven_armed && snapshot.current_price >= breakeven_price {
                return Ok(Some("breakeven_stop".to_string()));
            }
        }

        match self.config.ladder.take_profit_mode {
            LiveTakeProfitMode::FixedAtr => {
                let take_profit_price = short.average_entry_price
                    - self.config.ladder.take_profit_atr * short.atr_at_entry;
                if snapshot.current_price <= take_profit_price {
                    return Ok(Some("take_profit".to_string()));
                }
            }
            LiveTakeProfitMode::AtrTrailing => {
                if self
                    .update_trailing_take_profit_state(symbol, snapshot.current_price)
                    .await?
                    .is_some()
                {
                    return Ok(Some("trailing_take_profit".to_string()));
                }
            }
        }

        let held_bars = held_bars_since(
            short.opened_at,
            snapshot.close_time,
            self.decision_interval_secs(),
        );
        if held_bars >= self.config.ladder.max_hold_bars {
            return Ok(Some("time_stop".to_string()));
        }

        if !is_in_session(snapshot.close_time, &self.config) {
            return Ok(Some("session_end".to_string()));
        }

        Ok(None)
    }

    async fn update_trailing_take_profit_state(
        &self,
        symbol: &str,
        current_price: f64,
    ) -> Result<Option<f64>> {
        let mut runtime = self.runtime.write().await;
        let Some(position) = runtime
            .symbols
            .get_mut(symbol)
            .and_then(|state| state.short.as_mut())
        else {
            return Ok(None);
        };

        let result = position.update_short_trailing_take_profit(
            current_price,
            self.config.ladder.trailing_take_profit_activation_atr,
            self.config.ladder.trailing_take_profit_distance_atr,
        );
        if result.is_some() {
            logging::info(Some(symbol), "动态 ATR 跟踪止盈触发");
        }
        Ok(result)
    }

    async fn arm_breakeven_if_needed(
        &self,
        symbol: &str,
        short: &super::model::LiveShortPosition,
        snapshot: &ShortLadderSignalSnapshot,
    ) -> Result<()> {
        if !self.config.ladder.breakeven_stop || short.breakeven_armed {
            return Ok(());
        }
        let trigger_price = short.average_entry_price
            - self.config.ladder.breakeven_trigger_atr * short.atr_at_entry;
        if snapshot.current_price > trigger_price {
            return Ok(());
        }

        let mut runtime = self.runtime.write().await;
        if let Some(state) = runtime.symbols.get_mut(symbol) {
            if let Some(position) = state.short.as_mut() {
                position.breakeven_armed = true;
                logging::info(Some(symbol), "???????");
            }
        }
        Ok(())
    }

    async fn can_submit_order(&self, symbol: &str) -> Result<bool> {
        let last_order_at = {
            let runtime = self.runtime.read().await;
            runtime
                .symbols
                .get(symbol)
                .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?
                .last_order_at
        };

        let Some(last_order_at) = last_order_at else {
            return Ok(true);
        };
        let elapsed = Utc::now()
            .signed_duration_since(last_order_at)
            .num_seconds()
            .max(0) as u64;
        Ok(elapsed >= self.config.execution.order_cooldown_secs)
    }

    async fn fetch_signal_klines(&self, symbol: &str) -> Result<Vec<crate::core::types::Kline>> {
        let interval = parse_interval(&self.config.data.decision_interval);
        self.exchange
            .get_klines(
                symbol,
                interval,
                self.market_type,
                Some(self.config.data.kline_limit),
            )
            .await
            .map_err(|err| anyhow!("get_klines failed for {}: {}", symbol, err))
    }

    fn enabled_symbols(&self) -> Vec<String> {
        self.config
            .symbols
            .iter()
            .filter(|cfg| cfg.enabled)
            .map(|cfg| cfg.symbol.clone())
            .collect()
    }

    fn decision_interval_secs(&self) -> u64 {
        match self.config.data.decision_interval.as_str() {
            "1m" => 60,
            "3m" => 180,
            "5m" => 300,
            "15m" => 900,
            "30m" => 1_800,
            "1h" => 3_600,
            _ => 300,
        }
    }

    async fn record_symbol_error(&self, symbol: &str, message: String) {
        let mut runtime = self.runtime.write().await;
        runtime.last_error = Some(format!("{}: {}", symbol, message));
        if let Some(state) = runtime.symbols.get_mut(symbol) {
            state.last_error = Some(message);
        }
    }
}
