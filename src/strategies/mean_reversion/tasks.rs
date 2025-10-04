use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::time::{interval, sleep, Duration, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::data::{parse_kline_message, verify_sequence, IncomingKline};
use super::logging;
use super::model::{SymbolMeta, SymbolSnapshot, SymbolState};
use super::planner;
use super::utils::precision_from_step;
use super::MeanReversionStrategy;

impl MeanReversionStrategy {
    pub(super) fn exchange_symbol(&self, symbol: &str) -> String {
        symbol.to_string()
    }

    fn stream_symbol(&self, symbol: &str) -> String {
        let base = symbol.replace('/', "");
        match self.account.exchange_name.as_str() {
            "binance" | "bybit" => base.to_lowercase(),
            "okx" => symbol.replace('/', "-").to_lowercase(),
            "bitmart" => symbol.replace('/', "_").to_lowercase(),
            "hyperliquid" => symbol.split('/').next().unwrap_or(symbol).to_lowercase(),
            other => {
                logging::warn(
                    None,
                    format!("未识别的交易所 {}，使用默认stream符号格式", other),
                );
                base.to_lowercase()
            }
        }
    }

    async fn canonical_from_exchange_symbol(&self, symbol: &str) -> Option<String> {
        let aliases = self.symbol_aliases.read().await;
        aliases.get(&symbol.to_uppercase()).cloned()
    }
    pub(super) async fn spawn_tasks(&self) -> Result<()> {
        self.load_initial_data().await?;

        let ws_handle = self.spawn_websocket_loop().await?;
        let scan_handle = self.spawn_scan_loop();
        let order_handle = self.spawn_order_manager_loop();

        let mut handles = self.task_handles.lock().await;
        handles.push(ws_handle);
        handles.push(scan_handle);
        handles.push(order_handle);
        Ok(())
    }

    async fn load_initial_data(&self) -> Result<()> {
        let symbols: Vec<String> = {
            let states = self.symbol_states.read().await;
            states.keys().cloned().collect()
        };

        for symbol in symbols {
            if let Err(err) = self.load_symbol_history(&symbol).await {
                log::error!("加载{}历史数据失败: {}", symbol, err);
                if let Some(state) = self.symbol_states.write().await.get_mut(&symbol) {
                    state.freeze(30, format!("historical load failed: {}", err));
                }
            }
        }

        Ok(())
    }

    async fn load_symbol_history(&self, symbol: &str) -> Result<()> {
        let exchange_symbol = self.exchange_symbol(symbol);
        let one_limit = self.config.cache.max_1m_bars.max(600);
        let five_limit = self.config.cache.max_5m_bars.max(500);
        let fifteen_limit = self
            .config
            .cache
            .max_15m_bars
            .max(self.config.data.rest.min_klines_15m)
            .max(400);

        let exchange = self.account.exchange.clone();
        let mut state_guard = self.symbol_states.write().await;
        let state = state_guard
            .get_mut(symbol)
            .ok_or_else(|| anyhow!("symbol {} not configured", symbol))?;

        let one_bars = exchange
            .get_klines(
                &exchange_symbol,
                crate::core::types::Interval::OneMinute,
                self.market_type,
                Some(one_limit as u32),
            )
            .await?;
        for bar in one_bars {
            state.push_one_minute(bar, &self.config.cache);
        }

        let five_bars = exchange
            .get_klines(
                &exchange_symbol,
                crate::core::types::Interval::FiveMinutes,
                self.market_type,
                Some(five_limit as u32),
            )
            .await?;
        for bar in five_bars {
            state.push_five_minute(bar, &self.config.cache);
        }

        let fifteen_bars = exchange
            .get_klines(
                &exchange_symbol,
                crate::core::types::Interval::FifteenMinutes,
                self.market_type,
                Some(fifteen_limit as u32),
            )
            .await?;
        for bar in fifteen_bars {
            state.push_fifteen_minute(bar, &self.config.cache);
        }

        Self::rebuild_bbw_history(
            state,
            self.config.indicators.bollinger.period,
            self.config.indicators.bollinger.std_dev,
        );

        drop(state_guard);

        self.ensure_symbol_metadata(symbol).await?;

        Ok(())
    }

    pub(super) async fn ensure_symbol_metadata(&self, symbol: &str) -> Result<()> {
        let mut meta_guard = self.symbol_meta.write().await;
        if meta_guard.contains_key(symbol) {
            return Ok(());
        }

        let symbol_info = self
            .account
            .exchange
            .get_symbol_info(&self.exchange_symbol(symbol), self.market_type)
            .await?;

        let price_precision = precision_from_step(symbol_info.tick_size);
        let amount_precision = precision_from_step(symbol_info.step_size);

        meta_guard.insert(
            symbol.to_string(),
            SymbolMeta {
                symbol: symbol.to_string(),
                tick_size: symbol_info.tick_size,
                step_size: symbol_info.step_size,
                min_notional: symbol_info.min_notional,
                min_order_size: symbol_info.min_order_size,
                max_order_size: symbol_info.max_order_size,
                price_precision,
                amount_precision,
            },
        );

        logging::info(
            Some(symbol),
            format!(
                "更新精度: tick_size={} step_size={} price_precision={} amount_precision={} min_notional={:?}",
                symbol_info.tick_size,
                symbol_info.step_size,
                price_precision,
                amount_precision,
                symbol_info.min_notional
            ),
        );
        Ok(())
    }

    pub(super) fn rebuild_bbw_history(state: &mut SymbolState, period: usize, std_dev: f64) {
        state.bbw_history.clear();
        state.mid_history.clear();
        state.sigma_history.clear();
        if state.fifteen_minute.len() < period {
            return;
        }
        let closes: Vec<f64> = state.fifteen_minute.iter().map(|k| k.close).collect();
        for window in closes.windows(period) {
            let (upper, middle, lower) =
                match crate::utils::indicators::functions::bollinger_bands(window, period, std_dev)
                {
                    Some(res) => res,
                    None => continue,
                };
            let sigma = (upper - middle) / std_dev.max(1e-9);
            let bbw = if middle.abs() < 1e-9 {
                0.0
            } else {
                (upper - lower) / middle
            };
            state.update_bbw_series(bbw, middle, sigma);
        }
    }

    async fn enabled_symbols(&self) -> Vec<String> {
        let states = self.symbol_states.read().await;
        states.keys().cloned().collect()
    }

    async fn build_ws_url(&self) -> Result<String> {
        let symbols = self.enabled_symbols().await;
        if symbols.is_empty() {
            return Err(anyhow!("no enabled symbols"));
        }
        let streams = self.compose_streams(&symbols);
        if streams.is_empty() {
            return Err(anyhow!("no websocket streams requested"));
        }
        let base = if let Some(url) = self.config.data.websocket.url_override.clone() {
            url
        } else {
            self.account.exchange.get_websocket_url(self.market_type)
        };

        let joined = streams.join("/");
        let url = if base.contains("stream?") {
            if base.ends_with('=') || base.ends_with('/') {
                format!("{}{}", base.trim_end_matches('/'), joined)
            } else {
                format!("{}/{}", base.trim_end_matches('/'), joined)
            }
        } else if base.ends_with("/ws") {
            let prefix = base.trim_end_matches("/ws");
            format!("{}/stream?streams={}", prefix.trim_end_matches('/'), joined)
        } else {
            format!("{}/stream?streams={}", base.trim_end_matches('/'), joined)
        };
        Ok(url)
    }

    fn compose_streams(&self, symbols: &[String]) -> Vec<String> {
        let subs = &self.config.data.websocket.subscriptions;
        let mut streams = Vec::new();
        for symbol in symbols {
            let formatted = self.stream_symbol(symbol);
            if subs.kline_1m {
                streams.push(format!("{}@kline_1m", formatted));
            }
            if subs.kline_5m {
                streams.push(format!("{}@kline_5m", formatted));
            }
            if subs.kline_15m {
                streams.push(format!("{}@kline_15m", formatted));
            }
            if let Some(depth) = subs.partial_book_depth {
                streams.push(format!("{}@depth{}", formatted, depth));
            }
        }
        streams
    }

    pub(super) async fn is_running(&self) -> bool {
        *self.running.read().await
    }

    async fn spawn_websocket_loop(&self) -> Result<tokio::task::JoinHandle<()>> {
        let strategy = self.clone();
        let handle = tokio::spawn(async move {
            strategy.websocket_task().await;
        });
        Ok(handle)
    }

    async fn websocket_task(self) {
        let mut delay =
            Duration::from_millis(self.config.data.websocket.reconnect.initial_delay_ms);
        let max_delay = Duration::from_millis(self.config.data.websocket.reconnect.max_delay_ms);
        let multiplier = self.config.data.websocket.reconnect.multiplier;

        while self.is_running().await {
            let url = match self.build_ws_url().await {
                Ok(u) => u,
                Err(err) => {
                    log::error!("构建WebSocket URL失败: {}", err);
                    sleep(delay).await;
                    delay = (delay.mul_f64(multiplier)).min(max_delay);
                    continue;
                }
            };

            log::info!("连接行情WebSocket: {}", url);

            match connect_async(&url).await {
                Ok((mut ws_stream, _)) => {
                    log::info!("WebSocket连接已建立");
                    delay = Duration::from_millis(
                        self.config.data.websocket.reconnect.initial_delay_ms,
                    );

                    while self.is_running().await {
                        match ws_stream.next().await {
                            Some(Ok(Message::Text(text))) => match parse_kline_message(&text) {
                                Ok(Some(kline)) => {
                                    if let Err(err) = self.handle_incoming_kline(kline).await {
                                        log::warn!("处理K线失败: {}", err);
                                    }
                                }
                                Ok(None) => {}
                                Err(err) => {
                                    log::warn!("解析WebSocket消息失败: {}", err);
                                }
                            },
                            Some(Ok(Message::Binary(_))) => continue,
                            Some(Ok(Message::Frame(_))) => continue,
                            Some(Ok(Message::Ping(data))) => {
                                if let Err(err) = ws_stream.send(Message::Pong(data)).await {
                                    log::warn!("发送Pong失败: {}", err);
                                    break;
                                }
                            }
                            Some(Ok(Message::Pong(_))) => continue,
                            Some(Ok(Message::Close(frame))) => {
                                log::warn!("WebSocket关闭: {:?}", frame);
                                break;
                            }
                            Some(Err(err)) => {
                                log::error!("WebSocket错误: {}", err);
                                break;
                            }
                            None => {
                                log::warn!("WebSocket流结束");
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    log::error!("WebSocket连接失败: {}", err);
                }
            }

            if !self.is_running().await {
                break;
            }

            log::info!("等待 {:?} 后重连WebSocket", delay);
            sleep(delay).await;
            delay = (delay.mul_f64(multiplier)).min(max_delay);
        }
    }

    async fn handle_incoming_kline(&self, incoming: IncomingKline) -> Result<()> {
        let interval = incoming.interval.to_lowercase();
        let canonical_symbol = match self.canonical_from_exchange_symbol(&incoming.symbol).await {
            Some(symbol) => symbol,
            None => {
                log::debug!("[mean_reversion] 未识别行情符号: {}", incoming.symbol);
                return Ok(());
            }
        };

        let mut states = self.symbol_states.write().await;
        let Some(state) = states.get_mut(&canonical_symbol) else {
            return Ok(());
        };

        if !state.config.enabled {
            return Ok(());
        }

        let validation = &self.config.data.validation;
        let expected = match interval.as_str() {
            "1m" => Some(1),
            "5m" => Some(5),
            "15m" => Some(15),
            _ => None,
        };

        if let Some(exp) = expected {
            let last_close = match exp {
                1 => state.last_one_minute_close,
                5 => state.last_five_minute_close,
                15 => state.last_fifteen_minute_close,
                _ => None,
            };
            if validation.enable_sequence_check
                && !verify_sequence(last_close, incoming.kline.close_time, exp)
            {
                log::warn!(
                    "{} {} {} K线缺口: last={:?}, new={:?}",
                    self.config.strategy.name,
                    canonical_symbol,
                    interval,
                    last_close,
                    incoming.kline.close_time
                );
                if validation.enable_missing_bar_freeze {
                    state.freeze(30, format!("{} gap", interval));
                }
            }
        }

        match interval.as_str() {
            "1m" => state.push_one_minute(incoming.kline.clone(), &self.config.cache),
            "5m" => state.push_five_minute(incoming.kline.clone(), &self.config.cache),
            "15m" => state.push_fifteen_minute(incoming.kline.clone(), &self.config.cache),
            _ => state.push_one_minute(incoming.kline.clone(), &self.config.cache),
        }

        if interval == "15m" {
            if let Some((bbw, mid, sigma)) = planner::compute_latest_bbw(
                state,
                self.config.indicators.bollinger.period,
                self.config.indicators.bollinger.std_dev,
            ) {
                state.update_bbw_series(bbw, mid, sigma);
            }
        }

        Ok(())
    }

    fn spawn_scan_loop(&self) -> tokio::task::JoinHandle<()> {
        let strategy = self.clone();
        tokio::spawn(async move {
            strategy.scan_task().await;
        })
    }

    async fn scan_task(self) {
        let mut ticker = interval(Duration::from_secs(
            self.config.data.scan_interval_secs.max(30),
        ));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        if let Err(err) = self.scan_once().await {
            log::error!("首次市场扫描失败: {}", err);
        }
        while self.is_running().await {
            ticker.tick().await;
            if !self.is_running().await {
                break;
            }
            if let Err(err) = self.scan_once().await {
                log::error!("市场扫描失败: {}", err);
            }
        }
    }

    async fn scan_once(&self) -> Result<()> {
        let symbols = self.enabled_symbols().await;
        for symbol in symbols {
            if let Err(err) = self.evaluate_symbol(&symbol).await {
                log::warn!("评估{}失败: {}", symbol, err);
            }
        }
        Ok(())
    }

    pub(super) async fn snapshot_symbol(&self, symbol: &str) -> Option<SymbolSnapshot> {
        let states = self.symbol_states.read().await;
        states.get(symbol).map(|state| state.snapshot())
    }
}
