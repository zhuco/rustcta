use anyhow::{anyhow, Result};
use chrono::Utc;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::time::{sleep, Duration as TokioDuration};
use tokio_tungstenite::connect_async;

use super::logging;
use super::ShortLadderLiveStrategy;

#[derive(Debug, Deserialize)]
struct BookTickerMessage {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    best_bid: String,
    #[serde(rename = "a")]
    best_ask: String,
}

impl ShortLadderLiveStrategy {
    pub(super) async fn spawn_websocket_exit_task(&self) -> Result<()> {
        if !self.config.execution.websocket.enabled {
            logging::info(None, "short ladder WebSocket 风控未启用");
            return Ok(());
        }

        let worker = self.clone();
        let handle = tokio::spawn(async move {
            loop {
                if !*worker.running.read().await {
                    break;
                }
                if let Err(err) = worker.run_websocket_exit_loop().await {
                    logging::warn(None, format!("WebSocket 风控循环异常，将重连: {}", err));
                    sleep(TokioDuration::from_secs(
                        worker
                            .config
                            .execution
                            .websocket
                            .reconnect_delay_secs
                            .max(1),
                    ))
                    .await;
                }
            }
        });
        self.task_handles.lock().await.push(handle);
        Ok(())
    }

    async fn run_websocket_exit_loop(&self) -> Result<()> {
        let streams = self
            .enabled_symbols()
            .into_iter()
            .map(|symbol| format!("{}@bookTicker", to_binance_stream_symbol(&symbol)))
            .collect::<Vec<_>>()
            .join("/");
        if streams.is_empty() {
            return Ok(());
        }
        let url = format!("wss://fstream.binance.com/stream?streams={}", streams);
        logging::info(None, format!("short ladder WebSocket 风控连接: {}", url));
        let (ws_stream, _) = connect_async(&url)
            .await
            .map_err(|err| anyhow!("connect websocket failed: {}", err))?;
        logging::info(None, "short ladder WebSocket 风控已连接");

        let (_, mut read) = ws_stream.split();
        while let Some(message) = read.next().await {
            if !*self.running.read().await {
                break;
            }
            let message = message.map_err(|err| anyhow!("websocket read failed: {}", err))?;
            if !message.is_text() {
                continue;
            }
            let text = message.into_text()?;
            if let Some(ticker) = parse_book_ticker(&text) {
                let symbol = from_binance_stream_symbol(&ticker.symbol);
                let best_ask = ticker.best_ask.parse::<f64>().unwrap_or(0.0);
                let best_bid = ticker.best_bid.parse::<f64>().unwrap_or(0.0);
                if best_ask > 0.0 {
                    if let Err(err) = self
                        .process_websocket_exit_tick(&symbol, best_bid, best_ask)
                        .await
                    {
                        self.record_symbol_error(&symbol, err.to_string()).await;
                        logging::error(Some(&symbol), format!("WebSocket 风控处理失败: {}", err));
                    }
                }
            }
        }
        Err(anyhow!("websocket closed"))
    }

    async fn process_websocket_exit_tick(
        &self,
        symbol: &str,
        best_bid: f64,
        best_ask: f64,
    ) -> Result<()> {
        let now = Utc::now();
        let (short, has_pending_exit, should_check, should_update_trailing) = {
            let mut runtime = self.runtime.write().await;
            let Some(state) = runtime.symbols.get_mut(symbol) else {
                return Ok(());
            };
            let Some(short) = state.short.clone() else {
                return Ok(());
            };
            let has_pending_exit = state.pending_exit.is_some();
            let check_interval_ms = self
                .config
                .execution
                .websocket
                .exit_check_interval_ms
                .max(100);
            let should_check = state
                .last_ws_exit_check_at
                .map(|last| {
                    now.signed_duration_since(last).num_milliseconds() >= check_interval_ms as i64
                })
                .unwrap_or(true);
            if should_check {
                state.last_ws_exit_check_at = Some(now);
            }
            let trailing_interval = self
                .config
                .execution
                .websocket
                .trailing_update_interval_secs
                .max(1);
            let should_update_trailing = state
                .last_trailing_update_at
                .map(|last| {
                    now.signed_duration_since(last).num_seconds() >= trailing_interval as i64
                })
                .unwrap_or(true);
            if should_update_trailing {
                state.last_trailing_update_at = Some(now);
            }
            (
                short,
                has_pending_exit,
                should_check,
                should_update_trailing,
            )
        };

        self.fallback_stale_exit_to_market(symbol).await?;
        if has_pending_exit || !should_check {
            return Ok(());
        }

        let close_reference = best_ask;
        let stop_price =
            short.average_entry_price + self.config.ladder.stop_loss_atr * short.atr_at_entry;
        if close_reference >= stop_price {
            logging::info(
                Some(symbol),
                format!(
                    "WebSocket 止损触发: ask={:.6} stop={:.6} bid={:.6}",
                    best_ask, stop_price, best_bid
                ),
            );
            self.close_short(symbol, close_reference, "stop_loss")
                .await?;
            return Ok(());
        }

        if self.config.ladder.breakeven_stop && short.breakeven_armed {
            let breakeven_price = short.average_entry_price
                * (1.0 - self.config.ladder.breakeven_buffer_bps.max(0.0) / 10_000.0);
            if close_reference >= breakeven_price {
                self.close_short(symbol, close_reference, "breakeven_stop")
                    .await?;
                return Ok(());
            }
        }

        if matches!(
            self.config.ladder.take_profit_mode,
            super::config::LiveTakeProfitMode::AtrTrailing
        ) && should_update_trailing
        {
            if self
                .update_trailing_take_profit_state(symbol, close_reference)
                .await?
                .is_some()
            {
                logging::info(
                    Some(symbol),
                    format!(
                        "WebSocket 移动止盈触发: ask={:.6} bid={:.6}",
                        best_ask, best_bid
                    ),
                );
                self.close_short(symbol, close_reference, "trailing_take_profit")
                    .await?;
            }
        }

        Ok(())
    }
}

fn parse_book_ticker(text: &str) -> Option<BookTickerMessage> {
    let value = serde_json::from_str::<serde_json::Value>(text).ok()?;
    let data = value.get("data").unwrap_or(&value);
    serde_json::from_value(data.clone()).ok()
}

fn to_binance_stream_symbol(symbol: &str) -> String {
    symbol.replace('/', "").to_ascii_lowercase()
}

fn from_binance_stream_symbol(symbol: &str) -> String {
    let upper = symbol.to_ascii_uppercase();
    if let Some(base) = upper.strip_suffix("USDC") {
        format!("{}/USDC", base)
    } else if let Some(base) = upper.strip_suffix("USDT") {
        format!("{}/USDT", base)
    } else {
        upper
    }
}
