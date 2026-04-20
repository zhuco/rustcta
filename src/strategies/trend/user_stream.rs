use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use log::{debug, error, info, warn};
use tokio::sync::Mutex;

use crate::core::error::ExchangeError;
use crate::core::types::{MarketType, OrderSide, OrderStatus};
use crate::core::websocket::{BaseWebSocketClient, WebSocketClient};
use crate::cta::account_manager::AccountInfo;
use crate::exchanges::binance::BinanceExchange;
use crate::strategies::trend::order_tracker::{FillEvent, OrderTracker};
use crate::strategies::trend::risk_control::RiskController;

#[derive(Clone)]
pub struct TrendUserStream {
    account: Arc<AccountInfo>,
    order_tracker: Arc<OrderTracker>,
    risk_controller: Arc<RiskController>,
    running: Arc<AtomicBool>,
    handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl TrendUserStream {
    pub fn new(
        account: Arc<AccountInfo>,
        order_tracker: Arc<OrderTracker>,
        risk_controller: Arc<RiskController>,
    ) -> Self {
        Self {
            account,
            order_tracker,
            risk_controller,
            running: Arc::new(AtomicBool::new(false)),
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let listen_key = self.acquire_listen_key().await?;
        let ws_url = format!("wss://fstream.binance.com/ws/{}", listen_key);
        info!(
            "TrendUserStream: 启动监听，listen_key={}...",
            &listen_key.chars().take(8).collect::<String>()
        );
        let service = self.clone();
        let handle = tokio::spawn(async move {
            loop {
                if !service.running.load(Ordering::SeqCst) {
                    break;
                }

                if let Err(err) = service.run_stream(&ws_url).await {
                    error!("TrendUserStream: WebSocket错误: {}", err);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });

        self.handles.lock().await.push(handle);
        Ok(())
    }

    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        let mut handles = self.handles.lock().await;
        for handle in handles.drain(..) {
            handle.abort();
        }
    }

    async fn run_stream(&self, ws_url: &str) -> Result<()> {
        let mut client = BaseWebSocketClient::new(ws_url.to_string(), "binance".to_string());
        client.connect().await?;
        info!("TrendUserStream: 已连接 {}", ws_url);

        while self.running.load(Ordering::SeqCst) {
            match client.receive().await {
                Ok(Some(message)) => {
                    if let Err(err) = self.handle_message(&message).await {
                        warn!("TrendUserStream: 处理消息失败: {}", err);
                    }
                }
                Ok(None) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(err) => {
                    warn!("TrendUserStream: 接收失败: {}", err);
                    break;
                }
            }
        }

        Ok(())
    }

    async fn acquire_listen_key(&self) -> Result<String> {
        if let Some(binance) = self
            .account
            .exchange
            .as_ref()
            .as_any()
            .downcast_ref::<BinanceExchange>()
        {
            match binance
                .create_listen_key_with_auto_renewal(MarketType::Futures)
                .await
            {
                Ok(key) => {
                    info!("TrendUserStream: 使用自动续期ListenKey");
                    return Ok(key);
                }
                Err(err) => {
                    warn!("TrendUserStream: 自动续期失败，降级: {}", err);
                }
            }
        }

        let key = self
            .account
            .exchange
            .create_user_data_stream(MarketType::Futures)
            .await?;
        info!("TrendUserStream: 使用手动ListenKey");
        Ok(key)
    }

    async fn handle_message(&self, payload: &str) -> Result<()> {
        let event: serde_json::Value = serde_json::from_str(payload)?;
        if let Some(event_type) = event.get("e").and_then(|v| v.as_str()) {
            match event_type {
                "ORDER_TRADE_UPDATE" => {
                    debug!(
                        "TrendUserStream: 收到 ORDER_TRADE_UPDATE: {}",
                        event
                            .get("E")
                            .and_then(|v| v.as_i64())
                            .map(|t| t.to_string())
                            .unwrap_or_else(|| "unknown_ts".to_string())
                    );
                    self.handle_order_update(&event).await?
                }
                "ACCOUNT_UPDATE" => {
                    debug!("TrendUserStream: 收到 ACCOUNT_UPDATE");
                    self.handle_account_update(&event).await?
                }
                other => {
                    debug!("TrendUserStream: 收到忽略事件 {}", other);
                }
            }
        }
        Ok(())
    }

    async fn handle_order_update(&self, event: &serde_json::Value) -> Result<()> {
        let order_object = event
            .get("o")
            .ok_or_else(|| ExchangeError::Other("缺少订单字段".to_string()))?;

        let client_order_id = order_object
            .get("c")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let order_id = order_object
            .get("i")
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let symbol = order_object
            .get("s")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let side = match order_object.get("S").and_then(|v| v.as_str()) {
            Some("BUY") => OrderSide::Buy,
            Some("SELL") => OrderSide::Sell,
            _ => OrderSide::Buy,
        };

        let status = match order_object.get("X").and_then(|v| v.as_str()) {
            Some("NEW") => OrderStatus::Open,
            Some("PARTIALLY_FILLED") => OrderStatus::PartiallyFilled,
            Some("FILLED") => OrderStatus::Closed,
            Some("CANCELED") => OrderStatus::Canceled,
            Some("REJECTED") => OrderStatus::Rejected,
            _ => OrderStatus::Open,
        };

        let filled_qty = order_object
            .get("z")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let last_filled_qty = order_object
            .get("l")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let avg_price = order_object
            .get("ap")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| {
                order_object
                    .get("p")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
            })
            .unwrap_or(0.0);
        let last_price = order_object
            .get("L")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(avg_price);

        let reduce_only = order_object
            .get("R")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let event_time = order_object
            .get("T")
            .and_then(|v| v.as_i64())
            .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
            .unwrap_or_else(Utc::now);

        let fill_event = FillEvent {
            order_id,
            client_order_id,
            symbol,
            side,
            status,
            filled_qty,
            last_filled_qty,
            average_price: avg_price,
            last_filled_price: last_price,
            reduce_only,
            event_time,
        };

        let should_notify = matches!(
            fill_event.status,
            OrderStatus::PartiallyFilled | OrderStatus::Closed
        ) || fill_event.last_filled_qty > 0.0
            || fill_event.filled_qty > 0.0;

        if should_notify {
            info!(
                "TrendUserStream: 成交事件 symbol={} status={:?} filled={:.6} last={:.6} reduce_only={}",
                fill_event.symbol,
                fill_event.status,
                fill_event.filled_qty,
                fill_event.last_filled_qty,
                fill_event.reduce_only
            );
            self.order_tracker.notify_fill(fill_event).await;
        } else {
            debug!(
                "TrendUserStream: 忽略订单事件 symbol={} status={:?} filled={:.6}",
                fill_event.symbol, fill_event.status, fill_event.filled_qty
            );
        }
        Ok(())
    }

    async fn handle_account_update(&self, event: &serde_json::Value) -> Result<()> {
        let account = event
            .get("a")
            .ok_or_else(|| ExchangeError::Other("缺少账户字段".to_string()))?;

        if let Some(balances) = account.get("B").and_then(|v| v.as_array()) {
            let mut total = 0.0;
            let mut available = 0.0;
            for balance in balances {
                let wallet = balance
                    .get("wb")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let cross = balance
                    .get("cw")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                total += wallet;
                available += cross;
            }

            self.risk_controller
                .update_account_metrics(total, available, total - available, 0.0)
                .await;
        }

        Ok(())
    }
}
