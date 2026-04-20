use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::{oneshot, Mutex};
use tokio::time;

use crate::core::error::ExchangeError;
use crate::core::types::{OrderSide, OrderStatus};

/// 订单填充事件
#[derive(Debug, Clone)]
pub struct FillEvent {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub symbol: String,
    pub side: OrderSide,
    pub status: OrderStatus,
    pub filled_qty: f64,
    pub last_filled_qty: f64,
    pub average_price: f64,
    pub last_filled_price: f64,
    pub reduce_only: bool,
    pub event_time: DateTime<Utc>,
}

/// 订单跟踪器，用于等待WebSocket回报
#[derive(Default)]
pub struct OrderTracker {
    client_waiters: Mutex<HashMap<String, Vec<oneshot::Sender<FillEvent>>>>,
    order_waiters: Mutex<HashMap<String, Vec<oneshot::Sender<FillEvent>>>>,
}

impl OrderTracker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            client_waiters: Mutex::new(HashMap::new()),
            order_waiters: Mutex::new(HashMap::new()),
        })
    }

    /// 监听指定 `client_order_id` 的成交事件
    pub async fn watch_client(&self, client_id: String) -> oneshot::Receiver<FillEvent> {
        let (tx, rx) = oneshot::channel();
        self.client_waiters
            .lock()
            .await
            .entry(client_id)
            .or_default()
            .push(tx);
        rx
    }

    /// 监听指定 `order_id`
    pub async fn watch_order(&self, order_id: String) -> oneshot::Receiver<FillEvent> {
        let (tx, rx) = oneshot::channel();
        self.order_waiters
            .lock()
            .await
            .entry(order_id)
            .or_default()
            .push(tx);
        rx
    }

    /// 基于client id等待成交并支持超时
    pub async fn wait_client_with_timeout(
        &self,
        client_id: String,
        timeout: Duration,
        operation: &str,
    ) -> Result<FillEvent, ExchangeError> {
        let receiver = self.watch_client(client_id.clone()).await;
        match time::timeout(timeout, receiver).await {
            Ok(Ok(event)) => Ok(event),
            Ok(Err(_)) => {
                self.client_waiters.lock().await.remove(&client_id);
                Err(ExchangeError::Other("订单跟踪通道被关闭".to_string()))
            }
            Err(_) => {
                self.client_waiters.lock().await.remove(&client_id);
                Err(ExchangeError::TimeoutError {
                    operation: operation.to_string(),
                    timeout_seconds: timeout.as_secs().max(1),
                })
            }
        }
    }

    /// 通知成交事件
    pub async fn notify_fill(&self, event: FillEvent) {
        if let Some(client_id) = &event.client_order_id {
            if let Some(waiters) = self.client_waiters.lock().await.remove(client_id) {
                for waiter in waiters {
                    let _ = waiter.send(event.clone());
                }
            }
        }

        if let Some(waiters) = self.order_waiters.lock().await.remove(&event.order_id) {
            for waiter in waiters {
                let _ = waiter.send(event.clone());
            }
        }
    }
}
