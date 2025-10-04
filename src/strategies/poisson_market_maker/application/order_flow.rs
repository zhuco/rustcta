use super::{PoissonMarketMaker, Result};
use crate::analysis::TradeData;
use crate::core::{
    error::ExchangeError,
    types::*,
    websocket::{BaseWebSocketClient, ConnectionState, WebSocketClient},
};
use crate::strategies::poisson_market_maker::domain::{
    OrderEventType, OrderFlowEvent, PoissonParameters,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

impl PoissonMarketMaker {
    fn parse_orderbook_from_value(&self, data: &Value) -> WsMessage {
        let mut bids = Vec::new();
        let mut asks = Vec::new();

        if let Some(bid_array) = data.get("b").and_then(|b| b.as_array()) {
            for bid in bid_array.iter().take(20) {
                if let Some(arr) = bid.as_array() {
                    if arr.len() >= 2 {
                        let price = arr[0]
                            .as_str()
                            .and_then(|p| p.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let qty = arr[1]
                            .as_str()
                            .and_then(|q| q.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        bids.push([price, qty]);
                    }
                }
            }
        }

        if let Some(ask_array) = data.get("a").and_then(|a| a.as_array()) {
            for ask in ask_array.iter().take(20) {
                if let Some(arr) = ask.as_array() {
                    if arr.len() >= 2 {
                        let price = arr[0]
                            .as_str()
                            .and_then(|p| p.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let qty = arr[1]
                            .as_str()
                            .and_then(|q| q.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        asks.push([price, qty]);
                    }
                }
            }
        }

        let timestamp = data
            .get("E")
            .or_else(|| data.get("T"))
            .and_then(|v| v.as_i64())
            .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
            .unwrap_or_else(Utc::now);

        WsMessage::OrderBook(OrderBook {
            symbol: self.config.trading.symbol.clone(),
            bids,
            asks,
            timestamp,
        })
    }

    /// 连接WebSocket
    pub(crate) async fn connect_websocket(&self) -> Result<()> {
        log::info!("📡 连接WebSocket获取实时数据...");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| {
                ExchangeError::Other(format!("账户不存在: {}", self.config.account.account_id))
            })?;

        // 根据交易所创建WebSocket URL
        let ws_url = match self.config.account.exchange.as_str() {
            "binance" => "wss://fstream.binance.com/ws".to_string(),
            _ => {
                return Err(ExchangeError::Other(format!(
                    "不支持的交易所: {}",
                    self.config.account.exchange
                )));
            }
        };

        // 创建WebSocket客户端
        let mut ws_client = BaseWebSocketClient::new(ws_url, self.config.account.exchange.clone());

        ws_client.connect().await?;

        // 发送订阅请求
        self.subscribe_market_streams(&mut ws_client).await?;

        let ws_client = Arc::new(Mutex::new(ws_client));

        *self.ws_client.write().await = Some(ws_client);

        // 启动心跳任务
        let heartbeat_runner = self.clone();
        let heartbeat_handle = tokio::spawn(async move {
            heartbeat_runner.run_market_data_heartbeat().await;
        });
        self.register_handle(heartbeat_handle).await;

        log::info!("✅ WebSocket连接成功");
        Ok(())
    }

    fn build_market_streams(&self) -> Vec<String> {
        let raw_symbol = self.config.trading.symbol.trim();
        let exchange = self.config.account.exchange.to_lowercase();

        let stream_symbol = if exchange == "binance" {
            let parts: Vec<&str> = raw_symbol.split('/').collect();
            let base = parts.get(0).cloned().unwrap_or("NEAR").to_lowercase();
            let mut quote = parts.get(1).cloned().unwrap_or("USDT").to_lowercase();

            // Binance USD-M 合约主流使用 USDT 结算，即便配置写成 USDC/BUSD 也转换成 USDT
            if quote == "usdc" || quote == "busd" {
                quote = "usdt".to_string();
            }

            format!("{}{}", base, quote)
        } else {
            raw_symbol.to_lowercase().replace('/', "")
        };

        vec![
            format!("{}@depth20@100ms", stream_symbol),
            format!("{}@trade", stream_symbol),
        ]
    }

    async fn subscribe_market_streams(&self, client: &mut BaseWebSocketClient) -> Result<()> {
        if self.config.account.exchange == "binance" {
            let payload = json!({
                "method": "SUBSCRIBE",
                "params": self.build_market_streams(),
                "id": 1,
            });
            client.send(payload.to_string()).await?;
        }
        Ok(())
    }

    async fn run_market_data_heartbeat(self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(25));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        while *self.running.read().await {
            interval.tick().await;

            let ws_option = { self.ws_client.read().await.clone() };
            let Some(ws) = ws_option else {
                continue;
            };

            let mut guard = ws.lock().await;
            if let Err(err) = guard.ping().await {
                log::debug!(
                    "⚠️ 市场数据心跳失败: {} (state={:?})",
                    err,
                    guard.get_state()
                );
            }
        }

        log::debug!("🔚 市场数据心跳任务退出（running=false）");
    }

    /// 收集订单流数据
    pub(crate) async fn collect_order_flow(&self) -> Result<()> {
        log::info!("📊 开始收集订单流数据...");

        let mut message_count = 0;
        let mut last_log_time = Utc::now();

        let mut reconnect_failures = 0usize;

        while *self.running.read().await {
            let ws_option = { self.ws_client.read().await.clone() };

            if let Some(ws) = ws_option {
                let mut ws_guard = ws.lock().await;
                match ws_guard.receive().await {
                    Ok(Some(message)) => {
                        message_count += 1;
                        reconnect_failures = 0;

                        if message_count <= 5 {
                            let preview = if message.len() > 400 {
                                format!("{}...", &message[..400])
                            } else {
                                message.clone()
                            };
                            log::info!("📥 首批WebSocket消息[{}]: {}", message_count, preview);
                        }

                        // 每10秒或每100条消息打印一次统计
                        let now = Utc::now();
                        if message_count % 100 == 0
                            || now.signed_duration_since(last_log_time).num_seconds() > 10
                        {
                            log::debug!("📊 已接收 {} 条WebSocket消息", message_count);
                            last_log_time = now;
                        }

                        // 将字符串消息解析为WsMessage
                        match self.parse_websocket_message(&message).await {
                            Ok(ws_msg) => {
                                if let Err(e) = self.process_ws_message(ws_msg).await {
                                    log::error!("处理WebSocket消息失败: {}", e);
                                }
                            }
                            Err(e) => {
                                log::debug!(
                                    "解析WebSocket消息失败: {}, 消息前50字符: {:?}",
                                    e,
                                    &message.chars().take(50).collect::<String>()
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        if matches!(ws_guard.get_state(), ConnectionState::Connected) {
                            // 非文本消息（如Ping/Pong）或空消息，连接仍然存活
                            continue;
                        }

                        log::warn!("📴 WebSocket连接已关闭，准备重连...");
                        if let Err(err) = ws_guard.disconnect().await {
                            log::debug!("断开WebSocket时出错: {}", err);
                        }
                        drop(ws_guard);

                        reconnect_failures += 1;
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

                        let mut reconnect_guard = ws.lock().await;
                        match reconnect_guard.connect().await {
                            Ok(_) => {
                                log::info!("🔄 WebSocket重连成功 (关闭后)");
                                if let Err(sub_err) =
                                    self.subscribe_market_streams(&mut reconnect_guard).await
                                {
                                    log::error!("❌ 重连后订阅市场流失败: {}", sub_err);
                                }
                                reconnect_failures = 0;
                            }
                            Err(err) => {
                                log::error!("WebSocket重连失败: {}", err);
                                if reconnect_failures % 5 == 0 {
                                    log::warn!(
                                        "WebSocket已连续{}次重连失败，等待更长时间再试",
                                        reconnect_failures
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("WebSocket接收错误: {}", e);
                        if let Err(err) = ws_guard.disconnect().await {
                            log::debug!("断开异常WebSocket失败: {}", err);
                        }
                        drop(ws_guard);

                        reconnect_failures += 1;
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

                        let mut reconnect_guard = ws.lock().await;
                        match reconnect_guard.connect().await {
                            Ok(_) => {
                                log::info!("🔄 WebSocket重连成功 (错误后)");
                                if let Err(sub_err) =
                                    self.subscribe_market_streams(&mut reconnect_guard).await
                                {
                                    log::error!("❌ 重连后订阅市场流失败: {}", sub_err);
                                }
                                reconnect_failures = 0;
                            }
                            Err(err) => {
                                log::error!("WebSocket重连失败: {}", err);
                                if reconnect_failures % 5 == 0 {
                                    log::warn!(
                                        "WebSocket已连续{}次重连失败，继续等待重试",
                                        reconnect_failures
                                    );
                                }
                            }
                        }
                    }
                }
            } else {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }

        log::warn!(
            "🛑 订单流采集任务结束，running={}",
            *self.running.read().await
        );
        Ok(())
    }

    /// 解析WebSocket消息
    pub(crate) async fn parse_websocket_message(&self, message: &str) -> Result<WsMessage> {
        // 解析Binance WebSocket消息
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(message) {
            // 处理Binance流格式（有stream字段）
            let data = if json.get("stream").is_some() && json.get("data").is_some() {
                // 这是流格式消息，获取内部的data
                &json["data"]
            } else {
                // 直接消息格式
                &json
            };

            // 首先检查是否是用户数据流事件（ORDER_TRADE_UPDATE）
            if let Some(event_type) = data.get("e").and_then(|e| e.as_str()) {
                if event_type == "ORDER_TRADE_UPDATE" {
                    // 处理订单更新事件
                    let order_data = &data["o"];
                    let order_status = order_data["X"].as_str().unwrap_or("");

                    if order_status == "FILLED" || order_status == "PARTIALLY_FILLED" {
                        log::info!(
                            "📡 检测到ORDER_TRADE_UPDATE订单更新事件: 状态={}",
                            order_status
                        );

                        // 解析成交信息
                        let symbol = order_data["s"].as_str().unwrap_or("");
                        let order_id = order_data["c"].as_str().unwrap_or("");
                        let side = order_data["S"].as_str().unwrap_or("");
                        let price = order_data["ap"]
                            .as_str()
                            .and_then(|p| p.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let executed_qty = order_data["z"]
                            .as_str()
                            .and_then(|q| q.parse::<f64>().ok())
                            .unwrap_or(0.0);

                        // 记录成交到数据库
                        if let Some(ref collector) = self.collector {
                            // 获取真实的交易时间
                            let trade_time = order_data["T"]
                                .as_i64()
                                .map(|ts| {
                                    DateTime::<Utc>::from_timestamp_millis(ts)
                                        .unwrap_or_else(|| Utc::now())
                                })
                                .unwrap_or_else(|| Utc::now());

                            let trade_data = TradeData {
                                trade_time,
                                strategy_name: self.config.name.clone(),
                                account_id: self.config.account.account_id.clone(),
                                exchange: self.config.account.exchange.clone(),
                                symbol: self.config.trading.symbol.clone(),
                                side: side.to_string(),
                                order_type: Some("Limit".to_string()),
                                price: Decimal::from_str(&price.to_string()).unwrap_or_default(),
                                amount: Decimal::from_str(&executed_qty.to_string())
                                    .unwrap_or_default(),
                                value: Some(
                                    Decimal::from_str(&(price * executed_qty).to_string())
                                        .unwrap_or_default(),
                                ),
                                fee: None,
                                fee_currency: Some("USDT".to_string()),
                                realized_pnl: None,
                                pnl_percentage: None,
                                order_id: order_id.to_string(),
                                parent_order_id: None,
                                position_side: None,
                                metadata: None,
                            };

                            if let Err(e) = collector.record_trade(trade_data).await {
                                log::error!("记录交易失败: {}", e);
                            } else {
                                log::info!(
                                    "✅ 通过WebSocket记录成交: {} {} @ {} x {}",
                                    symbol,
                                    side,
                                    price,
                                    executed_qty
                                );
                            }
                        }

                        // 返回Trade消息供策略处理
                        return Ok(WsMessage::Trade(Trade {
                            id: order_data["t"].to_string(),
                            symbol: self.config.trading.symbol.clone(),
                            price,
                            amount: executed_qty,
                            side: if side == "BUY" {
                                OrderSide::Buy
                            } else {
                                OrderSide::Sell
                            },
                            timestamp: Utc::now(),
                            fee: None,
                            order_id: Some(order_id.to_string()),
                        }));
                    }
                }
            }

            // 检查是否包含stream字段（Binance格式）
            if let Some(stream) = json.get("stream").and_then(|s| s.as_str()) {
                if let Some(data) = json.get("data") {
                    if stream.contains("trade") {
                        // 解析成交数据
                        let price = data["p"]
                            .as_str()
                            .and_then(|p| p.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let amount = data["q"]
                            .as_str()
                            .and_then(|q| q.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let is_buyer_maker = data["m"].as_bool().unwrap_or(false);

                        return Ok(WsMessage::Trade(Trade {
                            id: data["t"].to_string(),
                            symbol: self.config.trading.symbol.clone(),
                            price,
                            amount,
                            side: if is_buyer_maker {
                                OrderSide::Buy
                            } else {
                                OrderSide::Sell
                            },
                            timestamp: Utc::now(),
                            fee: None,
                            order_id: None,
                        }));
                    } else if stream.contains("depth") {
                        return Ok(self.parse_orderbook_from_value(data));
                    }
                }
            }

            // 处理直接的事件消息（不带stream字段）
            if json.get("stream").is_none() {
                if let Some(event_type) = data.get("e").and_then(|e| e.as_str()) {
                    match event_type {
                        "trade" | "aggTrade" => {
                            let price = data["p"]
                                .as_str()
                                .and_then(|p| p.parse::<f64>().ok())
                                .unwrap_or(0.0);
                            let amount = data["q"]
                                .as_str()
                                .and_then(|q| q.parse::<f64>().ok())
                                .unwrap_or(0.0);
                            let is_buyer_maker = data["m"].as_bool().unwrap_or(false);
                            let trade_time = data
                                .get("T")
                                .and_then(|v| v.as_i64())
                                .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
                                .unwrap_or_else(Utc::now);

                            return Ok(WsMessage::Trade(Trade {
                                id: data["t"].to_string(),
                                symbol: self.config.trading.symbol.clone(),
                                price,
                                amount,
                                side: if is_buyer_maker {
                                    OrderSide::Buy
                                } else {
                                    OrderSide::Sell
                                },
                                timestamp: trade_time,
                                fee: None,
                                order_id: None,
                            }));
                        }
                        "depthUpdate" => {
                            return Ok(self.parse_orderbook_from_value(data));
                        }
                        _ => {}
                    }
                } else if data.get("b").is_some() && data.get("a").is_some() {
                    return Ok(self.parse_orderbook_from_value(data));
                }
            }
        }

        Ok(WsMessage::Text(message.to_string()))
    }

    /// 处理WebSocket消息
    pub(crate) async fn process_ws_message(&self, message: WsMessage) -> Result<()> {
        match message {
            WsMessage::Trade(trade) => {
                // 记录成交事件
                let event = OrderFlowEvent {
                    timestamp: Utc::now(),
                    side: trade.side.clone(),
                    price: trade.price,
                    quantity: trade.amount,
                    event_type: OrderEventType::Trade,
                };

                // 更新当前价格
                let mut price_guard = self.current_price.write().await;
                if *price_guard <= 0.0 {
                    log::info!(
                        "💡 首次成交价就绪: side={:?}, price={:.5}, qty={:.3}",
                        trade.side,
                        trade.price,
                        trade.amount
                    );
                }
                *price_guard = trade.price;

                // 添加到缓冲区
                let mut buffer = self.order_flow_buffer.write().await;
                buffer.push_back(event);

                // 限制缓冲区大小
                while buffer.len() > 10000 {
                    buffer.pop_front();
                }
            }
            WsMessage::OrderBook(depth) => {
                // 更新订单簿
                let mut orderbook = self.orderbook.write().await;
                orderbook.bids = depth.bids.iter().map(|b| (b[0], b[1])).collect();
                orderbook.asks = depth.asks.iter().map(|a| (a[0], a[1])).collect();
                orderbook.last_update = Utc::now();

                // 更新当前价格（使用最佳买卖价的中间价）
                if !depth.bids.is_empty() && !depth.asks.is_empty() {
                    let best_bid = depth.bids[0][0];
                    let best_ask = depth.asks[0][0];
                    let mid_price = (best_bid + best_ask) / 2.0;
                    if mid_price > 0.0 {
                        let mut price_guard = self.current_price.write().await;
                        if *price_guard <= 0.0 {
                            log::info!(
                                "🎯 首次盘口价格就绪: bid={:.5}, ask={:.5}, mid={:.5}",
                                best_bid,
                                best_ask,
                                mid_price
                            );
                        }
                        *price_guard = mid_price;
                    }
                }

                // 不记录订单簿更新为订单流事件，只记录真实成交
            }
            _ => {}
        }

        Ok(())
    }

    /// 估计泊松参数
    pub(crate) async fn estimate_poisson_parameters(&self) -> Result<()> {
        log::info!("📈 开始估计泊松参数...");

        loop {
            if !*self.running.read().await {
                break;
            }

            // 获取观察窗口内的数据
            let window = Duration::seconds(self.config.poisson.observation_window_secs as i64);
            let now = Utc::now();
            let cutoff = now - window;

            let buffer = self.order_flow_buffer.read().await;
            let recent_events: Vec<_> = buffer
                .iter()
                .filter(|e| e.timestamp > cutoff)
                .cloned()
                .collect();
            drop(buffer);

            if recent_events.len() >= self.config.poisson.min_samples {
                // 计算到达率和成交率
                let (lambda_bid, lambda_ask, mu_bid, mu_ask) = self.calculate_rates(&recent_events);

                // 计算平均队列长度 (M/M/1模型: L = λ/(μ-λ))
                let avg_queue_bid = if mu_bid > lambda_bid {
                    lambda_bid / (mu_bid - lambda_bid)
                } else {
                    10.0 // 上限
                };

                let avg_queue_ask = if mu_ask > lambda_ask {
                    lambda_ask / (mu_ask - lambda_ask)
                } else {
                    10.0
                };

                // 更新参数(使用EMA平滑)
                let mut params = self.poisson_params.write().await;
                let alpha = self.config.poisson.smoothing_alpha;

                if params.lambda_bid == 0.0 {
                    // 首次初始化
                    params.lambda_bid = lambda_bid;
                    params.lambda_ask = lambda_ask;
                    params.mu_bid = mu_bid;
                    params.mu_ask = mu_ask;
                    params.avg_queue_bid = avg_queue_bid;
                    params.avg_queue_ask = avg_queue_ask;
                } else {
                    // EMA更新
                    params.lambda_bid = alpha * lambda_bid + (1.0 - alpha) * params.lambda_bid;
                    params.lambda_ask = alpha * lambda_ask + (1.0 - alpha) * params.lambda_ask;
                    params.mu_bid = alpha * mu_bid + (1.0 - alpha) * params.mu_bid;
                    params.mu_ask = alpha * mu_ask + (1.0 - alpha) * params.mu_ask;
                    params.avg_queue_bid =
                        alpha * avg_queue_bid + (1.0 - alpha) * params.avg_queue_bid;
                    params.avg_queue_ask =
                        alpha * avg_queue_ask + (1.0 - alpha) * params.avg_queue_ask;
                }

                params.last_update = now;

                log::debug!("泊松参数更新: λ_bid={:.2}, λ_ask={:.2}, μ_bid={:.2}, μ_ask={:.2}, L_bid={:.2}, L_ask={:.2}",
                    params.lambda_bid, params.lambda_ask, params.mu_bid, params.mu_ask,
                    params.avg_queue_bid, params.avg_queue_ask
                );
            }

            // 等待下次更新
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        Ok(())
    }

    /// 计算到达率和成交率
    pub(crate) fn calculate_rates(&self, events: &[OrderFlowEvent]) -> (f64, f64, f64, f64) {
        if events.is_empty() {
            return (0.0, 0.0, 0.0, 0.0);
        }

        let duration = events
            .last()
            .unwrap()
            .timestamp
            .signed_duration_since(events.first().unwrap().timestamp)
            .num_seconds() as f64;

        if duration <= 0.0 {
            return (0.0, 0.0, 0.0, 0.0);
        }

        // 只统计成交事件，不统计订单簿更新
        let mut bid_trades = 0;
        let mut ask_trades = 0;

        for event in events {
            if let OrderEventType::Trade = event.event_type {
                match event.side {
                    OrderSide::Buy => bid_trades += 1,
                    OrderSide::Sell => ask_trades += 1,
                }
            }
        }

        // 使用成交率作为订单流强度的指标
        let bid_rate = bid_trades as f64 / duration;
        let ask_rate = ask_trades as f64 / duration;

        // 简化模型：到达率设为成交率的1.5倍（经验值）
        (
            bid_rate * 1.5, // λ_bid
            ask_rate * 1.5, // λ_ask
            bid_rate,       // μ_bid
            ask_rate,       // μ_ask
        )
    }
}
