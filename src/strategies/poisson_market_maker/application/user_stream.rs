use super::{PoissonMarketMaker, Result};
use crate::analysis::TradeData;
use crate::core::{
    error::ExchangeError,
    types::*,
    websocket::{BaseWebSocketClient, WebSocketClient},
};
use crate::strategies::poisson_market_maker::state::OrderIntent;
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

impl PoissonMarketMaker {
    /// 初始化用户数据流
    pub(crate) async fn init_user_stream(&self) -> Result<()> {
        log::info!("🔌 初始化用户数据流WebSocket...");

        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;

        // 调试：检查 exchange 的实际类型
        log::info!(
            "🔍🔍 账户 {} 的交易所类型: {}",
            account.id,
            account.exchange.name()
        );

        // 尝试向下转型到BinanceExchange以使用自动续期功能
        if let Some(binance_exchange) = account
            .exchange
            .as_any()
            .downcast_ref::<crate::exchanges::binance::BinanceExchange>(
        ) {
            log::info!("🔍 使用BinanceExchange的自动续期ListenKey");

            // 使用带自动续期的ListenKey创建
            match binance_exchange
                .create_listen_key_with_auto_renewal(MarketType::Futures)
                .await
            {
                Ok(listen_key) => {
                    log::info!(
                        "✅ 获得ListenKey（已启动自动续期）: {}...",
                        &listen_key[..8.min(listen_key.len())]
                    );
                    self.setup_user_stream_with_key(listen_key).await?;
                    return Ok(());
                }
                Err(e) => {
                    log::warn!("⚠️ 自动续期ListenKey创建失败: {}，回退到普通模式", e);
                    // 回退到普通模式
                }
            }
        }

        // 回退：创建普通用户数据流
        let listen_key = account
            .exchange
            .create_user_data_stream(MarketType::Futures)
            .await?;
        log::info!(
            "✅ 获得ListenKey（手动续期）: {}...",
            &listen_key[..8.min(listen_key.len())]
        );
        self.setup_user_stream_with_key(listen_key).await?;

        log::info!("✅ 用户数据流初始化完成");
        Ok(())
    }

    /// 设置用户数据流WebSocket
    pub(crate) async fn setup_user_stream_with_key(&self, listen_key: String) -> Result<()> {
        // 构建WebSocket URL
        let ws_url = format!("wss://fstream.binance.com/ws/{}", listen_key);

        // 创建WebSocket连接
        let mut user_stream_client =
            BaseWebSocketClient::new(ws_url.clone(), self.config.account.exchange.clone());

        if let Err(e) = user_stream_client.connect().await {
            log::error!("❌ 用户数据流WebSocket连接失败: {}", e);
            return Err(e);
        }

        // 保存客户端引用
        let client_arc = Arc::new(Mutex::new(user_stream_client));
        *self.user_stream_client.write().await = Some(client_arc.clone());

        // 启动消息处理任务（直接在spawn中处理，避免额外的函数调用）
        let processor = self.clone();
        let handle = tokio::spawn(async move {
            log::info!("📨 开始处理用户数据流消息");
            let mut message_count = 0;

            while *processor.running.read().await {
                // 从保存的客户端读取消息
                if let Some(ws_client) = &*processor.user_stream_client.read().await {
                    let mut client_guard = ws_client.lock().await;
                    match client_guard.receive().await {
                        Ok(Some(message)) => {
                            message_count += 1;
                            if message_count % 100 == 1 {
                                log::debug!("📨 已处理 {} 条用户数据流消息", message_count);
                            }

                            if let Err(e) = processor.handle_user_stream_message(&message).await {
                                log::error!("处理用户数据流消息失败: {}", e);
                            }
                        }
                        Ok(None) => {
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                        Err(e) => {
                            log::error!("接收用户数据流消息失败: {}", e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    }
                } else {
                    log::warn!("⚠️ 用户数据流客户端未初始化，退出处理循环");
                    break;
                }
            }

            log::info!(
                "📨 用户数据流消息处理结束（共处理 {} 条消息）",
                message_count
            );
        });
        self.register_handle(handle).await;

        log::info!("✅ 用户数据流WebSocket连接成功");
        Ok(())
    }

    /// 处理用户数据流消息
    pub(crate) async fn handle_user_stream_message(&self, message: &str) -> Result<()> {
        let json: serde_json::Value = serde_json::from_str(message)?;

        // 添加调试日志
        if let Some(event_type) = json.get("e").and_then(|v| v.as_str()) {
            log::info!("🔔 收到用户数据流事件: {}", event_type);

            match event_type {
                "ORDER_TRADE_UPDATE" => {
                    log::info!("📡 处理ORDER_TRADE_UPDATE事件");
                    // 订单更新 (期货)
                    self.handle_order_update(&json).await?;
                }
                "ACCOUNT_UPDATE" => {
                    // 账户更新
                    log::info!("📊 收到账户更新事件");
                }
                _ => {
                    log::debug!("收到未处理的事件类型: {}", event_type);
                }
            }
        } else {
            log::debug!(
                "收到非事件消息: {}",
                message.chars().take(100).collect::<String>()
            );
        }

        Ok(())
    }

    /// 处理订单更新
    pub(crate) async fn handle_order_update(&self, json: &serde_json::Value) -> Result<()> {
        if let Some(order_object) = json.get("o") {
            let event_symbol = order_object
                .get("s")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let normalized_event_symbol = event_symbol.replace('/', "").to_uppercase();
            let normalized_config_symbol =
                self.config.trading.symbol.replace('/', "").to_uppercase();

            if normalized_event_symbol != normalized_config_symbol {
                log::debug!(
                    "🚫 忽略其他交易对订单更新: {} (当前策略: {})",
                    event_symbol,
                    self.config.trading.symbol
                );
                return Ok(());
            }

            let client_order_id = order_object
                .get("c")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let exchange_order_id = order_object.get("i").and_then(|v| match v {
                Value::Number(num) => num.as_i64().map(|id| id.to_string()),
                Value::String(s) => Some(s.clone()),
                _ => None,
            });

            if client_order_id.is_none() && exchange_order_id.is_none() {
                log::debug!("ORDER_TRADE_UPDATE缺少订单ID: {}", json);
                return Ok(());
            }

            let order_label = client_order_id
                .as_deref()
                .or(exchange_order_id.as_deref())
                .unwrap_or("未知订单");

            let status = order_object.get("X").and_then(|v| v.as_str());

            let mut cache_invalidate_ids: Vec<String> = Vec::new();

            if let Some(status_str) = status {
                match status_str {
                    "FILLED" => {
                        log::info!("🎯 收到订单成交通知: {}", order_label);

                        let mut state = self.state.lock().await;

                        let mut fill_side: Option<String> = None;
                        let mut fill_price: f64 = 0.0;
                        let mut fill_qty: f64 = 0.0;
                        let mut fill_reduce_only = false;
                        let mut fill_position_side = String::new();

                        if let Some(side) = order_object.get("S").and_then(|v| v.as_str()) {
                            if let Some(qty) = order_object
                                .get("z")
                                .and_then(|v| v.as_str())
                                .and_then(|s| s.parse::<f64>().ok())
                            {
                                let price = order_object
                                    .get("ap")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<f64>().ok())
                                    .unwrap_or(0.0);

                                log::info!(
                                    "📦 订单成交详情: {} {} @ {} x {}",
                                    self.config.trading.symbol,
                                    side,
                                    price,
                                    qty
                                );

                                let ps = order_object
                                    .get("ps")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("BOTH");
                                let reduce_only = order_object
                                    .get("R")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false);

                                let mut local_pos = self.local_position.write().await;
                                let ps_upper = ps.to_ascii_uppercase();
                                match ps_upper.as_str() {
                                    "LONG" => {
                                        if side == "BUY" && !reduce_only {
                                            state.long_inventory += qty;
                                        } else {
                                            state.long_inventory =
                                                (state.long_inventory - qty).max(0.0);
                                        }
                                    }
                                    "SHORT" => {
                                        if side == "SELL" && !reduce_only {
                                            state.short_inventory += qty;
                                        } else {
                                            state.short_inventory =
                                                (state.short_inventory - qty).max(0.0);
                                        }
                                    }
                                    _ => {
                                        if reduce_only {
                                            if *local_pos > 0.0 {
                                                *local_pos -= qty;
                                            } else {
                                                *local_pos += qty;
                                            }
                                        } else if side == "BUY" {
                                            *local_pos += qty;
                                        } else {
                                            *local_pos -= qty;
                                        }

                                        if *local_pos >= 0.0 {
                                            state.long_inventory = (*local_pos).max(0.0);
                                            state.short_inventory = 0.0;
                                        } else {
                                            state.long_inventory = 0.0;
                                            state.short_inventory = (*local_pos).abs();
                                        }
                                    }
                                }

                                state.inventory = state.long_inventory - state.short_inventory;
                                *local_pos = state.inventory;

                                log::debug!(
                                    "📦 更新本地持仓: 净={} 多头={} 空头={} [{}] (side: {}, ps: {}, reduce: {})",
                                    state.inventory,
                                    state.long_inventory,
                                    state.short_inventory,
                                    self.config.trading.symbol,
                                    side,
                                    ps_upper,
                                    reduce_only
                                );

                                fill_side = Some(side.to_string());
                                fill_price = price;
                                fill_qty = qty;
                                fill_reduce_only = reduce_only;
                                fill_position_side = ps.to_string();
                            }
                        }

                        let mut is_buy_filled = false;
                        let mut is_sell_filled = false;
                        let mut removed_orders: Vec<(OrderIntent, Order)> = Vec::new();
                        let mut seen_ids = HashSet::new();

                        if let Some(client_id) = client_order_id.as_ref() {
                            if let Some((intent, order)) = state.detach_order_by_client(client_id) {
                                if seen_ids.insert(order.id.clone()) {
                                    removed_orders.push((intent, order));
                                }
                            }
                        }

                        if let Some(exchange_id) = exchange_order_id.as_ref() {
                            if let Some((intent, order)) = state.detach_order_by_exchange(exchange_id)
                            {
                                if seen_ids.insert(order.id.clone()) {
                                    removed_orders.push((intent, order));
                                }
                            }
                        }

                        for (intent, order) in removed_orders {
                            cache_invalidate_ids.push(order.id.clone());
                            match intent.side() {
                                OrderSide::Buy => is_buy_filled = true,
                                OrderSide::Sell => is_sell_filled = true,
                            }
                        }

                        state.trade_count += 1;
                        if let Some(fill_side) = fill_side.as_ref() {
                            log::info!(
                                "📦 成交 | 订单={} | 方向={} | 价格={:.5} | 数量={:.3} | reduce_only={} | position_side={}",
                                order_label,
                                fill_side,
                                fill_price,
                                fill_qty,
                                fill_reduce_only,
                                fill_position_side
                            );
                        } else {
                            log::info!("📦 泊松策略订单 {} 已成交", order_label);
                        }

                        drop(state);

                        if is_buy_filled || is_sell_filled {
                            log::info!(
                                "🔄 成交即补：立即补充{}订单",
                                if is_buy_filled { "买" } else { "卖" }
                            );

                            self.update_poisson_params_on_fill().await;

                            if let Err(e) = self.execute_immediate_replenishment().await {
                                log::error!("补单失败: {}", e);
                            }
                        } else {
                            log::debug!(
                                "成交回报未匹配本地挂单: {} (可能已通过轮询同步)",
                                order_label
                            );
                        }
                    }
                    "CANCELED" | "EXPIRED" | "REJECTED" => {
                        let mut state = self.state.lock().await;

                        let mut removed_orders: Vec<(OrderIntent, Order)> = Vec::new();
                        let mut seen_ids = HashSet::new();

                        if let Some(client_id) = client_order_id.as_ref() {
                            if let Some((intent, order)) = state.detach_order_by_client(client_id) {
                                if seen_ids.insert(order.id.clone()) {
                                    removed_orders.push((intent, order));
                                }
                            }
                        }

                        if let Some(exchange_id) = exchange_order_id.as_ref() {
                            if let Some((intent, order)) = state.detach_order_by_exchange(exchange_id)
                            {
                                if seen_ids.insert(order.id.clone()) {
                                    removed_orders.push((intent, order));
                                }
                            }
                        }

                        let removed = !removed_orders.is_empty();
                        for (_intent, order) in removed_orders {
                            cache_invalidate_ids.push(order.id.clone());
                        }

                        drop(state);

                        if removed {
                            log::debug!("泊松策略订单 {} 状态: {}", order_label, status_str);
                        } else {
                            log::debug!(
                                "收到状态更新但本地无对应挂单 {} (状态: {})",
                                order_label,
                                status_str
                            );
                        }
                    }
                    _ => {}
                }

                if let Some(exchange_id) = exchange_order_id.as_ref() {
                    if !cache_invalidate_ids.iter().any(|id| id == exchange_id) {
                        cache_invalidate_ids.push(exchange_id.clone());
                    }
                } else if let Some(client_id) = client_order_id.as_ref() {
                    if !cache_invalidate_ids.iter().any(|id| id == client_id) {
                        cache_invalidate_ids.push(client_id.clone());
                    }
                }

                for cache_id in cache_invalidate_ids {
                    self.order_cache.invalidate_order(&cache_id).await;
                }
            }
        }

        Ok(())
    }

    /// 保持用户数据流活跃
    pub(crate) async fn keep_user_stream_alive(&self, listen_key: String) {
        log::info!("💓 启动用户数据流保活任务");

        while *self.running.read().await {
            tokio::time::sleep(tokio::time::Duration::from_secs(1800)).await; // 每30分钟

            let account = match self
                .account_manager
                .get_account(&self.config.account.account_id)
            {
                Some(acc) => acc,
                None => continue,
            };

            let result = account
                .exchange
                .keepalive_user_data_stream(&listen_key, MarketType::Futures)
                .await;

            match result {
                Ok(_) => log::debug!("✅ 用户数据流保活成功"),
                Err(e) => log::error!("❌ 用户数据流保活失败: {}", e),
            }
        }

        log::info!("💔 用户数据流保活任务结束");
    }
}
