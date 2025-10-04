use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::Arc;

use chrono::{Local, Utc};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Duration;

use super::planner;
use crate::analysis::TradeCollector;
use crate::core::{
    error::ExchangeError,
    types::{MarketType, OrderRequest, OrderSide, OrderStatus, OrderType, WsMessage},
    websocket::{MessageHandler, ReconnectManager, WebSocketClient},
};
use crate::cta::account_manager::AccountManager;
use crate::exchanges::binance::{BinanceExchange, BinanceMessageHandler, BinanceWebSocketClient};
use crate::strategies::trend_grid::application::handler::TradeHandler;
use crate::strategies::trend_grid::domain::config::{
    BatchSettings, GridManagement, LoggingConfig, TradingConfig, TrendAdjustment, WebSocketConfig,
};
use crate::strategies::trend_grid::domain::state::{
    ConfigState, TrendAdjustmentRequest, TrendStrength,
};
use crate::utils::{generate_order_id, generate_order_id_with_tag};

pub(crate) fn write_log(config_id: &str, level: &str, message: &str) {
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S.%3f");
    let log_dir = "logs";
    let today = Local::now().format("%Y%m%d");
    let log_file = format!("{}/trend_grid_{}_{}.log", log_dir, config_id, today);

    let _ = fs::create_dir_all(log_dir);

    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&log_file) {
        let log_line = format!("[{}] [{}] {}\n", timestamp, level, message);
        let _ = file.write_all(log_line.as_bytes());
    }

    match level {
        "ERROR" => log::error!("[{}] {}", config_id, message),
        "WARN" => log::warn!("[{}] {}", config_id, message),
        "INFO" => log::info!("[{}] {}", config_id, message),
        _ => log::debug!("[{}] {}", config_id, message),
    }
}

pub(crate) async fn update_and_log_trend(
    state: &Arc<Mutex<ConfigState>>,
    account_manager: Arc<AccountManager>,
    logging_config: &LoggingConfig,
) -> Result<()> {
    let mut state_guard = state.lock().await;

    if let Some(account) = account_manager.get_account(&state_guard.config.account.id) {
        if let Ok(ticker) = account
            .exchange
            .get_ticker(&state_guard.config.symbol, MarketType::Futures)
            .await
        {
            state_guard.current_price = ticker.last;

            if let Some(trend_value) = state_guard.trend_calculator.update(ticker.last) {
                let new_strength = crate::utils::indicators::trend_strength_to_enum(trend_value);
                let old_strength = state_guard.trend_strength;
                state_guard.trend_strength = new_strength;

                if logging_config.show_trend_changes && !matches!(old_strength, new_strength) {
                    log::info!(
                        "🔄 {} 趋势变化: {:?} -> {:?} (值: {:.3}, 价格: {:.4})",
                        state_guard.config.config_id,
                        old_strength,
                        new_strength,
                        trend_value,
                        ticker.last
                    );
                }
            }
        }
    }

    Ok(())
}

pub(crate) async fn log_grid_status(
    state: &Arc<Mutex<ConfigState>>,
    logging_config: &LoggingConfig,
) -> Result<()> {
    let state_guard = state.lock().await;

    let active_buy_orders = state_guard
        .active_orders
        .values()
        .filter(|o| o.side == OrderSide::Buy)
        .count();
    let active_sell_orders = state_guard
        .active_orders
        .values()
        .filter(|o| o.side == OrderSide::Sell)
        .count();

    if logging_config.show_position {
        log::info!(
            "📊 {} - 价格: {:.4}, 净仓: {:.2}, 盈亏: {:.6}U (已实现: {:.6}, 未实现: {:.6}, 费: {:.6})",
            state_guard.config.config_id,
            state_guard.current_price,
            state_guard.net_position,
            state_guard.pnl,
            state_guard.realized_pnl,
            state_guard.unrealized_pnl,
            state_guard.total_fee
        );
    }

    log::debug!(
        "订单统计 -> 买单: {}, 卖单: {}",
        active_buy_orders,
        active_sell_orders
    );

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_websocket_for_config(
    config: &TradingConfig,
    state: &Arc<Mutex<ConfigState>>,
    account_manager: Arc<AccountManager>,
    websocket_config: &WebSocketConfig,
    config_states: Arc<RwLock<HashMap<String, Arc<Mutex<ConfigState>>>>>,
    grid_management: &GridManagement,
    trend_adjustment: &TrendAdjustment,
    batch_settings: &BatchSettings,
    collector: &Option<Arc<TradeCollector>>,
) -> Result<()> {
    let account = account_manager
        .get_account(&config.account.id)
        .ok_or_else(|| ExchangeError::Other(format!("账户 {} 不存在", config.account.id)))?;

    if config.account.exchange == "binance"
        && (websocket_config.subscribe_order_updates || websocket_config.subscribe_trade_updates)
    {
        let global_config = crate::core::config::GlobalConfig::from_file("config/exchanges.yaml")
            .unwrap_or_else(|_| crate::core::config::GlobalConfig::default());
        let exchange_config = crate::core::config::Config::from_exchange_config(
            global_config.get_exchange_config("binance").unwrap_or(
                &crate::core::config::ExchangeConfig {
                    name: "binance".to_string(),
                    testnet: false,
                    base_url: "https://api.binance.com".to_string(),
                    websocket_url: "wss://stream.binance.com:9443".to_string(),
                    symbol_separator: "".to_string(),
                    symbol_format: "{base}{quote}".to_string(),
                    rate_limits: crate::core::config::RateLimits {
                        requests_per_minute: Some(1200),
                        requests_per_second: Some(20),
                        orders_per_minute: Some(100),
                    },
                    endpoints: HashMap::new(),
                },
            ),
        );

        let binance_exchange = crate::exchanges::binance::BinanceExchange::new(
            exchange_config,
            crate::core::config::ApiKeys {
                api_key: std::env::var(format!("{}_API_KEY", config.account.env_prefix))
                    .unwrap_or_default(),
                api_secret: std::env::var(format!("{}_SECRET_KEY", config.account.env_prefix))
                    .unwrap_or_default(),
                passphrase: None,
                memo: None,
            },
        );

        if let Ok(listen_key) = binance_exchange
            .create_listen_key_with_auto_renewal(MarketType::Futures)
            .await
        {
            log::info!("✅ {} 获取到listenKey（已启动自动续期）", config.config_id);

            let ws_url = format!("wss://fstream.binance.com/ws/{}", listen_key);
            let mut ws_client =
                crate::exchanges::binance::BinanceWebSocketClient::new(ws_url, MarketType::Futures);

            ws_client.connect().await?;

            let handler = TradeHandler::new(
                config.config_id.clone(),
                config.clone(),
                state.clone(),
                account_manager.clone(),
                config_states.clone(),
                grid_management.clone(),
                trend_adjustment.clone(),
                batch_settings.clone(),
                websocket_config.log_all_trades,
                collector.clone(),
            );

            let binance_handler = crate::exchanges::binance::BinanceMessageHandler::new(
                Box::new(handler),
                MarketType::Futures,
            );

            let ws_config_id = config.config_id.clone();
            tokio::spawn(async move {
                log::debug!("🔄 {} WebSocket消息接收循环已启动", ws_config_id);
                let mut msg_count = 0;
                loop {
                    match ws_client.receive().await {
                        Ok(Some(msg)) => {
                            msg_count += 1;
                            if msg_count % 100 == 0 {
                                log::debug!("🔔 {} 已处理 {} 条消息", ws_config_id, msg_count);
                            }

                            if msg.contains("listenKeyExpired") {
                                log::error!(
                                    "❌ {} ListenKey已过期！自动续期应该已处理此问题",
                                    ws_config_id
                                );
                                break;
                            }

                            match ws_client.parse_binance_message(&msg) {
                                Ok(ws_msg) => {
                                    if let Err(e) = binance_handler.handle_message(ws_msg).await {
                                        log::error!(
                                            "❌ {} WebSocket消息处理错误: {}",
                                            ws_config_id,
                                            e
                                        );
                                    }
                                }
                                Err(e) => {
                                    log::debug!("⚠️ {} 消息解析失败: {}", ws_config_id, e);
                                }
                            }
                        }
                        Ok(None) => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Err(e) => {
                            log::error!("❌ {} WebSocket接收错误: {}", ws_config_id, e);
                            break;
                        }
                    }
                }
            });
        }

        return Ok(());
    }

    if config.account.exchange == "hyperliquid"
        && (websocket_config.subscribe_order_updates
            || websocket_config.subscribe_trade_updates
            || websocket_config.subscribe_ticker)
    {
        let ws_url =
            crate::core::websocket::get_websocket_url("hyperliquid", MarketType::Futures, false);
        let mut ws_client =
            crate::core::websocket::BaseWebSocketClient::new(ws_url, "hyperliquid".to_string());

        ws_client.connect().await?;

        let mut subscription_messages = Vec::new();
        if websocket_config.subscribe_trade_updates {
            subscription_messages.push(crate::core::websocket::build_subscribe_message(
                "hyperliquid",
                "trade",
                &config.symbol,
            ));
        }

        if websocket_config.subscribe_ticker {
            subscription_messages.push(crate::core::websocket::build_subscribe_message(
                "hyperliquid",
                "ticker",
                &config.symbol,
            ));
        }

        for message in &subscription_messages {
            if let Err(err) = ws_client.send(message.clone()).await {
                log::warn!(
                    "{} Hyperliquid订阅消息发送失败: {} | 消息={}",
                    config.config_id,
                    err,
                    message
                );
            }
        }

        let handler = TradeHandler::new(
            config.config_id.clone(),
            config.clone(),
            state.clone(),
            account_manager.clone(),
            config_states,
            grid_management.clone(),
            trend_adjustment.clone(),
            batch_settings.clone(),
            websocket_config.log_all_trades,
            collector.clone(),
        );

        let ws_config_id = config.config_id.clone();
        let expected_symbol = config.symbol.clone();
        let reconnect_enabled = websocket_config.reconnect_on_disconnect;
        let heartbeat_interval = websocket_config.heartbeat_interval.max(5);
        let reconnect_manager = ReconnectManager::new(20, 3);
        let subscription_messages_clone = subscription_messages.clone();
        tokio::spawn(async move {
            let mut ping_interval = tokio::time::interval(Duration::from_secs(heartbeat_interval));
            ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = ping_interval.tick() => {
                        if let Err(err) = ws_client.ping().await {
                            log::debug!(
                                "{} Hyperliquid心跳发送失败: {} (state={:?})",
                                ws_config_id,
                                err,
                                ws_client.get_state()
                            );
                        }
                    }
                    recv_result = ws_client.receive() => {
                        match recv_result {
                            Ok(Some(msg)) => {
                                if reconnect_enabled {
                                    reconnect_manager.reset().await;
                                }

                                let raw = msg.trim();
                                if raw.is_empty() || raw == "Inactive" {
                                    continue;
                                }

                                match crate::exchanges::hyperliquid::parse_public_ws_messages(raw) {
                                    Ok(messages) => {
                                        for message in messages {
                                            let should_dispatch = match &message {
                                                WsMessage::Ticker(ticker) => ticker.symbol == expected_symbol,
                                                WsMessage::Trade(trade) => trade.symbol == expected_symbol,
                                                WsMessage::OrderBook(book) => book.symbol == expected_symbol,
                                                WsMessage::ExecutionReport(_) => true,
                                                WsMessage::Balance(_) => true,
                                                WsMessage::Order(_) => true,
                                                WsMessage::Position(_) => true,
                                                WsMessage::Error(_) => true,
                                                WsMessage::Text(_) => true,
                                                WsMessage::Kline(_) => false,
                                            };

                                            if !should_dispatch {
                                                continue;
                                            }

                                            if let Err(e) = handler.handle_message(message).await {
                                                log::error!(
                                                    "{} Hyperliquid WebSocket消息处理错误: {}",
                                                    ws_config_id,
                                                    e
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        log::debug!(
                                            "{} 解析Hyperliquid WebSocket消息失败: {} | 原始: {}",
                                            ws_config_id,
                                            e,
                                            raw
                                        );
                                    }
                                }
                            }
                            Ok(None) => {
                                if !reconnect_enabled {
                                    log::warn!(
                                        "{} Hyperliquid WebSocket连接关闭且未开启重连，监听结束",
                                        ws_config_id
                                    );
                                    break;
                                }

                                log::warn!("{} Hyperliquid WebSocket连接关闭，尝试重连...", ws_config_id);

                                if reconnect_manager.try_reconnect(&mut ws_client).await.is_ok() {
                                    for message in &subscription_messages_clone {
                                        if let Err(err) = ws_client.send(message.clone()).await {
                                            log::warn!("{} 重连后订阅消息发送失败: {}", ws_config_id, err);
                                        }
                                    }
                                    continue;
                                }

                                log::error!(
                                    "{} Hyperliquid WebSocket多次重连失败，监听结束",
                                    ws_config_id
                                );
                                break;
                            }
                            Err(e) => {
                                log::error!("{} Hyperliquid WebSocket接收错误: {}", ws_config_id, e);

                                if !reconnect_enabled {
                                    break;
                                }

                                if reconnect_manager.try_reconnect(&mut ws_client).await.is_ok() {
                                    for message in &subscription_messages_clone {
                                        if let Err(err) = ws_client.send(message.clone()).await {
                                            log::warn!("{} 重连后订阅消息发送失败: {}", ws_config_id, err);
                                        }
                                    }
                                    continue;
                                }

                                log::error!(
                                    "{} Hyperliquid WebSocket重连失败次数已达上限，退出监听",
                                    ws_config_id
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });

        return Ok(());
    }

    let mut ws_client = account
        .exchange
        .create_websocket_client(MarketType::Futures)
        .await?;

    ws_client.connect().await?;

    if websocket_config.subscribe_trade_updates {
        let subscribe_msg =
            crate::core::websocket::build_subscribe_message("binance", "trade", &config.symbol);
        let _ = ws_client.send(subscribe_msg).await;
    }

    if websocket_config.subscribe_ticker {
        let subscribe_msg =
            crate::core::websocket::build_subscribe_message("binance", "ticker", &config.symbol);
        let _ = ws_client.send(subscribe_msg).await;
    }

    let handler = TradeHandler::new(
        config.config_id.clone(),
        config.clone(),
        state.clone(),
        account_manager.clone(),
        config_states,
        grid_management.clone(),
        trend_adjustment.clone(),
        batch_settings.clone(),
        websocket_config.log_all_trades,
        collector.clone(),
    );

    let ws_config_id = config.config_id.clone();
    tokio::spawn(async move {
        loop {
            match ws_client.receive().await {
                Ok(Some(msg)) => {
                    if let Err(e) = handler.handle_message(WsMessage::Text(msg)).await {
                        log::error!("{} WebSocket消息处理错误: {}", ws_config_id, e);
                    }
                }
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    log::error!("{} WebSocket接收错误: {}", ws_config_id, e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });

    Ok(())
}

pub(super) type Result<T> = std::result::Result<T, ExchangeError>;
