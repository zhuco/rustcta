/// 统一的WebSocket管理模块 - 支持自动续期和心跳保活
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, sleep, Duration};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::core::{
    error::ExchangeError,
    types::{MarketType, WsMessage},
};

pub type Result<T> = std::result::Result<T, ExchangeError>;

// ============= WebSocket基础定义 =============

/// WebSocket连接状态
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    Connecting,
    Connected,
    Disconnected,
    Reconnecting,
    Error(String),
}

/// WebSocket配置
#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    /// 交易所名称
    pub exchange: String,
    /// WebSocket URL
    pub url: String,
    /// 心跳间隔（秒）
    pub heartbeat_interval: u64,
    /// 重连延迟（秒）
    pub reconnect_delay: u64,
    /// 最大重连次数
    pub max_reconnect_attempts: u32,
    /// 是否自动重连
    pub auto_reconnect: bool,
    /// 自定义心跳消息
    pub ping_message: Option<String>,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            exchange: "unknown".to_string(),
            url: String::new(),
            heartbeat_interval: 30,
            reconnect_delay: 5,
            max_reconnect_attempts: 10,
            auto_reconnect: true,
            ping_message: None,
        }
    }
}

// ============= WebSocket Trait定义 =============

/// WebSocket客户端trait
#[async_trait]
pub trait WebSocketClient: Send + Sync {
    /// 连接到WebSocket服务器
    async fn connect(&mut self) -> Result<()>;

    /// 断开连接
    async fn disconnect(&mut self) -> Result<()>;

    /// 发送消息
    async fn send(&mut self, message: String) -> Result<()>;

    /// 接收消息
    async fn receive(&mut self) -> Result<Option<String>>;

    /// 发送心跳
    async fn ping(&self) -> Result<()>;

    /// 获取连接状态
    fn get_state(&self) -> ConnectionState;
}

/// WebSocket消息处理器trait
#[async_trait]
pub trait MessageHandler: Send + Sync {
    /// 处理接收到的消息
    async fn handle_message(&self, message: WsMessage) -> Result<()>;

    /// 处理错误
    async fn handle_error(&self, error: ExchangeError) -> Result<()> {
        error!("WebSocket错误: {:?}", error);
        Ok(())
    }
}

// ============= Binance ListenKey管理 =============

/// Binance ListenKey管理器 - 用于用户数据流自动续期
pub struct BinanceListenKeyManager {
    api_key: String,
    api_secret: String,
    listen_key: Arc<RwLock<Option<String>>>,
    last_renewal: Arc<RwLock<DateTime<Utc>>>,
    renewal_interval: Duration,
    market_type: MarketType,
}

impl BinanceListenKeyManager {
    pub fn new(api_key: String, api_secret: String, market_type: MarketType) -> Self {
        Self {
            api_key,
            api_secret,
            listen_key: Arc::new(RwLock::new(None)),
            last_renewal: Arc::new(RwLock::new(Utc::now())),
            renewal_interval: Duration::from_secs(30 * 60), // 30分钟
            market_type,
        }
    }

    /// 创建ListenKey
    pub async fn create_listen_key(&self) -> Result<String> {
        let client = reqwest::Client::new();
        let url = match self.market_type {
            MarketType::Spot => "https://api.binance.com/api/v3/userDataStream",
            MarketType::Futures => "https://fapi.binance.com/fapi/v1/listenKey",
        };

        let response = client
            .post(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| ExchangeError::NetworkError(e))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(ExchangeError::Other(format!(
                "创建ListenKey失败: {}",
                error_text
            )));
        }

        #[derive(Deserialize)]
        struct ListenKeyResponse {
            #[serde(rename = "listenKey")]
            listen_key: String,
        }

        let data: ListenKeyResponse = response
            .json()
            .await
            .map_err(|e| ExchangeError::ParseError(e.to_string()))?;

        *self.listen_key.write().await = Some(data.listen_key.clone());
        *self.last_renewal.write().await = Utc::now();

        info!("✅ Binance ListenKey创建成功");
        Ok(data.listen_key)
    }

    /// 续期ListenKey
    pub async fn renew_listen_key(&self) -> Result<()> {
        let listen_key = self
            .listen_key
            .read()
            .await
            .clone()
            .ok_or_else(|| ExchangeError::Other("ListenKey不存在".to_string()))?;

        let client = reqwest::Client::new();
        let url = match self.market_type {
            MarketType::Spot => "https://api.binance.com/api/v3/userDataStream",
            MarketType::Futures => "https://fapi.binance.com/fapi/v1/listenKey",
        };

        let response = client
            .put(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .query(&[("listenKey", listen_key)])
            .send()
            .await
            .map_err(|e| ExchangeError::NetworkError(e))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(ExchangeError::Other(format!(
                "续期ListenKey失败: {}",
                error_text
            )));
        }

        *self.last_renewal.write().await = Utc::now();
        info!("✅ Binance ListenKey续期成功");
        Ok(())
    }

    /// 启动自动续期任务
    pub async fn start_auto_renewal(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = interval(self.renewal_interval);

            loop {
                interval.tick().await;

                if self.listen_key.read().await.is_some() {
                    info!("🔄 自动续期Binance ListenKey");
                    if let Err(e) = self.renew_listen_key().await {
                        error!("ListenKey续期失败: {}", e);

                        // 尝试重新创建
                        if let Err(e) = self.create_listen_key().await {
                            error!("重新创建ListenKey失败: {}", e);
                        }
                    }
                }
            }
        });
    }

    /// 获取当前ListenKey
    pub async fn get_listen_key(&self) -> Option<String> {
        self.listen_key.read().await.clone()
    }
}

// ============= 心跳管理器 =============

/// 通用心跳管理器
pub struct HeartbeatManager {
    exchange: String,
    interval_secs: u64,
    ping_message: String,
}

impl HeartbeatManager {
    pub fn new(exchange: &str) -> Self {
        let (interval_secs, ping_message) = match exchange {
            "binance" => (30, "pong".to_string()),
            "okx" => (30, "ping".to_string()),
            "bitmart" => (15, "ping".to_string()), // Bitmart使用纯文本ping
            "bybit" => (20, r#"{"op":"ping"}"#.to_string()),
            _ => (30, "ping".to_string()),
        };

        Self {
            exchange: exchange.to_string(),
            interval_secs,
            ping_message,
        }
    }

    /// 获取心跳间隔
    pub fn get_interval(&self) -> Duration {
        Duration::from_secs(self.interval_secs)
    }

    /// 获取心跳消息
    pub fn get_ping_message(&self) -> String {
        self.ping_message.clone()
    }

    /// 启动心跳任务
    pub async fn start_heartbeat<T: WebSocketClient + Send + Sync + 'static>(
        self: Arc<Self>,
        client: Arc<Mutex<T>>,
    ) {
        tokio::spawn(async move {
            let mut interval = interval(self.get_interval());

            loop {
                interval.tick().await;

                let client = client.lock().await;
                if client.get_state() == ConnectionState::Connected {
                    debug!("💓 发送心跳: {} - {}", self.exchange, self.ping_message);
                    if let Err(e) = client.ping().await {
                        error!("心跳发送失败: {}", e);
                    }
                }
            }
        });
    }
}

// ============= 重连管理器 =============

/// 自动重连管理器
pub struct ReconnectManager {
    max_attempts: u32,
    delay_secs: u64,
    current_attempts: Arc<RwLock<u32>>,
}

impl ReconnectManager {
    pub fn new(max_attempts: u32, delay_secs: u64) -> Self {
        Self {
            max_attempts,
            delay_secs,
            current_attempts: Arc::new(RwLock::new(0)),
        }
    }

    /// 尝试重连
    pub async fn try_reconnect<T: WebSocketClient>(&self, client: &mut T) -> Result<()> {
        let mut attempts = self.current_attempts.write().await;

        if *attempts >= self.max_attempts {
            error!("❌ 达到最大重连次数: {}", self.max_attempts);
            return Err(ExchangeError::Other("达到最大重连次数".to_string()));
        }

        *attempts += 1;
        info!("🔄 尝试重连 {}/{}", *attempts, self.max_attempts);

        // 等待重连延迟
        sleep(Duration::from_secs(self.delay_secs)).await;

        // 尝试重新连接
        match client.connect().await {
            Ok(()) => {
                info!("✅ 重连成功");
                *attempts = 0; // 重置计数
                Ok(())
            }
            Err(e) => {
                warn!("重连失败: {}", e);
                Err(e)
            }
        }
    }

    /// 重置重连计数
    pub async fn reset(&self) {
        *self.current_attempts.write().await = 0;
    }
}

// ============= 工具函数 =============

/// 解析WebSocket URL，根据交易所和市场类型
pub fn get_websocket_url(exchange: &str, market_type: MarketType, is_private: bool) -> String {
    match exchange {
        "binance" => match (market_type, is_private) {
            (MarketType::Spot, false) => "wss://stream.binance.com:9443/ws",
            (MarketType::Spot, true) => "wss://stream.binance.com:9443/ws",
            (MarketType::Futures, false) => "wss://fstream.binance.com/ws",
            (MarketType::Futures, true) => "wss://fstream.binance.com/ws",
        },
        "okx" => {
            if is_private {
                "wss://ws.okx.com:8443/ws/v5/private"
            } else {
                "wss://ws.okx.com:8443/ws/v5/public"
            }
        }
        "bitmart" => match market_type {
            MarketType::Spot => "wss://ws-manager-compress.bitmart.com/api?protocol=1.1",
            MarketType::Futures => "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1",
        },
        "bybit" => match (market_type, is_private) {
            (MarketType::Spot, false) => "wss://stream.bybit.com/v5/public/spot",
            (MarketType::Spot, true) => "wss://stream.bybit.com/v5/private",
            (MarketType::Futures, false) => "wss://stream.bybit.com/v5/public/linear",
            (MarketType::Futures, true) => "wss://stream.bybit.com/v5/private",
        },
        _ => "wss://unknown",
    }
    .to_string()
}

/// 构建订阅消息
pub fn build_subscribe_message(exchange: &str, channel: &str, symbol: &str) -> String {
    match exchange {
        "binance" => {
            format!(
                r#"{{"method":"SUBSCRIBE","params":["{}"],"id":1}}"#,
                format!("{}@{}", symbol.to_lowercase(), channel)
            )
        }
        "okx" => {
            format!(
                r#"{{"op":"subscribe","args":[{{"channel":"{}","instId":"{}"}}]}}"#,
                channel, symbol
            )
        }
        "bitmart" => {
            format!(
                r#"{{"op":"subscribe","args":["{}/{}:{}"]"#,
                "spot", channel, symbol
            )
        }
        "bybit" => {
            format!(r#"{{"op":"subscribe","args":["{}.{}"]"#, channel, symbol)
        }
        _ => format!(r#"{{"subscribe":"{}"}}"#, channel),
    }
}

/// 解析心跳响应
pub fn is_heartbeat_response(exchange: &str, message: &str) -> bool {
    match exchange {
        "binance" => message == "pong" || message.contains("\"result\":null"),
        "okx" => message == "pong",
        "bitmart" => message == "pong", // Bitmart返回纯文本pong
        "bybit" => message.contains(r#""op":"pong"#),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_websocket_url() {
        assert_eq!(
            get_websocket_url("binance", MarketType::Spot, false),
            "wss://stream.binance.com:9443/ws"
        );

        assert_eq!(
            get_websocket_url("okx", MarketType::Futures, true),
            "wss://ws.okx.com:8443/ws/v5/private"
        );
    }

    #[test]
    fn test_heartbeat_manager() {
        let manager = HeartbeatManager::new("binance");
        assert_eq!(manager.interval_secs, 30);
        assert_eq!(manager.ping_message, "pong");

        let manager = HeartbeatManager::new("bitmart");
        assert_eq!(manager.interval_secs, 15);
        assert_eq!(manager.ping_message, "ping");
    }
}

// ============= 基础WebSocket客户端实现 =============

/// 基础WebSocket客户端实现
#[derive(Clone)]
pub struct BaseWebSocketClient {
    url: String,
    exchange: String,
    state: Arc<RwLock<ConnectionState>>,
    ws_stream: Arc<Mutex<Option<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
}

impl BaseWebSocketClient {
    pub fn new(url: String, exchange: String) -> Self {
        Self {
            url,
            exchange,
            state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            ws_stream: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl WebSocketClient for BaseWebSocketClient {
    async fn connect(&mut self) -> Result<()> {
        *self.state.write().await = ConnectionState::Connecting;

        log::info!("🔌 正在连接WebSocket: {}", self.url);

        match connect_async(&self.url).await {
            Ok((ws_stream, _)) => {
                log::info!("✅ WebSocket连接成功: {}", self.url);
                *self.ws_stream.lock().await = Some(ws_stream);
                *self.state.write().await = ConnectionState::Connected;
                Ok(())
            }
            Err(e) => {
                log::error!("❌ WebSocket连接失败: {}", e);
                *self.state.write().await = ConnectionState::Disconnected;
                Err(ExchangeError::WebSocketError(format!(
                    "Connection failed: {}",
                    e
                )))
            }
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        if let Some(mut ws_stream) = self.ws_stream.lock().await.take() {
            let _ = ws_stream.close(None).await;
            log::info!("🔌 WebSocket连接已断开");
        }
        *self.state.write().await = ConnectionState::Disconnected;
        Ok(())
    }

    async fn send(&mut self, message: String) -> Result<()> {
        let mut ws_guard = self.ws_stream.lock().await;
        if let Some(ws_stream) = ws_guard.as_mut() {
            ws_stream
                .send(Message::Text(message.clone()))
                .await
                .map_err(|e| {
                    log::error!("❌ 发送WebSocket消息失败: {}", e);
                    ExchangeError::WebSocketError(format!("Send failed: {}", e))
                })?;
            log::trace!("📤 发送WebSocket消息: {}", message);
            Ok(())
        } else {
            Err(ExchangeError::WebSocketError("Not connected".to_string()))
        }
    }

    async fn receive(&mut self) -> Result<Option<String>> {
        let mut ws_guard = self.ws_stream.lock().await;
        if let Some(ws_stream) = ws_guard.as_mut() {
            match ws_stream.next().await {
                Some(Ok(Message::Text(text))) => {
                    // 只在TRACE级别记录原始消息
                    log::trace!(
                        "📥 接收WebSocket消息: {}",
                        if text.len() <= 200 {
                            &text
                        } else {
                            &text[..200]
                        }
                    );
                    Ok(Some(text))
                }
                Some(Ok(Message::Ping(data))) => {
                    // 自动回复Pong
                    let _ = ws_stream.send(Message::Pong(data)).await;
                    log::trace!("🎾 回复WebSocket Ping");
                    Ok(None)
                }
                Some(Ok(Message::Close(_))) => {
                    log::info!("🔚 WebSocket连接关闭");
                    *self.state.write().await = ConnectionState::Disconnected;
                    Ok(None)
                }
                Some(Ok(_)) => Ok(None), // 其他消息类型忽略
                Some(Err(e)) => {
                    log::error!("❌ WebSocket接收错误: {}", e);
                    Err(ExchangeError::WebSocketError(format!(
                        "Receive error: {}",
                        e
                    )))
                }
                None => {
                    log::debug!("🔄 WebSocket流结束");
                    *self.state.write().await = ConnectionState::Disconnected;
                    Ok(None)
                }
            }
        } else {
            Err(ExchangeError::WebSocketError("Not connected".to_string()))
        }
    }

    async fn ping(&self) -> Result<()> {
        let mut ws_guard = self.ws_stream.lock().await;
        if let Some(ws_stream) = ws_guard.as_mut() {
            // 根据不同交易所发送不同的心跳消息
            let ping_msg = match self.exchange.as_str() {
                "bitmart" => {
                    // Bitmart期货使用JSON格式的ping
                    Message::Text(r#"{"action":"ping"}"#.to_string())
                }
                "binance" => Message::Text("ping".to_string()),
                "okx" => Message::Text("ping".to_string()),
                "bybit" => Message::Text(r#"{"op":"ping"}"#.to_string()),
                _ => Message::Ping(vec![]),
            };

            ws_stream.send(ping_msg.clone()).await.map_err(|e| {
                log::error!("❌ 发送心跳失败: {}", e);
                ExchangeError::WebSocketError(format!("Ping failed: {}", e))
            })?;
            log::trace!("💓 发送心跳到 {} - 消息: {:?}", self.exchange, ping_msg);
            Ok(())
        } else {
            Err(ExchangeError::WebSocketError("Not connected".to_string()))
        }
    }

    fn get_state(&self) -> ConnectionState {
        // 由于这是同步方法，我们需要使用try_read
        self.state
            .try_read()
            .map(|state| state.clone())
            .unwrap_or(ConnectionState::Disconnected)
    }
}
