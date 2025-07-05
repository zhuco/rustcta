//! 统一管理所有API端点和WebSocket URL

use crate::utils::symbol::MarketType;

/// 币安API端点配置
#[derive(Debug, Clone)]
pub struct BinanceEndpoints;

impl BinanceEndpoints {
    // API 基础URL
    pub const SPOT_API_BASE_URL: &'static str = "https://api.binance.com";
    pub const FUTURES_API_BASE_URL: &'static str = "https://fapi.binance.com";

    // WebSocket 基础URL
    pub const SPOT_WS_BASE_URL: &'static str = "wss://stream.binance.com:9443";
    pub const FUTURES_WS_BASE_URL: &'static str = "wss://fstream.binance.com";

    pub fn new() -> Self {
        Self
    }

    /// 获取基础URL
    pub fn get_base_url(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::UsdFutures => Self::FUTURES_API_BASE_URL,
            MarketType::Spot => Self::SPOT_API_BASE_URL,
        }
    }

    /// 获取WebSocket URL
    pub fn get_ws_url(&self, market_type: MarketType, listen_key: &str) -> String {
        match market_type {
            MarketType::UsdFutures => format!("{}/ws/{}", Self::FUTURES_WS_BASE_URL, listen_key),
            MarketType::Spot => format!("{}/ws/{}", Self::SPOT_WS_BASE_URL, listen_key),
        }
    }

    /// 获取价格端点
    pub fn get_price_endpoint(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::UsdFutures => "/fapi/v1/ticker/price",
            MarketType::Spot => "/api/v3/ticker/price",
        }
    }

    /// 获取订单端点
    pub fn get_order_endpoint(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::UsdFutures => "/fapi/v1/order",
            MarketType::Spot => "/api/v3/order",
        }
    }

    /// 获取批量订单端点
    pub fn get_batch_orders_endpoint(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::UsdFutures => "/fapi/v1/batchOrders",
            MarketType::Spot => "/api/v3/order/oco", // Spot doesn't have batch orders, fallback
        }
    }

    /// 获取取消所有订单端点
    pub fn get_cancel_all_orders_endpoint(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::UsdFutures => "/fapi/v1/allOpenOrders",
            MarketType::Spot => "/api/v3/openOrders",
        }
    }

    /// 获取K线端点
    pub fn get_klines_endpoint(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::UsdFutures => "/fapi/v1/klines",
            MarketType::Spot => "/api/v3/klines",
        }
    }

    /// 获取交易所信息端点
    pub fn get_exchange_info_endpoint(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::UsdFutures => "/fapi/v1/exchangeInfo",
            MarketType::Spot => "/api/v3/exchangeInfo",
        }
    }

    /// 获取账户信息端点
    pub fn get_account_info_endpoint(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::UsdFutures => "/fapi/v2/account",
            MarketType::Spot => "/api/v3/account",
        }
    }

    /// 获取listen key端点
    pub fn get_listen_key_endpoint(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::UsdFutures => "/fapi/v1/listenKey",
            MarketType::Spot => "/api/v3/userDataStream",
        }
    }

    /// 获取服务器时间端点
    pub fn get_server_time_endpoint(&self, market_type: MarketType) -> &'static str {
        match market_type {
            MarketType::UsdFutures => "/fapi/v1/time",
            MarketType::Spot => "/api/v3/time",
        }
    }
}

/// WebSocket连接状态
#[derive(Debug, Clone)]
pub enum WsConnectionStatus {
    Disconnected,
    Connecting,
    Connected,
    Error(String),
}

impl PartialEq for WsConnectionStatus {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (WsConnectionStatus::Disconnected, WsConnectionStatus::Disconnected) => true,
            (WsConnectionStatus::Connecting, WsConnectionStatus::Connecting) => true,
            (WsConnectionStatus::Connected, WsConnectionStatus::Connected) => true,
            (WsConnectionStatus::Error(_), WsConnectionStatus::Error(_)) => true,
            _ => false,
        }
    }
}

/// WebSocket连接配置
#[derive(Debug, Clone)]
pub struct WsConnectionConfig {
    pub market_type: MarketType,
    pub listen_key: String,
    pub max_reconnect_attempts: u32,
    pub reconnect_delay_seconds: u64,
}

impl Default for WsConnectionConfig {
    fn default() -> Self {
        Self {
            market_type: MarketType::UsdFutures,
            listen_key: String::new(),
            max_reconnect_attempts: 5,
            reconnect_delay_seconds: 5,
        }
    }
}

impl WsConnectionConfig {
    pub fn new(market_type: MarketType, listen_key: String) -> Self {
        Self {
            market_type,
            listen_key,
            max_reconnect_attempts: 5,
            reconnect_delay_seconds: 5,
        }
    }
}
