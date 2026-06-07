pub mod binance;
pub mod bitget;
pub mod bitmart;
pub mod client_order_id;
pub mod coinex;
pub mod config;
pub mod gateio;
pub mod gateway;
pub mod gateway_exchange;
pub mod market_adapters;
pub mod mexc;
pub mod mock;
pub mod okx;
pub mod paper;
pub mod private_perp;
pub mod registry;
pub mod spot_reservation;
pub mod symbol_registry;
pub mod toobit;
pub mod trading_adapters;
pub mod unified;
// pub mod meteora;  // DEX交易所 - 暂时禁用，待修复编译错误
pub mod hyperliquid; // Hyperliquid 永续（单向持仓）
pub mod kraken;
pub mod kucoin;
// pub mod bybit;  // 暂时禁用
// pub mod htx;  // 暂时禁用

// 导出交易所实现
pub use binance::{BinanceExchange, BinanceSpotClient, BinanceSpotConfig};
pub use bitget::{BitgetSpotClient, BitgetSpotConfig};
pub use bitmart::BitmartExchange;
pub use client_order_id::*;
pub use coinex::{CoinExSpotClient, CoinExSpotConfig};
pub use gateio::{GateIoSpotClient, GateIoSpotConfig};
pub use gateway_exchange::GatewayExchange;
pub use mexc::{MexcSpotClient, MexcSpotConfig};
pub use mock::MockExchange;
pub use okx::{OkxExchange, OkxSpotClient, OkxSpotConfig};
pub use paper::{PaperExchangeClient, PaperExchangeConfig};
pub use symbol_registry::*;
pub use toobit::{
    ToobitConfig, ToobitPerpClient, ToobitPerpConfig, ToobitSpotClient, ToobitSpotConfig,
};
// pub use meteora::MeteoraExchange;  // 暂时禁用
pub use hyperliquid::HyperliquidExchange;
pub use kraken::{KrakenSpotClient, KrakenSpotConfig};
pub use kucoin::{KuCoinSpotClient, KuCoinSpotConfig};
