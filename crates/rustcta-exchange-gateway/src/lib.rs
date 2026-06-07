mod adapters;
mod client;
mod client_helpers;
mod error;
mod http;
mod mock;
mod mock_handlers;
mod protocol;
mod security;
mod status;
pub mod streams;

pub use adapters::{
    AdapterBackedGateway, BinanceGatewayConfig, BitgetGatewayConfig, CoinExGatewayConfig,
    GateIoGatewayConfig, GatewayAdapter, KuCoinGatewayConfig, MexcGatewayConfig, OkxGatewayConfig,
};
pub use client::{ExchangeGateway, GatewayClient, InProcessGatewayClient, LocalGateway};
pub use error::GatewayError;
pub use http::gateway_router;
pub use mock::MockExchangeGateway;
pub use protocol::*;
pub use security::{secret_key_name_is_forbidden, value_contains_forbidden_secret_key};
pub use status::*;

pub(crate) use security::ensure_secret_free_serializable;

pub const GATEWAY_API_VERSION: &str = "2026-06-06";
pub const GATEWAY_PROTOCOL_SCHEMA_VERSION: u16 = 1;

#[cfg(test)]
mod tests;
