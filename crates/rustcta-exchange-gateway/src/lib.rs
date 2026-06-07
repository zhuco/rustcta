mod adapters;
mod batch;
mod client;
mod client_helpers;
mod error;
mod http;
mod mock;
mod mock_handlers;
pub mod orderbook_state;
mod protocol;
mod reconciliation;
pub mod request_spec;
mod security;
pub mod signing_spec;
mod status;
pub mod streams;

pub use adapters::{
    AdapterBackedGateway, AscendexGatewayConfig, BackpackGatewayConfig, BiconomyGatewayConfig,
    BigOneGatewayConfig, BinanceGatewayConfig, BingxGatewayConfig, BitgetGatewayConfig,
    BitkanGatewayConfig, BitmexGatewayConfig, BitrueGatewayConfig, BitunixGatewayConfig,
    BlofinGatewayConfig, CoinDcxGatewayConfig, CoinExGatewayConfig, CoinbaseGatewayConfig,
    CoinstoreGatewayConfig, CointrGatewayConfig, CoinwGatewayConfig, CryptoComGatewayConfig,
    DeepcoinGatewayConfig, DigiFinexGatewayConfig, GateIoGatewayConfig, GatewayAdapter,
    HashKeyGlobalGatewayConfig, KrakenGatewayConfig, KuCoinGatewayConfig, LBankGatewayConfig,
    MexcGatewayConfig, OkxGatewayConfig, OrangeXGatewayConfig, PhemexGatewayConfig,
    PoloniexGatewayConfig, TapbitGatewayConfig, ToobitGatewayConfig, WeexGatewayConfig,
    WhiteBitGatewayConfig, WooGatewayConfig, XtGatewayConfig,
};
pub use batch::{BatchExecutionChunk, BatchExecutionPlan, BatchOperationKind, BatchPlanner};
pub use client::{ExchangeGateway, GatewayClient, InProcessGatewayClient, LocalGateway};
pub use error::GatewayError;
pub use http::gateway_router;
pub use mock::MockExchangeGateway;
pub use protocol::*;
pub use reconciliation::{ReconciliationPlanner, ReconciliationPlannerConfig};
pub use rustcta_exchange_api::{
    ExchangeErrorKind, ExchangeErrorMapping, OrderLevelError, PageCursor, PageRequest,
    RateLimitBucket, RateLimitPlan, RateLimitScope, RetryAfter,
};
pub use security::{secret_key_name_is_forbidden, value_contains_forbidden_secret_key};
pub use status::*;

pub(crate) use security::ensure_secret_free_serializable;

pub const GATEWAY_API_VERSION: &str = "2026-06-06";
pub const GATEWAY_PROTOCOL_SCHEMA_VERSION: u16 = 1;

#[cfg(test)]
mod tests;
