// Gateway adapters share a rich ExchangeApiError payload and venue-specific
// request builders whose signatures mirror exchange API fields.
#![allow(
    clippy::bind_instead_of_map,
    clippy::bool_assert_comparison,
    clippy::nonminimal_bool,
    clippy::collapsible_if,
    clippy::collapsible_str_replace,
    clippy::derivable_impls,
    clippy::excessive_precision,
    clippy::field_reassign_with_default,
    clippy::if_same_then_else,
    clippy::large_enum_variant,
    clippy::manual_range_patterns,
    clippy::manual_repeat_n,
    clippy::manual_strip,
    clippy::needless_borrows_for_generic_args,
    clippy::needless_lifetimes,
    clippy::needless_question_mark,
    clippy::needless_update,
    clippy::redundant_closure,
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::unnecessary_filter_map,
    clippy::unnecessary_get_then_check,
    clippy::unnecessary_lazy_evaluations,
    clippy::unnecessary_map_or,
    clippy::unnecessary_sort_by,
    clippy::useless_format
)]

mod adapters;
mod admin_audit;
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
    AarkGatewayConfig, AdapterBackedGateway, AftermathGatewayConfig, AlpacaGatewayConfig,
    ApexGatewayConfig, ApolloxDexGatewayConfig, ArkhamGatewayConfig, AscendexGatewayConfig,
    AsterGatewayConfig, BackpackGatewayConfig, BequantGatewayConfig, BiconomyGatewayConfig,
    BigOneGatewayConfig, BinanceCoinMGatewayConfig, BinanceGatewayConfig, BinanceUsGatewayConfig,
    BingxGatewayConfig, Bit2cGatewayConfig, BitbankGatewayConfig, BitbnsGatewayConfig,
    BitfinexGatewayConfig, BitflyerGatewayConfig, BitgetGatewayConfig, BithumbGatewayConfig,
    BitkanGatewayConfig, BitmartGatewayConfig, BitmexGatewayConfig, BitoproGatewayConfig,
    BitrueGatewayConfig, BitsoGatewayConfig, BitstampGatewayConfig, BitteamGatewayConfig,
    BittradeGatewayConfig, BitunixGatewayConfig, BitvavoGatewayConfig, BlockchainComGatewayConfig,
    BlofinGatewayConfig, BsxGatewayConfig, BtcMarketsGatewayConfig, BtcTurkGatewayConfig,
    BtcboxGatewayConfig, BullishGatewayConfig, BybitGatewayConfig, BybiteuGatewayConfig,
    BydfiGatewayConfig, CexGatewayConfig, Cod3xGatewayConfig, CoinDcxGatewayConfig,
    CoinExGatewayConfig, CoinbaseExchangeGatewayConfig, CoinbaseGatewayConfig,
    CoincheckGatewayConfig, CoinmateGatewayConfig, CoinmetroGatewayConfig, CoinoneGatewayConfig,
    CoinsPhGatewayConfig, CoinspotGatewayConfig, CoinstoreGatewayConfig, CointrGatewayConfig,
    CoinwGatewayConfig, CryptoComGatewayConfig, CryptomusGatewayConfig, D8xGatewayConfig,
    DeepcoinGatewayConfig, DeltaGatewayConfig, DeribitGatewayConfig, DeriveChainPerpsGatewayConfig,
    DeriveGatewayConfig, DigiFinexGatewayConfig, DydxGatewayConfig, EquationGatewayConfig,
    ExmoGatewayConfig, FmfwioGatewayConfig, FoxbitGatewayConfig, GateIoGatewayConfig,
    GatewayAdapter, GeminiGatewayConfig, GrvtGatewayConfig, HashKeyGlobalGatewayConfig,
    HibachiGatewayConfig, HitbtcGatewayConfig, HollaexGatewayConfig, HtxGatewayConfig,
    HuobiGatewayConfig, HyperliquidGatewayConfig, IndependentReserveGatewayConfig,
    IndodaxGatewayConfig, KrakenFuturesGatewayConfig, KrakenGatewayConfig,
    KuCoinFuturesGatewayConfig, KuCoinGatewayConfig, LBankGatewayConfig, LatokenGatewayConfig,
    LighterGatewayConfig, LunoGatewayConfig, MangoMarketsGatewayConfig, MercadoGatewayConfig,
    MexcGatewayConfig, ModetradeGatewayConfig, MyOkxGatewayConfig, NdaxGatewayConfig,
    NovadaxGatewayConfig, OkxGatewayConfig, OkxusGatewayConfig, OneTradingGatewayConfig,
    OrangeXGatewayConfig, OxfunGatewayConfig, P2bGatewayConfig, PacificaGatewayConfig,
    ParadexGatewayConfig, PaymiumGatewayConfig, PhemexGatewayConfig, PoloniexGatewayConfig,
    TapbitGatewayConfig, TokocryptoGatewayConfig, ToobitGatewayConfig, UpbitGatewayConfig,
    WavesExchangeGatewayConfig, WeexGatewayConfig, WhiteBitGatewayConfig, WooGatewayConfig,
    WoofiproGatewayConfig, XtGatewayConfig, YobitGatewayConfig, ZaifGatewayConfig,
    ZebpayGatewayConfig, ZetaMarketsGatewayConfig,
};
pub use admin_audit::{
    AdminAuditCheck, AdminAuditCheckKind, AdminAuditPlan, AdminAuditPlanner, AdminAuditSafety,
};
pub use batch::{BatchExecutionChunk, BatchExecutionPlan, BatchOperationKind, BatchPlanner};
pub use client::{
    ExchangeGateway, GatewayClient, GatewayExchangeClient, InProcessGatewayClient, LocalGateway,
};
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
