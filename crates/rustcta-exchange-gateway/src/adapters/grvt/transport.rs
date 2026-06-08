#![cfg_attr(not(test), allow(dead_code))]

pub const GRVT_MAINNET_MARKET_DATA_REST: &str = "https://market-data.grvt.io/full";
pub const GRVT_MAINNET_TRADING_REST: &str = "https://trades.grvt.io/full";
pub const GRVT_MAINNET_AUTH: &str = "https://edge.grvt.io/auth";
pub const GRVT_TESTNET_MARKET_DATA_REST: &str = "https://market-data.testnet.grvt.io/full";
pub const GRVT_TESTNET_TRADING_REST: &str = "https://trades.testnet.grvt.io/full";
pub const GRVT_TESTNET_AUTH: &str = "https://edge.testnet.grvt.io/auth";
pub const GRVT_PUBLIC_WS_PING_INTERVAL_MS: i64 = 30_000;
pub const GRVT_PUBLIC_WS_PONG_TIMEOUT_MS: i64 = 45_000;
pub const GRVT_PUBLIC_WS_STALE_MESSAGE_MS: i64 = 60_000;

pub fn grvt_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        GRVT_PUBLIC_WS_PING_INTERVAL_MS,
        GRVT_PUBLIC_WS_PONG_TIMEOUT_MS,
        GRVT_PUBLIC_WS_STALE_MESSAGE_MS,
    )
}
