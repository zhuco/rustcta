#![cfg_attr(not(test), allow(dead_code))]

pub const LIGHTER_MAINNET_REST: &str = "https://mainnet.zklighter.elliot.ai/api/v1";
pub const LIGHTER_TESTNET_REST: &str = "https://testnet.zklighter.elliot.ai/api/v1";
pub const LIGHTER_MAINNET_WS: &str = "wss://mainnet.zklighter.elliot.ai/stream";
pub const LIGHTER_TESTNET_WS: &str = "wss://testnet.zklighter.elliot.ai/stream";
pub const LIGHTER_WS_KEEPALIVE_INTERVAL_MS: i64 = 60_000;
pub const LIGHTER_WS_KEEPALIVE_TIMEOUT_MS: i64 = 120_000;
pub const LIGHTER_WS_STALE_MESSAGE_MS: i64 = 180_000;

pub fn lighter_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        LIGHTER_WS_KEEPALIVE_INTERVAL_MS,
        LIGHTER_WS_KEEPALIVE_TIMEOUT_MS,
        LIGHTER_WS_STALE_MESSAGE_MS,
    )
}
