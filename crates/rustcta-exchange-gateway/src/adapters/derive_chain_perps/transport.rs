#![cfg_attr(not(test), allow(dead_code))]

pub const DERIVE_CHAIN_PERPS_DOCS_BASE_URL: &str = "https://docs.derive.xyz";
pub const DERIVE_CHAIN_PERPS_APP_BASE_URL: &str = "https://www.derive.xyz";
pub const DERIVE_CHAIN_PERPS_REST_BASE_URL: &str = "https://api.derive.xyz";
pub const DERIVE_CHAIN_PERPS_RPC_URL: &str = "https://rpc.lyra.finance";

pub fn derive_chain_perps_reconnect_policy_ms() -> (i64, i64, i64) {
    (30_000, 45_000, 60_000)
}
