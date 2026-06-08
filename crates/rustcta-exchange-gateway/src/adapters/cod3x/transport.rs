#![cfg_attr(not(test), allow(dead_code))]

pub const COD3X_DOCS_BASE_URL: &str = "https://docs.cod3x.org";
pub const COD3X_WEBSITE_BASE_URL: &str = "https://www.cod3x.org";
pub const COD3X_APP_BASE_URL: &str = "https://app.cod3x.org";

pub fn cod3x_reconnect_policy_ms() -> (i64, i64, i64) {
    (30_000, 45_000, 60_000)
}
