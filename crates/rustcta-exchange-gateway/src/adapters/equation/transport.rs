#![cfg_attr(not(test), allow(dead_code))]

pub const EQUATION_DOCS_BASE_URL: &str = "https://docs.equation.org";
pub const EQUATION_APP_BASE_URL: &str = "https://app.equation.org";

pub fn equation_reconnect_policy_ms() -> (i64, i64, i64) {
    (30_000, 45_000, 60_000)
}
