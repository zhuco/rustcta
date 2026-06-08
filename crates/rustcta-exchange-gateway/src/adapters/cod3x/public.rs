#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::PublicStreamSubscription;

pub const COD3X_MARKETS_UNVERIFIED: &str = "cod3x.markets_unverified";
pub const COD3X_RISK_UNVERIFIED: &str = "cod3x.risk_and_fees_unverified";

pub fn cod3x_public_channel(subscription: &PublicStreamSubscription) -> String {
    format!(
        "unsupported.cod3x.{:?}.{}",
        subscription.symbol.market_type, subscription.symbol.exchange_symbol.symbol
    )
}
