#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::PublicStreamSubscription;

pub const EQUATION_MARKETS_UNVERIFIED: &str = "equation.markets_unverified";
pub const EQUATION_RISK_UNVERIFIED: &str = "equation.risk_and_fees_unverified";

pub fn equation_public_channel(subscription: &PublicStreamSubscription) -> String {
    format!(
        "unsupported.equation.{:?}.{}",
        subscription.symbol.market_type, subscription.symbol.exchange_symbol.symbol
    )
}
