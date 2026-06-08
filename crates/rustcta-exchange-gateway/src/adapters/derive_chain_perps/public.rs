#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::PublicStreamSubscription;

pub const DERIVE_CHAIN_PERPS_MARKETS_UNVERIFIED: &str = "derive_chain_perps.markets_unverified";
pub const DERIVE_CHAIN_PERPS_RISK_UNVERIFIED: &str = "derive_chain_perps.risk_and_fees_unverified";

pub fn derive_chain_perps_public_channel(subscription: &PublicStreamSubscription) -> String {
    format!(
        "unsupported.derive_chain_perps.{:?}.{}",
        subscription.symbol.market_type, subscription.symbol.exchange_symbol.symbol
    )
}
