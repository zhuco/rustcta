#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub const DERIVE_CHAIN_PERPS_CHAIN_SIGNING_UNVERIFIED: &str =
    "derive_chain_perps.derive_chain_order_signing_unverified";

pub fn derive_chain_perps_wallet_signature_boundary(
    wallet_address: Option<&str>,
) -> ExchangeApiResult<Vec<(String, String)>> {
    let Some(wallet_address) = wallet_address
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Err(ExchangeApiError::Unsupported {
            operation: DERIVE_CHAIN_PERPS_CHAIN_SIGNING_UNVERIFIED,
        });
    };

    Ok(vec![
        (
            "x-derive_chain_perps-wallet".to_string(),
            wallet_address.to_string(),
        ),
        (
            "x-derive-chain-perps-signing-status".to_string(),
            "unsupported_unverified".to_string(),
        ),
    ])
}
