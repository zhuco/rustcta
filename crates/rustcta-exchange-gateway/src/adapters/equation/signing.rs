#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub const EQUATION_EVM_SIGNING_UNVERIFIED: &str = "equation.evm_order_signing_unverified";

pub fn equation_wallet_signature_boundary(
    wallet_address: Option<&str>,
) -> ExchangeApiResult<Vec<(String, String)>> {
    let Some(wallet_address) = wallet_address
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Err(ExchangeApiError::Unsupported {
            operation: EQUATION_EVM_SIGNING_UNVERIFIED,
        });
    };

    Ok(vec![
        ("x-equation-wallet".to_string(), wallet_address.to_string()),
        (
            "x-equation-signing-status".to_string(),
            "unsupported_unverified".to_string(),
        ),
    ])
}
