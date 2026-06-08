#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub const COD3X_ROUTED_SIGNING_UNVERIFIED: &str = "cod3x.routed_order_signing_unverified";

pub fn cod3x_routed_signature_boundary(
    venue_profile: Option<&str>,
) -> ExchangeApiResult<Vec<(String, String)>> {
    let Some(venue_profile) = venue_profile
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Err(ExchangeApiError::Unsupported {
            operation: COD3X_ROUTED_SIGNING_UNVERIFIED,
        });
    };

    Ok(vec![
        (
            "x-cod3x-venue-profile".to_string(),
            venue_profile.to_string(),
        ),
        (
            "x-cod3x-signing-status".to_string(),
            "unsupported_unverified".to_string(),
        ),
    ])
}
