#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterBearerAuth {
    pub token: String,
    pub account_index: String,
}

pub fn lighter_bearer_auth(
    auth_token: Option<&str>,
    account_index: Option<&str>,
) -> ExchangeApiResult<LighterBearerAuth> {
    let token = auth_token
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(ExchangeApiError::Unsupported {
            operation: "lighter.auth_token_unavailable",
        })?;
    let account_index = account_index
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(ExchangeApiError::Unsupported {
            operation: "lighter.auth_token_unavailable",
        })?;
    Ok(LighterBearerAuth {
        token: token.to_string(),
        account_index: account_index.to_string(),
    })
}
