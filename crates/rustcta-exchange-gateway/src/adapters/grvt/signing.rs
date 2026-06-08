#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrvtSessionAuthHeaders {
    pub cookie: String,
    pub account_id: String,
}

pub fn grvt_session_auth_headers(
    session_cookie: Option<&str>,
    account_id: Option<&str>,
) -> ExchangeApiResult<GrvtSessionAuthHeaders> {
    let cookie = session_cookie
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(ExchangeApiError::Unsupported {
            operation: "grvt.private_session_unavailable",
        })?;
    let account_id = account_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(ExchangeApiError::Unsupported {
            operation: "grvt.private_session_unavailable",
        })?;
    Ok(GrvtSessionAuthHeaders {
        cookie: cookie.to_string(),
        account_id: account_id.to_string(),
    })
}
