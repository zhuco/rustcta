#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub(super) fn unsupported_private<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}
