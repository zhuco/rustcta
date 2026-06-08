#![allow(dead_code)]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

pub fn parse_private_rest_boundary(_value: &Value) -> ExchangeApiResult<()> {
    Err(ExchangeApiError::Unsupported {
        operation: "latoken.private_rest_parser_unverified",
    })
}
