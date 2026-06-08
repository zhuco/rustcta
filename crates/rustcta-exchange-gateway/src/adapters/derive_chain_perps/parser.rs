#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

pub const DERIVE_CHAIN_PERPS_CHAIN_ID: u64 = 957;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeriveChainPerpsAuditBoundary {
    pub chain_id: u64,
    pub stable_api_verified: bool,
    pub trade_enabled: bool,
    pub scan_only: bool,
}

pub fn parse_derive_chain_perps_audit_boundary(
    value: &Value,
) -> ExchangeApiResult<DeriveChainPerpsAuditBoundary> {
    Ok(DeriveChainPerpsAuditBoundary {
        chain_id: value
            .get("chain_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| serialization_error("missing chain_id"))?,
        stable_api_verified: value
            .get("stable_api_verified")
            .and_then(Value::as_bool)
            .ok_or_else(|| serialization_error("missing stable_api_verified"))?,
        trade_enabled: value
            .get("trade_enabled")
            .and_then(Value::as_bool)
            .ok_or_else(|| serialization_error("missing trade_enabled"))?,
        scan_only: value
            .get("scan_only")
            .and_then(Value::as_bool)
            .ok_or_else(|| serialization_error("missing scan_only"))?,
    })
}

pub fn parse_derive_chain_perps_audit_subjects(value: &Value) -> ExchangeApiResult<Vec<String>> {
    let subjects = value
        .get("audit_subjects")
        .and_then(Value::as_array)
        .ok_or_else(|| serialization_error("missing audit_subjects"))?;
    subjects
        .iter()
        .map(|subject| {
            subject
                .as_str()
                .map(ToString::to_string)
                .ok_or_else(|| serialization_error("audit subject must be a string"))
        })
        .collect()
}

fn serialization_error(message: impl Into<String>) -> ExchangeApiError {
    ExchangeApiError::Serialization {
        message: message.into(),
    }
}
