#![cfg_attr(not(test), allow(dead_code))]

use std::collections::BTreeMap;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{json, Value};

use crate::request_spec::ActualHttpRequest;

pub const AMEND_ORDER_UNSUPPORTED: &str =
    "derive.amend_requires_session_jwt_stark_signer_full_replace_fields_reconciliation_and_dry_run_guard";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeriveOfflineRequest {
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub body: Value,
}

impl DeriveOfflineRequest {
    pub fn actual_http_request(&self) -> ActualHttpRequest {
        ActualHttpRequest::new(self.method.clone(), self.path.clone())
            .with_headers(self.headers.clone())
            .with_body(Some(self.body.clone()))
    }
}

pub fn order_rpc_params_fixture() -> Value {
    json!({
        "subaccount_id": "fixture-subaccount",
        "instrument_name": "BTC-PERP",
        "direction": "buy",
        "order_type": "limit",
        "amount": "0.01",
        "limit_price": "65000",
        "label": "fixture-client-order-id"
    })
}

pub fn replace_rpc_params_fixture() -> Value {
    json!({
        "subaccount_id": "fixture-subaccount",
        "order_id": "fixture-order-id",
        "instrument_name": "BTC-PERP",
        "direction": "buy",
        "order_type": "limit",
        "amount": "0.01",
        "limit_price": "65100",
        "label": "fixture-client-order-id-replace"
    })
}

pub fn build_amend_order_replace_request_spec(
    session_key: &str,
    session_signature: &str,
    timestamp_ms: u64,
) -> ExchangeApiResult<DeriveOfflineRequest> {
    if session_key.trim().is_empty() || session_signature.trim().is_empty() {
        return Err(ExchangeApiError::Unsupported {
            operation: AMEND_ORDER_UNSUPPORTED,
        });
    }
    let mut headers = BTreeMap::new();
    headers.insert(
        "x-derive-session-key".to_string(),
        session_key.trim().to_string(),
    );
    headers.insert(
        "x-derive-signature".to_string(),
        session_signature.trim().to_string(),
    );
    headers.insert("x-derive-timestamp".to_string(), timestamp_ms.to_string());

    Ok(DeriveOfflineRequest {
        method: "POST".to_string(),
        path: "/".to_string(),
        headers,
        body: json!({
            "jsonrpc": "2.0",
            "id": 9,
            "method": "private/replace",
            "params": replace_rpc_params_fixture()
        }),
    })
}
