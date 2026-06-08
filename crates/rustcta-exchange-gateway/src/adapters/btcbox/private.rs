use std::collections::BTreeMap;

use rustcta_exchange_api::ExchangeApiResult;
use serde_json::{json, Value};

use super::signing::{canonical_form, sign_canonical_form};

pub fn private_post_request_spec(
    operation: &str,
    endpoint: &str,
    api_key: &str,
    private_key: &str,
    mut params: BTreeMap<String, String>,
) -> ExchangeApiResult<Value> {
    params.insert("key".to_string(), api_key.to_string());
    let canonical = canonical_form(&params);
    let signature = sign_canonical_form(private_key, &params)?;
    let body = format!("{canonical}&signature={signature}");
    Ok(json!({
        "exchange": "btcbox",
        "operation": operation,
        "method": "POST",
        "path": format!("/api/v1/{endpoint}"),
        "headers": {
            "Content-Type": "application/x-www-form-urlencoded"
        },
        "body": body,
        "canonical": canonical,
        "auth": "hmac_sha256_md5_private_key",
        "expected_signature": signature,
        "network": "offline_request_spec_only"
    }))
}
