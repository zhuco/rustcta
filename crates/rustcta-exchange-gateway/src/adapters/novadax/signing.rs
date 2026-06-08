#![cfg_attr(not(test), allow(dead_code))]

use std::collections::BTreeMap;

use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{json, Value};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub const SIGNATURE_METHOD: &str = "HMAC-SHA256";

pub fn novadax_private_request_headers(
    api_key: &str,
    api_secret: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    query: &BTreeMap<String, String>,
    body: Option<&str>,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    let canonical = novadax_signature_payload(method, path, query, body, timestamp);
    let signature = hmac_sha256_hex(api_secret, &canonical)?;
    Ok(BTreeMap::from([
        ("X-Nova-Access-Key".to_string(), api_key.to_string()),
        ("X-Nova-Signature".to_string(), signature),
        ("X-Nova-Timestamp".to_string(), timestamp.to_string()),
    ]))
}

pub fn novadax_signature_payload(
    method: &str,
    path: &str,
    query: &BTreeMap<String, String>,
    body: Option<&str>,
    timestamp: &str,
) -> String {
    let method = method.to_ascii_uppercase();
    if matches!(method.as_str(), "GET" | "DELETE" | "HEAD") {
        return format!(
            "{method}\n{path}\n{}\n{timestamp}",
            sorted_query_string(query)
        );
    }
    let body_md5 = format!("{:x}", md5::compute(body.unwrap_or("").as_bytes()));
    format!("{method}\n{path}\n{body_md5}\n{timestamp}")
}

pub fn sorted_query_string(query: &BTreeMap<String, String>) -> String {
    query
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

pub fn hmac_sha256_hex(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid NovaDAX API secret length: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn private_request_spec(
    method: &str,
    path: &str,
    query: &BTreeMap<String, String>,
    body: Option<Value>,
) -> Value {
    let mut spec = serde_json::Map::new();
    spec.insert("method".to_string(), json!(method.to_ascii_uppercase()));
    spec.insert("path".to_string(), json!(path));
    spec.insert("auth".to_string(), json!("hmac_sha256"));
    if !query.is_empty() {
        spec.insert("query".to_string(), json!(query));
    }
    spec.insert(
        "headers".to_string(),
        json!({
            "X-Nova-Access-Key": "<redacted>",
            "X-Nova-Signature": "<computed>",
            "X-Nova-Timestamp": "<timestamp>"
        }),
    );
    if let Some(body) = body {
        spec.insert("body".to_string(), body);
    }
    Value::Object(spec)
}
