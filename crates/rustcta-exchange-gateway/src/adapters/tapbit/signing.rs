use std::collections::HashMap;

use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn sign_tapbit_request(
    secret: &str,
    timestamp: &str,
    method: &str,
    request_path: &str,
    query: &str,
    body: &str,
) -> ExchangeApiResult<String> {
    let prehash = prehash(timestamp, method, request_path, query, body);
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Tapbit API secret length: {error}"),
        }
    })?;
    mac.update(prehash.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn prehash(
    timestamp: &str,
    method: &str,
    request_path: &str,
    query: &str,
    body: &str,
) -> String {
    let mut value = String::new();
    value.push_str(timestamp);
    value.push_str(&method.to_ascii_uppercase());
    value.push_str(request_path);
    if !query.is_empty() {
        value.push('?');
        value.push_str(query);
    }
    value.push_str(body);
    value
}

pub fn build_query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{}={}", percent_encode(key), percent_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn percent_encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}
