#![cfg_attr(not(test), allow(dead_code))]

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{json, Value};
use sha2::{Digest, Sha256, Sha512};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpbitJwtParts {
    pub query_string: String,
    pub query_hash: Option<String>,
    pub token: String,
}

pub fn build_query_string(params: &[(String, String)]) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn sha512_hex(input: &str) -> String {
    hex::encode(Sha512::digest(input.as_bytes()))
}

pub fn upbit_jwt(
    access_key: &str,
    secret_key: &str,
    params: &[(String, String)],
    nonce: &str,
) -> ExchangeApiResult<UpbitJwtParts> {
    let query_string = build_query_string(params);
    let query_hash = (!query_string.is_empty()).then(|| sha512_hex(&query_string));
    let mut payload = json!({
        "access_key": access_key,
        "nonce": nonce,
    });
    if let Some(query_hash) = &query_hash {
        payload["query_hash"] = Value::String(query_hash.clone());
        payload["query_hash_alg"] = Value::String("SHA512".to_string());
    }
    let token = hs256_jwt(&payload, secret_key)?;
    Ok(UpbitJwtParts {
        query_string,
        query_hash,
        token,
    })
}

fn hs256_jwt(payload: &Value, secret_key: &str) -> ExchangeApiResult<String> {
    let header = json!({"alg": "HS256", "typ": "JWT"});
    let header = base64url_json(&header)?;
    let payload = base64url_json(payload)?;
    let signing_input = format!("{header}.{payload}");
    let mut mac = HmacSha256::new_from_slice(secret_key.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Upbit secret for JWT signing: {error}"),
        }
    })?;
    mac.update(signing_input.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{signing_input}.{signature}"))
}

fn base64url_json(value: &Value) -> ExchangeApiResult<String> {
    let text = serde_json::to_vec(value).map_err(|error| ExchangeApiError::Serialization {
        message: format!("failed to serialize Upbit JWT JSON: {error}"),
    })?;
    Ok(URL_SAFE_NO_PAD.encode(text))
}
