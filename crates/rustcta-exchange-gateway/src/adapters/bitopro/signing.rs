#![allow(dead_code)]

use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{json, Value};
use sha2::Sha384;

type HmacSha384 = Hmac<Sha384>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitoproSignedHeaders {
    pub api_key: String,
    pub payload_base64: String,
    pub signature: String,
}

pub fn sign_headers(
    api_key: &str,
    api_secret: &str,
    payload_base64: String,
) -> ExchangeApiResult<BitoproSignedHeaders> {
    let signature = sign_payload(api_secret, &payload_base64)?;
    Ok(BitoproSignedHeaders {
        api_key: api_key.to_string(),
        payload_base64,
        signature,
    })
}

pub fn payload_for_get_delete(identity: &str, nonce_millis: i64) -> ExchangeApiResult<String> {
    payload_for_json(&json!({
        "identity": identity,
        "nonce": nonce_millis,
    }))
}

pub fn payload_for_body(body: &Value) -> ExchangeApiResult<String> {
    payload_for_json(body)
}

pub fn payload_for_json(value: &Value) -> ExchangeApiResult<String> {
    let text = serde_json::to_string(value).map_err(|error| ExchangeApiError::Serialization {
        message: error.to_string(),
    })?;
    Ok(general_purpose::STANDARD.encode(text.as_bytes()))
}

pub fn sign_payload(api_secret: &str, payload_base64: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha384::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid bitopro API secret: {error}"),
        }
    })?;
    mac.update(payload_base64.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}
