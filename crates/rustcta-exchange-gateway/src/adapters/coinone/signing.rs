use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha512;

type HmacSha512 = Hmac<Sha512>;

pub fn encode_payload(payload: &serde_json::Value) -> ExchangeApiResult<String> {
    let text = serde_json::to_string(payload).map_err(|error| ExchangeApiError::Serialization {
        message: error.to_string(),
    })?;
    Ok(general_purpose::STANDARD.encode(text.as_bytes()))
}

pub fn sign_payload(secret_key: &str, encoded_payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha512::new_from_slice(secret_key.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Coinone secret key length: {error}"),
        }
    })?;
    mac.update(encoded_payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn signed_headers(
    secret_key: &str,
    payload: &serde_json::Value,
) -> ExchangeApiResult<(String, String)> {
    let encoded_payload = encode_payload(payload)?;
    let signature = sign_payload(secret_key, &encoded_payload)?;
    Ok((encoded_payload, signature))
}
