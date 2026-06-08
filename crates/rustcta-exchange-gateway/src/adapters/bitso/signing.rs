use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn bitso_signature_payload(nonce: i64, method: &str, path: &str, body: &str) -> String {
    format!("{nonce}{}{}{}", method.to_ascii_uppercase(), path, body)
}

pub fn bitso_hmac_signature(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Bitso API secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn bitso_authorization_header(key: &str, nonce: i64, signature: &str) -> String {
    format!("Bitso {key}:{nonce}:{signature}")
}
