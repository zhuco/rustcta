use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn canonical_query(params: &std::collections::BTreeMap<String, String>) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{key}={}", urlencoding::encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn sign_query(secret: &str, query: &str) -> String {
    sign_raw_query(secret, query).expect("HMAC-SHA256 accepts arbitrary key length")
}

pub fn sign_raw_query(secret: &str, query: &str) -> ExchangeApiResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Coins.ph API secret length: {error}"),
        }
    })?;
    mac.update(query.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}
