use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn sign_raw_query(secret: &str, query: &str) -> ExchangeApiResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Bitrue API secret length: {error}"),
        }
    })?;
    mac.update(query.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn sign_futures_payload(
    secret: &str,
    timestamp_ms: i64,
    method: &str,
    endpoint: &str,
    body: &str,
) -> ExchangeApiResult<String> {
    let payload = format!(
        "{}{}{}{}",
        timestamp_ms,
        method.to_ascii_uppercase(),
        endpoint,
        body
    );
    sign_raw_query(secret, &payload)
}
