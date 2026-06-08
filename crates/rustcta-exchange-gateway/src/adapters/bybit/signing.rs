use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn sign_rest_payload(
    secret: &str,
    timestamp_ms: &str,
    api_key: &str,
    recv_window_ms: &str,
    payload: &str,
) -> ExchangeApiResult<String> {
    sign_raw(
        secret,
        &format!("{timestamp_ms}{api_key}{recv_window_ms}{payload}"),
    )
}

pub fn sign_ws_auth(secret: &str, expires_ms: i64) -> ExchangeApiResult<String> {
    sign_raw(secret, &format!("GET/realtime{expires_ms}"))
}

fn sign_raw(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Bybit API secret length: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}
