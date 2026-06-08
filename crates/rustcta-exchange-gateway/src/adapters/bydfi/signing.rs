use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_rest_request(
    api_key: &str,
    api_secret: &str,
    timestamp_millis: &str,
    query_string: &str,
    body: &str,
) -> ExchangeApiResult<String> {
    sign_hmac_sha256_hex(
        api_secret,
        &format!("{api_key}{timestamp_millis}{query_string}{body}"),
    )
}

pub fn sign_ws_login(
    api_key: &str,
    api_secret: &str,
    timestamp_millis: &str,
) -> ExchangeApiResult<String> {
    sign_hmac_sha256_hex(api_secret, &format!("{api_key}{timestamp_millis}"))
}

fn sign_hmac_sha256_hex(secret: &str, message: &str) -> ExchangeApiResult<String> {
    if secret.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "invalid BYDFi API secret".to_string(),
        });
    }
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid BYDFi HMAC key: {error}"),
        }
    })?;
    mac.update(message.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}
