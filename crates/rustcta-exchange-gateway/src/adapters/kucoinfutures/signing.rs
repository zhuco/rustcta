use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_base64(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid KuCoin API secret length: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}
