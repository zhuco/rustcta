use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha512;

type HmacSha512 = Hmac<Sha512>;

pub fn sign_hex(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha512::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid CoinSpot API secret length: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}
