use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn sign_prehash(secret: &str, prehash: &str) -> ExchangeApiResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid WEEX API secret length: {error}"),
        }
    })?;
    mac.update(prehash.as_bytes());
    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        mac.finalize().into_bytes(),
    ))
}

pub fn prehash(timestamp: &str, method: &str, endpoint: &str, query: &str, body: &str) -> String {
    if query.is_empty() {
        format!("{timestamp}{}{endpoint}{body}", method.to_ascii_uppercase())
    } else {
        format!(
            "{timestamp}{}{endpoint}?{query}{body}",
            method.to_ascii_uppercase()
        )
    }
}
