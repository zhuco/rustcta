use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn sign_request(
    api_secret: &str,
    timestamp: &str,
    memo: Option<&str>,
    body: &str,
) -> ExchangeApiResult<String> {
    if api_secret.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "invalid BitMart API secret".to_string(),
        });
    }
    let payload = match memo {
        Some(memo) if !memo.trim().is_empty() => format!("{timestamp}#{memo}#{body}"),
        _ => format!("{timestamp}#{body}"),
    };
    let mut mac = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: error.to_string(),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}
