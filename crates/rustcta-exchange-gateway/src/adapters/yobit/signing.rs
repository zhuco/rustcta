use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha512;

type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YobitPrivateHeaders {
    pub api_key: String,
    pub signature: String,
}

pub fn build_private_headers(
    api_key: &str,
    api_secret: &str,
    form_body: &str,
) -> ExchangeApiResult<YobitPrivateHeaders> {
    let api_key = api_key.trim();
    let api_secret = api_secret.trim();
    if api_key.is_empty() || api_secret.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "yobit private signing requires non-empty API key and secret".to_string(),
        });
    }
    let signature = hmac_sha512_hex(api_secret, form_body)?;
    Ok(YobitPrivateHeaders {
        api_key: api_key.to_string(),
        signature,
    })
}

pub fn hmac_sha512_hex(api_secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha512::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid yobit api secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn form_encode(params: &[(&str, String)]) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{key}={}", urlencoding::encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}
