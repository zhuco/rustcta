use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha512;

type HmacSha512 = Hmac<Sha512>;

pub fn form_encode(params: &[(String, String)]) -> String {
    params
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

pub fn rest_signature(api_secret: &str, post_data: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha512::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid EXMO API secret length: {error}"),
        }
    })?;
    mac.update(post_data.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn rest_signed_body_and_signature(
    api_secret: &str,
    params: &[(String, String)],
) -> ExchangeApiResult<(String, String)> {
    let post_data = form_encode(params);
    let signature = rest_signature(api_secret, &post_data)?;
    Ok((post_data, signature))
}

pub fn ws_login_signature(
    api_key: &str,
    api_secret: &str,
    nonce: &str,
) -> ExchangeApiResult<String> {
    let mut mac = HmacSha512::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid EXMO API secret length: {error}"),
        }
    })?;
    mac.update(format!("{api_key}{nonce}").as_bytes());
    Ok(general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}
