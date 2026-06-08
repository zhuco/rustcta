use base64::{engine::general_purpose, Engine as _};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub fn basic_authorization_header(api_key: &str, api_secret: &str) -> ExchangeApiResult<String> {
    let api_key = api_key.trim();
    let api_secret = api_secret.trim();
    if api_key.is_empty() || api_secret.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitteam Basic auth requires non-empty API key and secret".to_string(),
        });
    }
    Ok(format!(
        "Basic {}",
        general_purpose::STANDARD.encode(format!("{api_key}:{api_secret}"))
    ))
}
