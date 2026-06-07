use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn sign_payload(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Poloniex API secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    use base64::Engine;
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

pub fn signature_payload(
    method: &str,
    endpoint: &str,
    params: &std::collections::HashMap<String, String>,
    timestamp: &str,
    body: Option<&str>,
) -> String {
    let mut signed_params = params.clone();
    if let Some(body) = body.filter(|body| !body.is_empty()) {
        signed_params.insert("requestBody".to_string(), body.to_string());
    }
    signed_params.insert("signTimestamp".to_string(), timestamp.to_string());
    format!(
        "{}\n{}\n{}",
        method.to_ascii_uppercase(),
        endpoint,
        crate::adapters::poloniex::transport::build_query_string(&signed_params)
    )
}
