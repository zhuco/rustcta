use std::collections::BTreeMap;

use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn ndax_signature_payload(
    timestamp: &str,
    method: &str,
    request_path: &str,
    body: &str,
) -> String {
    format!(
        "{}{}{}{}",
        timestamp,
        method.to_ascii_uppercase(),
        request_path,
        body
    )
}

pub fn ndax_hmac_sha256_base64(decoded_secret: &[u8], payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha256::new_from_slice(decoded_secret).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid NDAX API secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

pub fn ndax_signed_headers(
    api_key: &str,
    api_secret_base64: &str,
    passphrase: Option<&str>,
    timestamp: &str,
    method: &str,
    request_path: &str,
    body: &str,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    let decoded_secret = general_purpose::STANDARD
        .decode(api_secret_base64)
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("NDAX API secret must be base64 encoded: {error}"),
        })?;
    let payload = ndax_signature_payload(timestamp, method, request_path, body);
    let signature = ndax_hmac_sha256_base64(&decoded_secret, &payload)?;
    let mut headers = BTreeMap::from([
        ("X-NDAX-APIKEY".to_string(), api_key.to_string()),
        ("X-NDAX-SIGNATURE".to_string(), signature),
        ("X-NDAX-TIMESTAMP".to_string(), timestamp.to_string()),
    ]);
    if let Some(passphrase) = passphrase.filter(|value| !value.trim().is_empty()) {
        headers.insert("X-NDAX-PASSPHRASE".to_string(), passphrase.to_string());
    }
    Ok(headers)
}

pub fn ndax_redacted_signed_headers() -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "X-NDAX-APIKEY".to_string(),
            "<redacted:api_key>".to_string(),
        ),
        (
            "X-NDAX-SIGNATURE".to_string(),
            "<redacted:signature>".to_string(),
        ),
        ("X-NDAX-TIMESTAMP".to_string(), "<timestamp>".to_string()),
    ])
}
