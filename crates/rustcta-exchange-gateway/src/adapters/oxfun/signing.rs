#![cfg_attr(not(test), allow(dead_code))]

use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_rest_request(
    api_secret: &str,
    timestamp: &str,
    nonce: &str,
    method: &str,
    host: &str,
    path: &str,
    body_or_query: &str,
) -> ExchangeApiResult<String> {
    if api_secret.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "invalid OX.FUN API secret".to_string(),
        });
    }
    let payload = oxfun_rest_signing_payload(timestamp, nonce, method, host, path, body_or_query);
    let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid OX.FUN API secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

pub fn oxfun_rest_signing_payload(
    timestamp: &str,
    nonce: &str,
    method: &str,
    host: &str,
    path: &str,
    body_or_query: &str,
) -> String {
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        timestamp,
        nonce,
        method.to_ascii_uppercase(),
        host,
        path,
        body_or_query
    )
}

pub fn sign_ws_login(api_secret: &str, timestamp_ms: &str) -> ExchangeApiResult<String> {
    if api_secret.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "invalid OX.FUN API secret".to_string(),
        });
    }
    let payload = oxfun_ws_login_payload(timestamp_ms);
    let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid OX.FUN API secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

pub fn oxfun_ws_login_payload(timestamp_ms: &str) -> String {
    format!("{timestamp_ms}GET/auth/self/verify")
}

#[cfg(test)]
mod tests {
    use super::{sign_rest_request, sign_ws_login};

    #[test]
    fn oxfun_rest_signature_should_match_vector() {
        let signature = sign_rest_request(
            "secret",
            "2026-06-08T00:00:00Z",
            "nonce-1",
            "GET",
            "api.ox.fun",
            "/v3/account",
            "asset=OX",
        )
        .expect("signature");
        assert_eq!(signature, "8N+Ov1G0potibfVQkbwA5ZaOGPASA7I3xnxqR+F+DTw=");
    }

    #[test]
    fn oxfun_ws_login_signature_should_match_vector() {
        let signature = sign_ws_login("secret", "1700000000000").expect("signature");
        assert_eq!(signature, "qNyCEuPA4GPpD3gYnapr/2/0OOe1C+GUn5QWMIV8GoY=");
    }
}
