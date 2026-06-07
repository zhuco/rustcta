use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_request(
    api_secret: &str,
    request_path: &str,
    method: &str,
    timestamp_ms: &str,
    nonce: &str,
    body: &str,
) -> ExchangeApiResult<String> {
    if api_secret.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "invalid BloFin API secret".to_string(),
        });
    }
    let prehash = format!(
        "{request_path}{}{timestamp_ms}{nonce}{body}",
        method.to_uppercase()
    );
    let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid BloFin API secret: {error}"),
        }
    })?;
    mac.update(prehash.as_bytes());
    let hex_signature = hex::encode(mac.finalize().into_bytes());
    Ok(general_purpose::STANDARD.encode(hex_signature.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::sign_request;

    #[test]
    fn sign_request_should_match_blofin_algorithm_shape() {
        let sign = sign_request(
            "secret",
            "/api/v1/trade/order",
            "POST",
            "1700000000000",
            "nonce-1",
            r#"{"instId":"BTC-USDT"}"#,
        )
        .unwrap();
        assert!(!sign.is_empty());
        assert!(sign.ends_with('='));
    }
}
