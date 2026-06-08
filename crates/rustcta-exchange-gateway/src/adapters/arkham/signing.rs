use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn arkham_signature(
    api_key: &str,
    base64_secret: &str,
    expires_us: i64,
    method: &str,
    path: &str,
    body: &str,
) -> ExchangeApiResult<String> {
    let secret = general_purpose::STANDARD
        .decode(base64_secret)
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid arkham base64 secret: {error}"),
        })?;
    let sign_path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    let payload = format!(
        "{api_key}{expires_us}{}{}{}",
        method.to_ascii_uppercase(),
        sign_path,
        body
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(&secret).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid arkham HMAC secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use base64::{engine::general_purpose, Engine as _};

    use super::arkham_signature;

    #[test]
    fn arkham_signature_should_match_offline_vector() {
        let secret = general_purpose::STANDARD.encode(b"test-secret");
        let signature = arkham_signature(
            "test-api-key",
            &secret,
            1_700_000_300_000_000,
            "POST",
            "/orders/new",
            r#"{"symbol":"BTC_USDT","side":"buy","type":"limitGtc","size":"0.01","price":"64000","clientOrderId":"offline-fixture"}"#,
        )
        .expect("signature");
        assert_eq!(signature, "vwiUScbPa9jCGtDVaqN07WRuP5Wno2Kd8BcRPMzRbMU=");
    }
}
