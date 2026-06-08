use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn btcturk_signature(
    api_key: &str,
    base64_secret: &str,
    stamp_ms: i64,
) -> ExchangeApiResult<String> {
    let secret = general_purpose::STANDARD
        .decode(base64_secret)
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid btcturk base64 secret: {error}"),
        })?;
    let payload = format!("{api_key}{stamp_ms}");
    let mut mac = Hmac::<Sha256>::new_from_slice(&secret).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid btcturk HMAC secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use base64::{engine::general_purpose, Engine as _};

    use super::btcturk_signature;

    #[test]
    fn btcturk_signature_should_be_base64_hmac_sha256_of_key_and_stamp() {
        let secret = general_purpose::STANDARD.encode(b"test-secret");
        let signature =
            btcturk_signature("public-key", &secret, 1_700_000_000_000).expect("signature");
        assert_eq!(signature, "+SnO0CxNQWHcwr3HM2sieexZuruIuuUInDpNb2Yh0vk=");
    }
}
