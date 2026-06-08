use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub fn bullish_hmac_payload(
    timestamp_ms: i64,
    nonce: u64,
    method: &str,
    path: &str,
    body_json_without_spaces: &str,
) -> String {
    format!(
        "{timestamp_ms}{nonce}{}{path}{body_json_without_spaces}",
        method.to_ascii_uppercase()
    )
}

pub fn bullish_hmac_signature(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let digest = Sha256::digest(payload.as_bytes());
    let digest_hex = hex::encode(digest);
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Bullish HMAC secret: {error}"),
        }
    })?;
    mac.update(digest_hex.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::{bullish_hmac_payload, bullish_hmac_signature};

    #[test]
    fn bullish_hmac_payload_should_match_documented_field_order() {
        let payload = bullish_hmac_payload(
            1_700_000_000_000,
            1_700_000_000_001,
            "post",
            "/trading-api/v2/orders",
            r#"{"commandType":"V3CreateOrder","symbol":"BTCUSDC"}"#,
        );
        assert_eq!(
            payload,
            r#"17000000000001700000000001POST/trading-api/v2/orders{"commandType":"V3CreateOrder","symbol":"BTCUSDC"}"#
        );
    }

    #[test]
    fn bullish_hmac_signature_should_be_hex_sha256_hmac() {
        let signature = bullish_hmac_signature("test-secret", "payload").expect("signature");
        assert_eq!(signature.len(), 64);
        assert!(signature.chars().all(|value| value.is_ascii_hexdigit()));
    }
}
