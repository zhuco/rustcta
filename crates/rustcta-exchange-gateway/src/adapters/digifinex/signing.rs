use hmac::{Hmac, Mac};
use sha2::Sha256;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

type HmacSha256 = Hmac<Sha256>;

pub fn sign_payload(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid DigiFinex API secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn signature_payload(
    timestamp_ms: i64,
    method: &str,
    path_with_query: &str,
    body: &str,
) -> String {
    format!(
        "{}{}{}{}",
        timestamp_ms,
        method.to_ascii_uppercase(),
        path_with_query,
        body
    )
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Deserialize)]
    struct SigningVector {
        secret: String,
        timestamp: i64,
        method: String,
        request_path: String,
        body: String,
        expected_signature: String,
    }

    #[test]
    fn digifinex_signature_should_match_hmac_sha256_vector() {
        let vector: SigningVector = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/digifinex/signing_vectors/private_post_order.json"
        ))
        .expect("signing vector");
        let payload = signature_payload(
            vector.timestamp,
            &vector.method,
            &vector.request_path,
            &vector.body,
        );
        let signature = sign_payload(&vector.secret, &payload).expect("signature");
        assert_eq!(signature, vector.expected_signature);
        assert!(!payload.contains("secret"));
    }
}
