use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn prehash(timestamp_ms: &str, api_path: &str) -> String {
    format!("{timestamp_ms}+{}", api_path.trim_start_matches('/'))
}

pub fn sign_prehash(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid AscendEX API secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    use base64::Engine;
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::{prehash, sign_prehash};

    #[test]
    fn ascendex_signing_should_match_fixture_vectors() {
        let path = format!(
            "{}/../../tests/fixtures/exchanges/ascendex/signing_vectors.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(path).expect("signing fixture");
        let vectors: serde_json::Value = serde_json::from_str(&text).expect("signing JSON");
        let vectors = vectors.as_array().expect("vector array");
        assert!(vectors.len() >= 3);
        for vector in vectors {
            let timestamp = vector["timestamp_ms"].as_str().expect("timestamp");
            let api_path = vector["api_path"].as_str().expect("api path");
            let expected_prehash = vector["prehash"].as_str().expect("prehash");
            let expected_signature = vector["signature"].as_str().expect("signature");
            let secret = vector["secret"].as_str().expect("secret");
            let payload = prehash(timestamp, api_path);
            assert_eq!(payload, expected_prehash, "{}", vector["name"]);
            let signature = sign_prehash(secret, &payload).expect("signature");
            assert_eq!(signature, expected_signature, "{}", vector["name"]);
        }
    }
}
