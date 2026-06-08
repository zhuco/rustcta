#![allow(dead_code)]

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_request(
    secret: &str,
    timestamp: &str,
    method: &str,
    request_path: &str,
    body: &str,
) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC-SHA256 accepts arbitrary key length");
    mac.update(
        format!(
            "{timestamp}{}{request_path}{body}",
            method.to_ascii_uppercase()
        )
        .as_bytes(),
    );
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::sign_request;

    #[test]
    fn bitflyer_signature_should_match_fixture() {
        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitflyer/signing_vectors/send_child_order_limit.json"
        ))
        .expect("bitflyer signing vector");
        let signature = sign_request(
            vector["api_secret"].as_str().unwrap(),
            vector["timestamp"].as_str().unwrap(),
            vector["method"].as_str().unwrap(),
            vector["path"].as_str().unwrap(),
            vector["body"].as_str().unwrap(),
        );
        assert_eq!(signature, vector["expected_signature"].as_str().unwrap());
    }
}
