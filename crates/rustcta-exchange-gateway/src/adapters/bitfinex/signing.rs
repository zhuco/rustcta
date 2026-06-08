use hmac::{Hmac, Mac};
use sha2::Sha384;

type HmacSha384 = Hmac<Sha384>;

pub fn bitfinex_rest_payload(path: &str, nonce: &str, body: &str) -> String {
    format!("/api{}{}{}", path, nonce, body)
}

pub fn bitfinex_rest_signature(secret: &str, path: &str, nonce: &str, body: &str) -> String {
    hmac_sha384_hex(secret, &bitfinex_rest_payload(path, nonce, body))
}

pub fn bitfinex_ws_auth_payload(nonce: &str) -> String {
    format!("AUTH{nonce}")
}

pub fn bitfinex_ws_auth_signature(secret: &str, nonce: &str) -> String {
    hmac_sha384_hex(secret, &bitfinex_ws_auth_payload(nonce))
}

fn hmac_sha384_hex(secret: &str, payload: &str) -> String {
    let mut mac =
        HmacSha384::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::{bitfinex_rest_payload, bitfinex_rest_signature, bitfinex_ws_auth_signature};

    #[test]
    fn bitfinex_rest_signature_should_be_deterministic() {
        let payload = bitfinex_rest_payload(
            "/v2/auth/w/order/submit",
            "1700000000000000",
            "{\"amount\":\"0.01\",\"price\":\"65000\",\"symbol\":\"tBTCUSD\",\"type\":\"EXCHANGE LIMIT\"}",
        );
        assert_eq!(payload, "/api/v2/auth/w/order/submit1700000000000000{\"amount\":\"0.01\",\"price\":\"65000\",\"symbol\":\"tBTCUSD\",\"type\":\"EXCHANGE LIMIT\"}");
        assert_eq!(
            bitfinex_rest_signature(
                "bitfinex-test-secret",
                "/v2/auth/w/order/submit",
                "1700000000000000",
                "{\"amount\":\"0.01\",\"price\":\"65000\",\"symbol\":\"tBTCUSD\",\"type\":\"EXCHANGE LIMIT\"}",
            ),
            "a67be9b46285af26271b437c40fc428843042d78b1c0481d1930cc8654c63325f0a752a0335a50ecee7dcd2c39d02482"
        );
    }

    #[test]
    fn bitfinex_ws_signature_should_be_deterministic() {
        assert_eq!(
            bitfinex_ws_auth_signature("bitfinex-test-secret", "1700000000000000"),
            "6ab8f4e19527c631cda6fe2b5500103020f449d7f44215e08ee83fcbb9b24f4c9593fae2e8894f97a807d1afb765c8d9"
        );
    }
}
