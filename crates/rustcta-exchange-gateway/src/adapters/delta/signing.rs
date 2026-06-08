use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct DeltaPrivateCredentials {
    pub api_key: String,
    pub api_secret: String,
}

pub fn sign_request(secret: &str, method: &str, timestamp: &str, path: &str, body: &str) -> String {
    let prehash = format!(
        "{}{}{}{}",
        method.to_ascii_uppercase(),
        timestamp,
        path,
        body
    );
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC supports any key length");
    mac.update(prehash.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn sign_ws_auth(secret: &str, timestamp: &str) -> String {
    sign_request(secret, "GET", timestamp, "/live", "")
}

#[cfg(test)]
mod tests {
    use super::{sign_request, sign_ws_auth};
    use crate::signing_spec::SigningVector;

    #[test]
    fn delta_signing_should_use_method_timestamp_path_body_prehash() {
        let signature = sign_request(
            "secret",
            "POST",
            "1700000000",
            "/v2/orders",
            r#"{"product_symbol":"BTCUSDT","side":"buy"}"#,
        );
        assert_eq!(
            signature,
            "92a98608ff13d281de176de8d7d16a9c633de386eafcc95fe882f1a99909b205"
        );
    }

    #[test]
    fn delta_ws_auth_should_sign_live_path() {
        let signature = sign_ws_auth("secret", "1700000000");
        assert_eq!(
            signature,
            "21c22df1589945dad979a72af38c6c065dfd7ab8501a6fca580308cd02aed4f9"
        );
    }

    #[test]
    fn delta_signing_should_verify_fixture_vector() {
        let vector: SigningVector = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/delta/signing_vectors/rest_hmac_sha256.json"
        ))
        .expect("signing vector fixture");
        assert_eq!(vector.exchange, "delta");
        vector.verify().expect("signing vector");
    }
}
