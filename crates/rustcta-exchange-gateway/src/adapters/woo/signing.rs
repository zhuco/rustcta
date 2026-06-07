use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct WooPrivateCredentials {
    pub api_key: String,
    pub api_secret: String,
}

pub fn sign_request(secret: &str, timestamp: &str, method: &str, path: &str, body: &str) -> String {
    let prehash = format!(
        "{}{}{}{}",
        timestamp,
        method.to_ascii_uppercase(),
        path,
        body
    );
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC supports any key length");
    mac.update(prehash.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::sign_request;
    use crate::signing_spec::SigningVector;

    #[test]
    fn woo_signing_should_use_hex_hmac_sha256_prehash() {
        let signature = sign_request(
            "QHKRXHPAW1MC9YGZMAT8YDJG2HPR",
            "1578565539808",
            "GET",
            "/v3/algo/orders?algoType=STOP",
            "",
        );
        assert_eq!(
            signature,
            "fddab579aff8c193f920c2d4f0cda6a21a60f06fd9c3916541ecc4f6461f5977"
        );
    }

    #[test]
    fn woo_signing_should_verify_fixture_vector() {
        let vector: SigningVector = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/woo/signing_vectors/rest_hmac_sha256.json"
        ))
        .expect("signing vector fixture");
        assert_eq!(vector.exchange, "woo");
        vector.verify().expect("signing vector");
    }
}
