use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn orangex_auth_signature(
    client_secret: &str,
    client_id: &str,
    timestamp_ms: &str,
    nonce: &str,
) -> String {
    let string_to_sign = format!("{client_id}\n{timestamp_ms}\n{nonce}\n");
    let mut mac =
        HmacSha256::new_from_slice(client_secret.as_bytes()).expect("HMAC supports any key length");
    mac.update(string_to_sign.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn sign_client_signature(
    client_secret: &str,
    client_id: &str,
    timestamp_ms: &str,
    nonce: &str,
) -> String {
    orangex_auth_signature(client_secret, client_id, timestamp_ms, nonce)
}

#[cfg(test)]
mod tests {
    use super::orangex_auth_signature;

    #[test]
    fn orangex_auth_signature_should_match_hmac_sha256_vector() {
        let signature = orangex_auth_signature("secret", "client", "1700000000000", "nonce");
        assert_eq!(
            signature,
            "d1d00486bfe1c1cf571ee5170a39f6e9b4eba1e072e890160ee078057f6a1253"
        );
    }
}
