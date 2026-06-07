use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct BitgetPrivateCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
}

pub fn sign_request(secret: &str, timestamp: &str, method: &str, path: &str, body: &str) -> String {
    let prehash = format!("{timestamp}{method}{path}{body}");
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC supports any key length");
    mac.update(prehash.as_bytes());
    general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}
