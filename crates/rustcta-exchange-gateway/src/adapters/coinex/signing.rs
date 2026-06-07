use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_request(
    secret: &str,
    method: &str,
    request_path: &str,
    body: &str,
    timestamp: &str,
) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC-SHA256 accepts arbitrary key length");
    mac.update(format!("{method}{request_path}{body}{timestamp}").as_bytes());
    hex::encode(mac.finalize().into_bytes())
}
