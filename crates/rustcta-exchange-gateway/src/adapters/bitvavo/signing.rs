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
    mac.update(format!("{timestamp}{method}{request_path}{body}").as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn sign_ws_auth(secret: &str, timestamp: &str) -> String {
    sign_request(secret, timestamp, "GET", "/v2/websocket", "")
}
