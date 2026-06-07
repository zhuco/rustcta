use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_phemex_request(
    secret: &str,
    path: &str,
    query: &str,
    expiry: &str,
    body: &str,
) -> String {
    let prehash = format!("{path}{query}{expiry}{body}");
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC supports any key length");
    mac.update(prehash.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn sign_phemex_ws_auth(secret: &str, api_key: &str, expiry: i64) -> String {
    let prehash = format!("{api_key}{expiry}");
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC supports any key length");
    mac.update(prehash.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}
