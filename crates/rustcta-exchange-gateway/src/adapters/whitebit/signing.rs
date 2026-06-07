use hmac::{Hmac, Mac};
use sha2::Sha512;

type HmacSha512 = Hmac<Sha512>;

pub fn sign_payload(secret: &str, payload: &str) -> String {
    let mut mac = HmacSha512::new_from_slice(secret.as_bytes())
        .expect("HMAC-SHA512 accepts arbitrary key length");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}
