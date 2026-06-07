use hmac::{Hmac, Mac};
use sha2::{Digest, Sha512};

type HmacSha512 = Hmac<Sha512>;

pub fn sign_gateio_request(
    secret: &str,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
    timestamp: &str,
) -> String {
    let body_hash = hex::encode(Sha512::digest(body.as_bytes()));
    let prehash = format!("{method}\n{path}\n{query}\n{body_hash}\n{timestamp}");
    let mut mac =
        HmacSha512::new_from_slice(secret.as_bytes()).expect("HMAC supports any key length");
    mac.update(prehash.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn signed_request_path(base_url: &str, endpoint: &str) -> String {
    let base_path = reqwest::Url::parse(base_url)
        .ok()
        .map(|url| url.path().trim_end_matches('/').to_string())
        .unwrap_or_default();
    format!("{}{}", base_path, endpoint)
}
