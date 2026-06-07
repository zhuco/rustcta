use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::config::BitmexGatewayConfig;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct BitmexPrivateCredentials {
    pub api_key: String,
    pub api_secret: String,
}

impl BitmexPrivateCredentials {
    pub fn from_config(config: &BitmexGatewayConfig) -> Option<Self> {
        if !config.private_rest_available() {
            return None;
        }
        Some(Self {
            api_key: config.api_key.clone().unwrap_or_default(),
            api_secret: config.api_secret.clone().unwrap_or_default(),
        })
    }
}

pub fn bitmex_signature(
    secret: &str,
    method: &str,
    request_path: &str,
    expires: i64,
    body: &str,
) -> String {
    let message = format!(
        "{}{}{}{}",
        method.to_ascii_uppercase(),
        request_path,
        expires,
        body
    );
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts secret keys of any length");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn bitmex_ws_auth_signature(secret: &str, expires: i64) -> String {
    bitmex_signature(secret, "GET", "/realtime", expires, "")
}

#[cfg(test)]
mod tests {
    use super::bitmex_signature;

    #[test]
    fn bitmex_signature_should_match_hmac_sha256_vector() {
        let signature = bitmex_signature(
            "secret",
            "GET",
            "/api/v1/instrument?symbol=XBTUSD",
            1_518_064_237,
            "",
        );
        assert_eq!(
            signature,
            "39278e0e75111f370be05ea10773b7399db40cc10006e6a974b44699926ea8fe"
        );
    }
}
