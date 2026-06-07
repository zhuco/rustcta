use base64::{engine::general_purpose, Engine as _};
use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::config::DeepcoinGatewayConfig;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct DeepcoinPrivateCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
}

impl DeepcoinPrivateCredentials {
    pub fn from_config(config: &DeepcoinGatewayConfig) -> Option<Self> {
        if !config.private_rest_enabled() {
            return None;
        }
        Some(Self {
            api_key: config.api_key.clone().unwrap_or_default(),
            api_secret: config.api_secret.clone().unwrap_or_default(),
            passphrase: config.passphrase.clone().unwrap_or_default(),
        })
    }
}

pub fn deepcoin_timestamp() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string()
}

pub fn deepcoin_signature(
    secret: &str,
    timestamp: &str,
    method: &str,
    request_path: &str,
    body: &str,
) -> String {
    let prehash = format!("{timestamp}{method}{request_path}{body}");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts secret keys of any length");
    mac.update(prehash.as_bytes());
    general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}
