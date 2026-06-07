use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;
use sha2::Sha256;

use super::config::CoinDcxGatewayConfig;

#[derive(Debug, Clone)]
pub struct CoinDcxPrivateCredentials {
    pub api_key: String,
    pub api_secret: String,
}

impl CoinDcxPrivateCredentials {
    pub fn from_config(config: &CoinDcxGatewayConfig) -> Option<Self> {
        if !config.private_rest_available() {
            return None;
        }
        Some(Self {
            api_key: config.api_key.clone().unwrap_or_default(),
            api_secret: config.api_secret.clone().unwrap_or_default(),
        })
    }
}

pub fn coindcx_signature(secret: &str, body: &Value) -> ExchangeApiResult<String> {
    let payload = serde_json::to_string(body).map_err(|error| ExchangeApiError::Serialization {
        message: error.to_string(),
    })?;
    coindcx_signature_for_payload(secret, &payload)
}

pub fn coindcx_signature_for_payload(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid CoinDCX HMAC secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{coindcx_signature, coindcx_signature_for_payload};

    #[test]
    fn coindcx_signature_should_sign_compact_json_body() {
        let body = json!({
            "timestamp": 1_725_000_000_000_i64,
            "market": "BTCUSDT",
            "side": "buy",
        });
        let signature = coindcx_signature("secret", &body).expect("signature");
        let payload = serde_json::to_string(&body).expect("json");

        assert_eq!(
            signature,
            coindcx_signature_for_payload("secret", &payload).expect("signature")
        );
        assert_eq!(signature.len(), 64);
    }
}
