use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinstorePrivateCredentials {
    pub api_key: String,
    pub api_secret: String,
}

impl CoinstorePrivateCredentials {
    pub fn from_config(config: &super::config::CoinstoreGatewayConfig) -> Option<Self> {
        if !config.private_rest_available() {
            return None;
        }
        Some(Self {
            api_key: config.api_key.clone().unwrap_or_default(),
            api_secret: config.api_secret.clone().unwrap_or_default(),
        })
    }
}

pub fn sign_coinstore_payload(
    api_secret: &str,
    expires_ms: i64,
    payload: &str,
) -> ExchangeApiResult<String> {
    let time_key = (expires_ms / 30_000).to_string();
    let derived_key = hmac_sha256_hex(api_secret.as_bytes(), time_key.as_bytes())?;
    hmac_sha256_hex(derived_key.as_bytes(), payload.as_bytes())
}

pub fn build_query_string(params: &[(String, String)]) -> String {
    params
        .iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn hmac_sha256_hex(key: &[u8], payload: &[u8]) -> ExchangeApiResult<String> {
    let mut mac =
        HmacSha256::new_from_slice(key).map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid Coinstore HMAC key: {error}"),
        })?;
    mac.update(payload);
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coinstore_signature_should_be_stable() {
        let signature =
            sign_coinstore_payload("secret", 1_717_171_717_171, "symbol=BTCUSDT{\"a\":1}")
                .expect("signature");
        assert_eq!(signature.len(), 64);
        assert_eq!(
            signature,
            sign_coinstore_payload("secret", 1_717_171_717_171, "symbol=BTCUSDT{\"a\":1}")
                .expect("signature")
        );
    }

    #[test]
    fn coinstore_query_string_should_preserve_input_order() {
        let params = vec![
            ("symbol".to_string(), "BTCUSDT".to_string()),
            ("clientOrderId".to_string(), "abc 123".to_string()),
        ];
        assert_eq!(
            build_query_string(&params),
            "symbol=BTCUSDT&clientOrderId=abc%20123"
        );
    }
}
