use std::collections::HashMap;

use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

use super::config::BackpackGatewayConfig;

#[derive(Debug, Clone)]
pub struct BackpackPrivateCredentials {
    pub api_key: String,
    pub api_secret: String,
}

impl BackpackPrivateCredentials {
    pub fn from_config(config: &BackpackGatewayConfig) -> Option<Self> {
        if !config.private_rest_available() {
            return None;
        }
        Some(Self {
            api_key: config.api_key.clone().unwrap_or_default(),
            api_secret: config.api_secret.clone().unwrap_or_default(),
        })
    }
}

pub fn backpack_signature(secret: &str, message: &str) -> ExchangeApiResult<String> {
    let key_bytes = decode_secret_key(secret)?;
    let signing_key = SigningKey::from_bytes(&key_bytes);
    let signature = signing_key.sign(message.as_bytes());
    Ok(base64::engine::general_purpose::STANDARD.encode(signature.to_bytes()))
}

pub fn canonical_signing_payload(
    instruction: &str,
    fields: &HashMap<String, String>,
    timestamp_ms: i64,
    window_ms: u64,
) -> String {
    let mut parts = vec![format!("instruction={instruction}")];
    let mut pairs = fields.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    for (key, value) in pairs {
        parts.push(format!("{key}={value}"));
    }
    parts.push(format!("timestamp={timestamp_ms}"));
    parts.push(format!("window={window_ms}"));
    parts.join("&")
}

pub fn canonical_batch_payload(
    instruction: &str,
    orders: &[HashMap<String, String>],
    timestamp_ms: i64,
    window_ms: u64,
) -> String {
    let mut parts = Vec::new();
    for order in orders {
        parts.push(format!("instruction={instruction}"));
        let mut pairs = order.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        for (key, value) in pairs {
            parts.push(format!("{key}={value}"));
        }
    }
    parts.push(format!("timestamp={timestamp_ms}"));
    parts.push(format!("window={window_ms}"));
    parts.join("&")
}

pub fn json_object_to_string_map(value: &Value) -> ExchangeApiResult<HashMap<String, String>> {
    let object = value
        .as_object()
        .ok_or_else(|| ExchangeApiError::Serialization {
            message: "backpack signed body must be a JSON object".to_string(),
        })?;
    let mut map = HashMap::new();
    for (key, value) in object {
        let value = match value {
            Value::Null => continue,
            Value::Bool(value) => value.to_string(),
            Value::Number(value) => value.to_string(),
            Value::String(value) => value.clone(),
            _ => {
                return Err(ExchangeApiError::Serialization {
                    message: format!("backpack signing does not support nested field {key}"),
                });
            }
        };
        map.insert(key.clone(), value);
    }
    Ok(map)
}

fn decode_secret_key(secret: &str) -> ExchangeApiResult<[u8; 32]> {
    let trimmed = secret.trim();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(trimmed)
        .or_else(|_| hex::decode(trimmed))
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid Backpack Ed25519 secret: {error}"),
        })?;
    let bytes = if decoded.len() >= 32 {
        &decoded[..32]
    } else {
        return Err(ExchangeApiError::InvalidRequest {
            message: "Backpack Ed25519 secret must decode to at least 32 bytes".to_string(),
        });
    };
    bytes
        .try_into()
        .map_err(|_| ExchangeApiError::InvalidRequest {
            message: "Backpack Ed25519 secret must decode to 32 bytes".to_string(),
        })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use base64::Engine;

    use super::{backpack_signature, canonical_batch_payload, canonical_signing_payload};

    #[test]
    fn backpack_signature_should_sign_ed25519_payload() {
        let secret = base64::engine::general_purpose::STANDARD.encode([7_u8; 32]);
        let signature = backpack_signature(
            &secret,
            "instruction=orderCancel&orderId=28&symbol=BTC_USDT&timestamp=1614550000000&window=5000",
        )
        .expect("signature");

        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(signature)
                .expect("base64")
                .len(),
            64
        );
    }

    #[test]
    fn backpack_canonical_payload_should_sort_fields() {
        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), "BTC_USDT".to_string());
        fields.insert("orderId".to_string(), "28".to_string());

        assert_eq!(
            canonical_signing_payload("orderCancel", &fields, 1_614_550_000_000, 5_000),
            "instruction=orderCancel&orderId=28&symbol=BTC_USDT&timestamp=1614550000000&window=5000"
        );
    }

    #[test]
    fn backpack_batch_payload_should_repeat_instruction_per_order() {
        let mut first = HashMap::new();
        first.insert("symbol".to_string(), "SOL_USDC_PERP".to_string());
        first.insert("side".to_string(), "Bid".to_string());
        first.insert("orderType".to_string(), "Limit".to_string());
        first.insert("price".to_string(), "141".to_string());
        first.insert("quantity".to_string(), "12".to_string());
        let mut second = first.clone();
        second.insert("price".to_string(), "140".to_string());
        second.insert("quantity".to_string(), "11".to_string());

        assert_eq!(
            canonical_batch_payload("orderExecute", &[first, second], 1_750_793_021_519, 5_000),
            "instruction=orderExecute&orderType=Limit&price=141&quantity=12&side=Bid&symbol=SOL_USDC_PERP&instruction=orderExecute&orderType=Limit&price=140&quantity=11&side=Bid&symbol=SOL_USDC_PERP&timestamp=1750793021519&window=5000"
        );
    }
}
