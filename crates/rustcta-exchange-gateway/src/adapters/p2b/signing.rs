use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;
use sha2::Sha512;

type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct P2bPrivateHeaders {
    pub api_key: String,
    pub payload: String,
    pub signature: String,
}

pub fn build_private_headers(
    api_key: &str,
    api_secret: &str,
    body: &Value,
) -> ExchangeApiResult<P2bPrivateHeaders> {
    let api_key = api_key.trim();
    let api_secret = api_secret.trim();
    if api_key.is_empty() || api_secret.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "p2b private signing requires non-empty API key and secret".to_string(),
        });
    }
    let body_text = canonical_json(body)?;
    let payload = general_purpose::STANDARD.encode(body_text.as_bytes());
    let mut mac = HmacSha512::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid p2b api secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());
    Ok(P2bPrivateHeaders {
        api_key: api_key.to_string(),
        payload,
        signature,
    })
}

fn canonical_json(value: &Value) -> ExchangeApiResult<String> {
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            let mut parts = Vec::with_capacity(keys.len());
            for key in keys {
                let key_text = serde_json::to_string(key).map_err(serialization_error)?;
                let value_text = canonical_json(&map[key])?;
                parts.push(format!("{key_text}:{value_text}"));
            }
            Ok(format!("{{{}}}", parts.join(",")))
        }
        Value::Array(values) => {
            let parts = values
                .iter()
                .map(canonical_json)
                .collect::<ExchangeApiResult<Vec<_>>>()?;
            Ok(format!("[{}]", parts.join(",")))
        }
        _ => serde_json::to_string(value).map_err(serialization_error),
    }
}

fn serialization_error(error: serde_json::Error) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: format!("cannot serialize p2b private payload: {error}"),
    }
}
