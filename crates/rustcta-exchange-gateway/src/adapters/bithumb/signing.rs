#![cfg_attr(not(test), allow(dead_code))]

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{json, Value};
use sha2::{Digest, Sha256, Sha512};
use uuid::Uuid;

use super::config::BithumbGatewayConfig;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BithumbPrivateCredentials {
    pub api_key: String,
    pub api_secret: String,
}

impl BithumbPrivateCredentials {
    pub fn from_config(config: &BithumbGatewayConfig) -> Option<Self> {
        Some(Self {
            api_key: config.api_key.as_ref()?.trim().to_string(),
            api_secret: config.api_secret.as_ref()?.trim().to_string(),
        })
        .filter(|credentials| !credentials.api_key.is_empty() && !credentials.api_secret.is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BithumbJwtParts {
    pub token: String,
    pub nonce: String,
    pub timestamp_millis: i64,
    pub query_hash: Option<String>,
}

pub fn bithumb_query_hash(query: &str) -> Option<String> {
    if query.trim().is_empty() {
        return None;
    }
    Some(hex::encode(Sha512::digest(query.as_bytes())))
}

pub fn bithumb_jwt(
    api_key: &str,
    api_secret: &str,
    query: &str,
    nonce: &str,
    timestamp_millis: i64,
) -> ExchangeApiResult<BithumbJwtParts> {
    let query_hash = bithumb_query_hash(query);
    let header = json!({
        "alg": "HS256",
        "typ": "JWT",
    });
    let mut payload = serde_json::Map::new();
    payload.insert("access_key".to_string(), json!(api_key));
    payload.insert("nonce".to_string(), json!(nonce));
    payload.insert("timestamp".to_string(), json!(timestamp_millis));
    if let Some(query_hash) = &query_hash {
        payload.insert("query_hash".to_string(), json!(query_hash));
        payload.insert("query_hash_alg".to_string(), json!("SHA512"));
    }
    let header = base64url_json(&header)?;
    let payload = base64url_json(&Value::Object(payload))?;
    let signing_input = format!("{header}.{payload}");
    let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Bithumb API secret for JWT signing: {error}"),
        }
    })?;
    mac.update(signing_input.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(BithumbJwtParts {
        token: format!("{signing_input}.{signature}"),
        nonce: nonce.to_string(),
        timestamp_millis,
        query_hash,
    })
}

pub fn bithumb_jwt_now(
    api_key: &str,
    api_secret: &str,
    query: &str,
    timestamp_millis: i64,
) -> ExchangeApiResult<BithumbJwtParts> {
    bithumb_jwt(
        api_key,
        api_secret,
        query,
        &Uuid::new_v4().to_string(),
        timestamp_millis,
    )
}

fn base64url_json(value: &Value) -> ExchangeApiResult<String> {
    let bytes = serde_json::to_vec(value).map_err(|error| ExchangeApiError::Serialization {
        message: format!("failed to serialize Bithumb JWT json: {error}"),
    })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}
