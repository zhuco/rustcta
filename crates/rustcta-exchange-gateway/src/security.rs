use serde::Serialize;
use serde_json::Value;

use crate::GatewayError;

pub fn secret_key_name_is_forbidden(key: &str) -> bool {
    let normalized = key
        .trim()
        .replace('-', "_")
        .replace(' ', "_")
        .to_ascii_lowercase();

    matches!(
        normalized.as_str(),
        "api_key"
            | "apikey"
            | "api_secret"
            | "secret"
            | "secret_key"
            | "private_key"
            | "passphrase"
            | "password"
            | "authorization"
            | "auth_token"
            | "bearer_token"
            | "access_token"
            | "refresh_token"
            | "signature"
    ) || normalized.contains("api_secret")
        || normalized.contains("passphrase")
        || normalized.contains("private_key")
}

pub fn value_contains_forbidden_secret_key(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(key, value)| {
            secret_key_name_is_forbidden(key) || value_contains_forbidden_secret_key(value)
        }),
        Value::Array(items) => items.iter().any(value_contains_forbidden_secret_key),
        _ => false,
    }
}

pub(crate) fn ensure_secret_free_serializable<T: Serialize>(
    value: &T,
    direction: &'static str,
) -> Result<(), GatewayError> {
    let value = serde_json::to_value(value).map_err(|error| GatewayError::InvalidPayload {
        message: error.to_string(),
    })?;
    ensure_secret_free_value(&value, direction)
}

pub(crate) fn ensure_secret_free_value(
    value: &Value,
    direction: &'static str,
) -> Result<(), GatewayError> {
    if value_contains_forbidden_secret_key(value) {
        return Err(GatewayError::SecretPayloadRejected { direction });
    }
    Ok(())
}
