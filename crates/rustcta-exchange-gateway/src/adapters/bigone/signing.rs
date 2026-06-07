use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

type HmacSha256 = Hmac<Sha256>;

pub fn bigone_jwt(
    api_key: &str,
    api_secret: &str,
    timestamp_millis: i64,
) -> ExchangeApiResult<String> {
    let header = json!({
        "alg": "HS256",
        "typ": "JWT",
    });
    let payload = json!({
        "type": "OpenAPI",
        "sub": api_key,
        "nonce": timestamp_millis.to_string(),
    });
    let header = base64url_json(&header)?;
    let payload = base64url_json(&payload)?;
    let signing_input = format!("{header}.{payload}");
    let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid BigONE api secret for jwt signing: {error}"),
        }
    })?;
    mac.update(signing_input.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{signing_input}.{signature}"))
}

fn base64url_json(value: &serde_json::Value) -> ExchangeApiResult<String> {
    let bytes = serde_json::to_vec(value).map_err(|error| ExchangeApiError::InvalidRequest {
        message: format!("failed to serialize BigONE jwt json: {error}"),
    })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bigone_jwt_should_be_three_part_base64url_token() {
        let token = bigone_jwt("key", "secret", 1_700_000_000_000).expect("token");
        let parts = token.split('.').collect::<Vec<_>>();
        assert_eq!(parts.len(), 3);
        assert!(!token.contains("secret"));
    }
}
