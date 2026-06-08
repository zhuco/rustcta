use base64::{engine::general_purpose, Engine as _};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub fn luno_basic_auth_header(key_id: &str, key_secret: &str) -> ExchangeApiResult<String> {
    if key_id.trim().is_empty() || key_secret.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "luno Basic auth requires non-empty key id and secret".to_string(),
        });
    }
    Ok(format!(
        "Basic {}",
        general_purpose::STANDARD.encode(format!("{key_id}:{key_secret}"))
    ))
}

#[cfg(test)]
mod tests {
    use super::luno_basic_auth_header;

    #[test]
    fn luno_auth_header_should_use_basic_auth() {
        let header = luno_basic_auth_header("key-id", "key-secret").expect("header");
        assert_eq!(header, "Basic a2V5LWlkOmtleS1zZWNyZXQ=");
    }
}
