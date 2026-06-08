use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlpacaAuthHeaders {
    pub api_key_header: (String, String),
    pub api_secret_header: (String, String),
}

impl AlpacaAuthHeaders {
    pub fn into_vec(self) -> Vec<(String, String)> {
        vec![self.api_key_header, self.api_secret_header]
    }
}

pub fn auth_headers(api_key: &str, api_secret: &str) -> ExchangeApiResult<AlpacaAuthHeaders> {
    let api_key = api_key.trim();
    let api_secret = api_secret.trim();
    if api_key.is_empty() || api_secret.is_empty() {
        return Err(ExchangeApiError::Unsupported {
            operation: "alpaca.auth_missing_key_secret",
        });
    }
    Ok(AlpacaAuthHeaders {
        api_key_header: ("APCA-API-KEY-ID".to_string(), api_key.to_string()),
        api_secret_header: ("APCA-API-SECRET-KEY".to_string(), api_secret.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::auth_headers;

    #[test]
    fn alpaca_auth_headers_should_match_fixture_vector() {
        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/alpaca/signing_vectors/header_auth.json"
        ))
        .expect("signing vector");

        let headers = auth_headers(
            vector["api_key"].as_str().expect("api key"),
            vector["api_secret"].as_str().expect("api secret"),
        )
        .expect("headers");
        assert_eq!(vector["exchange"], "alpaca");
        assert_eq!(vector["algorithm"], "header_key_secret");
        assert_eq!(headers.api_key_header.0, "APCA-API-KEY-ID");
        assert_eq!(headers.api_secret_header.0, "APCA-API-SECRET-KEY");
        assert_eq!(headers.api_key_header.1, "test-key");
        assert_eq!(headers.api_secret_header.1, "test-secret");
    }

    #[test]
    fn alpaca_auth_headers_should_reject_missing_secret() {
        assert!(auth_headers("key", "").is_err());
        assert!(auth_headers("", "secret").is_err());
    }
}
