use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub fn bearer_authorization(token: &str) -> ExchangeApiResult<String> {
    let token = token.trim();
    if token.is_empty() {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinbase.private_rest_missing_bearer_token",
        });
    }
    if token.to_ascii_lowercase().starts_with("bearer ") {
        Ok(token.to_string())
    } else {
        Ok(format!("Bearer {token}"))
    }
}

#[cfg(test)]
mod tests {
    use super::bearer_authorization;

    #[test]
    fn coinbase_bearer_authorization_should_normalize_token_vector() {
        assert_eq!(
            bearer_authorization(" test.jwt ").expect("bearer"),
            "Bearer test.jwt"
        );
        assert_eq!(
            bearer_authorization("Bearer existing.jwt").expect("bearer"),
            "Bearer existing.jwt"
        );
        assert!(bearer_authorization("   ").is_err());
    }

    #[test]
    fn coinbase_bearer_authorization_should_match_fixture_vector() {
        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/coinbase/signing_vectors/bearer_authorization.json"
        ))
        .expect("signing vector fixture");

        assert_eq!(vector["exchange"], "coinbase");
        assert_eq!(vector["algorithm"], "bearer_header");
        assert_eq!(
            bearer_authorization(vector["input_token"].as_str().expect("input token"))
                .expect("bearer"),
            vector["expected_authorization"]
                .as_str()
                .expect("expected authorization")
        );
        assert_eq!(
            bearer_authorization(
                vector["already_prefixed_token"]
                    .as_str()
                    .expect("prefixed token")
            )
            .expect("bearer"),
            vector["expected_already_prefixed_authorization"]
                .as_str()
                .expect("expected prefixed authorization")
        );
    }
}
