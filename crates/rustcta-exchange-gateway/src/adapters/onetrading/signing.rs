use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub fn onetrading_authorization(token: &str) -> ExchangeApiResult<String> {
    let token = token.trim();
    if token.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "onetrading bearer token is empty".to_string(),
        });
    }
    Ok(format!("Bearer {token}"))
}

#[cfg(test)]
mod tests {
    use super::onetrading_authorization;

    #[test]
    fn onetrading_authorization_should_build_bearer_header() {
        let header = onetrading_authorization("<redacted:token>").expect("header");
        assert_eq!(header, "Bearer <redacted:token>");
    }
}
