use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZebpayPrivateHeaders {
    pub client_id: String,
    pub timestamp: String,
    pub request_id: String,
    pub authorization: String,
}

pub fn build_bearer_private_headers(
    client_id: &str,
    access_token: &str,
    timestamp: &str,
    request_id: &str,
) -> ExchangeApiResult<ZebpayPrivateHeaders> {
    let client_id = client_id.trim();
    let access_token = access_token.trim();
    if client_id.is_empty() || access_token.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "zebpay private request spec requires non-empty client_id and access_token"
                .to_string(),
        });
    }
    Ok(ZebpayPrivateHeaders {
        client_id: client_id.to_string(),
        timestamp: timestamp.to_string(),
        request_id: request_id.to_string(),
        authorization: format!("Bearer {access_token}"),
    })
}
