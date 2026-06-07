use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::{Digest, Sha256};

pub fn sign_request(
    api_key: &str,
    api_secret: &str,
    nonce: &str,
    timestamp: &str,
    query_params: &str,
    body: &str,
) -> ExchangeApiResult<String> {
    if api_secret.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "invalid Bitunix API secret".to_string(),
        });
    }
    let digest = sha256_hex(&format!("{nonce}{timestamp}{api_key}{query_params}{body}"));
    Ok(sha256_hex(&format!("{digest}{api_secret}")))
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}
