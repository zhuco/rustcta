use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_base64_decoded_secret(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let key = general_purpose::STANDARD
        .decode(secret.trim())
        .unwrap_or_else(|_| secret.as_bytes().to_vec());
    let mut mac =
        HmacSha256::new_from_slice(&key).map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid Coinbase Exchange API secret: {error}"),
        })?;
    mac.update(payload.as_bytes());
    Ok(general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

pub fn ws_auth_signature(secret: &str, timestamp: &str) -> ExchangeApiResult<String> {
    sign_base64_decoded_secret(secret, &format!("{timestamp}GET/users/self/verify"))
}

#[cfg(test)]
mod tests {
    use super::sign_base64_decoded_secret;

    #[test]
    fn coinbaseexchange_signing_should_match_vector() {
        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/coinbaseexchange/signing_vectors/private_get_accounts.json"
        ))
        .expect("vector");
        let signature = sign_base64_decoded_secret(
            vector["secret"].as_str().unwrap(),
            vector["payload"].as_str().unwrap(),
        )
        .expect("signature");
        assert_eq!(signature, vector["expected_signature"].as_str().unwrap());
    }
}
