use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_hex(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Coincheck API secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::sign_hex;

    #[test]
    fn coincheck_signing_should_match_vector() {
        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/coincheck/signing_vectors/private_get_balance.json"
        ))
        .expect("vector");
        assert_eq!(
            sign_hex(
                vector["secret"].as_str().unwrap(),
                vector["payload"].as_str().unwrap(),
            )
            .unwrap(),
            vector["expected_signature"].as_str().unwrap()
        );
    }
}
