use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_payload(
    api_secret: &str,
    timestamp: i64,
    method: &str,
    path: &str,
    body: &str,
) -> ExchangeApiResult<String> {
    let payload = format!("{timestamp}{}{}{}", method.to_ascii_uppercase(), path, body);
    let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Biconomy API secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::sign_payload;

    #[derive(Deserialize)]
    struct SigningVector {
        secret: String,
        timestamp: i64,
        method: String,
        request_path: String,
        body: String,
        expected_signature: String,
    }

    #[test]
    fn biconomy_signing_should_include_timestamp_method_path_and_body() {
        let vector: SigningVector = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/biconomy/signing_vectors/private_post_order.json"
        ))
        .expect("signing vector");
        let signature = sign_payload(
            &vector.secret,
            vector.timestamp,
            &vector.method,
            &vector.request_path,
            &vector.body,
        )
        .expect("signature");
        assert_eq!(signature, vector.expected_signature);
    }
}
