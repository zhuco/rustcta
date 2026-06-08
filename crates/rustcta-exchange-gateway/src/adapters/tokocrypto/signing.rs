#![allow(dead_code)]

use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn sign_raw_query(api_secret: &str, query: &str) -> ExchangeApiResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Tokocrypto API secret length: {error}"),
        }
    })?;
    mac.update(query.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn append_signature(api_secret: &str, query: &str) -> ExchangeApiResult<String> {
    let signature = sign_raw_query(api_secret, query)?;
    if query.is_empty() {
        Ok(format!("signature={signature}"))
    } else {
        Ok(format!("{query}&signature={signature}"))
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::{append_signature, sign_raw_query};

    #[derive(Debug, Deserialize)]
    struct SigningVector {
        api_secret: String,
        query: String,
        expected_signature: String,
        expected_signed_query: String,
    }

    #[test]
    fn tokocrypto_signature_vectors_should_match_fixtures() {
        for name in ["place_order.json", "query_order.json", "listen_token.json"] {
            let vector = load_vector(name);
            assert_eq!(
                sign_raw_query(&vector.api_secret, &vector.query).expect("signature"),
                vector.expected_signature
            );
            assert_eq!(
                append_signature(&vector.api_secret, &vector.query).expect("signed query"),
                vector.expected_signed_query
            );
        }
    }

    fn load_vector(name: &str) -> SigningVector {
        let path = format!(
            "{}/../../tests/fixtures/exchanges/tokocrypto/signing_vectors/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(path).expect("signing vector fixture");
        serde_json::from_str(&text).expect("signing vector fixture")
    }
}
