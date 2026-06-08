#![allow(dead_code)]

use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbankSignedRequest {
    pub method: String,
    pub path_or_body: String,
    pub access_key: String,
    pub access_nonce: String,
    pub access_signature: String,
}

pub fn sign_get_request(
    api_key: &str,
    api_secret: &str,
    nonce: &str,
    path_with_query: &str,
) -> ExchangeApiResult<BitbankSignedRequest> {
    let payload = format!("{nonce}{path_with_query}");
    Ok(BitbankSignedRequest {
        method: "GET".to_string(),
        path_or_body: path_with_query.to_string(),
        access_key: api_key.to_string(),
        access_nonce: nonce.to_string(),
        access_signature: hmac_sha256_hex(api_secret, &payload)?,
    })
}

pub fn sign_body_request(
    api_key: &str,
    api_secret: &str,
    nonce: &str,
    method: &str,
    body: &str,
) -> ExchangeApiResult<BitbankSignedRequest> {
    let payload = format!("{nonce}{body}");
    Ok(BitbankSignedRequest {
        method: method.to_ascii_uppercase(),
        path_or_body: body.to_string(),
        access_key: api_key.to_string(),
        access_nonce: nonce.to_string(),
        access_signature: hmac_sha256_hex(api_secret, &payload)?,
    })
}

pub fn hmac_sha256_hex(api_secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid bitbank API secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::{sign_body_request, sign_get_request};

    fn signing_fixture(name: &str) -> serde_json::Value {
        let text = match name {
            "assets_get.json" => include_str!(
                "../../../../../tests/fixtures/exchanges/bitbank/signing_vectors/assets_get.json"
            ),
            "place_order_post.json" => include_str!(
                "../../../../../tests/fixtures/exchanges/bitbank/signing_vectors/place_order_post.json"
            ),
            _ => panic!("unknown bitbank signing fixture {name}"),
        };
        serde_json::from_str(text).expect("bitbank signing fixture")
    }

    #[test]
    fn bitbank_get_signature_should_match_fixture() {
        let vector = signing_fixture("assets_get.json");
        let request = sign_get_request(
            vector["api_key"].as_str().unwrap(),
            vector["api_secret"].as_str().unwrap(),
            vector["nonce"].as_str().unwrap(),
            vector["path"].as_str().unwrap(),
        )
        .expect("request");
        assert_eq!(
            request.access_signature,
            vector["expected_signature"].as_str().unwrap()
        );
    }

    #[test]
    fn bitbank_post_signature_should_match_fixture() {
        let vector = signing_fixture("place_order_post.json");
        let request = sign_body_request(
            vector["api_key"].as_str().unwrap(),
            vector["api_secret"].as_str().unwrap(),
            vector["nonce"].as_str().unwrap(),
            vector["method"].as_str().unwrap(),
            vector["body"].as_str().unwrap(),
        )
        .expect("request");
        assert_eq!(
            request.access_signature,
            vector["expected_signature"].as_str().unwrap()
        );
    }
}
