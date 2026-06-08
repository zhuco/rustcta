#![allow(dead_code)]

use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha512;

type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZaifSignedRequest {
    pub key: String,
    pub sign: String,
    pub body: String,
}

pub fn sign_form_request(
    api_key: &str,
    api_secret: &str,
    body: &str,
) -> ExchangeApiResult<ZaifSignedRequest> {
    Ok(ZaifSignedRequest {
        key: api_key.to_string(),
        sign: hmac_sha512_hex(api_secret, body)?,
        body: body.to_string(),
    })
}

pub fn hmac_sha512_hex(api_secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha512::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid zaif API secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::sign_form_request;

    #[test]
    fn zaif_form_signature_should_match_fixture() {
        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/zaif/signing_vectors/trade_hmac_sha512.json"
        ))
        .expect("vector");
        let signed = sign_form_request(
            vector["api_key"].as_str().unwrap(),
            vector["api_secret"].as_str().unwrap(),
            vector["body"].as_str().unwrap(),
        )
        .expect("signature");
        assert_eq!(signed.key, vector["api_key"].as_str().unwrap());
        assert_eq!(signed.sign, vector["expected_signature"].as_str().unwrap());
    }
}
