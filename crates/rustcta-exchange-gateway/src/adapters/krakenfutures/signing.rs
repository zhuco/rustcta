#![allow(dead_code)]

use std::collections::BTreeMap;

use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::{Digest, Sha256, Sha512};

type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KrakenFuturesSignedRequest {
    pub path: String,
    pub query_or_body: String,
    pub api_key: String,
    pub nonce: String,
    pub authent: String,
}

pub fn sign_futures_request(
    api_key: &str,
    api_secret: &str,
    endpoint_path: &str,
    params: &BTreeMap<String, String>,
    nonce: &str,
) -> ExchangeApiResult<KrakenFuturesSignedRequest> {
    let query_or_body = form_encode(params);
    let message = format!("{query_or_body}{nonce}{endpoint_path}");
    let authent = hmac_sha512_base64(api_secret, message.as_bytes())?;
    Ok(KrakenFuturesSignedRequest {
        path: endpoint_path.to_string(),
        query_or_body,
        api_key: api_key.to_string(),
        nonce: nonce.to_string(),
        authent,
    })
}

pub fn sign_futures_ws_challenge(api_secret: &str, challenge: &str) -> ExchangeApiResult<String> {
    let challenge = challenge.trim();
    if challenge.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "Kraken Futures WebSocket challenge must not be empty".to_string(),
        });
    }
    let digest = Sha256::digest(challenge.as_bytes());
    hmac_sha512_base64(api_secret, &digest)
}

pub fn form_encode(params: &BTreeMap<String, String>) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{}={}", url_encode(key), url_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn hmac_sha512_base64(secret: &str, message: &[u8]) -> ExchangeApiResult<String> {
    let secret = general_purpose::STANDARD.decode(secret).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("Kraken API secret must be base64: {error}"),
        }
    })?;
    let mut mac =
        HmacSha512::new_from_slice(&secret).map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid Kraken API secret: {error}"),
        })?;
    mac.update(message);
    Ok(general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn url_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{sign_futures_request, sign_futures_ws_challenge};

    fn signing_fixture(name: &str) -> serde_json::Value {
        let text = match name {
            "futures_openorders.json" => include_str!(
                "../../../../../tests/fixtures/exchanges/krakenfutures/signing_vectors/futures_openorders.json"
            ),
            "futures_ws_challenge.json" => include_str!(
                "../../../../../tests/fixtures/exchanges/krakenfutures/signing_vectors/futures_ws_challenge.json"
            ),
            _ => panic!("unknown krakenfutures signing fixture {name}"),
        };
        serde_json::from_str(text).expect("krakenfutures signing fixture")
    }

    #[test]
    fn krakenfutures_futures_signed_request_should_include_new_auth_inputs() {
        let vector = signing_fixture("futures_openorders.json");
        let mut params = BTreeMap::new();
        params.insert("symbol".to_string(), "PF_XBTUSDT".to_string());
        let request = sign_futures_request(
            vector["api_key"].as_str().unwrap(),
            vector["secret"].as_str().unwrap(),
            vector["endpoint_path"].as_str().unwrap(),
            &params,
            vector["nonce"].as_str().unwrap(),
        )
        .expect("request");

        assert_eq!(request.api_key, vector["api_key"].as_str().unwrap());
        assert_eq!(request.nonce, vector["nonce"].as_str().unwrap());
        assert_eq!(
            request.query_or_body,
            vector["expected_query_or_body"].as_str().unwrap()
        );
        assert_eq!(
            request.authent,
            vector["expected_authent"].as_str().unwrap()
        );
    }

    #[test]
    fn krakenfutures_futures_ws_challenge_signature_should_be_deterministic() {
        let vector = signing_fixture("futures_ws_challenge.json");
        let secret = vector["secret"].as_str().unwrap();
        let challenge = vector["challenge"].as_str().unwrap();
        let first = sign_futures_ws_challenge(secret, challenge).expect("first");
        let second = sign_futures_ws_challenge(secret, challenge).expect("second");

        assert_eq!(first, second);
        assert_eq!(first, vector["expected_signature"].as_str().unwrap());
        assert!(sign_futures_ws_challenge(secret, " ").is_err());
    }
}
