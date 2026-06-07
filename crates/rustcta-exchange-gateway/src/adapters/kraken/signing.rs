#![allow(dead_code)]

use std::collections::BTreeMap;

use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::{Digest, Sha256, Sha512};

type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KrakenSpotSignedRequest {
    pub path: String,
    pub body: String,
    pub api_key: String,
    pub api_sign: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KrakenFuturesSignedRequest {
    pub path: String,
    pub query_or_body: String,
    pub api_key: String,
    pub nonce: String,
    pub authent: String,
}

pub fn sign_spot_request(
    api_key: &str,
    api_secret: &str,
    endpoint: &str,
    mut params: BTreeMap<String, String>,
    nonce: &str,
) -> ExchangeApiResult<KrakenSpotSignedRequest> {
    params.insert("nonce".to_string(), nonce.to_string());
    let path = format!("/0/private/{}", endpoint.trim_start_matches('/'));
    let body = form_encode(&params);
    let api_sign = kraken_spot_signature(api_secret, &path, &body)?;
    Ok(KrakenSpotSignedRequest {
        path,
        body,
        api_key: api_key.to_string(),
        api_sign,
    })
}

pub fn kraken_spot_signature(
    api_secret: &str,
    path: &str,
    encoded_payload: &str,
) -> ExchangeApiResult<String> {
    let nonce = encoded_payload
        .split('&')
        .find_map(|part| part.strip_prefix("nonce="))
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "Kraken spot signed payload requires nonce".to_string(),
        })?;
    let mut sha = Sha256::new();
    sha.update(nonce.as_bytes());
    sha.update(encoded_payload.as_bytes());
    let payload_hash = sha.finalize();
    let mut message = Vec::with_capacity(path.len() + payload_hash.len());
    message.extend_from_slice(path.as_bytes());
    message.extend_from_slice(&payload_hash);
    hmac_sha512_base64(api_secret, &message)
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

    use super::{
        kraken_spot_signature, sign_futures_request, sign_futures_ws_challenge, sign_spot_request,
    };

    fn signing_fixture(name: &str) -> serde_json::Value {
        let text = match name {
            "spot_add_order_official.json" => include_str!(
                "../../../../../tests/fixtures/exchanges/kraken/signing_vectors/spot_add_order_official.json"
            ),
            "futures_openorders.json" => include_str!(
                "../../../../../tests/fixtures/exchanges/kraken/signing_vectors/futures_openorders.json"
            ),
            "futures_ws_challenge.json" => include_str!(
                "../../../../../tests/fixtures/exchanges/kraken/signing_vectors/futures_ws_challenge.json"
            ),
            _ => panic!("unknown kraken signing fixture {name}"),
        };
        serde_json::from_str(text).expect("kraken signing fixture")
    }

    #[test]
    fn kraken_spot_signature_should_match_official_vector() {
        let vector = signing_fixture("spot_add_order_official.json");
        let signature = kraken_spot_signature(
            vector["secret"].as_str().unwrap(),
            vector["path"].as_str().unwrap(),
            vector["body"].as_str().unwrap(),
        )
        .expect("signature");

        assert_eq!(signature, vector["expected_signature"].as_str().unwrap());
    }

    #[test]
    fn kraken_spot_signed_request_should_build_form_body_and_headers() {
        let mut params = BTreeMap::new();
        params.insert("pair".to_string(), "XBTUSD".to_string());
        let request = sign_spot_request(
            "key",
            "kQH5HW/8p1uGOVjbgWA7FunAmGO8lsSUXNsu3eow76sz84Q18fWxnyRzBHCd3pd5nE9qa99HAZtuZuj6F1huXg==",
            "Balance",
            params,
            "1616492376594",
        )
        .expect("request");

        assert_eq!(request.path, "/0/private/Balance");
        assert_eq!(request.api_key, "key");
        assert!(request.body.contains("nonce=1616492376594"));
        assert!(request.body.contains("pair=XBTUSD"));
        assert!(!request.api_sign.is_empty());
    }

    #[test]
    fn kraken_futures_signed_request_should_include_new_auth_inputs() {
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
    fn kraken_futures_ws_challenge_signature_should_be_deterministic() {
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
