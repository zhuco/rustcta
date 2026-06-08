#![allow(dead_code)]

use std::collections::BTreeMap;

use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{json, Value};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_v2(
    secret: &str,
    method: &str,
    host: &str,
    path: &str,
    params: &BTreeMap<String, String>,
) -> ExchangeApiResult<String> {
    let query = canonical_query(params);
    let payload = signing_payload(method, host, path, &query);
    hmac_sha256_base64(secret, &payload)
}

pub fn signing_payload(method: &str, host: &str, path: &str, query: &str) -> String {
    format!(
        "{}\n{}\n{}\n{}",
        method.to_ascii_uppercase(),
        host.to_ascii_lowercase(),
        path,
        query
    )
}

pub fn canonical_query(params: &BTreeMap<String, String>) -> String {
    params
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

pub fn auth_params(api_key: &str, timestamp: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("AccessKeyId".to_string(), api_key.to_string()),
        ("SignatureMethod".to_string(), "HmacSHA256".to_string()),
        ("SignatureVersion".to_string(), "2".to_string()),
        ("Timestamp".to_string(), timestamp.to_string()),
    ])
}

pub fn signed_params(
    api_key: &str,
    api_secret: &str,
    method: &str,
    host: &str,
    path: &str,
    timestamp: &str,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    signed_params_with_extra(api_key, api_secret, method, host, path, timestamp, [])
}

pub fn signed_params_with_extra(
    api_key: &str,
    api_secret: &str,
    method: &str,
    host: &str,
    path: &str,
    timestamp: &str,
    extra: impl IntoIterator<Item = (String, String)>,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    let mut params = auth_params(api_key, timestamp);
    params.extend(extra);
    let signature = sign_v2(api_secret, method, host, path, &params)?;
    params.insert("Signature".to_string(), signature);
    Ok(params)
}

pub fn private_ws_auth_payload(
    api_key: &str,
    api_secret: &str,
    host: &str,
    timestamp: &str,
) -> ExchangeApiResult<Value> {
    let params = BTreeMap::from([
        ("accessKey".to_string(), api_key.to_string()),
        ("signatureMethod".to_string(), "HmacSHA256".to_string()),
        ("signatureVersion".to_string(), "2.1".to_string()),
        ("timestamp".to_string(), timestamp.to_string()),
    ]);
    let signature = sign_v2(api_secret, "GET", host, "/ws/v2", &params)?;
    Ok(json!({
        "action": "req",
        "ch": "auth",
        "params": {
            "authType": "api",
            "accessKey": api_key,
            "signatureMethod": "HmacSHA256",
            "signatureVersion": "2.1",
            "timestamp": timestamp,
            "signature": signature,
        }
    }))
}

fn hmac_sha256_base64(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid BitTrade API secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        mac.finalize().into_bytes(),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::Deserialize;

    use super::{canonical_query, private_ws_auth_payload, sign_v2, signing_payload};

    #[derive(Debug, Deserialize)]
    struct BittradeSigningVector {
        secret: String,
        method: String,
        host: String,
        request_path: String,
        query: BTreeMap<String, String>,
        payload: String,
        expected_signature: String,
    }

    #[test]
    fn bittrade_rest_signatures_should_match_fixture_vectors() {
        for name in ["rest_get_order.json", "rest_place_order.json"] {
            let vector = load_signing_vector(name);
            let payload = signing_payload(
                &vector.method,
                &vector.host,
                &vector.request_path,
                &canonical_query(&vector.query),
            );
            assert_eq!(payload, vector.payload);
            let actual = sign_v2(
                &vector.secret,
                &vector.method,
                &vector.host,
                &vector.request_path,
                &vector.query,
            )
            .expect("signature");
            assert_eq!(actual, vector.expected_signature);
        }
    }

    #[test]
    fn bittrade_private_ws_auth_should_match_fixture_vector() {
        let vector = load_signing_vector("private_ws_auth.json");
        let actual = sign_v2(
            &vector.secret,
            &vector.method,
            &vector.host,
            &vector.request_path,
            &vector.query,
        )
        .expect("signature");
        assert_eq!(actual, vector.expected_signature);
        let payload = private_ws_auth_payload(
            vector.query.get("accessKey").unwrap(),
            &vector.secret,
            &vector.host,
            vector.query.get("timestamp").unwrap(),
        )
        .expect("ws payload");
        assert_eq!(
            payload["params"]["signature"].as_str(),
            Some(vector.expected_signature.as_str())
        );
    }

    fn load_signing_vector(name: &str) -> BittradeSigningVector {
        let path = format!(
            "{}/../../tests/fixtures/exchanges/bittrade/signing_vectors/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(path).expect("signing vector fixture");
        serde_json::from_str(&text).expect("signing vector fixture")
    }
}
