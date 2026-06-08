use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;
use std::collections::BTreeMap;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_v2(
    secret: &str,
    method: &str,
    host: &str,
    path: &str,
    params: &BTreeMap<String, String>,
) -> ExchangeApiResult<String> {
    let query = canonical_query(params);
    let payload = format!(
        "{}\n{}\n{}\n{}",
        method.to_ascii_uppercase(),
        host,
        path,
        query
    );
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid HTX API secret: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        mac.finalize().into_bytes(),
    ))
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

pub fn signed_params(
    api_key: &str,
    api_secret: &str,
    method: &str,
    host: &str,
    path: &str,
    timestamp: &str,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    let mut params = auth_params(api_key, timestamp);
    let signature = sign_v2(api_secret, method, host, path, &params)?;
    params.insert("Signature".to_string(), signature);
    Ok(params)
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

pub fn auth_params(api_key: &str, timestamp: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("AccessKeyId".to_string(), api_key.to_string()),
        ("SignatureMethod".to_string(), "HmacSHA256".to_string()),
        ("SignatureVersion".to_string(), "2".to_string()),
        ("Timestamp".to_string(), timestamp.to_string()),
    ])
}

#[cfg(test)]
mod tests {
    use super::{sign_v2, signed_params, signed_params_with_extra};
    use serde::Deserialize;
    use std::collections::BTreeMap;

    #[derive(Debug, Deserialize)]
    struct HtxSigningVector {
        secret: String,
        method: String,
        host: String,
        request_path: String,
        query: BTreeMap<String, String>,
        payload: String,
        expected_signature: String,
    }

    #[test]
    fn htx_signing_should_match_fixture_vectors() {
        for fixture in [
            "htx/signing_vectors/rest_linear_place_order.json",
            "htx/signing_vectors/private_ws_auth.json",
        ] {
            let vector = load_signing_vector(fixture);
            let payload = format!(
                "{}\n{}\n{}\n{}",
                vector.method,
                vector.host,
                vector.request_path,
                super::canonical_query(&vector.query)
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
    fn htx_signed_params_should_add_signature() {
        let params = signed_params(
            "key",
            "secret",
            "POST",
            "api.hbdm.com",
            "/linear-swap-api/v1/swap_cross_order",
            "2026-06-08T00:00:00",
        )
        .expect("params");
        assert_eq!(params.get("AccessKeyId").map(String::as_str), Some("key"));
        assert!(params
            .get("Signature")
            .is_some_and(|value| !value.is_empty()));
    }

    #[test]
    fn htx_signed_get_params_should_sign_business_query() {
        let params = signed_params_with_extra(
            "key",
            "secret",
            "GET",
            "api.huobi.pro",
            "/v1/order/openOrders",
            "2026-06-08T00:00:00",
            [("symbol".to_string(), "btcusdt".to_string())],
        )
        .expect("params");
        let mut signing_input = params.clone();
        signing_input.remove("Signature");
        let expected = sign_v2(
            "secret",
            "GET",
            "api.huobi.pro",
            "/v1/order/openOrders",
            &signing_input,
        )
        .expect("signature");
        assert_eq!(params.get("Signature"), Some(&expected));
        assert_eq!(params.get("symbol").map(String::as_str), Some("btcusdt"));
    }

    fn load_signing_vector(path: &str) -> HtxSigningVector {
        let path = format!(
            "{}/../../tests/fixtures/exchanges/{path}",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(path).expect("signing vector fixture");
        serde_json::from_str(&text).expect("signing vector fixture")
    }
}
