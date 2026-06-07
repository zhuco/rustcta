use std::collections::HashMap;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::config::CoinwGatewayConfig;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct CoinwPrivateCredentials {
    pub api_key: String,
    pub api_secret: String,
}

impl CoinwPrivateCredentials {
    pub fn from_config(config: &CoinwGatewayConfig) -> Option<Self> {
        if !config.private_rest_available() {
            return None;
        }
        Some(Self {
            api_key: config.api_key.clone().unwrap_or_default(),
            api_secret: config.api_secret.clone().unwrap_or_default(),
        })
    }
}

pub fn coinw_spot_signature(params: &HashMap<String, String>, secret_key: &str) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    let mut payload = String::new();
    for (key, value) in pairs {
        payload.push_str(key);
        payload.push('=');
        payload.push_str(value);
        payload.push('&');
    }
    payload.push_str("secret_key=");
    payload.push_str(secret_key);
    format!("{:x}", md5::compute(payload.as_bytes())).to_ascii_uppercase()
}

pub fn coinw_futures_signature(
    secret_key: &str,
    timestamp_ms: i64,
    method: &str,
    path_with_query: &str,
    body: &str,
) -> String {
    let message = format!(
        "{}{}{}{}",
        timestamp_ms,
        method.to_ascii_uppercase(),
        path_with_query,
        body
    );
    let mut mac = HmacSha256::new_from_slice(secret_key.as_bytes())
        .expect("HMAC accepts secret keys of any length");
    mac.update(message.as_bytes());
    base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        mac.finalize().into_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::coinw_futures_signature;
    use super::coinw_spot_signature;

    #[test]
    fn coinw_spot_signature_should_sort_params_and_uppercase_md5() {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), "BTC_USDT".to_string());
        params.insert("api_key".to_string(), "key".to_string());
        params.insert("amount".to_string(), "0.01".to_string());

        let signature = coinw_spot_signature(&params, "secret");

        assert_eq!(signature, "1F739D1FCDD9E1A86F45D47630B27909");
    }

    #[test]
    fn coinw_futures_signature_should_match_hmac_sha256_base64_vector() {
        let signature = coinw_futures_signature(
            "secret",
            1_701_945_494_169,
            "POST",
            "/v1/fapi/open",
            r#"{"instrument":"ADA"}"#,
        );

        assert_eq!(signature, "lHHB2LP6weApCS4ezLsIndvCuZNIEsMnvoB01/O7lXc=");
    }

    #[test]
    fn coinw_signing_fixture_vectors_should_match_helpers() {
        let spot: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/coinw/signing_vectors/spot_md5_query.json"
        ))
        .expect("spot signing vector");
        let futures: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/coinw/signing_vectors/futures_hmac_headers.json"
        ))
        .expect("futures signing vector");

        let spot_params = spot
            .get("params")
            .and_then(serde_json::Value::as_object)
            .expect("spot params")
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    value.as_str().expect("string param").to_string(),
                )
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(
            coinw_spot_signature(&spot_params, spot["secret"].as_str().expect("secret")),
            spot["expected_signature"].as_str().expect("expected")
        );

        assert_eq!(
            coinw_futures_signature(
                futures["secret"].as_str().expect("secret"),
                futures["timestamp_ms"].as_i64().expect("timestamp"),
                futures["method"].as_str().expect("method"),
                futures["path_with_query"].as_str().expect("path"),
                futures["body"].as_str().expect("body"),
            ),
            futures["expected_signature"].as_str().expect("expected")
        );
    }
}
