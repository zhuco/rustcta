use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::config::IndependentReserveGatewayConfig;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct IndependentReservePrivateCredentials {
    pub api_key: String,
    pub api_secret: String,
}

impl IndependentReservePrivateCredentials {
    pub fn from_config(config: &IndependentReserveGatewayConfig) -> Option<Self> {
        if !config.private_rest_available() {
            return None;
        }
        Some(Self {
            api_key: config.api_key.clone().unwrap_or_default(),
            api_secret: config.api_secret.clone().unwrap_or_default(),
        })
    }
}

pub fn independentreserve_v1_signature(
    endpoint: &str,
    nonce: i64,
    api_key: &str,
    params: &[(String, String)],
    api_secret: &str,
) -> String {
    let mut payload = format!("{endpoint},{nonce},{api_key}");
    append_sorted_params(&mut payload, params);
    hmac_sha256_hex_upper(api_secret, &payload)
}

pub fn independentreserve_v2_signature(
    path: &str,
    nonce: i64,
    api_key: &str,
    params: &[(String, String)],
    api_secret: &str,
) -> String {
    let mut payload = format!("{path},{nonce},{api_key}");
    append_sorted_params(&mut payload, params);
    hmac_sha256_hex_upper(api_secret, &payload)
}

fn append_sorted_params(payload: &mut String, params: &[(String, String)]) {
    let mut sorted = params.iter().collect::<Vec<_>>();
    sorted.sort_by(|left, right| left.0.cmp(&right.0));
    for (key, value) in sorted {
        payload.push(',');
        payload.push_str(key);
        payload.push('=');
        payload.push_str(value);
    }
}

fn hmac_sha256_hex_upper(secret: &str, payload: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(payload.as_bytes());
    hex::encode_upper(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::{independentreserve_v1_signature, independentreserve_v2_signature};

    #[test]
    fn independentreserve_v1_signature_should_match_fixture_vector() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/independentreserve/signing_vectors/v1_nonce_signature.json"
        ))
        .expect("v1 signing vector");
        let params = fixture_params(&fixture);

        assert_eq!(
            independentreserve_v1_signature(
                fixture["endpoint"].as_str().expect("endpoint"),
                fixture["nonce"].as_i64().expect("nonce"),
                fixture["api_key"].as_str().expect("api key"),
                &params,
                fixture["api_secret"].as_str().expect("api secret"),
            ),
            fixture["expected_signature"].as_str().expect("expected")
        );
    }

    #[test]
    fn independentreserve_v2_signature_should_match_fixture_vector() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/independentreserve/signing_vectors/v2_nonce_signature.json"
        ))
        .expect("v2 signing vector");
        let params = fixture_params(&fixture);

        assert_eq!(
            independentreserve_v2_signature(
                fixture["path"].as_str().expect("path"),
                fixture["nonce"].as_i64().expect("nonce"),
                fixture["api_key"].as_str().expect("api key"),
                &params,
                fixture["api_secret"].as_str().expect("api secret"),
            ),
            fixture["expected_signature"].as_str().expect("expected")
        );
    }

    fn fixture_params(value: &serde_json::Value) -> Vec<(String, String)> {
        value["params"]
            .as_object()
            .expect("params")
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().expect("string").to_string()))
            .collect()
    }
}
