use base64::{engine::general_purpose, Engine as _};
use serde_json::Value;

pub fn sign_body(body: &str, api_key: &str) -> String {
    let encoded = general_purpose::STANDARD.encode(body.as_bytes());
    format!("{:x}", md5::compute(format!("{encoded}{api_key}")))
}

pub fn canonical_body(value: Option<&Value>) -> Result<String, serde_json::Error> {
    value
        .map(serde_json::to_string)
        .transpose()
        .map(|body| body.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{canonical_body, sign_body};

    #[test]
    fn cryptomus_signature_should_match_documented_md5_shape() {
        let body = canonical_body(Some(&json!({"market":"TRX_USDT","quantity":"20"}))).unwrap();
        let signature = sign_body(&body, "fixture-api-key");
        assert_eq!(signature, "7a25ed96c474b99df63c8caad23459bc");
    }
}
