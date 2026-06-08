use base64::{engine::general_purpose, Engine as _};

pub fn basic_auth_value(api_key: &str, api_secret: &str) -> String {
    format!(
        "Basic {}",
        general_purpose::STANDARD.encode(format!("{api_key}:{api_secret}").as_bytes())
    )
}

#[cfg(test)]
mod tests {
    use super::basic_auth_value;

    #[test]
    fn bequant_basic_auth_should_match_vector() {
        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bequant/signing_vectors/basic_auth.json"
        ))
        .expect("vector");
        assert_eq!(
            basic_auth_value(
                vector["api_key"].as_str().unwrap(),
                vector["api_secret"].as_str().unwrap(),
            ),
            vector["expected_authorization"].as_str().unwrap()
        );
    }
}
