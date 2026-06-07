use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_payload(
    api_secret: &str,
    timestamp: i64,
    method: &str,
    path: &str,
    body: &str,
) -> ExchangeApiResult<String> {
    let payload = format!("{timestamp}{}{}{}", method.to_ascii_uppercase(), path, body);
    let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Cointr API secret: {error}"),
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
    use super::sign_payload;
    use crate::signing_spec::SigningVector;

    #[test]
    fn cointr_signing_should_include_timestamp_method_path_and_body() {
        let signature = sign_payload(
            "secret",
            1_700_000_000_000,
            "post",
            "/api/v2/private/order",
            r#"{"symbol":"BTCUSDT"}"#,
        )
        .expect("signature");
        assert_eq!(signature.len(), 44);
        assert_eq!(
            signature,
            sign_payload(
                "secret",
                1_700_000_000_000,
                "POST",
                "/api/v2/private/order",
                r#"{"symbol":"BTCUSDT"}"#,
            )
            .expect("signature")
        );
    }

    #[test]
    fn cointr_signing_vectors_should_verify_rest_and_private_ws_payloads() {
        for fixture in [
            "cointr/signing_vectors/rest_place_order_spot_limit.json",
            "cointr/signing_vectors/private_ws_login.json",
        ] {
            let vector = load_signing_vector(fixture);
            vector.verify().expect("signing vector verifies");
            let timestamp = vector
                .timestamp
                .as_deref()
                .expect("timestamp")
                .parse()
                .expect("timestamp is i64");
            let actual = sign_payload(
                &vector.secret,
                timestamp,
                vector.method.as_deref().expect("method"),
                vector.request_path.as_deref().expect("request path"),
                vector.body.as_deref().unwrap_or_default(),
            )
            .expect("adapter signature");
            assert_eq!(actual, vector.expected_signature);
        }
    }

    fn load_signing_vector(path: &str) -> SigningVector {
        let path = format!(
            "{}/../../tests/fixtures/exchanges/{path}",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(path).expect("signing vector fixture");
        serde_json::from_str(&text).expect("signing vector fixture")
    }
}
