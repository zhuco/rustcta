use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn paymium_rest_signature(
    nonce: &str,
    full_url: &str,
    body: &str,
    api_secret: &str,
) -> ExchangeApiResult<String> {
    let payload = format!("{nonce}{full_url}{body}");
    let mut mac = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Paymium API secret length: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn paymium_private_headers(
    api_key: &str,
    api_secret: &str,
    nonce: &str,
    full_url: &str,
    body: &str,
) -> ExchangeApiResult<Vec<(String, String)>> {
    Ok(vec![
        ("Api-Key".to_string(), api_key.to_string()),
        (
            "Api-Signature".to_string(),
            paymium_rest_signature(nonce, full_url, body, api_secret)?,
        ),
        ("Api-Nonce".to_string(), nonce.to_string()),
    ])
}

#[cfg(test)]
mod tests {
    use super::{paymium_private_headers, paymium_rest_signature};

    #[test]
    fn paymium_rest_signature_should_be_hmac_sha256_hex() {
        let signature = paymium_rest_signature(
            "1700000000000",
            "https://paymium.com/api/v1/user/orders",
            r#"{"currency":"EUR"}"#,
            "fixture-secret",
        )
        .expect("signature");
        assert_eq!(
            signature,
            "764b5fae6a293d027c6d9228806f4bf82d8b147b6d39ee1e6a129a0fe95ac3d8"
        );
    }

    #[test]
    fn paymium_private_headers_should_include_required_api_headers() {
        let headers = paymium_private_headers(
            "fixture-key",
            "fixture-secret",
            "1700000000000",
            "https://paymium.com/api/v1/user/orders",
            "",
        )
        .expect("headers");
        assert_eq!(
            headers[0],
            ("Api-Key".to_string(), "fixture-key".to_string())
        );
        assert_eq!(headers[1].0, "Api-Signature");
        assert_eq!(
            headers[2],
            ("Api-Nonce".to_string(), "1700000000000".to_string())
        );
    }
}
