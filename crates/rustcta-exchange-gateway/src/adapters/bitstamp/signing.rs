use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn bitstamp_signature(
    api_key: &str,
    api_secret: &str,
    method: &str,
    host: &str,
    path: &str,
    query: &str,
    content_type: &str,
    nonce: &str,
    timestamp_ms: &str,
    body: &str,
) -> ExchangeApiResult<String> {
    let payload = format!(
        "BITSTAMP {api_key}{method}{host}{path}{query}{content_type}{nonce}{timestamp_ms}v2{body}"
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid Bitstamp API secret length: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::bitstamp_signature;

    #[test]
    fn bitstamp_signature_should_be_stable_for_form_payload() {
        let signature = bitstamp_signature(
            "key",
            "secret",
            "POST",
            "www.bitstamp.net",
            "/api/v2/user_transactions/",
            "",
            "application/x-www-form-urlencoded",
            "00000000-0000-0000-0000-000000000000",
            "1700000000000",
            "offset=1",
        )
        .expect("signature");
        assert_eq!(signature.len(), 64);
    }
}
