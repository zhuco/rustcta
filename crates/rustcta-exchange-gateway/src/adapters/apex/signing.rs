use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub fn apex_sorted_form_body(fields: &[(&str, Option<&str>)]) -> String {
    let mut pairs = fields
        .iter()
        .filter_map(|(key, value)| value.map(|value| (*key, value)))
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn apex_signing_payload(timestamp: &str, method: &str, path: &str, data: &str) -> String {
    format!("{timestamp}{}{path}{data}", method.to_ascii_uppercase())
}

pub fn apex_hmac_signature(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let key = base64::engine::general_purpose::STANDARD
        .decode(secret)
        .unwrap_or_else(|_| secret.as_bytes().to_vec());
    let mut mac =
        Hmac::<Sha256>::new_from_slice(&key).map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid ApeX HMAC secret: {error}"),
        })?;
    mac.update(payload.as_bytes());
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use base64::Engine;

    use super::{apex_hmac_signature, apex_signing_payload, apex_sorted_form_body};

    #[test]
    fn apex_form_body_should_sort_fields_and_skip_none() {
        assert_eq!(
            apex_sorted_form_body(&[
                ("symbol", Some("BTC-USDT")),
                ("price", Some("65000")),
                ("ignored", None),
                ("size", Some("0.01")),
            ]),
            "price=65000&size=0.01&symbol=BTC-USDT"
        );
    }

    #[test]
    fn apex_payload_should_concatenate_timestamp_method_path_and_data() {
        assert_eq!(
            apex_signing_payload(
                "2026-06-08T00:00:00.000Z",
                "post",
                "/api/v3/delete-order",
                "id=123&symbol=BTC-USDT",
            ),
            "2026-06-08T00:00:00.000ZPOST/api/v3/delete-orderid=123&symbol=BTC-USDT"
        );
    }

    #[test]
    fn apex_hmac_signature_should_be_base64() {
        let signature = apex_hmac_signature("secret", "payload").expect("signature");
        assert!(base64::engine::general_purpose::STANDARD
            .decode(signature)
            .is_ok());
    }
}
