use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn rest_hmac_sha256_payload(
    method: &str,
    sign_path: &str,
    expires_s: u64,
    body: Option<&str>,
) -> String {
    let mut payload = format!("{}{}{}", method.to_ascii_uppercase(), sign_path, expires_s);
    if let Some(body) = body.filter(|value| !value.is_empty()) {
        payload.push_str(body);
    }
    payload
}

pub fn ws_connect_hmac_sha256_payload(expires_s: u64) -> String {
    format!("CONNECT/stream{expires_s}")
}

pub fn sign_hmac_sha256_hex(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid HollaEx API secret length: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::{rest_hmac_sha256_payload, sign_hmac_sha256_hex, ws_connect_hmac_sha256_payload};
    use crate::adapters::hollaex::private::place_order_json_body;

    #[test]
    fn hollaex_rest_signing_should_match_fixed_order_vector() {
        let payload = rest_hmac_sha256_payload(
            "POST",
            "/v2/order",
            1_710_000_000,
            Some(place_order_json_body()),
        );
        assert_eq!(
            payload,
            "POST/v2/order1710000000{\"symbol\":\"xht-usdt\",\"side\":\"sell\",\"size\":0.1,\"type\":\"limit\",\"price\":1}"
        );
        assert_eq!(
            sign_hmac_sha256_hex("test-secret", &payload).expect("signature"),
            "fb6f4b9e508511deb347dee3439adcac2edd5a39f644262987d29c0bf5294258"
        );
    }

    #[test]
    fn hollaex_ws_signing_should_match_fixed_connect_vector() {
        let payload = ws_connect_hmac_sha256_payload(1_710_000_000);
        assert_eq!(payload, "CONNECT/stream1710000000");
        assert_eq!(
            sign_hmac_sha256_hex("test-secret", &payload).expect("signature"),
            "f99cca59fee066d50d972736fde368157df8a23a4a1d09bd48d19a09c491f986"
        );
    }
}
