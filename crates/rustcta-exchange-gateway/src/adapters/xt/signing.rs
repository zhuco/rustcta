use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use sha2::Sha256;

pub fn sign_payload(secret: &str, payload: &str) -> ExchangeApiResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid XT API secret length: {error}"),
        }
    })?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn spot_signature_payload(
    api_key: &str,
    timestamp_ms: i64,
    recv_window_ms: u64,
    method: &str,
    endpoint: &str,
    query: &str,
    body: &str,
) -> String {
    let headers = format!(
        "validate-algorithms=HmacSHA256&validate-appkey={api_key}&validate-recvwindow={recv_window_ms}&validate-timestamp={timestamp_ms}"
    );
    let data = method_path_payload(method, endpoint, query, body);
    format!("{headers}{data}")
}

pub fn futures_signature_payload(
    api_key: &str,
    timestamp_ms: i64,
    endpoint: &str,
    query: &str,
    body: &str,
) -> String {
    let headers = format!("validate-appkey={api_key}&validate-timestamp={timestamp_ms}");
    if !query.is_empty() {
        format!("{headers}#{endpoint}#{query}")
    } else if !body.is_empty() {
        format!("{headers}#{endpoint}#{body}")
    } else {
        format!("{headers}#{endpoint}")
    }
}

fn method_path_payload(method: &str, endpoint: &str, query: &str, body: &str) -> String {
    if !query.is_empty() && !body.is_empty() {
        format!("#{method}#{endpoint}#{query}#{body}")
    } else if !query.is_empty() {
        format!("#{method}#{endpoint}#{query}")
    } else if !body.is_empty() {
        format!("#{method}#{endpoint}#{body}")
    } else {
        format!("#{method}#{endpoint}")
    }
}

#[cfg(test)]
mod tests {
    use super::{futures_signature_payload, sign_payload, spot_signature_payload};

    #[test]
    fn xt_spot_signature_payload_should_match_documented_shape() {
        let payload = spot_signature_payload(
            "2063495b-85ec-41b3-a810-be84ceb78751",
            1666026215729,
            60000,
            "POST",
            "/v4/order",
            "",
            r#"{"symbol":"XT_USDT","side":"BUY","type":"LIMIT","timeInForce":"GTC","bizType":"SPOT","price":3,"quantity":2}"#,
        );
        assert_eq!(
            payload,
            r#"validate-algorithms=HmacSHA256&validate-appkey=2063495b-85ec-41b3-a810-be84ceb78751&validate-recvwindow=60000&validate-timestamp=1666026215729#POST#/v4/order#{"symbol":"XT_USDT","side":"BUY","type":"LIMIT","timeInForce":"GTC","bizType":"SPOT","price":3,"quantity":2}"#
        );
    }

    #[test]
    fn xt_futures_signature_payload_should_match_documented_shape() {
        let payload = futures_signature_payload(
            "++++++",
            12345,
            "/future/user/v1/balance/detail",
            "coin=btc",
            "",
        );
        assert_eq!(
            payload,
            "validate-appkey=++++++&validate-timestamp=12345#/future/user/v1/balance/detail#coin=btc"
        );
    }

    #[test]
    fn xt_signature_should_be_hmac_sha256_hex() {
        let signature = sign_payload("secret", "payload").expect("signature");
        assert_eq!(
            signature,
            "b82fcb791acec57859b989b430a826488ce2e479fdf92326bd0a2e8375a42ba4"
        );
    }
}
