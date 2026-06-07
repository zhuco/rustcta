use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const MAX_PARAM_DEPTH: u8 = 3;

pub fn sign_request(
    secret: &str,
    method: &str,
    request_id: u64,
    api_key: &str,
    params: &Value,
    nonce: i64,
) -> String {
    let payload = format!(
        "{method}{request_id}{api_key}{}{nonce}",
        params_to_string(params, 0)
    );
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC-SHA256 accepts arbitrary key length");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn params_to_string(value: &Value, depth: u8) -> String {
    if depth >= MAX_PARAM_DEPTH {
        return primitive_to_string(value);
    }
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            keys.into_iter()
                .map(|key| format!("{key}{}", params_to_string(&map[key], depth + 1)))
                .collect::<Vec<_>>()
                .join("")
        }
        Value::Array(items) => items
            .iter()
            .map(|item| params_to_string(item, depth + 1))
            .collect::<Vec<_>>()
            .join(""),
        _ => primitive_to_string(value),
    }
}

fn primitive_to_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{params_to_string, sign_request};

    #[test]
    fn cryptocom_signing_should_sort_nested_params_like_official_algorithm() {
        let params = json!({
            "order_list": [
                {"side": "BUY", "quantity": "1.0", "price": "0.24", "instrument_name": "ONE_USDT"}
            ],
            "contingency_type": "LIST"
        });

        assert_eq!(
            params_to_string(&params, 0),
            "contingency_typeLISTorder_listinstrument_nameONE_USDTprice0.24quantity1.0sideBUY"
        );
        let signature = sign_request(
            "SECRET_KEY",
            "private/create-order-list",
            14,
            "API_KEY",
            &params,
            1_610_905_028_000,
        );
        assert_eq!(signature.len(), 64);
    }
}
