#![allow(dead_code)]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

use super::parser::normalize_zaif_pair;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZaifRequestSpec {
    pub method: String,
    pub path: String,
    pub body: String,
}

pub fn get_info2_spec(nonce: &str) -> ExchangeApiResult<ZaifRequestSpec> {
    form_spec(vec![("nonce", nonce), ("method", "get_info2")])
}

pub fn active_orders_spec(nonce: &str, pair: &str) -> ExchangeApiResult<ZaifRequestSpec> {
    let pair = normalize_zaif_pair(pair)?;
    form_spec(vec![
        ("nonce", nonce),
        ("method", "active_orders"),
        ("currency_pair", pair.as_str()),
    ])
}

pub fn trade_history_spec(
    nonce: &str,
    pair: &str,
    count: u32,
) -> ExchangeApiResult<ZaifRequestSpec> {
    let pair = normalize_zaif_pair(pair)?;
    let count = count.to_string();
    form_spec(vec![
        ("nonce", nonce),
        ("method", "trade_history"),
        ("currency_pair", pair.as_str()),
        ("count", count.as_str()),
    ])
}

pub fn trade_limit_spec(
    nonce: &str,
    pair: &str,
    action: &str,
    price: &str,
    amount: &str,
) -> ExchangeApiResult<ZaifRequestSpec> {
    let pair = normalize_zaif_pair(pair)?;
    form_spec(vec![
        ("nonce", nonce),
        ("method", "trade"),
        ("currency_pair", pair.as_str()),
        ("action", normalize_action(action)?),
        ("price", price),
        ("amount", amount),
    ])
}

pub fn cancel_order_spec(nonce: &str, order_id: &str) -> ExchangeApiResult<ZaifRequestSpec> {
    form_spec(vec![
        ("nonce", nonce),
        ("method", "cancel_order"),
        ("order_id", order_id),
    ])
}

fn form_spec(params: Vec<(&str, &str)>) -> ExchangeApiResult<ZaifRequestSpec> {
    Ok(ZaifRequestSpec {
        method: "POST".to_string(),
        path: "/tapi".to_string(),
        body: params
            .into_iter()
            .map(|(key, value)| format!("{}={}", form_encode(key), form_encode(value)))
            .collect::<Vec<_>>()
            .join("&"),
    })
}

fn normalize_action(action: &str) -> ExchangeApiResult<&'static str> {
    match action.trim().to_ascii_lowercase().as_str() {
        "bid" | "buy" => Ok("bid"),
        "ask" | "sell" => Ok("ask"),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported zaif action {action}"),
        }),
    }
}

fn form_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            b' ' => vec!['+'],
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{
        active_orders_spec, cancel_order_spec, get_info2_spec, trade_history_spec, trade_limit_spec,
    };

    #[test]
    fn zaif_request_specs_should_match_fixtures() {
        let trade =
            trade_limit_spec("1710000000", "btc_jpy", "buy", "6500000", "0.01").expect("trade");
        assert_spec(
            trade,
            include_str!(
                "../../../../../tests/fixtures/exchanges/zaif/request_specs/trade_limit.json"
            ),
        );

        let cancel = cancel_order_spec("1710000001", "123456789").expect("cancel");
        assert_spec(
            cancel,
            include_str!(
                "../../../../../tests/fixtures/exchanges/zaif/request_specs/cancel_order.json"
            ),
        );

        assert_eq!(
            get_info2_spec("1710000002").expect("get_info2").body,
            "nonce=1710000002&method=get_info2"
        );
        assert_eq!(
            active_orders_spec("1710000003", "btc_jpy")
                .expect("active")
                .body,
            "nonce=1710000003&method=active_orders&currency_pair=btc_jpy"
        );
        assert_eq!(
            trade_history_spec("1710000004", "btc_jpy", 10)
                .expect("history")
                .body,
            "nonce=1710000004&method=trade_history&currency_pair=btc_jpy&count=10"
        );
    }

    fn assert_spec(spec: super::ZaifRequestSpec, fixture: &str) {
        let expected: Value = serde_json::from_str(fixture).expect("fixture");
        assert_eq!(spec.method, expected["method"].as_str().unwrap());
        assert_eq!(spec.path, expected["path"].as_str().unwrap());
        assert_eq!(spec.body, expected["body"].as_str().unwrap());
    }
}
