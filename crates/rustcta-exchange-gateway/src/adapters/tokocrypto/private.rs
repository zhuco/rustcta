#![allow(dead_code)]

use std::collections::BTreeMap;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

use super::parser::normalize_tokocrypto_symbol;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokocryptoRequestSpec {
    pub method: String,
    pub path: String,
    pub query: String,
    pub body: String,
}

pub fn place_limit_order_spec(
    symbol: &str,
    side: &str,
    price: &str,
    quantity: &str,
    timestamp: u64,
    recv_window: u64,
) -> ExchangeApiResult<TokocryptoRequestSpec> {
    let params = BTreeMap::from([
        ("price".to_string(), price.to_string()),
        ("quantity".to_string(), quantity.to_string()),
        ("recvWindow".to_string(), recv_window.to_string()),
        ("side".to_string(), side_code(side)?.to_string()),
        ("symbol".to_string(), normalize_tokocrypto_symbol(symbol)?),
        ("timeInForce".to_string(), "1".to_string()),
        ("timestamp".to_string(), timestamp.to_string()),
        ("type".to_string(), "1".to_string()),
    ]);
    Ok(TokocryptoRequestSpec {
        method: "POST".to_string(),
        path: "/open/v1/orders".to_string(),
        query: params
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&"),
        body: String::new(),
    })
}

pub fn cancel_order_spec(
    symbol: &str,
    order_id: u64,
    timestamp: u64,
    recv_window: u64,
) -> ExchangeApiResult<TokocryptoRequestSpec> {
    Ok(TokocryptoRequestSpec {
        method: "POST".to_string(),
        path: "/open/v1/orders/cancel".to_string(),
        query: format!(
            "orderId={order_id}&recvWindow={recv_window}&symbol={}&timestamp={timestamp}",
            normalize_tokocrypto_symbol(symbol)?
        ),
        body: String::new(),
    })
}

pub fn query_order_spec(
    symbol: &str,
    order_id: u64,
    timestamp: u64,
    recv_window: u64,
) -> ExchangeApiResult<TokocryptoRequestSpec> {
    Ok(TokocryptoRequestSpec {
        method: "GET".to_string(),
        path: "/open/v1/orders/detail".to_string(),
        query: format!(
            "orderId={order_id}&recvWindow={recv_window}&symbol={}&timestamp={timestamp}",
            normalize_tokocrypto_symbol(symbol)?
        ),
        body: String::new(),
    })
}

pub fn account_spec(timestamp: u64, recv_window: u64) -> TokocryptoRequestSpec {
    TokocryptoRequestSpec {
        method: "GET".to_string(),
        path: "/open/v1/account/spot".to_string(),
        query: signed_base_query(timestamp, recv_window),
        body: String::new(),
    }
}

pub fn open_orders_spec(
    symbol: Option<&str>,
    timestamp: u64,
    recv_window: u64,
) -> ExchangeApiResult<TokocryptoRequestSpec> {
    let mut params = BTreeMap::from([
        ("recvWindow".to_string(), recv_window.to_string()),
        ("timestamp".to_string(), timestamp.to_string()),
    ]);
    if let Some(symbol) = symbol {
        params.insert("symbol".to_string(), normalize_tokocrypto_symbol(symbol)?);
    }
    Ok(TokocryptoRequestSpec {
        method: "GET".to_string(),
        path: "/open/v1/orders".to_string(),
        query: params
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&"),
        body: String::new(),
    })
}

pub fn listen_token_spec(timestamp: u64, recv_window: u64) -> TokocryptoRequestSpec {
    TokocryptoRequestSpec {
        method: "POST".to_string(),
        path: "/open/v1/user-listen-token".to_string(),
        query: signed_base_query(timestamp, recv_window),
        body: String::new(),
    }
}

fn signed_base_query(timestamp: u64, recv_window: u64) -> String {
    let params = BTreeMap::from([
        ("recvWindow".to_string(), recv_window.to_string()),
        ("timestamp".to_string(), timestamp.to_string()),
    ]);
    params
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn side_code(side: &str) -> ExchangeApiResult<i32> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" | "0" => Ok(0),
        "sell" | "1" => Ok(1),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported tokocrypto side {side}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{
        account_spec, cancel_order_spec, listen_token_spec, open_orders_spec,
        place_limit_order_spec, query_order_spec,
    };

    #[test]
    fn tokocrypto_request_specs_should_match_fixtures() {
        assert_spec(
            "place_limit_order.json",
            place_limit_order_spec("BTC_USDT", "buy", "7500", "0.16", 1581720670624, 5000)
                .expect("place"),
        );
        assert_spec("account_get.json", account_spec(1581720670624, 5000));
        assert_spec(
            "open_orders_get.json",
            open_orders_spec(Some("BTC_USDT"), 1581720670624, 5000).expect("open orders"),
        );
        assert_spec(
            "cancel_order.json",
            cancel_order_spec("BTC_USDT", 12345, 1581720670624, 5000).expect("cancel"),
        );
        assert_spec(
            "query_order.json",
            query_order_spec("BTC_USDT", 12345, 1581720670624, 5000).expect("query"),
        );
        assert_spec("listen_token.json", listen_token_spec(1581720670624, 5000));
    }

    fn assert_spec(name: &str, actual: super::TokocryptoRequestSpec) {
        let fixture: Value = serde_json::from_str(
            &std::fs::read_to_string(format!(
                "{}/../../tests/fixtures/exchanges/tokocrypto/request_specs/{name}",
                env!("CARGO_MANIFEST_DIR")
            ))
            .expect("fixture"),
        )
        .expect("fixture json");
        assert_eq!(actual.method, fixture["method"].as_str().unwrap());
        assert_eq!(actual.path, fixture["path"].as_str().unwrap());
        assert_eq!(actual.query, fixture["query"].as_str().unwrap());
        if actual.body.is_empty() {
            assert!(fixture["body"].is_null());
        } else {
            assert_eq!(
                serde_json::from_str::<Value>(&actual.body).unwrap(),
                fixture["body"]
            );
        }
    }
}
