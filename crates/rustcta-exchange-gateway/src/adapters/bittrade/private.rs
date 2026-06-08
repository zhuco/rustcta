#![allow(dead_code)]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{json, Value};

use super::parser::normalize_bittrade_symbol;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BittradeRequestSpec {
    pub method: String,
    pub path: String,
    pub body: String,
}

pub fn accounts_spec() -> BittradeRequestSpec {
    BittradeRequestSpec {
        method: "GET".to_string(),
        path: "/v1/account/accounts".to_string(),
        body: String::new(),
    }
}

pub fn balances_spec(account_id: u64) -> BittradeRequestSpec {
    BittradeRequestSpec {
        method: "GET".to_string(),
        path: format!("/v1/account/accounts/{account_id}/balance"),
        body: String::new(),
    }
}

pub fn place_limit_order_spec(
    account_id: u64,
    symbol: &str,
    side: &str,
    price: &str,
    amount: &str,
) -> ExchangeApiResult<BittradeRequestSpec> {
    spec(
        "POST",
        "/v1/order/orders/place",
        json!({
            "account-id": account_id,
            "amount": amount,
            "price": price,
            "source": "api",
            "symbol": normalize_bittrade_symbol(symbol)?,
            "type": format!("{}-limit", normalize_side(side)?),
        }),
    )
}

pub fn cancel_order_spec(order_id: u64) -> BittradeRequestSpec {
    BittradeRequestSpec {
        method: "POST".to_string(),
        path: format!("/v1/order/orders/{order_id}/submitcancel"),
        body: "{}".to_string(),
    }
}

pub fn batch_cancel_orders_spec(order_ids: &[u64]) -> ExchangeApiResult<BittradeRequestSpec> {
    let order_ids = order_ids
        .iter()
        .map(|order_id| order_id.to_string())
        .collect::<Vec<_>>();
    spec(
        "POST",
        "/v1/order/orders/batchcancel",
        json!({ "order-ids": order_ids }),
    )
}

pub fn query_order_spec(order_id: u64) -> BittradeRequestSpec {
    BittradeRequestSpec {
        method: "GET".to_string(),
        path: format!("/v1/order/orders/{order_id}"),
        body: String::new(),
    }
}

fn spec(method: &str, path: &str, body: Value) -> ExchangeApiResult<BittradeRequestSpec> {
    Ok(BittradeRequestSpec {
        method: method.to_string(),
        path: path.to_string(),
        body: serde_json::to_string(&body).map_err(|error| ExchangeApiError::Serialization {
            message: error.to_string(),
        })?,
    })
}

fn normalize_side(side: &str) -> ExchangeApiResult<&'static str> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" => Ok("buy"),
        "sell" => Ok("sell"),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported bittrade side {side}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{
        accounts_spec, balances_spec, batch_cancel_orders_spec, cancel_order_spec,
        place_limit_order_spec, query_order_spec,
    };

    #[test]
    fn bittrade_request_specs_should_match_fixtures() {
        assert_spec("accounts_get.json", accounts_spec());
        assert_spec("balance_get.json", balances_spec(100009));
        assert_spec(
            "place_limit_order.json",
            place_limit_order_spec(100009, "BTC/JPY", "buy", "3000000", "0.01").expect("place"),
        );
        assert_spec("cancel_order.json", cancel_order_spec(59378));
        assert_spec(
            "batch_cancel_orders.json",
            batch_cancel_orders_spec(&[1, 2, 3]).expect("batch cancel"),
        );
        assert_spec("query_order.json", query_order_spec(59378));
    }

    fn assert_spec(name: &str, actual: super::BittradeRequestSpec) {
        let fixture: Value = serde_json::from_str(
            &std::fs::read_to_string(format!(
                "{}/../../tests/fixtures/exchanges/bittrade/request_specs/{name}",
                env!("CARGO_MANIFEST_DIR")
            ))
            .expect("fixture"),
        )
        .expect("fixture json");
        assert_eq!(actual.method, fixture["method"].as_str().unwrap());
        assert_eq!(actual.path, fixture["path"].as_str().unwrap());
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
