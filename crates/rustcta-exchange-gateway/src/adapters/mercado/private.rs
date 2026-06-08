#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult, PlaceOrderRequest};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::mercado_symbol;

pub const ACCOUNTS_PATH: &str = "/accounts";

pub fn mercado_orders_path(account_id: &str, symbol: &str) -> String {
    format!("/accounts/{account_id}/{}/orders", mercado_symbol(symbol))
}

pub fn mercado_order_path(account_id: &str, symbol: &str, order_id: &str) -> String {
    format!("{}/{order_id}", mercado_orders_path(account_id, symbol))
}

pub fn mercado_cancel_all_path(account_id: &str) -> String {
    format!("/accounts/{account_id}/cancel_all_open_orders")
}

pub fn mercado_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let order_type = match request.order_type {
        OrderType::Market => "market",
        OrderType::Limit => "limit",
        OrderType::PostOnly => "post-only",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "mercado.unsupported_order_type",
            })
        }
    };
    let mut body = serde_json::Map::new();
    body.insert(
        "side".to_string(),
        json!(match request.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        }),
    );
    body.insert("type".to_string(), json!(order_type));
    if let Some(client_id) = &request.client_order_id {
        body.insert("externalId".to_string(), json!(client_id));
    }
    if let Some(quote_quantity) = &request.quote_quantity {
        body.insert("cost".to_string(), json!(quote_quantity));
    } else {
        body.insert("qty".to_string(), json!(request.quantity));
    }
    if !matches!(request.order_type, OrderType::Market) {
        body.insert(
            "limitPrice".to_string(),
            json!(request.price.as_deref().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "mercado limit order requires price".to_string(),
                }
            })?),
        );
    }
    Ok(Value::Object(body))
}

pub fn create_order_request_spec_fixture() -> Value {
    json!({
        "method": "POST",
        "path": "/accounts/<accountId>/BTC-BRL/orders",
        "auth": "bearer",
        "headers": {
            "Authorization": "Bearer <redacted>",
            "Content-Type": "application/json"
        },
        "body": {
            "side": "buy",
            "type": "limit",
            "qty": "0.01",
            "limitPrice": "350000",
            "externalId": "offline-fixture"
        }
    })
}
