#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult, PlaceOrderRequest};
use rustcta_types::{OrderSide, OrderType, TimeInForce};
use serde_json::{json, Value};

use super::parser::foxbit_symbol;

pub const ORDERS_PATH: &str = "/orders";
pub const BALANCES_PATH: &str = "/balances";
pub const OPEN_ORDERS_PATH: &str = "/orders";
pub const FILLS_PATH: &str = "/trades";

pub fn foxbit_order_path(order_id: &str) -> String {
    format!("{ORDERS_PATH}/{order_id}")
}

pub fn foxbit_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let order_type = match request.order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit => "LIMIT",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "foxbit.unsupported_order_type",
            })
        }
    };
    let mut body = serde_json::Map::new();
    body.insert(
        "side".to_string(),
        json!(match request.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }),
    );
    body.insert("type".to_string(), json!(order_type));
    body.insert(
        "market_symbol".to_string(),
        json!(foxbit_symbol(&request.symbol.exchange_symbol.symbol)),
    );
    if let Some(client_id) = &request.client_order_id {
        body.insert("client_order_id".to_string(), json!(client_id));
    }
    if let Some(quote_quantity) = &request.quote_quantity {
        body.insert("amount".to_string(), json!(quote_quantity));
    } else {
        body.insert("quantity".to_string(), json!(request.quantity));
    }
    if !matches!(request.order_type, OrderType::Market) {
        body.insert(
            "price".to_string(),
            json!(request.price.as_deref().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "foxbit limit order requires price".to_string(),
                }
            })?),
        );
    }
    if let Some(time_in_force) = request.time_in_force {
        body.insert(
            "time_in_force".to_string(),
            json!(foxbit_time_in_force(time_in_force)),
        );
    }
    Ok(Value::Object(body))
}

fn foxbit_time_in_force(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTX",
    }
}

pub fn create_order_request_spec_fixture() -> Value {
    super::transport::signed_request_spec(
        "POST",
        ORDERS_PATH,
        "",
        Some(json!({
            "side": "BUY",
            "type": "LIMIT",
            "market_symbol": "btcbrl",
            "quantity": "0.01",
            "price": "350000",
            "client_order_id": "offline-fixture"
        })),
    )
}
