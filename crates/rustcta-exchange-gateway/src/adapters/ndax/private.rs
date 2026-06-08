#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult, PlaceOrderRequest, TimeInForce};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::ndax_symbol;
use super::transport::signed_rest_request_spec;

pub const GET_ACCOUNTS_PATH: &str = "/accounts";
pub const GET_ACCOUNT_POSITIONS_PATH: &str = "/accounts/{account_id}/positions";
pub const GET_OPEN_ORDERS_PATH: &str = "/orders";
pub const SEND_ORDER_PATH: &str = "/orders";
pub const CANCEL_ORDER_PATH_PREFIX: &str = "/orders";
pub const GET_FILLS_PATH: &str = "/fills";

pub fn balances_request_spec() -> Value {
    signed_rest_request_spec("GET", GET_ACCOUNTS_PATH, None)
}

pub fn open_orders_request_spec(symbol: Option<&str>) -> Value {
    let mut spec = signed_rest_request_spec("GET", GET_OPEN_ORDERS_PATH, None);
    if let Some(symbol) = symbol {
        spec["query"] = json!({ "symbol": ndax_symbol(symbol) });
    }
    spec
}

pub fn fills_request_spec(symbol: Option<&str>) -> Value {
    let mut spec = signed_rest_request_spec("GET", GET_FILLS_PATH, None);
    if let Some(symbol) = symbol {
        spec["query"] = json!({ "symbol": ndax_symbol(symbol) });
    }
    spec
}

pub fn ndax_limit_order_body(
    request: &PlaceOrderRequest,
    account_id: i64,
    instrument_id: i64,
) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "ndax.reduce_only_unsupported_spot",
        });
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        return Err(ExchangeApiError::Unsupported {
            operation: "ndax.post_only_unverified",
        });
    }
    if request.order_type != OrderType::Limit {
        return Err(ExchangeApiError::Unsupported {
            operation: "ndax.only_limit_orders_mapped_offline",
        });
    }
    let price = request
        .price
        .as_ref()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "NDAX limit order request requires price".to_string(),
        })?;
    let mut body = serde_json::Map::new();
    body.insert("OMSId".to_string(), json!(1));
    body.insert("AccountId".to_string(), json!(account_id));
    body.insert("InstrumentId".to_string(), json!(instrument_id));
    body.insert("Side".to_string(), json!(ndax_order_side(request.side)));
    body.insert("OrderType".to_string(), json!(2));
    body.insert("Quantity".to_string(), json!(request.quantity));
    body.insert("LimitPrice".to_string(), json!(price));
    body.insert(
        "TimeInForce".to_string(),
        json!(ndax_time_in_force(request.time_in_force)?),
    );
    if let Some(client_order_id) = request.client_order_id.as_ref() {
        body.insert("ClientOrderId".to_string(), json!(client_order_id));
    }
    Ok(Value::Object(body))
}

pub fn place_order_request_spec(body: Value) -> Value {
    signed_rest_request_spec("POST", SEND_ORDER_PATH, Some(body))
}

pub fn cancel_order_body(account_id: i64, order_id: &str) -> Value {
    json!({
        "OMSId": 1,
        "AccountId": account_id,
        "OrderId": order_id
    })
}

pub fn cancel_order_request_spec(order_id: &str) -> Value {
    signed_rest_request_spec(
        "DELETE",
        &format!("{CANCEL_ORDER_PATH_PREFIX}/{order_id}"),
        None,
    )
}

fn ndax_order_side(side: OrderSide) -> i64 {
    match side {
        OrderSide::Buy => 0,
        OrderSide::Sell => 1,
    }
}

fn ndax_time_in_force(time_in_force: Option<TimeInForce>) -> ExchangeApiResult<i64> {
    match time_in_force.unwrap_or(TimeInForce::GTC) {
        TimeInForce::GTC => Ok(1),
        TimeInForce::IOC => Ok(3),
        TimeInForce::FOK => Ok(4),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "ndax.unsupported_time_in_force",
        }),
    }
}
