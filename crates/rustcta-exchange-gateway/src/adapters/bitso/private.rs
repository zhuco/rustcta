#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult, PlaceOrderRequest, TimeInForce};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::bitso_book;

pub const PLACE_ORDER_PATH: &str = "/orders";
pub const CANCEL_ORDER_PATH_PREFIX: &str = "/orders";
pub const OPEN_ORDERS_PATH: &str = "/open_orders";
pub const USER_TRADES_PATH: &str = "/user_trades";

pub fn bitso_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let order_type = match request.order_type {
        OrderType::Market => "market",
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => "limit",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitso.unsupported_order_type",
            })
        }
    };
    let mut body = serde_json::Map::new();
    body.insert(
        "book".to_string(),
        json!(bitso_book(&request.symbol.exchange_symbol.symbol)),
    );
    body.insert(
        "side".to_string(),
        json!(match request.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        }),
    );
    body.insert("type".to_string(), json!(order_type));
    if let Some(client_id) = &request.client_order_id {
        body.insert("origin_id".to_string(), json!(client_id));
    }
    if matches!(request.order_type, OrderType::Market) {
        if let Some(quote_quantity) = &request.quote_quantity {
            body.insert("minor".to_string(), json!(quote_quantity));
        } else {
            body.insert("major".to_string(), json!(request.quantity));
        }
    } else {
        body.insert("major".to_string(), json!(request.quantity));
        body.insert(
            "price".to_string(),
            json!(request.price.as_deref().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "bitso limit order requires price".to_string(),
                }
            })?),
        );
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        body.insert("time_in_force".to_string(), json!("postonly"));
    } else if let Some(tif) = request.time_in_force {
        body.insert(
            "time_in_force".to_string(),
            json!(bitso_time_in_force(tif)?),
        );
    }
    Ok(Value::Object(body))
}

pub fn bitso_cancel_order_path(exchange_order_id: &str) -> String {
    format!("{CANCEL_ORDER_PATH_PREFIX}/{exchange_order_id}")
}

pub fn create_order_request_spec_fixture() -> Value {
    json!({
        "method": "POST",
        "path": "/api/v3/orders",
        "auth": "bitso_hmac_sha256",
        "headers": {
            "Authorization": "Bitso <key>:<nonce>:<signature>",
            "Content-Type": "application/json"
        },
        "body": {
            "book": "btc_mxn",
            "side": "buy",
            "type": "limit",
            "major": "0.01",
            "price": "650000",
            "time_in_force": "goodtillcancelled",
            "origin_id": "offline-fixture"
        }
    })
}

fn bitso_time_in_force(tif: TimeInForce) -> ExchangeApiResult<&'static str> {
    match tif {
        TimeInForce::GTC => Ok("goodtillcancelled"),
        TimeInForce::IOC => Ok("immediateorcancel"),
        TimeInForce::FOK => Ok("fillorkill"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "bitso.unsupported_time_in_force",
        }),
    }
}
