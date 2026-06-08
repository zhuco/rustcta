#![cfg_attr(not(test), allow(dead_code))]

use std::collections::BTreeMap;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult, PlaceOrderRequest};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::novadax_symbol;
use super::signing::private_request_spec;

pub const ACCOUNT_BALANCE_PATH: &str = "/v1/account/getBalance";
pub const CREATE_ORDER_PATH: &str = "/v1/orders/create";
pub const BATCH_CREATE_ORDER_PATH: &str = "/v1/orders/batch-create";
pub const CANCEL_ORDER_PATH: &str = "/v1/orders/cancel";
pub const BATCH_CANCEL_ORDER_PATH: &str = "/v1/orders/batch-cancel";
pub const CANCEL_BY_SYMBOL_PATH: &str = "/v1/orders/cancel-by-symbol";
pub const GET_ORDER_PATH: &str = "/v1/orders/get";
pub const LIST_ORDERS_PATH: &str = "/v1/orders/list";
pub const FILLS_PATH: &str = "/v1/orders/fills";

pub fn novadax_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let order_type = match request.order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit => "LIMIT",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "novadax.unsupported_order_type",
            })
        }
    };
    let side = match request.side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    };
    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        json!(novadax_symbol(&request.symbol.exchange_symbol.symbol)),
    );
    body.insert("side".to_string(), json!(side));
    body.insert("type".to_string(), json!(order_type));
    if let Some(client_id) = &request.client_order_id {
        body.insert("clientOrderId".to_string(), json!(client_id));
    }
    if matches!(request.order_type, OrderType::Market)
        && matches!(request.side, OrderSide::Buy)
        && request.quote_quantity.is_some()
    {
        body.insert(
            "value".to_string(),
            json!(request.quote_quantity.as_deref()),
        );
    } else {
        body.insert("amount".to_string(), json!(request.quantity));
    }
    if matches!(request.order_type, OrderType::Limit) {
        body.insert(
            "price".to_string(),
            json!(request.price.as_deref().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "novadax limit order requires price".to_string(),
                }
            })?),
        );
    }
    Ok(Value::Object(body))
}

pub fn create_order_request_spec_fixture() -> Value {
    let body = json!({
        "symbol": "BTC_BRL",
        "side": "BUY",
        "type": "LIMIT",
        "amount": "0.01",
        "price": "350000",
        "clientOrderId": "offline-fixture"
    });
    private_request_spec("POST", CREATE_ORDER_PATH, &BTreeMap::new(), Some(body))
}

pub fn cancel_order_request_spec_fixture() -> Value {
    let body = json!({ "id": "order-1" });
    private_request_spec("POST", CANCEL_ORDER_PATH, &BTreeMap::new(), Some(body))
}

pub fn cancel_by_symbol_request_spec_fixture(symbol: &str) -> Value {
    let body = json!({ "symbol": novadax_symbol(symbol) });
    private_request_spec("POST", CANCEL_BY_SYMBOL_PATH, &BTreeMap::new(), Some(body))
}

pub fn open_orders_query(symbol: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("symbol".to_string(), novadax_symbol(symbol)),
        ("status".to_string(), "SUBMITTED,PARTIAL_FILLED".to_string()),
        ("page".to_string(), "1".to_string()),
        ("limit".to_string(), "100".to_string()),
    ])
}

pub fn fills_query(symbol: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("symbol".to_string(), novadax_symbol(symbol)),
        ("page".to_string(), "1".to_string()),
        ("limit".to_string(), "100".to_string()),
    ])
}
