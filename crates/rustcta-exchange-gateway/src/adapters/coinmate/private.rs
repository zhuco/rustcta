#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult, PlaceOrderRequest, TimeInForce};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::coinmate_pair;

pub const BALANCES_PATH: &str = "/balances";
pub const FEES_PATH: &str = "/traderFees";
pub const OPEN_ORDERS_PATH: &str = "/openOrders";
pub const QUERY_ORDER_PATH: &str = "/orderById";
pub const ORDER_HISTORY_PATH: &str = "/orderHistory";
pub const TRADE_HISTORY_PATH: &str = "/tradeHistory";
pub const BUY_LIMIT_PATH: &str = "/buyLimit";
pub const SELL_LIMIT_PATH: &str = "/sellLimit";
pub const CANCEL_ORDER_PATH: &str = "/cancelOrder";
pub const CANCEL_ALL_ORDERS_PATH: &str = "/cancelAllOpenOrders";
pub const REPLACE_BUY_LIMIT_PATH: &str = "/replaceByBuyLimit";
pub const REPLACE_SELL_LIMIT_PATH: &str = "/replaceBySellLimit";

pub fn coinmate_limit_order_path(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => BUY_LIMIT_PATH,
        OrderSide::Sell => SELL_LIMIT_PATH,
    }
}

pub fn coinmate_replace_limit_path(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => REPLACE_BUY_LIMIT_PATH,
        OrderSide::Sell => REPLACE_SELL_LIMIT_PATH,
    }
}

pub fn coinmate_limit_order_form(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinmate.reduce_only_unsupported_spot",
        });
    }
    if !matches!(request.order_type, OrderType::Limit | OrderType::PostOnly) {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinmate.only_limit_orders_mapped_offline",
        });
    }
    let price = request
        .price
        .as_ref()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinmate limit order requires price".to_string(),
        })?;
    let mut body = serde_json::Map::new();
    body.insert(
        "currencyPair".to_string(),
        json!(coinmate_pair(&request.symbol.exchange_symbol.symbol)),
    );
    body.insert("amount".to_string(), json!(request.quantity));
    body.insert("price".to_string(), json!(price));
    if request.post_only || request.order_type == OrderType::PostOnly {
        body.insert("postOnly".to_string(), json!(true));
    }
    if let Some(time_in_force) = request.time_in_force {
        match time_in_force {
            TimeInForce::GTC => {}
            TimeInForce::IOC => {
                body.insert("immediateOrCancel".to_string(), json!(true));
            }
            _ => {
                return Err(ExchangeApiError::Unsupported {
                    operation: "coinmate.unsupported_time_in_force",
                })
            }
        }
    }
    if let Some(client_order_id) = &request.client_order_id {
        body.insert("clientOrderId".to_string(), json!(client_order_id));
    }
    Ok(Value::Object(body))
}

pub fn coinmate_cancel_order_form(order_id: &str) -> Value {
    json!({ "orderId": order_id })
}

pub fn coinmate_cancel_all_form(symbol: Option<&str>) -> Value {
    let mut body = serde_json::Map::new();
    if let Some(symbol) = symbol {
        body.insert("currencyPair".to_string(), json!(coinmate_pair(symbol)));
    }
    Value::Object(body)
}

pub fn coinmate_query_order_form(order_id: &str) -> Value {
    json!({ "id": order_id })
}

pub fn coinmate_history_form(symbol: &str, limit: Option<u32>) -> Value {
    let mut body = serde_json::Map::new();
    body.insert("currencyPair".to_string(), json!(coinmate_pair(symbol)));
    if let Some(limit) = limit {
        body.insert("limit".to_string(), json!(limit));
    }
    Value::Object(body)
}

pub fn redacted_auth_fields() -> Value {
    json!({
        "clientId": "<redacted:client_id>",
        "publicKey": "<redacted:public_key>",
        "nonce": "<nonce>",
        "signature": "<redacted:signature>"
    })
}
