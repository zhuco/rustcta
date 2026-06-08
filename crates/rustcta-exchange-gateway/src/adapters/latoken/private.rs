use rustcta_exchange_api::{ExchangeApiResult, PlaceOrderRequest, TimeInForce};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::split_symbol;
use super::signing::{signed_rest_headers, LatokenDigest};

pub const LATOKEN_BALANCES_PATH: &str = "/v2/auth/account";
pub const LATOKEN_OPEN_ORDERS_PATH: &str = "/v2/auth/order/active";
pub const LATOKEN_PLACE_ORDER_PATH: &str = "/v2/auth/order/place";
pub const LATOKEN_CANCEL_ORDER_PATH: &str = "/v2/auth/order/cancel";
pub const LATOKEN_CANCEL_ALL_PATH: &str = "/v2/auth/order/cancelAll";
pub const LATOKEN_RECENT_FILLS_PATH: &str = "/v2/auth/trade";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatokenPrivateRequestSpec {
    pub method: &'static str,
    pub path: &'static str,
    pub query_params: Vec<(String, String)>,
    pub body_params: Vec<(String, String)>,
    pub body: Option<Value>,
    pub api_key: String,
    pub signature: String,
    pub digest: String,
    pub canonical_payload: String,
}

pub fn build_signed_private_request_spec(
    method: &'static str,
    path: &'static str,
    query_params: Vec<(String, String)>,
    body_params: Vec<(String, String)>,
    body: Option<Value>,
    api_key: &str,
    api_secret: &str,
) -> ExchangeApiResult<LatokenPrivateRequestSpec> {
    let headers = signed_rest_headers(
        api_key,
        api_secret,
        method,
        path,
        &query_params,
        &body_params,
        LatokenDigest::Sha256,
    )?;
    Ok(LatokenPrivateRequestSpec {
        method,
        path,
        query_params,
        body_params,
        body,
        api_key: headers.api_key,
        signature: headers.signature,
        digest: headers.digest,
        canonical_payload: headers.payload,
    })
}

pub fn request_spec_body_from_order(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<(Value, Vec<(String, String)>)> {
    let (base, quote) = split_symbol(&request.symbol.exchange_symbol.symbol)?;
    let condition = time_in_force_as_str(request.time_in_force);
    let order_type = order_type_as_str(request.order_type);
    let mut body_params = vec![
        ("baseCurrency".to_string(), base.clone()),
        ("quoteCurrency".to_string(), quote.clone()),
        (
            "side".to_string(),
            order_side_as_str(request.side).to_string(),
        ),
        ("condition".to_string(), condition.to_string()),
        ("type".to_string(), order_type.to_string()),
    ];
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body_params.push(("clientOrderId".to_string(), client_order_id.to_string()));
    }
    if let Some(price) = request.price.as_deref() {
        body_params.push(("price".to_string(), price.to_string()));
    }
    body_params.push(("quantity".to_string(), request.quantity.clone()));

    let mut body = serde_json::Map::new();
    for (key, value) in &body_params {
        body.insert(key.clone(), json!(value));
    }
    Ok((Value::Object(body), body_params))
}

pub fn cancel_order_body(order_id: &str) -> (Value, Vec<(String, String)>) {
    (
        json!({ "id": order_id }),
        vec![("id".to_string(), order_id.to_string())],
    )
}

fn order_side_as_str(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn order_type_as_str(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => "LIMIT",
        OrderType::StopMarket | OrderType::StopLimit => "LIMIT",
    }
}

fn time_in_force_as_str(time_in_force: Option<TimeInForce>) -> &'static str {
    match time_in_force.unwrap_or(TimeInForce::GTC) {
        TimeInForce::GTC | TimeInForce::GTX => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
    }
}
