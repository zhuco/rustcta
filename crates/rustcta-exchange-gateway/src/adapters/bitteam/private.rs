use rustcta_exchange_api::{ExchangeApiResult, PlaceOrderRequest};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::signing::basic_authorization_header;

pub const BITTEAM_BALANCE_PATH: &str = "/trade/api/ccxt/balance";
pub const BITTEAM_PLACE_ORDER_PATH: &str = "/trade/api/ccxt/ordercreate";
pub const BITTEAM_CANCEL_ORDER_PATH: &str = "/trade/api/ccxt/cancelorder";
pub const BITTEAM_OPEN_ORDERS_PATH: &str = "/trade/api/ccxt/ordersOfUser";
pub const BITTEAM_RECENT_FILLS_PATH: &str = "/trade/api/ccxt/tradesOfUser";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitteamPrivateRequestSpec {
    pub method: &'static str,
    pub path: &'static str,
    pub authorization: String,
    pub body: Option<Value>,
}

pub fn build_basic_private_request_spec(
    method: &'static str,
    path: &'static str,
    body: Option<Value>,
    api_key: &str,
    api_secret: &str,
) -> ExchangeApiResult<BitteamPrivateRequestSpec> {
    Ok(BitteamPrivateRequestSpec {
        method,
        path,
        authorization: basic_authorization_header(api_key, api_secret)?,
        body,
    })
}

pub fn request_spec_body_from_order(request: &PlaceOrderRequest, pair_id: u64) -> Value {
    json!({
        "pairId": pair_id,
        "side": order_side_as_str(request.side),
        "type": order_type_as_str(request.order_type),
        "amount": request.quantity,
        "price": request.price.clone().unwrap_or_default()
    })
}

fn order_side_as_str(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn order_type_as_str(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "market",
        OrderType::Limit => "limit",
        OrderType::PostOnly => "limit",
        OrderType::IOC => "limit",
        OrderType::FOK => "limit",
        OrderType::StopMarket | OrderType::StopLimit => "conditional",
    }
}
