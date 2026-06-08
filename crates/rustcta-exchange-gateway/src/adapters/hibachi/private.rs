#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{json, Value};

pub const PRIVATE_REST_DISABLED: &str = "hibachi.private_rest_disabled";
pub const PLACE_ORDER_UNSUPPORTED: &str = "hibachi.trade_write_request_spec_only";
pub const CANCEL_ORDER_UNSUPPORTED: &str = "hibachi.cancel_write_request_spec_only";
pub const AMEND_ORDER_UNSUPPORTED: &str = "hibachi.amend_write_request_spec_only";
pub const BATCH_PLACE_UNSUPPORTED: &str = "hibachi.batch_write_request_spec_only";
pub const BATCH_CANCEL_UNSUPPORTED: &str = "hibachi.batch_cancel_request_spec_only";
pub const CANCEL_ALL_UNSUPPORTED: &str = "hibachi.cancel_all_request_spec_only";
pub const ORDER_LIST_UNSUPPORTED: &str = "hibachi.order_list_unsupported";
pub const QUOTE_MARKET_UNSUPPORTED: &str = "hibachi.quote_market_order_unsupported";

pub fn write_boundary<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

pub fn hibachi_place_limit_order_body(
    account_id: u64,
    nonce: u64,
    symbol: &str,
    side: &str,
    quantity: &str,
    price: &str,
    max_fees_percent: &str,
    signature: &str,
) -> Value {
    json!({
        "accountId": account_id,
        "nonce": nonce,
        "symbol": symbol,
        "side": side,
        "orderType": "LIMIT",
        "quantity": quantity,
        "price": price,
        "signature": signature,
        "maxFeesPercent": max_fees_percent
    })
}

pub fn hibachi_cancel_order_body(account_id: u64, order_id: u64, signature: &str) -> Value {
    json!({
        "accountId": account_id,
        "orderId": order_id,
        "signature": signature
    })
}

pub fn hibachi_cancel_all_orders_body(account_id: u64, nonce: u64, signature: &str) -> Value {
    json!({
        "accountId": account_id,
        "nonce": nonce,
        "signature": signature
    })
}
