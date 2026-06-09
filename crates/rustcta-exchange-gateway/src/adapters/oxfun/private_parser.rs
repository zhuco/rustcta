#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    BatchItemResult, BatchOperationReport, BatchPlaceOrdersRequest, BatchPlaceOrdersResponse,
    ExchangeApiError, ExchangeApiResult, OrderState, ReconcilePlan, ReconcileTrigger,
    ResponseMetadata, RetryReconcilePolicy, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    ExchangeError, ExchangeErrorClass, ExchangeId, OrderSide, OrderStatus, OrderType, PositionSide,
};
use serde_json::Value;

pub fn parse_order_ack(payload: &str) -> ExchangeApiResult<(bool, Option<String>)> {
    let value: Value =
        serde_json::from_str(payload).map_err(|error| ExchangeApiError::Serialization {
            message: format!("invalid OX.FUN private WS order payload: {error}"),
        })?;
    let submitted = value
        .get("submitted")
        .and_then(Value::as_bool)
        .ok_or_else(|| ExchangeApiError::Serialization {
            message: "OX.FUN private order ack missing submitted".to_string(),
        })?;
    let order_id = value
        .get("data")
        .and_then(|data| data.get("orderId"))
        .and_then(|value| match value {
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        });
    Ok((submitted, order_id))
}

pub fn parse_batch_place_orders_ack(
    exchange_id: &ExchangeId,
    request: &BatchPlaceOrdersRequest,
    payload: &str,
) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
    let value: Value =
        serde_json::from_str(payload).map_err(|error| ExchangeApiError::Serialization {
            message: format!("invalid OX.FUN batch place payload: {error}"),
        })?;
    let items = batch_items(&value).ok_or_else(|| ExchangeApiError::Serialization {
        message: "OX.FUN batch place ack missing data/dataArray".to_string(),
    })?;

    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(request.orders.len());
    for (index, order_request) in request.orders.iter().enumerate() {
        let Some(item) = items.get(index) else {
            results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                None,
                batch_item_error(
                    exchange_id,
                    "OX.FUN placeorders response omitted item",
                    Value::Null,
                ),
                Some(ReconcilePlan::for_place_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchResponseMissingItem,
                    order_request,
                    RetryReconcilePolicy::default(),
                    "OX.FUN placeorders omitted an item; private order stream or REST readback reconciliation is required before replay",
                )),
            ));
            continue;
        };

        if !item_submitted(&value, item) {
            results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                string_or_number(item.get("orderId")).or_else(|| string_or_number(item.get("id"))),
                batch_item_error(exchange_id, "OX.FUN placeorders item was not submitted", (*item).clone()),
                Some(ReconcilePlan::for_place_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchPlacePartialFailure,
                    order_request,
                    RetryReconcilePolicy::default(),
                    "OX.FUN placeorders item failed or was ambiguous; readback reconciliation is required",
                )),
            ));
            continue;
        }

        let order = parse_private_order(exchange_id, &order_request.symbol, item, true)?;
        results.push(BatchItemResult::success(index, order.clone()));
        orders.push(order);
    }

    Ok(BatchPlaceOrdersResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        orders,
        report: Some(BatchOperationReport {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            total_items: request.orders.len(),
            results,
        }),
    })
}

fn batch_items(value: &Value) -> Option<Vec<&Value>> {
    if let Some(items) = value.get("dataArray").and_then(Value::as_array) {
        return Some(items.iter().collect());
    }
    if let Some(items) = value.get("data").and_then(Value::as_array) {
        return Some(items.iter().collect());
    }
    value
        .get("data")
        .map(|item| vec![item])
        .or_else(|| Some(vec![value]))
}

fn item_submitted(root: &Value, item: &Value) -> bool {
    item.get("submitted")
        .and_then(Value::as_bool)
        .or_else(|| root.get("submitted").and_then(Value::as_bool))
        .unwrap_or_else(|| {
            item.get("success")
                .and_then(Value::as_bool)
                .or_else(|| item.get("accepted").and_then(Value::as_bool))
                .unwrap_or(false)
        })
}

fn parse_private_order(
    exchange_id: &ExchangeId,
    fallback_symbol: &SymbolScope,
    value: &Value,
    submitted: bool,
) -> ExchangeApiResult<OrderState> {
    let side = match value
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase()
        .as_str()
    {
        "SELL" | "ASK" => OrderSide::Sell,
        _ => OrderSide::Buy,
    };
    let order_type = match value
        .get("orderType")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase()
        .as_str()
    {
        "MARKET" => OrderType::Market,
        "POST_ONLY" => OrderType::PostOnly,
        _ => OrderType::Limit,
    };
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: fallback_symbol.market_type,
        canonical_symbol: fallback_symbol.canonical_symbol.clone(),
        exchange_symbol: fallback_symbol.exchange_symbol.clone(),
        client_order_id: string_or_number(value.get("clientOrderId")),
        exchange_order_id: string_or_number(value.get("orderId"))
            .or_else(|| string_or_number(value.get("id"))),
        side,
        position_side: Some(PositionSide::None),
        order_type,
        time_in_force: None,
        status: if submitted {
            OrderStatus::Open
        } else {
            OrderStatus::Rejected
        },
        quantity: string_or_number(value.get("quantity")).unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    })
}

fn batch_item_error(exchange_id: &ExchangeId, message: &str, raw: Value) -> ExchangeError {
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::Unknown,
        message,
        Utc::now(),
    );
    error.raw = Some(raw);
    error
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}
