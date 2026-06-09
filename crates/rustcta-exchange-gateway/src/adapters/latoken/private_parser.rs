#![allow(dead_code)]

use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchItemResult,
    BatchOperationReport, BatchPlaceOrdersRequest, BatchPlaceOrdersResponse,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OpenOrdersResponse, OrderState,
    PlaceOrderRequest, PlaceOrderResponse, QueryOrderResponse, RecentFillsResponse,
    ResponseMetadata, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeError, ExchangeErrorClass, ExchangeId, Fill, FillStatus,
    LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

pub fn parse_private_rest_boundary(_value: &Value) -> ExchangeApiResult<()> {
    Err(ExchangeApiError::Unsupported {
        operation: "latoken.private_rest_parser_unverified",
    })
}

pub fn parse_balances_ack(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let rows = order_rows(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "LATOKEN balances response missing balance array".to_string(),
    })?;
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for row in rows {
        let asset = row
            .get("currency")
            .or_else(|| row.get("asset"))
            .and_then(Value::as_str)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "LATOKEN balance row missing currency".to_string(),
            })?
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = number_from_value(row.get("available")).unwrap_or(0.0);
        let locked =
            number_from_value(row.get("blocked").or_else(|| row.get("reserved"))).unwrap_or(0.0);
        let total = number_from_value(row.get("total").or_else(|| row.get("balance")))
            .unwrap_or(available + locked);
        if total > 0.0 || available > 0.0 || locked > 0.0 || !requested.is_empty() {
            balances.push(
                AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
            );
        }
    }
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_fee_snapshot(
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<FeeRateSnapshot> {
    let fee = value
        .get("message")
        .or_else(|| value.get("result"))
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let maker_rate = string_or_number(
        fee.get("makerFee")
            .or_else(|| fee.get("maker_fee"))
            .or_else(|| fee.get("makerRate"))
            .or_else(|| fee.get("maker_rate"))
            .or_else(|| fee.get("maker")),
    )
    .ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "LATOKEN fee response missing maker fee".to_string(),
    })?;
    let taker_rate = string_or_number(
        fee.get("takerFee")
            .or_else(|| fee.get("taker_fee"))
            .or_else(|| fee.get("takerRate"))
            .or_else(|| fee.get("taker_rate"))
            .or_else(|| fee.get("taker")),
    )
    .ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "LATOKEN fee response missing taker fee".to_string(),
    })?;
    Ok(FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: symbol.clone(),
        maker_rate,
        taker_rate,
        source: Some("latoken.auth_trade_fee".to_string()),
        updated_at: Utc::now(),
    })
}

pub fn parse_place_order_ack(
    exchange_id: &ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> ExchangeApiResult<PlaceOrderResponse> {
    let item = first_success_item(value).unwrap_or(value);
    Ok(PlaceOrderResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        order: order_state_from_place_item(exchange_id, request, item),
    })
}

pub fn parse_cancel_order_ack(
    exchange_id: &ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> ExchangeApiResult<CancelOrderResponse> {
    let item = first_success_item(value).unwrap_or(value);
    Ok(CancelOrderResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        order: order_state_from_cancel_item(exchange_id, request, item),
        cancelled: true,
    })
}

pub fn parse_cancel_all_ack(
    exchange_id: &ExchangeId,
    request: &CancelAllOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<CancelAllOrdersResponse> {
    let items = bulk_items(value).cloned().unwrap_or_default();
    let mut orders = Vec::new();
    for item in &items {
        if item_status_is_success(item) || item.get("id").is_some() {
            let symbol =
                request
                    .symbol
                    .clone()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "LATOKEN cancel_all parser requires symbol for order state"
                            .to_string(),
                    })?;
            let cancel = CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request.context.clone(),
                symbol,
                client_order_id: None,
                exchange_order_id: string_or_number(item.get("id")),
            };
            orders.push(order_state_from_cancel_item(exchange_id, &cancel, item));
        }
    }
    Ok(CancelAllOrdersResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        cancelled_count: orders.len() as u32,
        orders,
    })
}

pub fn parse_query_order_ack(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<QueryOrderResponse> {
    Ok(QueryOrderResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        order: Some(order_state_from_value(exchange_id, symbol, value)?),
    })
}

pub fn parse_open_orders_ack(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OpenOrdersResponse> {
    let rows = order_rows(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "LATOKEN open orders response missing order array".to_string(),
    })?;
    let orders = rows
        .iter()
        .map(|row| order_state_from_value(exchange_id, symbol, row))
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(OpenOrdersResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        orders,
    })
}

pub fn parse_recent_fills_ack(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<RecentFillsResponse> {
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "LATOKEN recent fills require canonical_symbol".to_string(),
            })?;
    let rows = order_rows(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "LATOKEN trade response missing trade array".to_string(),
    })?;
    let fills = rows
        .iter()
        .map(|trade| {
            let price = number_from_value(trade.get("price")).unwrap_or(0.0);
            let quantity = number_from_value(
                trade
                    .get("quantity")
                    .or_else(|| trade.get("filledQuantity"))
                    .or_else(|| trade.get("amount")),
            )
            .unwrap_or(0.0);
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: string_or_number(trade.get("orderId").or_else(|| trade.get("order_id"))),
                client_order_id: trade
                    .get("clientOrderId")
                    .or_else(|| trade.get("client_order_id"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                fill_id: string_or_number(trade.get("id").or_else(|| trade.get("tradeId"))),
                side: parse_side(
                    trade
                        .get("side")
                        .or_else(|| trade.get("orderSide"))
                        .and_then(Value::as_str),
                ),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity_role(
                    trade
                        .get("liquidity")
                        .or_else(|| trade.get("makerTaker"))
                        .and_then(Value::as_str),
                ),
                price,
                quantity,
                quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
                fee_asset: trade
                    .get("feeCurrency")
                    .or_else(|| trade.get("fee_currency"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| Some(canonical_symbol.quote_asset().to_string())),
                fee_amount: number_from_value(trade.get("fee")),
                fee_rate: None,
                realized_pnl: None,
                filled_at: parse_timestamp(trade).unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(RecentFillsResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        fills,
    })
}

pub fn parse_batch_place_orders_ack(
    exchange_id: &ExchangeId,
    request: &BatchPlaceOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
    let items = bulk_items(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "LATOKEN placeBulk response missing message array".to_string(),
    })?;

    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(items.len().max(request.orders.len()));
    for (index, order_request) in request.orders.iter().enumerate() {
        let item = items.get(index);
        match item {
            Some(item) if item_status_is_success(item) => {
                let order = order_state_from_place_item(exchange_id, order_request, item);
                orders.push(order.clone());
                results.push(BatchItemResult::success(index, order));
            }
            Some(item) => results.push(BatchItemResult::failed(
                index,
                item.get("clientOrderId")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| order_request.client_order_id.clone()),
                string_or_number(item.get("id")),
                item_error(exchange_id, item, ExchangeErrorClass::OrderRejected),
                None,
            )),
            None => results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                None,
                missing_item_error(exchange_id, "LATOKEN placeBulk response omitted item"),
                None,
            )),
        }
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

pub fn parse_batch_cancel_orders_ack(
    exchange_id: &ExchangeId,
    request: &BatchCancelOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<BatchCancelOrdersResponse> {
    let items = bulk_items(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "LATOKEN cancelBulk response missing message array".to_string(),
    })?;

    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(items.len().max(request.cancels.len()));
    for (index, cancel_request) in request.cancels.iter().enumerate() {
        let item = items.get(index);
        match item {
            Some(item) if item_status_is_success(item) => {
                let order = order_state_from_cancel_item(exchange_id, cancel_request, item);
                orders.push(order.clone());
                results.push(BatchItemResult::success(index, order));
            }
            Some(item) => results.push(BatchItemResult::failed(
                index,
                cancel_request.client_order_id.clone(),
                string_or_number(item.get("id"))
                    .or_else(|| cancel_request.exchange_order_id.clone()),
                item_error(exchange_id, item, ExchangeErrorClass::OrderRejected),
                None,
            )),
            None => results.push(BatchItemResult::failed(
                index,
                cancel_request.client_order_id.clone(),
                cancel_request.exchange_order_id.clone(),
                missing_item_error(exchange_id, "LATOKEN cancelBulk response omitted item"),
                None,
            )),
        }
    }

    Ok(BatchCancelOrdersResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        cancelled_count: orders.len() as u32,
        orders,
        report: Some(BatchOperationReport {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            total_items: request.cancels.len(),
            results,
        }),
    })
}

fn bulk_items(value: &Value) -> Option<&Vec<Value>> {
    if let Some(items) = value.get("message").and_then(Value::as_array) {
        return Some(items);
    }
    value
        .as_array()
        .and_then(|values| values.first())
        .and_then(|reply| reply.get("message"))
        .and_then(Value::as_array)
}

fn first_success_item(value: &Value) -> Option<&Value> {
    bulk_items(value)
        .and_then(|items| items.iter().find(|item| item_status_is_success(item)))
        .or_else(|| value.get("message").filter(|message| message.is_object()))
        .or_else(|| value.get("result").filter(|message| message.is_object()))
}

fn order_state_from_place_item(
    exchange_id: &ExchangeId,
    request: &rustcta_exchange_api::PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    let now = Utc::now();
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value
            .get("clientOrderId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: string_or_number(value.get("id")),
        side: request.side,
        position_side: request.position_side,
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only,
        created_at: Some(now),
        updated_at: now,
    }
}

fn order_state_from_cancel_item(
    exchange_id: &ExchangeId,
    request: &rustcta_exchange_api::CancelOrderRequest,
    value: &Value,
) -> OrderState {
    let now = Utc::now();
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: string_or_number(value.get("id"))
            .or_else(|| request.exchange_order_id.clone()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: None,
        status: OrderStatus::Cancelled,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: now,
    }
}

fn item_status_is_success(value: &Value) -> bool {
    value
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("SUCCESS"))
}

fn order_state_from_value(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: value
            .get("clientOrderId")
            .or_else(|| value.get("client_order_id"))
            .and_then(Value::as_str)
            .map(str::to_string),
        exchange_order_id: string_or_number(
            value
                .get("id")
                .or_else(|| value.get("orderId"))
                .or_else(|| value.get("order_id")),
        ),
        side: parse_side(value.get("side").and_then(Value::as_str)),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(value.get("type").and_then(Value::as_str)),
        time_in_force: None,
        status: parse_order_status(value.get("status").and_then(Value::as_str)),
        quantity: string_or_number(
            value
                .get("quantity")
                .or_else(|| value.get("amount"))
                .or_else(|| value.get("origQty")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")),
        filled_quantity: string_or_number(
            value
                .get("filledQuantity")
                .or_else(|| value.get("executedQuantity"))
                .or_else(|| value.get("cumQty")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(
            value.get("avgPrice").or_else(|| value.get("averagePrice")),
        ),
        reduce_only: false,
        post_only: false,
        created_at: parse_timestamp(value),
        updated_at: parse_timestamp(value).unwrap_or(now),
    })
}

fn item_error(
    exchange_id: &ExchangeId,
    value: &Value,
    fallback_class: ExchangeErrorClass,
) -> ExchangeError {
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("LATOKEN batch item failed");
    let class = classify_item_error(
        value
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        message,
        fallback_class,
    );
    let mut error = ExchangeError::new(exchange_id.clone(), class, message, Utc::now());
    error.code = value
        .get("error")
        .and_then(Value::as_str)
        .map(str::to_string);
    error.order_id = string_or_number(value.get("id"));
    error.client_order_id = value
        .get("clientOrderId")
        .and_then(Value::as_str)
        .map(str::to_string);
    error.raw = Some(value.clone());
    error
}

fn missing_item_error(exchange_id: &ExchangeId, message: &str) -> ExchangeError {
    ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::UnknownOrderState,
        message,
        Utc::now(),
    )
}

fn classify_item_error(
    code: &str,
    message: &str,
    fallback_class: ExchangeErrorClass,
) -> ExchangeErrorClass {
    let code = code.to_ascii_uppercase();
    let message = message.to_ascii_lowercase();
    if code.contains("INSUFFICIENT") || message.contains("not enough balance") {
        ExchangeErrorClass::InsufficientBalance
    } else if code.contains("VALIDATION") {
        ExchangeErrorClass::InvalidRequest
    } else if code.contains("NOT_FOUND") {
        ExchangeErrorClass::OrderNotFound
    } else {
        fallback_class
    }
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| {
        value
            .as_str()
            .map(str::to_string)
            .or_else(|| value.as_i64().map(|number| number.to_string()))
            .or_else(|| value.as_u64().map(|number| number.to_string()))
    })
}

fn number_from_value(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn order_rows(value: &Value) -> Option<&Vec<Value>> {
    value
        .as_array()
        .or_else(|| value.get("message").and_then(Value::as_array))
        .or_else(|| value.get("result").and_then(Value::as_array))
        .or_else(|| value.get("orders").and_then(Value::as_array))
        .or_else(|| value.get("data").and_then(Value::as_array))
}

fn parse_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "SELL" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "FILLED" | "CLOSED" | "EXECUTED" => OrderStatus::Filled,
        "PARTIALLY_FILLED" | "PARTIALLYFILLED" => OrderStatus::PartiallyFilled,
        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
        "REJECTED" | "FAILED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        "NEW" | "OPEN" | "ACTIVE" | "SUCCESS" => OrderStatus::New,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity_role(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" | "m" => LiquidityRole::Maker,
        "taker" | "t" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn parse_timestamp(value: &Value) -> Option<chrono::DateTime<Utc>> {
    for key in ["timestamp", "createdAt", "created_at", "time"] {
        let Some(raw) = value.get(key) else {
            continue;
        };
        if let Some(ms) = raw.as_i64().or_else(|| raw.as_str()?.parse().ok()) {
            return chrono::DateTime::<Utc>::from_timestamp_millis(ms);
        }
        if let Some(text) = raw.as_str() {
            if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(text) {
                return Some(timestamp.with_timezone(&Utc));
            }
        }
    }
    None
}
