use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, AmendOrderResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchItemResult, BatchOperationReport, ExchangeApiError, ExchangeApiResult, OrderState,
    ReconcilePlan, ReconcileTrigger, ResponseMetadata, RetryReconcilePolicy, SymbolRules,
    SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, Fill,
    FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    ensure_no_error(exchange_id, value)?;
    let products = value
        .as_array()
        .or_else(|| value.get("products").and_then(Value::as_array))
        .or_else(|| value.get("data").and_then(Value::as_array))
        .or_else(|| {
            value
                .get("data")
                .and_then(|data| data.get("products"))
                .and_then(Value::as_array)
        })
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "BSX products response missing products",
                value,
            )
        })?;
    products
        .iter()
        .filter(|product| is_perpetual_product(product))
        .map(|product| parse_product(exchange_id, product))
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    ensure_no_error(exchange_id, value)?;
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("bids"))?;
    let asks = parse_levels(exchange_id, data.get("asks"))?;
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .or_else(|| {
            data.get("product_id")
                .or_else(|| data.get("product"))
                .or_else(|| data.get("symbol"))
                .and_then(Value::as_str)
                .and_then(|product_id| canonical_from_product_id(product_id).ok())
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "BSX order book requires canonical_symbol or response product_id".to_string(),
        })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = data
        .get("sequence")
        .or_else(|| data.get("seq"))
        .or_else(|| data.get("gsn"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .or_else(|| data.get("time"))
        .and_then(value_as_i128)
        .and_then(timestamp_any);
    Ok(snapshot)
}

pub fn parse_amend_order_ack(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<AmendOrderResponse> {
    let order = parse_private_order(exchange_id, symbol, first_order_like(value)?)?;
    Ok(AmendOrderResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        order,
    })
}

pub fn parse_batch_cancel_orders_ack(
    exchange_id: &ExchangeId,
    request: &BatchCancelOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<BatchCancelOrdersResponse> {
    let items = value
        .get("results")
        .or_else(|| value.get("orders"))
        .or_else(|| value.get("data"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "BSX batch cancel ack missing results",
                value,
            )
        })?;

    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(request.cancels.len().max(items.len()));
    for (index, cancel) in request.cancels.iter().enumerate() {
        let item = items.get(index);
        match item {
            Some(item) if item_success(item) => {
                let order = parse_private_order(exchange_id, cancel.symbol.clone(), item)?;
                orders.push(order.clone());
                results.push(BatchItemResult::success(index, order));
            }
            Some(item) => results.push(BatchItemResult::failed(
                index,
                cancel.client_order_id.clone(),
                string_or_number(item.get("order_id"))
                    .or_else(|| string_or_number(item.get("id")))
                    .or_else(|| cancel.exchange_order_id.clone()),
                item_error(exchange_id, item, "BSX batch cancel item failed"),
                Some(ReconcilePlan::for_cancel_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchCancelPartialFailure,
                    cancel,
                    RetryReconcilePolicy::default(),
                    "BSX cancel result was rejected or ambiguous; query/open-orders reconciliation required before replay",
                )),
            )),
            None => results.push(BatchItemResult::failed(
                index,
                cancel.client_order_id.clone(),
                cancel.exchange_order_id.clone(),
                missing_item_error(exchange_id, "BSX batch cancel ack omitted item"),
                Some(ReconcilePlan::for_cancel_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchResponseMissingItem,
                    cancel,
                    RetryReconcilePolicy::default(),
                    "BSX batch cancel response omitted the item; query/open-orders reconciliation required",
                )),
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

pub fn parse_single_order(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    ensure_no_error(exchange_id, value)?;
    parse_private_order(exchange_id, symbol, first_order_like(value)?)
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    ensure_no_error(exchange_id, value)?;
    order_rows(value)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "BSX orders response missing rows",
                value,
            )
        })?
        .iter()
        .map(|order| {
            let symbol = symbol_scope_from_private_order(exchange_id, fallback_symbol, order)?;
            parse_private_order(exchange_id, symbol, order)
        })
        .collect()
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    ensure_no_error(exchange_id, value)?;
    fill_rows(value)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "BSX fills response missing rows",
                value,
            )
        })?
        .iter()
        .map(|fill| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                fallback_symbol,
                fill,
            )
        })
        .collect()
}

fn parse_product(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let product_id = required_str(exchange_id, value, "product_id")
        .or_else(|_| required_str(exchange_id, value, "id"))
        .or_else(|_| required_str(exchange_id, value, "symbol"))?;
    let canonical_symbol = canonical_from_product_id(product_id)?;
    let base_asset = value
        .get("base_asset")
        .or_else(|| value.get("base"))
        .or_else(|| value.get("underlying_asset"))
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.base_asset().to_string());
    let quote_asset = value
        .get("quote_asset")
        .or_else(|| value.get("quote"))
        .or_else(|| value.get("settlement_asset"))
        .or_else(|| value.get("collateral_asset"))
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.quote_asset().to_string());
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            product_id,
        )
        .map_err(validation_error)?,
    };
    let price_increment = decimal_path(value, &["tick_size"])
        .or_else(|| decimal_path(value, &["price_increment"]))
        .or_else(|| decimal_path(value, &["price_tick"]));
    let quantity_increment = decimal_path(value, &["step_size"])
        .or_else(|| decimal_path(value, &["size_increment"]))
        .or_else(|| decimal_path(value, &["quantity_increment"]));
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: decimal_path(value, &["min_price"]),
        max_price: decimal_path(value, &["max_price"]),
        min_quantity: decimal_path(value, &["min_size"])
            .or_else(|| decimal_path(value, &["min_quantity"])),
        max_quantity: decimal_path(value, &["max_size"])
            .or_else(|| decimal_path(value, &["max_quantity"])),
        min_notional: decimal_path(value, &["min_notional"]),
        max_notional: decimal_path(value, &["max_notional"]),
        price_precision: price_increment.as_deref().and_then(precision_hint),
        quantity_precision: quantity_increment.as_deref().and_then(precision_hint),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

fn parse_private_order(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value
            .get("client_order_id")
            .or_else(|| value.get("clientOrderId"))
            .and_then(Value::as_str)
            .map(str::to_string),
        exchange_order_id: string_or_number(value.get("order_id"))
            .or_else(|| string_or_number(value.get("id"))),
        side: parse_side(value.get("side").and_then(Value::as_str))?,
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(
            value
                .get("order_type")
                .or_else(|| value.get("type"))
                .and_then(Value::as_str),
        ),
        time_in_force: None,
        status: parse_order_status(value.get("status").and_then(Value::as_str)),
        quantity: string_or_number(value.get("size").or_else(|| value.get("quantity")))
            .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")),
        filled_quantity: string_or_number(
            value
                .get("filled_size")
                .or_else(|| value.get("filled_quantity"))
                .or_else(|| value.get("filled")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(
            value
                .get("avg_fill_price")
                .or_else(|| value.get("average_fill_price")),
        ),
        reduce_only: value
            .get("reduce_only")
            .or_else(|| value.get("reduceOnly"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: value
            .get("post_only")
            .or_else(|| value.get("postOnly"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: value
            .get("created_at")
            .or_else(|| value.get("createdAt"))
            .and_then(parse_datetime_value),
        updated_at: value
            .get("updated_at")
            .or_else(|| value.get("updatedAt"))
            .and_then(parse_datetime_value)
            .unwrap_or_else(Utc::now),
    })
}

fn first_order_like<'a>(value: &'a Value) -> ExchangeApiResult<&'a Value> {
    if let Some(data) = value.get("data") {
        return first_order_like(data);
    }
    if let Some(order) = value.get("order") {
        return Ok(order);
    }
    if value.is_object() {
        return Ok(value);
    }
    Err(ExchangeApiError::InvalidRequest {
        message: "BSX order response missing order object".to_string(),
    })
}

fn order_rows(value: &Value) -> Option<&Vec<Value>> {
    if let Some(data) = value.get("data") {
        if let Some(rows) = order_rows(data) {
            return Some(rows);
        }
    }
    value
        .get("orders")
        .or_else(|| value.get("rows"))
        .or_else(|| value.get("result"))
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
}

fn fill_rows(value: &Value) -> Option<&Vec<Value>> {
    if let Some(data) = value.get("data") {
        if let Some(rows) = fill_rows(data) {
            return Some(rows);
        }
    }
    value
        .get("trades")
        .or_else(|| value.get("fills"))
        .or_else(|| value.get("rows"))
        .or_else(|| value.get("result"))
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
}

fn symbol_scope_from_private_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = fallback_symbol {
        return Ok(symbol.clone());
    }
    symbol_scope_from_value(exchange_id, value, "BSX order missing product_id")
}

fn symbol_scope_from_value(
    exchange_id: &ExchangeId,
    value: &Value,
    missing_message: &str,
) -> ExchangeApiResult<SymbolScope> {
    let product_id = value
        .get("product_id")
        .or_else(|| value.get("product"))
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id.clone(), missing_message, value))?;
    let canonical_symbol = canonical_from_product_id(product_id)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            product_id,
        )
        .map_err(validation_error)?,
    })
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = fallback_symbol.cloned().map(Ok).unwrap_or_else(|| {
        symbol_scope_from_value(exchange_id, value, "BSX fill missing product_id")
    })?;
    let price = value
        .get("price")
        .or_else(|| value.get("fill_price"))
        .or_else(|| value.get("trade_price"))
        .and_then(value_as_f64)
        .ok_or_else(|| parse_error(exchange_id.clone(), "BSX fill missing price", value))?;
    let quantity = value
        .get("size")
        .or_else(|| value.get("quantity"))
        .or_else(|| value.get("qty"))
        .or_else(|| value.get("fill_size"))
        .and_then(value_as_f64)
        .ok_or_else(|| parse_error(exchange_id.clone(), "BSX fill missing quantity", value))?;
    let fill = Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "BSX fill requires canonical_symbol".to_string(),
            }
        })?,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("order_id"))
            .or_else(|| string_or_number(value.get("id"))),
        client_order_id: value
            .get("client_order_id")
            .or_else(|| value.get("clientOrderId"))
            .and_then(Value::as_str)
            .map(str::to_string),
        fill_id: string_or_number(value.get("trade_id"))
            .or_else(|| string_or_number(value.get("fill_id")))
            .or_else(|| string_or_number(value.get("match_id")))
            .or_else(|| string_or_number(value.get("id"))),
        side: parse_side(value.get("side").and_then(Value::as_str))?,
        position_side: PositionSide::Net,
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity_role(value),
        price,
        quantity,
        quote_quantity: value
            .get("notional")
            .or_else(|| value.get("quote_quantity"))
            .or_else(|| value.get("quoteQuantity"))
            .and_then(value_as_f64)
            .or(Some(price * quantity)),
        fee_asset: value
            .get("fee_asset")
            .or_else(|| value.get("feeAsset"))
            .and_then(Value::as_str)
            .map(str::to_string),
        fee_amount: value
            .get("fee")
            .or_else(|| value.get("fee_amount"))
            .or_else(|| value.get("feeAmount"))
            .and_then(value_as_f64),
        fee_rate: value
            .get("fee_rate")
            .or_else(|| value.get("feeRate"))
            .and_then(value_as_f64),
        realized_pnl: value
            .get("realized_pnl")
            .or_else(|| value.get("realizedPnl"))
            .and_then(value_as_f64),
        filled_at: value
            .get("created_at")
            .or_else(|| value.get("createdAt"))
            .or_else(|| value.get("timestamp"))
            .or_else(|| value.get("time"))
            .and_then(parse_datetime_value)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    };
    fill.validate().map_err(validation_error)?;
    Ok(fill)
}

fn parse_liquidity_role(value: &Value) -> LiquidityRole {
    if let Some(role) = value
        .get("liquidity")
        .or_else(|| value.get("liquidity_role"))
        .and_then(Value::as_str)
    {
        return match role.to_ascii_lowercase().as_str() {
            "maker" | "m" => LiquidityRole::Maker,
            "taker" | "t" => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        };
    }
    match value.get("is_maker").and_then(Value::as_bool) {
        Some(true) => LiquidityRole::Maker,
        Some(false) => LiquidityRole::Taker,
        None => LiquidityRole::Unknown,
    }
}

fn item_success(value: &Value) -> bool {
    value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| {
            value
                .get("status")
                .and_then(Value::as_str)
                .map(|status| {
                    let status = status.to_ascii_lowercase();
                    matches!(
                        status.as_str(),
                        "success" | "ok" | "cancelled" | "canceled" | "closed"
                    )
                })
                .unwrap_or(true)
        })
}

fn parse_side(side: Option<&str>) -> ExchangeApiResult<OrderSide> {
    match side.unwrap_or_default().to_ascii_lowercase().as_str() {
        "buy" | "bid" => Ok(OrderSide::Buy),
        "sell" | "ask" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unknown BSX order side {other}"),
        }),
    }
}

fn parse_order_type(order_type: Option<&str>) -> OrderType {
    match order_type.unwrap_or_default().to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_order_status(status: Option<&str>) -> OrderStatus {
    match status.unwrap_or_default().to_ascii_lowercase().as_str() {
        "open" | "new" | "created" | "accepted" => OrderStatus::New,
        "partially_filled" | "partial_fill" | "partiallyfilled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "cancelled" | "canceled" | "closed" => OrderStatus::Cancelled,
        "expired" => OrderStatus::Expired,
        "rejected" | "failed" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn parse_datetime_value(value: &Value) -> Option<DateTime<Utc>> {
    value
        .as_str()
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .or_else(|| value_as_i128(value).and_then(timestamp_any))
}

fn item_error(exchange_id: &ExchangeId, value: &Value, fallback: &str) -> ExchangeError {
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::OrderRejected,
        value
            .get("message")
            .or_else(|| value.get("reason"))
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or(fallback),
        Utc::now(),
    );
    error.code = value
        .get("code")
        .or_else(|| value.get("error_code"))
        .and_then(Value::as_str)
        .map(str::to_string);
    error.order_id =
        string_or_number(value.get("order_id")).or_else(|| string_or_number(value.get("id")));
    error.client_order_id = value
        .get("client_order_id")
        .or_else(|| value.get("clientOrderId"))
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

fn is_perpetual_product(value: &Value) -> bool {
    let product_type = value
        .get("product_type")
        .or_else(|| value.get("type"))
        .or_else(|| value.get("market_type"))
        .and_then(Value::as_str);
    product_type
        .map(|product_type| {
            let product_type = product_type.to_ascii_lowercase();
            product_type.contains("perp") || product_type.contains("future")
        })
        .unwrap_or_else(|| {
            value
                .get("product_id")
                .or_else(|| value.get("id"))
                .or_else(|| value.get("symbol"))
                .and_then(Value::as_str)
                .is_some_and(|product_id| product_id.to_ascii_uppercase().ends_with("-PERP"))
        })
}

fn canonical_from_product_id(product_id: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let normalized = product_id.trim().to_ascii_uppercase();
    if let Some(base) = normalized.strip_suffix("-PERP") {
        return CanonicalSymbol::new(base, "USDC").map_err(validation_error);
    }
    let parts = normalized.split('-').collect::<Vec<_>>();
    if parts.len() >= 2 {
        return CanonicalSymbol::new(parts[0], parts[1]).map_err(validation_error);
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("cannot infer BSX canonical symbol from {product_id}"),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BSX order book missing price levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| match level {
            Value::Array(values) => {
                let price = values.first().and_then(value_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?;
                let quantity = values.get(1).and_then(value_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            Value::Object(object) => {
                let price = object
                    .get("price")
                    .or_else(|| object.get("p"))
                    .and_then(value_as_f64)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid level price", level)
                    })?;
                let quantity = object
                    .get("quantity")
                    .or_else(|| object.get("size"))
                    .or_else(|| object.get("q"))
                    .and_then(value_as_f64)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid level quantity", level)
                    })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            _ => Err(parse_error(
                exchange_id.clone(),
                "BSX order book level must be an array or object",
                level,
            )),
        })
        .collect()
}

fn ensure_no_error(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<()> {
    if value.get("error").is_none() && value.get("errors").is_none() {
        return Ok(());
    }
    Err(parse_error(
        exchange_id.clone(),
        "BSX response contains error",
        value,
    ))
}

fn decimal_path(value: &Value, path: &[&str]) -> Option<String> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    value_as_decimal_string(cursor)
}

fn value_as_decimal_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

fn value_as_i128(value: &Value) -> Option<i128> {
    match value {
        Value::Number(number) => number.as_i64().map(i128::from),
        Value::String(text) => text.parse::<i128>().ok(),
        _ => None,
    }
}

fn timestamp_any(raw: i128) -> Option<DateTime<Utc>> {
    let millis = if raw > 10_000_000_000_000_000 {
        raw / 1_000_000
    } else if raw > 10_000_000_000 {
        raw
    } else {
        raw * 1_000
    };
    Utc.timestamp_millis_opt(millis.try_into().ok()?).single()
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("BSX response missing {field}"),
            value,
        )
    })
}

fn precision_hint(decimal: &str) -> Option<u32> {
    decimal
        .split_once('.')
        .map(|(_, fraction)| fraction.trim_end_matches('0').len() as u32)
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Decode,
        format!("{message}: {value}"),
        Utc::now(),
    );
    error.raw = Some(value.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
