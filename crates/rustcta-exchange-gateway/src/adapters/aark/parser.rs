use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, AmendOrderRequest, AmendOrderResponse, BatchItemResult, BatchOperationReport,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, ExchangeApiError, ExchangeApiResult,
    OrderState, ReconcilePlan, ReconcileTrigger, ResponseMetadata, RetryReconcilePolicy,
    SymbolRules, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, Fill,
    FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    ensure_success(exchange_id, value)?;
    let rows = value
        .get("data")
        .and_then(|data| data.get("rows"))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "Aark public info missing rows", value))?;
    rows.iter()
        .filter(|row| {
            row.get("symbol")
                .and_then(Value::as_str)
                .is_some_and(|symbol| symbol.starts_with("PERP_"))
        })
        .map(|row| parse_market(exchange_id, row))
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    ensure_success(exchange_id, value)?;
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("bids"))?;
    let asks = parse_levels(exchange_id, data.get("asks"))?;
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .or_else(|| {
            data.get("symbol")
                .and_then(Value::as_str)
                .and_then(|symbol| canonical_from_orderly_symbol(symbol).ok())
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "Aark order book requires canonical_symbol or response symbol".to_string(),
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
        .get("seq")
        .or_else(|| data.get("sequence"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .or_else(|| value.get("timestamp"))
        .and_then(value_as_i64)
        .and_then(timestamp_millis);
    Ok(snapshot)
}

pub fn parse_amend_order_ack(
    exchange_id: &ExchangeId,
    request: &AmendOrderRequest,
    value: &Value,
) -> ExchangeApiResult<AmendOrderResponse> {
    ensure_success(exchange_id, value)?;
    let data = response_data(value);
    let order = parse_order_state(exchange_id, Some(&request.symbol), data, Some(request))?;
    Ok(AmendOrderResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        order,
    })
}

pub fn parse_batch_place_orders_ack(
    exchange_id: &ExchangeId,
    request: &BatchPlaceOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
    ensure_success(exchange_id, value)?;
    let items = batch_items(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Aark batch order response missing item list",
            value,
        )
    })?;

    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(request.orders.len());
    for (index, order_request) in request.orders.iter().enumerate() {
        let Some(item) = items.get(index) else {
            let error = batch_item_error(
                exchange_id,
                "Aark batch create response omitted item",
                Value::Null,
            );
            results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                None,
                error,
                Some(ReconcilePlan::for_place_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchResponseMissingItem,
                    order_request,
                    RetryReconcilePolicy::default(),
                    "Aark/Orderly batch-create did not return a result for this request item",
                )),
            ));
            continue;
        };

        if !batch_item_success(item) {
            let error =
                batch_item_error(exchange_id, "Aark batch create item failed", item.clone());
            results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                string_or_number(item.get("order_id")).or_else(|| string_or_number(item.get("id"))),
                error,
                None,
            ));
            continue;
        }

        let order = parse_order_state(exchange_id, Some(&order_request.symbol), item, None)?;
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

pub fn parse_single_order(
    exchange_id: &ExchangeId,
    fallback_symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    ensure_success(exchange_id, value)?;
    parse_order_state(
        exchange_id,
        Some(fallback_symbol),
        response_data(value),
        None,
    )
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    ensure_success(exchange_id, value)?;
    order_rows(value)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Aark orders response missing rows",
                value,
            )
        })?
        .iter()
        .map(|row| parse_order_state(exchange_id, fallback_symbol, row, None))
        .collect()
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    ensure_success(exchange_id, value)?;
    order_rows(value)
        .or_else(|| trade_rows(value))
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Aark trades response missing rows",
                value,
            )
        })?
        .iter()
        .map(|row| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                fallback_symbol,
                row,
            )
        })
        .collect()
}

fn parse_market(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let symbol_text = required_str(exchange_id, value, "symbol")?;
    let canonical_symbol = canonical_from_orderly_symbol(symbol_text)?;
    let base_asset = value
        .get("base")
        .or_else(|| value.get("base_asset"))
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| canonical_symbol.base_asset().to_string());
    let quote_asset = value
        .get("quote")
        .or_else(|| value.get("quote_asset"))
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
            symbol_text,
        )
        .map_err(validation_error)?,
    };
    let price_increment = decimal_path(value, &["quote_tick"])
        .or_else(|| decimal_path(value, &["price_tick"]))
        .or_else(|| decimal_path(value, &["tick_size"]));
    let quantity_increment = decimal_path(value, &["base_tick"])
        .or_else(|| decimal_path(value, &["size_tick"]))
        .or_else(|| decimal_path(value, &["lot_size"]));
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: None,
        max_price: None,
        min_quantity: decimal_path(value, &["base_min"]),
        max_quantity: decimal_path(value, &["base_max"]),
        min_notional: decimal_path(value, &["min_notional"])
            .or_else(|| decimal_path(value, &["quote_min"])),
        max_notional: decimal_path(value, &["quote_max"]),
        price_precision: price_increment.as_deref().and_then(precision_hint),
        quantity_precision: quantity_increment.as_deref().and_then(precision_hint),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol_text = value
        .get("symbol")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| fallback_symbol.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| parse_error(exchange_id.clone(), "Aark fill missing symbol", value))?;
    let exchange_symbol =
        ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, &symbol_text)
            .map_err(validation_error)?;
    let price = value
        .get("executed_price")
        .or_else(|| value.get("trade_price"))
        .or_else(|| value.get("price"))
        .and_then(value_as_f64)
        .ok_or_else(|| parse_error(exchange_id.clone(), "Aark fill missing price", value))?;
    let quantity = value
        .get("executed_quantity")
        .or_else(|| value.get("trade_quantity"))
        .or_else(|| value.get("quantity"))
        .or_else(|| value.get("qty"))
        .and_then(value_as_f64)
        .ok_or_else(|| parse_error(exchange_id.clone(), "Aark fill missing quantity", value))?;
    let side = value
        .get("side")
        .and_then(Value::as_str)
        .and_then(|side| parse_order_side(Some(&Value::String(side.to_string()))))
        .unwrap_or(OrderSide::Buy);
    let fill = Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: canonical_from_orderly_symbol(&symbol_text)?,
        exchange_symbol: Some(exchange_symbol),
        order_id: string_or_number(value.get("order_id")),
        client_order_id: string_or_number(value.get("client_order_id")),
        fill_id: string_or_number(value.get("id"))
            .or_else(|| string_or_number(value.get("match_id")))
            .or_else(|| string_or_number(value.get("trade_id"))),
        side,
        position_side: PositionSide::Net,
        status: FillStatus::Confirmed,
        liquidity_role: match value.get("is_maker").and_then(value_as_i64) {
            Some(1) => LiquidityRole::Maker,
            Some(0) => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        },
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: value
            .get("fee_asset")
            .and_then(Value::as_str)
            .map(str::to_string),
        fee_amount: value.get("fee").and_then(value_as_f64),
        fee_rate: value.get("order_enum_fee_rate").and_then(value_as_f64),
        realized_pnl: value.get("realized_pnl").and_then(value_as_f64),
        filled_at: value
            .get("executed_timestamp")
            .or_else(|| value.get("timestamp"))
            .and_then(value_as_i64)
            .and_then(timestamp_millis)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    };
    fill.validate().map_err(validation_error)?;
    Ok(fill)
}

fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
    amend_hint: Option<&AmendOrderRequest>,
) -> ExchangeApiResult<OrderState> {
    let symbol_text = value
        .get("symbol")
        .and_then(Value::as_str)
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.as_str()))
        .ok_or_else(|| parse_error(exchange_id.clone(), "Aark order ack missing symbol", value))?;
    let exchange_symbol = symbol_hint
        .map(|symbol| symbol.exchange_symbol.clone())
        .unwrap_or_else(|| {
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol_text)
                .expect("validated exchange symbol")
        });
    let canonical_symbol = symbol_hint
        .and_then(|symbol| symbol.canonical_symbol.clone())
        .or_else(|| canonical_from_orderly_symbol(symbol_text).ok());
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol_hint
            .map(|symbol| symbol.market_type)
            .unwrap_or(MarketType::Perpetual),
        canonical_symbol,
        exchange_symbol,
        client_order_id: value
            .get("client_order_id")
            .or_else(|| value.get("clientOrderId"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| amend_hint.and_then(|request| request.client_order_id.clone())),
        exchange_order_id: string_or_number(value.get("order_id")).or_else(|| {
            string_or_number(value.get("id"))
                .or_else(|| amend_hint.and_then(|request| request.exchange_order_id.clone()))
        }),
        side: parse_order_side(value.get("side")).unwrap_or(OrderSide::Buy),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(value.get("order_type").or_else(|| value.get("type")))
            .unwrap_or(OrderType::Limit),
        time_in_force: parse_time_in_force(value.get("time_in_force").or_else(|| value.get("tif"))),
        status: parse_order_status(value.get("status").or_else(|| value.get("order_status")))
            .unwrap_or(OrderStatus::New),
        quantity: decimal_path(value, &["order_quantity"])
            .or_else(|| decimal_path(value, &["quantity"]))
            .or_else(|| decimal_path(value, &["qty"]))
            .or_else(|| amend_hint.map(|request| request.new_quantity.clone()))
            .unwrap_or_else(|| "0".to_string()),
        price: decimal_path(value, &["order_price"]).or_else(|| decimal_path(value, &["price"])),
        filled_quantity: decimal_path(value, &["executed_quantity"])
            .or_else(|| decimal_path(value, &["filled_quantity"]))
            .or_else(|| decimal_path(value, &["filled"]))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: decimal_path(value, &["average_executed_price"])
            .or_else(|| decimal_path(value, &["avg_price"])),
        reduce_only: value
            .get("reduce_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: value
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: value
            .get("created_time")
            .or_else(|| value.get("created_at"))
            .and_then(value_as_i64)
            .and_then(timestamp_millis),
        updated_at: value
            .get("updated_time")
            .or_else(|| value.get("updated_at"))
            .and_then(value_as_i64)
            .and_then(timestamp_millis)
            .unwrap_or(now),
    })
}

fn canonical_from_orderly_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let parts = symbol.trim().split('_').collect::<Vec<_>>();
    if parts.len() < 3 || !parts[0].eq_ignore_ascii_case("PERP") {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("cannot infer Aark canonical symbol from {symbol}"),
        });
    }
    CanonicalSymbol::new(parts[1], parts[2]).map_err(validation_error)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Aark order book missing price levels",
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
                "Aark order book level must be an array or object",
                level,
            )),
        })
        .collect()
}

fn ensure_success(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<()> {
    if value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        return Ok(());
    }
    Err(parse_error(
        exchange_id.clone(),
        "Aark response success=false",
        value,
    ))
}

fn response_data(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn batch_items(value: &Value) -> Option<&Vec<Value>> {
    if let Some(items) = response_data(value).as_array() {
        return Some(items);
    }
    response_data(value)
        .get("rows")
        .or_else(|| response_data(value).get("orders"))
        .or_else(|| response_data(value).get("data"))
        .and_then(Value::as_array)
}

fn order_rows(value: &Value) -> Option<&Vec<Value>> {
    response_data(value)
        .get("rows")
        .or_else(|| response_data(value).get("orders"))
        .or_else(|| response_data(value).get("data"))
        .and_then(Value::as_array)
        .or_else(|| response_data(value).as_array())
}

fn trade_rows(value: &Value) -> Option<&Vec<Value>> {
    response_data(value)
        .get("rows")
        .or_else(|| response_data(value).get("trades"))
        .and_then(Value::as_array)
}

fn batch_item_success(value: &Value) -> bool {
    if value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return true;
    }
    value
        .get("status")
        .or_else(|| value.get("order_status"))
        .and_then(Value::as_str)
        .is_some_and(|status| {
            matches!(
                status.to_ascii_uppercase().as_str(),
                "NEW" | "OPEN" | "PARTIAL_FILLED" | "PARTIALLY_FILLED" | "FILLED"
            )
        })
}

fn batch_item_error(exchange_id: &ExchangeId, fallback: &str, raw: Value) -> ExchangeError {
    let message = raw
        .get("message")
        .or_else(|| raw.get("error_message"))
        .or_else(|| raw.get("error"))
        .and_then(Value::as_str)
        .unwrap_or(fallback);
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::OrderRejected,
        message.to_string(),
        Utc::now(),
    );
    error.raw = Some(raw);
    error
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(text)) if !text.trim().is_empty() => Some(text.trim().to_string()),
        Some(Value::Number(number)) => Some(number.to_string()),
        _ => None,
    }
}

fn parse_order_side(value: Option<&Value>) -> Option<OrderSide> {
    let text = value.and_then(Value::as_str)?.to_ascii_uppercase();
    match text.as_str() {
        "BUY" | "BID" => Some(OrderSide::Buy),
        "SELL" | "ASK" => Some(OrderSide::Sell),
        _ => None,
    }
}

fn parse_order_type(value: Option<&Value>) -> Option<OrderType> {
    let text = value.and_then(Value::as_str)?.to_ascii_uppercase();
    match text.as_str() {
        "MARKET" => Some(OrderType::Market),
        "LIMIT" => Some(OrderType::Limit),
        "POST_ONLY" | "POSTONLY" | "LIMIT_MAKER" => Some(OrderType::PostOnly),
        "IOC" => Some(OrderType::IOC),
        "FOK" => Some(OrderType::FOK),
        _ => None,
    }
}

fn parse_time_in_force(value: Option<&Value>) -> Option<TimeInForce> {
    let text = value.and_then(Value::as_str)?.to_ascii_uppercase();
    match text.as_str() {
        "GTC" => Some(TimeInForce::GTC),
        "IOC" => Some(TimeInForce::IOC),
        "FOK" => Some(TimeInForce::FOK),
        "GTX" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_order_status(value: Option<&Value>) -> Option<OrderStatus> {
    let text = value.and_then(Value::as_str)?.to_ascii_uppercase();
    match text.as_str() {
        "NEW" => Some(OrderStatus::New),
        "OPEN" => Some(OrderStatus::Open),
        "PARTIAL_FILLED" | "PARTIALLY_FILLED" => Some(OrderStatus::PartiallyFilled),
        "FILLED" => Some(OrderStatus::Filled),
        "PENDING_CANCEL" => Some(OrderStatus::PendingCancel),
        "CANCELLED" | "CANCELED" => Some(OrderStatus::Cancelled),
        "REJECTED" => Some(OrderStatus::Rejected),
        "EXPIRED" => Some(OrderStatus::Expired),
        _ => Some(OrderStatus::Unknown),
    }
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

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
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

fn timestamp_millis(value: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(value).single()
}

fn precision_hint(value: &str) -> Option<u32> {
    if let Ok(number) = value.parse::<u32>() {
        return Some(number);
    }
    value
        .trim_end_matches('0')
        .trim_end_matches('.')
        .split('.')
        .nth(1)
        .map(|fraction| fraction.len() as u32)
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            format!("Aark public info missing {field}"),
            value,
        )
    })
}

fn parse_error(
    exchange_id: ExchangeId,
    message: impl Into<String>,
    raw: &Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Unknown,
        message.into(),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
