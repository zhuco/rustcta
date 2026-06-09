#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchItemResult, BatchOperationReport, BatchPlaceOrdersRequest, BatchPlaceOrdersResponse,
    ExchangeApiError, ExchangeApiResult, OrderState, ReconcilePlan, ReconcileTrigger,
    ResponseMetadata, RetryReconcilePolicy, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion, TenantId, TimeInForce,
};
use serde_json::Value;

pub fn parse_lighter_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    market_items(value)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Lighter markets missing order_books",
                value,
            )
        })?
        .iter()
        .filter(|item| {
            item.get("market_type")
                .or_else(|| item.get("type"))
                .or_else(|| item.get("filter"))
                .and_then(Value::as_str)
                .is_none_or(|kind| {
                    matches!(kind.to_ascii_lowercase().as_str(), "perp" | "perpetual")
                })
        })
        .map(|item| parse_lighter_symbol_rule(exchange_id, item))
        .collect()
}

fn parse_lighter_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let market_id = string_or_number(
        value
            .get("market_id")
            .or_else(|| value.get("market_index"))
            .or_else(|| value.get("id")),
    )
    .ok_or_else(|| parse_error(exchange_id.clone(), "Lighter market missing id", value))?;
    let symbol = string_or_number(value.get("symbol").or_else(|| value.get("name")))
        .unwrap_or_else(|| format!("market:{market_id}"));
    let (fallback_base, fallback_quote) =
        split_lighter_symbol(&symbol).unwrap_or_else(|| ("UNKNOWN".to_string(), "USD".to_string()));
    let base_asset = string_or_number(value.get("base").or_else(|| value.get("base_asset")))
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or(fallback_base);
    let quote_asset = string_or_number(value.get("quote").or_else(|| value.get("quote_asset")))
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or(fallback_quote);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let tradable = value
        .get("status")
        .and_then(Value::as_str)
        .map(|status| matches!(status.to_ascii_lowercase().as_str(), "active" | "open"))
        .unwrap_or(true);
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Perpetual,
                format!("market:{market_id}"),
            )
            .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_or_number(
            value
                .get("price_tick")
                .or_else(|| value.get("tick_size"))
                .or_else(|| value.get("min_price_increment")),
        ),
        quantity_increment: string_or_number(
            value
                .get("size_tick")
                .or_else(|| value.get("step_size"))
                .or_else(|| value.get("min_base_amount_increment")),
        ),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(
            value
                .get("min_base_amount")
                .or_else(|| value.get("min_quantity")),
        ),
        max_quantity: string_or_number(
            value
                .get("max_base_amount")
                .or_else(|| value.get("max_quantity")),
        ),
        min_notional: None,
        max_notional: None,
        price_precision: string_or_number(
            value
                .get("price_tick")
                .or_else(|| value.get("tick_size"))
                .or_else(|| value.get("min_price_increment")),
        )
        .and_then(decimal_precision),
        quantity_precision: string_or_number(
            value
                .get("size_tick")
                .or_else(|| value.get("step_size"))
                .or_else(|| value.get("min_base_amount_increment")),
        )
        .and_then(decimal_precision),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

pub fn parse_lighter_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let book = value
        .get("order_book")
        .or_else(|| value.get("orders"))
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let bids = parse_levels(exchange_id, book.get("bids"))?;
    let asks = parse_levels(exchange_id, book.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "Lighter order book request requires canonical_symbol".to_string(),
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
    snapshot.sequence = book.get("nonce").and_then(Value::as_u64);
    Ok(snapshot)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LighterOrderBookFrameMeta {
    pub channel: Option<String>,
    pub offset: Option<u64>,
    pub begin_nonce: Option<u64>,
    pub nonce: Option<u64>,
    pub checksum: Option<i64>,
}

impl LighterOrderBookFrameMeta {
    pub fn is_continuous_after(&self, previous_nonce: u64) -> bool {
        self.begin_nonce == Some(previous_nonce)
    }

    pub fn requires_resubscribe_after(&self, previous_nonce: u64) -> bool {
        !self.is_continuous_after(previous_nonce)
    }
}

pub fn parse_lighter_order_book_frame_meta(payload: &Value) -> LighterOrderBookFrameMeta {
    let book = payload.get("order_book").unwrap_or(payload);
    LighterOrderBookFrameMeta {
        channel: payload
            .get("channel")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        offset: payload
            .get("offset")
            .or_else(|| book.get("offset"))
            .and_then(Value::as_u64),
        begin_nonce: book.get("begin_nonce").and_then(Value::as_u64),
        nonce: book.get("nonce").and_then(Value::as_u64),
        checksum: book.get("checksum").and_then(Value::as_i64),
    }
}

pub fn parse_lighter_amend_order_ack(
    exchange_id: &ExchangeId,
    request: &AmendOrderRequest,
    value: &Value,
) -> ExchangeApiResult<AmendOrderResponse> {
    ensure_lighter_success(exchange_id, value)?;
    let order = parse_lighter_private_order(
        exchange_id,
        &request.symbol,
        first_order_like(value),
        Some(request),
        None,
    )?;
    Ok(AmendOrderResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        order,
    })
}

pub fn parse_lighter_batch_place_orders_ack(
    exchange_id: &ExchangeId,
    request: &BatchPlaceOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
    ensure_lighter_success(exchange_id, value)?;
    let items = batch_items(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Lighter batch place ack missing item list",
            value,
        )
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
                    "Lighter batch place response omitted item",
                    Value::Null,
                ),
                Some(ReconcilePlan::for_place_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchResponseMissingItem,
                    order_request,
                    RetryReconcilePolicy::default(),
                    "Lighter sendTxBatch omitted a create-order item; query/open-orders reconciliation is required",
                )),
            ));
            continue;
        };

        if !batch_item_success(item) {
            results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                string_or_number(item.get("order_index"))
                    .or_else(|| string_or_number(item.get("order_id")))
                    .or_else(|| string_or_number(item.get("id"))),
                batch_item_error(exchange_id, "Lighter batch place item failed", item.clone()),
                Some(ReconcilePlan::for_place_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchPlacePartialFailure,
                    order_request,
                    RetryReconcilePolicy::default(),
                    "Lighter sendTxBatch returned a rejected or ambiguous create-order item",
                )),
            ));
            continue;
        }

        let order =
            parse_lighter_private_order(exchange_id, &order_request.symbol, item, None, None)?;
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

pub fn parse_lighter_batch_cancel_orders_ack(
    exchange_id: &ExchangeId,
    request: &BatchCancelOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<BatchCancelOrdersResponse> {
    ensure_lighter_success(exchange_id, value)?;
    let items = batch_items(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Lighter batch cancel ack missing item list",
            value,
        )
    })?;

    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(request.cancels.len());
    for (index, cancel_request) in request.cancels.iter().enumerate() {
        let Some(item) = items.get(index) else {
            results.push(BatchItemResult::failed(
                index,
                cancel_request.client_order_id.clone(),
                cancel_request.exchange_order_id.clone(),
                batch_item_error(
                    exchange_id,
                    "Lighter batch cancel response omitted item",
                    Value::Null,
                ),
                Some(ReconcilePlan::for_cancel_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchResponseMissingItem,
                    cancel_request,
                    RetryReconcilePolicy::default(),
                    "Lighter sendTxBatch omitted a cancel-order item; query/open-orders reconciliation is required",
                )),
            ));
            continue;
        };

        if !batch_item_success(item) {
            results.push(BatchItemResult::failed(
                index,
                cancel_request.client_order_id.clone(),
                string_or_number(item.get("order_index"))
                    .or_else(|| string_or_number(item.get("order_id")))
                    .or_else(|| cancel_request.exchange_order_id.clone()),
                batch_item_error(
                    exchange_id,
                    "Lighter batch cancel item failed",
                    item.clone(),
                ),
                Some(ReconcilePlan::for_cancel_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchCancelPartialFailure,
                    cancel_request,
                    RetryReconcilePolicy::default(),
                    "Lighter sendTxBatch returned a rejected or ambiguous cancel-order item",
                )),
            ));
            continue;
        }

        let order = parse_lighter_private_order(
            exchange_id,
            &cancel_request.symbol,
            item,
            None,
            Some(cancel_request.exchange_order_id.clone()),
        )?;
        results.push(BatchItemResult::success(index, order.clone()));
        orders.push(order);
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

pub fn parse_lighter_query_order(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    order_id: &str,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let orders = parse_lighter_open_orders(exchange_id, symbol, value)?;
    Ok(orders.into_iter().find(|order| {
        order.exchange_order_id.as_deref() == Some(order_id)
            || order.client_order_id.as_deref() == Some(order_id)
    }))
}

pub fn parse_lighter_open_orders(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    order_items(value)
        .iter()
        .map(|item| parse_lighter_private_order(exchange_id, symbol, item, None, None))
        .collect()
}

pub fn parse_lighter_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "Lighter recent fills require canonical_symbol".to_string(),
            })?;
    trade_items(value)
        .iter()
        .map(|item| {
            let price = decimal_as_f64(
                item.get("price")
                    .or_else(|| item.get("trade_price"))
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "Lighter fill missing price", item)
                    })?,
            )
            .ok_or_else(|| parse_error(exchange_id.clone(), "invalid Lighter fill price", item))?;
            let quantity = decimal_as_f64(
                item.get("quantity")
                    .or_else(|| item.get("base_amount"))
                    .or_else(|| item.get("size"))
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "Lighter fill missing quantity", item)
                    })?,
            )
            .ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid Lighter fill quantity", item)
            })?;
            let now = Utc::now();
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: string_or_number(item.get("order_index"))
                    .or_else(|| string_or_number(item.get("order_id")))
                    .or_else(|| string_or_number(item.get("id"))),
                client_order_id: string_or_number(item.get("client_order_id"))
                    .or_else(|| string_or_number(item.get("clientOrderId")))
                    .or_else(|| string_or_number(item.get("client_order_index"))),
                fill_id: string_or_number(item.get("trade_id"))
                    .or_else(|| string_or_number(item.get("fill_id")))
                    .or_else(|| string_or_number(item.get("id"))),
                side: parse_order_side(item.get("side"))
                    .or_else(|| parse_lighter_is_ask(item.get("is_ask")))
                    .unwrap_or(OrderSide::Buy),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity_role(
                    item.get("liquidity_role").or_else(|| item.get("role")),
                ),
                price,
                quantity,
                quote_quantity: decimal_as_f64_opt(
                    item.get("quote_quantity")
                        .or_else(|| item.get("quote_amount"))
                        .or_else(|| item.get("notional")),
                )
                .or(Some(price * quantity)),
                fee_asset: string_or_number(item.get("fee_asset")).or_else(|| {
                    symbol
                        .canonical_symbol
                        .as_ref()
                        .map(|canonical| canonical.quote_asset().to_string())
                }),
                fee_amount: decimal_as_f64_opt(
                    item.get("fee")
                        .or_else(|| item.get("fee_amount"))
                        .or_else(|| item.get("fee_usd")),
                ),
                fee_rate: decimal_as_f64_opt(item.get("fee_rate")),
                realized_pnl: decimal_as_f64_opt(
                    item.get("realized_pnl")
                        .or_else(|| item.get("realized_pnl_usd")),
                ),
                filled_at: parse_timestamp(item).unwrap_or(now),
                received_at: now,
            })
        })
        .collect()
}

fn market_items(value: &Value) -> Option<&[Value]> {
    value
        .get("order_books")
        .or_else(|| value.get("markets"))
        .or_else(|| value.get("data"))
        .unwrap_or(value)
        .as_array()
        .map(Vec::as_slice)
}

fn parse_lighter_private_order(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
    amend_hint: Option<&AmendOrderRequest>,
    fallback_exchange_order_id: Option<Option<String>>,
) -> ExchangeApiResult<OrderState> {
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: string_or_number(value.get("client_order_id"))
            .or_else(|| string_or_number(value.get("clientOrderId")))
            .or_else(|| string_or_number(value.get("client_order_index")))
            .or_else(|| amend_hint.and_then(|request| request.client_order_id.clone())),
        exchange_order_id: string_or_number(value.get("order_index"))
            .or_else(|| string_or_number(value.get("order_id")))
            .or_else(|| string_or_number(value.get("id")))
            .or_else(|| fallback_exchange_order_id.flatten())
            .or_else(|| amend_hint.and_then(|request| request.exchange_order_id.clone())),
        side: parse_order_side(value.get("side"))
            .or_else(|| parse_lighter_is_ask(value.get("is_ask")))
            .unwrap_or(OrderSide::Buy),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(value.get("order_type").or_else(|| value.get("type")))
            .unwrap_or(OrderType::Limit),
        time_in_force: parse_time_in_force(value.get("time_in_force").or_else(|| value.get("tif"))),
        status: parse_order_status(value.get("status").or_else(|| value.get("order_status")))
            .unwrap_or(OrderStatus::New),
        quantity: string_or_number(value.get("quantity"))
            .or_else(|| string_or_number(value.get("base_amount")))
            .or_else(|| string_or_number(value.get("new_base_amount")))
            .or_else(|| amend_hint.map(|request| request.new_quantity.clone()))
            .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price"))
            .or_else(|| string_or_number(value.get("new_price"))),
        filled_quantity: string_or_number(value.get("filled_quantity"))
            .or_else(|| string_or_number(value.get("filled_base_amount")))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("average_fill_price"))
            .or_else(|| string_or_number(value.get("avg_price"))),
        reduce_only: value
            .get("reduce_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: value
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: None,
        updated_at: now,
    })
}

fn ensure_lighter_success(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<()> {
    if value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(true)
        && value
            .get("code")
            .and_then(value_as_i64)
            .is_none_or(|code| code == 0)
    {
        return Ok(());
    }
    Err(parse_error(
        exchange_id.clone(),
        "Lighter response indicates failure",
        value,
    ))
}

fn first_order_like(value: &Value) -> &Value {
    response_data(value)
        .get("order")
        .or_else(|| response_data(value).get("tx"))
        .or_else(|| response_data(value).get("result"))
        .unwrap_or_else(|| response_data(value))
}

fn response_data(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn batch_items(value: &Value) -> Option<&Vec<Value>> {
    if let Some(items) = response_data(value).as_array() {
        return Some(items);
    }
    response_data(value)
        .get("results")
        .or_else(|| response_data(value).get("orders"))
        .or_else(|| response_data(value).get("txs"))
        .or_else(|| response_data(value).get("items"))
        .and_then(Value::as_array)
}

fn order_items(value: &Value) -> Vec<&Value> {
    let data = response_data(value);
    if let Some(items) = data.as_array() {
        return items.iter().collect();
    }
    data.get("orders")
        .or_else(|| data.get("active_orders"))
        .or_else(|| data.get("account_active_orders"))
        .or_else(|| data.get("items"))
        .and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .unwrap_or_default()
}

fn trade_items(value: &Value) -> Vec<&Value> {
    let data = response_data(value);
    if let Some(items) = data.as_array() {
        return items.iter().collect();
    }
    data.get("trades")
        .or_else(|| data.get("fills"))
        .or_else(|| data.get("items"))
        .and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .unwrap_or_default()
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
                "ACCEPTED" | "PENDING" | "NEW" | "OPEN" | "FILLED" | "CANCELED" | "CANCELLED"
            )
        })
}

fn batch_item_error(exchange_id: &ExchangeId, fallback: &str, raw: Value) -> ExchangeError {
    let message = raw
        .get("message")
        .or_else(|| raw.get("error"))
        .or_else(|| raw.get("error_message"))
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

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Lighter order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            if let Some(array) = level.as_array() {
                let price = array.first().and_then(decimal_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level price", level)
                })?;
                let quantity = array.get(1).and_then(decimal_as_f64).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid level quantity", level)
                })?;
                return OrderBookLevel::new(price, quantity).map_err(validation_error);
            }
            let price = level
                .get("price")
                .and_then(decimal_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = level
                .get("size")
                .or_else(|| level.get("quantity"))
                .and_then(decimal_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn split_lighter_symbol(symbol: &str) -> Option<(String, String)> {
    let cleaned = symbol.trim().replace("-PERP", "");
    let mut parts = cleaned
        .split(['-', '/', '_'])
        .filter(|part| !part.is_empty() && !part.eq_ignore_ascii_case("PERP"));
    Some((
        parts.next()?.to_ascii_uppercase(),
        parts.next().unwrap_or("USD").to_ascii_uppercase(),
    ))
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value
        .and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            Value::Bool(flag) => Some(flag.to_string()),
            _ => None,
        })
        .filter(|value| !value.trim().is_empty())
}

fn parse_lighter_is_ask(value: Option<&Value>) -> Option<OrderSide> {
    match value? {
        Value::Bool(true) => Some(OrderSide::Sell),
        Value::Bool(false) => Some(OrderSide::Buy),
        _ => None,
    }
}

fn parse_order_side(value: Option<&Value>) -> Option<OrderSide> {
    let text = string_or_number(value)?.to_ascii_uppercase();
    match text.as_str() {
        "BUY" | "BID" => Some(OrderSide::Buy),
        "SELL" | "ASK" => Some(OrderSide::Sell),
        _ => None,
    }
}

fn parse_order_type(value: Option<&Value>) -> Option<OrderType> {
    let text = string_or_number(value)?.to_ascii_uppercase();
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
    let text = string_or_number(value)?.to_ascii_uppercase();
    match text.as_str() {
        "GTC" => Some(TimeInForce::GTC),
        "IOC" => Some(TimeInForce::IOC),
        "FOK" => Some(TimeInForce::FOK),
        "GTX" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_order_status(value: Option<&Value>) -> Option<OrderStatus> {
    let text = string_or_number(value)?.to_ascii_uppercase();
    match text.as_str() {
        "ACCEPTED" | "PENDING" | "NEW" => Some(OrderStatus::New),
        "OPEN" => Some(OrderStatus::Open),
        "PARTIAL_FILLED" | "PARTIALLY_FILLED" => Some(OrderStatus::PartiallyFilled),
        "FILLED" => Some(OrderStatus::Filled),
        "CANCELLED" | "CANCELED" => Some(OrderStatus::Cancelled),
        "REJECTED" => Some(OrderStatus::Rejected),
        "EXPIRED" => Some(OrderStatus::Expired),
        _ => Some(OrderStatus::Unknown),
    }
}

fn parse_liquidity_role(value: Option<&Value>) -> LiquidityRole {
    let Some(text) = string_or_number(value).map(|text| text.to_ascii_uppercase()) else {
        return LiquidityRole::Unknown;
    };
    match text.as_str() {
        "MAKER" | "M" => LiquidityRole::Maker,
        "TAKER" | "T" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn decimal_as_f64(value: &Value) -> Option<f64> {
    string_or_number(Some(value))?.parse().ok()
}

fn decimal_as_f64_opt(value: Option<&Value>) -> Option<f64> {
    value.and_then(decimal_as_f64)
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn parse_timestamp(value: &Value) -> Option<chrono::DateTime<Utc>> {
    let raw = value
        .get("timestamp")
        .or_else(|| value.get("time"))
        .or_else(|| value.get("created_at"))
        .or_else(|| value.get("executed_at"))?;
    if let Some(text) = raw.as_str() {
        if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(text) {
            return Some(timestamp.with_timezone(&Utc));
        }
        if let Ok(epoch) = text.parse::<i64>() {
            return timestamp_from_epoch(epoch);
        }
    }
    raw.as_i64().and_then(timestamp_from_epoch)
}

fn timestamp_from_epoch(value: i64) -> Option<chrono::DateTime<Utc>> {
    if value > 10_000_000_000_000 {
        chrono::DateTime::<Utc>::from_timestamp_millis(value)
    } else if value > 10_000_000_000 {
        chrono::DateTime::<Utc>::from_timestamp_millis(value)
    } else {
        chrono::DateTime::<Utc>::from_timestamp(value, 0)
    }
}

fn decimal_precision(value: String) -> Option<u32> {
    let trimmed = value.trim().trim_end_matches('0');
    trimmed
        .split_once('.')
        .map(|(_, fraction)| fraction.len() as u32)
        .or(Some(0))
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
