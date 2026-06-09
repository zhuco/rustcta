#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchItemResult, BatchOperationReport, BatchPlaceOrdersRequest, BatchPlaceOrdersResponse,
    ExchangeApiError, ExchangeApiResult, OrderState, ReconcilePlan, ReconcileTrigger,
    ResponseMetadata, RetryReconcilePolicy, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, TenantId,
};
use serde_json::Value;

pub fn parse_grvt_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    data_items(value)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "GRVT instruments missing result",
                value,
            )
        })?
        .iter()
        .filter(|item| {
            item.get("instrument_type")
                .or_else(|| item.get("kind"))
                .and_then(Value::as_str)
                .is_none_or(|kind| {
                    matches!(
                        kind.to_ascii_uppercase().as_str(),
                        "PERPETUAL" | "FUTURE" | "CALL" | "PUT" | "OPTION"
                    )
                })
        })
        .map(|item| parse_grvt_symbol_rule(exchange_id, item))
        .collect()
}

fn parse_grvt_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolRules> {
    let symbol = required_str(
        exchange_id,
        value,
        &["instrument", "instrument_name", "symbol"],
    )?;
    let market_type = match value
        .get("instrument_type")
        .or_else(|| value.get("kind"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .as_deref()
    {
        Some("CALL" | "PUT" | "OPTION") => MarketType::Option,
        _ => MarketType::Perpetual,
    };
    let (fallback_base, fallback_quote) =
        split_grvt_symbol(symbol).unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    let base_asset = string_or_number(
        value
            .get("base")
            .or_else(|| value.get("base_asset"))
            .or_else(|| value.get("baseCurrency")),
    )
    .map(|value| value.to_ascii_uppercase())
    .unwrap_or(fallback_base);
    let quote_asset = string_or_number(
        value
            .get("quote")
            .or_else(|| value.get("quote_asset"))
            .or_else(|| value.get("quoteCurrency"))
            .or_else(|| value.get("settlement_asset")),
    )
    .map(|value| value.to_ascii_uppercase())
    .unwrap_or(fallback_quote);
    let tradable = value
        .get("is_active")
        .and_then(Value::as_bool)
        .or_else(|| {
            value
                .get("status")
                .and_then(Value::as_str)
                .map(|status| matches!(status.to_ascii_uppercase().as_str(), "ACTIVE" | "LIVE"))
        })
        .unwrap_or(true);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol)
                .map_err(validation_error)?,
        },
        base_asset,
        quote_asset,
        price_increment: string_or_number(
            value
                .get("tick_size")
                .or_else(|| value.get("price_increment"))
                .or_else(|| value.get("min_price_increment")),
        ),
        quantity_increment: string_or_number(
            value
                .get("lot_size")
                .or_else(|| value.get("quantity_increment"))
                .or_else(|| value.get("min_size_increment")),
        ),
        min_price: string_or_number(value.get("min_price")),
        max_price: string_or_number(value.get("max_price")),
        min_quantity: string_or_number(value.get("min_size").or_else(|| value.get("min_quantity"))),
        max_quantity: string_or_number(value.get("max_size").or_else(|| value.get("max_quantity"))),
        min_notional: None,
        max_notional: None,
        price_precision: string_or_number(
            value
                .get("tick_size")
                .or_else(|| value.get("price_increment"))
                .or_else(|| value.get("min_price_increment")),
        )
        .and_then(decimal_precision),
        quantity_precision: string_or_number(
            value
                .get("lot_size")
                .or_else(|| value.get("quantity_increment"))
                .or_else(|| value.get("min_size_increment")),
        )
        .and_then(decimal_precision),
        supports_market_orders: tradable && market_type == MarketType::Perpetual,
        supports_limit_orders: tradable,
        supports_post_only: false,
        supports_reduce_only: market_type == MarketType::Perpetual,
        updated_at: Utc::now(),
    })
}

pub fn parse_grvt_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = data_payload(value);
    let bids = parse_levels(exchange_id, data.get("bids").or_else(|| data.get("b")))?;
    let asks = parse_levels(exchange_id, data.get("asks").or_else(|| data.get("a")))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "GRVT order book request requires canonical_symbol".to_string(),
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
        .get("sequence_number")
        .or_else(|| data.get("seq"))
        .and_then(Value::as_u64);
    snapshot.exchange_timestamp = data
        .get("event_time")
        .or_else(|| data.get("eventTime"))
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|timestamp| timestamp.with_timezone(&Utc));
    Ok(snapshot)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrvtBookFrameMeta {
    pub stream: Option<String>,
    pub sequence_number: Option<u64>,
    pub event_time: Option<String>,
    pub snapshot: bool,
}

pub fn parse_grvt_book_frame_meta(payload: &Value) -> GrvtBookFrameMeta {
    let sequence_number = payload
        .get("sequence_number")
        .or_else(|| payload.get("seq"))
        .and_then(Value::as_u64);
    GrvtBookFrameMeta {
        stream: payload
            .get("stream")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        sequence_number,
        event_time: payload
            .get("event_time")
            .or_else(|| payload.get("eventTime"))
            .and_then(Value::as_str)
            .map(ToString::to_string),
        snapshot: sequence_number == Some(0),
    }
}

pub fn parse_grvt_amend_order_ack(
    exchange_id: &ExchangeId,
    request: &AmendOrderRequest,
    value: &Value,
) -> ExchangeApiResult<AmendOrderResponse> {
    ensure_success(exchange_id, value)?;
    let order = parse_grvt_order_state(
        exchange_id,
        &request.symbol,
        first_order_like(value)?,
        Some(GrvtOrderFallback::Amend(request)),
    )?;
    Ok(AmendOrderResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        order,
    })
}

pub fn parse_grvt_batch_place_orders_ack(
    exchange_id: &ExchangeId,
    request: &BatchPlaceOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
    ensure_success(exchange_id, value)?;
    let items = batch_items(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "GRVT bulk create ack missing item results",
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
                missing_item_error(exchange_id, "GRVT bulk create response omitted item"),
                Some(ReconcilePlan::for_place_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchResponseMissingItem,
                    order_request,
                    RetryReconcilePolicy::default(),
                    "GRVT bulk create omitted an item; query/open-orders reconciliation is required before replay",
                )),
            ));
            continue;
        };

        if !item_success(item) {
            results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                string_or_number(item.get("order_id")).or_else(|| string_or_number(item.get("id"))),
                item_error(exchange_id, item, "GRVT bulk create item failed"),
                Some(ReconcilePlan::for_place_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchPlacePartialFailure,
                    order_request,
                    RetryReconcilePolicy::default(),
                    "GRVT bulk create item failed or was ambiguous; readback reconciliation is required",
                )),
            ));
            continue;
        }

        let order = parse_grvt_order_state(
            exchange_id,
            &order_request.symbol,
            item,
            Some(GrvtOrderFallback::Place(order_request)),
        )?;
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

pub fn parse_grvt_batch_cancel_orders_ack(
    exchange_id: &ExchangeId,
    request: &BatchCancelOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<BatchCancelOrdersResponse> {
    ensure_success(exchange_id, value)?;
    let items = batch_items(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "GRVT bulk cancel ack missing item results",
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
                missing_item_error(exchange_id, "GRVT bulk cancel response omitted item"),
                Some(ReconcilePlan::for_cancel_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchResponseMissingItem,
                    cancel_request,
                    RetryReconcilePolicy::default(),
                    "GRVT bulk cancel omitted an item; query/open-orders reconciliation is required",
                )),
            ));
            continue;
        };

        if !item_success(item) {
            results.push(BatchItemResult::failed(
                index,
                cancel_request.client_order_id.clone(),
                string_or_number(item.get("order_id"))
                    .or_else(|| string_or_number(item.get("id")))
                    .or_else(|| cancel_request.exchange_order_id.clone()),
                item_error(exchange_id, item, "GRVT bulk cancel item failed"),
                Some(ReconcilePlan::for_cancel_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchCancelPartialFailure,
                    cancel_request,
                    RetryReconcilePolicy::default(),
                    "GRVT bulk cancel item failed or was ambiguous; readback reconciliation is required",
                )),
            ));
            continue;
        }

        let order = parse_grvt_order_state(
            exchange_id,
            &cancel_request.symbol,
            item,
            Some(GrvtOrderFallback::Cancel(cancel_request)),
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

pub fn parse_grvt_single_order(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    ensure_success(exchange_id, value)?;
    parse_grvt_order_state(exchange_id, symbol, first_order_like(value)?, None)
}

pub fn parse_grvt_orders(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    ensure_success(exchange_id, value)?;
    batch_items(value)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "GRVT orders response missing array",
                value,
            )
        })?
        .iter()
        .map(|order| parse_grvt_order_state(exchange_id, symbol, order, None))
        .collect()
}

pub fn parse_grvt_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    ensure_success(exchange_id, value)?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "grvt get_recent_fills requires canonical_symbol".to_string(),
            })?;
    fill_items(value)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "GRVT fills response missing fills array",
                value,
            )
        })?
        .iter()
        .map(|fill| {
            let side = fill
                .get("side")
                .and_then(Value::as_str)
                .and_then(parse_side)
                .ok_or_else(|| parse_error(exchange_id.clone(), "GRVT fill missing side", fill))?;
            let price = string_or_number(
                fill.get("price")
                    .or_else(|| fill.get("fill_price"))
                    .or_else(|| fill.get("execution_price")),
            )
            .and_then(|value| value.parse::<f64>().ok())
            .ok_or_else(|| parse_error(exchange_id.clone(), "GRVT fill missing price", fill))?;
            let quantity = string_or_number(
                fill.get("size")
                    .or_else(|| fill.get("quantity"))
                    .or_else(|| fill.get("fill_size"))
                    .or_else(|| fill.get("executed_size")),
            )
            .and_then(|value| value.parse::<f64>().ok())
            .ok_or_else(|| parse_error(exchange_id.clone(), "GRVT fill missing quantity", fill))?;
            let filled_at = fill
                .get("created_at")
                .or_else(|| fill.get("updated_at"))
                .or_else(|| fill.get("timestamp"))
                .or_else(|| fill.get("time"))
                .and_then(timestamp_any_value)
                .unwrap_or_else(Utc::now);
            Ok(Fill {
                schema_version: rustcta_types::SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: string_or_number(fill.get("order_id").or_else(|| fill.get("id"))),
                client_order_id: string_or_number(
                    fill.get("client_order_id")
                        .or_else(|| fill.get("clientOrderId")),
                ),
                fill_id: string_or_number(
                    fill.get("trade_id")
                        .or_else(|| fill.get("fill_id"))
                        .or_else(|| fill.get("execution_id")),
                ),
                side,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: fill
                    .get("liquidity")
                    .or_else(|| fill.get("liquidity_role"))
                    .and_then(Value::as_str)
                    .and_then(parse_liquidity_role)
                    .unwrap_or(LiquidityRole::Unknown),
                price,
                quantity,
                quote_quantity: string_or_number(
                    fill.get("notional")
                        .or_else(|| fill.get("quote_quantity"))
                        .or_else(|| fill.get("quote_qty")),
                )
                .and_then(|value| value.parse::<f64>().ok()),
                fee_asset: string_or_number(
                    fill.get("fee_asset").or_else(|| fill.get("fee_currency")),
                ),
                fee_amount: string_or_number(fill.get("fee").or_else(|| fill.get("fee_amount")))
                    .and_then(|value| value.parse::<f64>().ok()),
                fee_rate: string_or_number(fill.get("fee_rate"))
                    .and_then(|value| value.parse::<f64>().ok()),
                realized_pnl: string_or_number(fill.get("realized_pnl"))
                    .and_then(|value| value.parse::<f64>().ok()),
                filled_at,
                received_at: Utc::now(),
            })
        })
        .collect()
}

enum GrvtOrderFallback<'a> {
    Place(&'a rustcta_exchange_api::PlaceOrderRequest),
    Amend(&'a AmendOrderRequest),
    Cancel(&'a rustcta_exchange_api::CancelOrderRequest),
}

fn parse_grvt_order_state(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
    fallback: Option<GrvtOrderFallback<'_>>,
) -> ExchangeApiResult<OrderState> {
    let side = value
        .get("side")
        .and_then(Value::as_str)
        .and_then(parse_side)
        .or_else(|| match fallback {
            Some(GrvtOrderFallback::Place(request)) => Some(request.side),
            _ => None,
        })
        .ok_or_else(|| parse_error(exchange_id.clone(), "GRVT order ack missing side", value))?;
    let order_type = value
        .get("order_type")
        .or_else(|| value.get("type"))
        .and_then(Value::as_str)
        .and_then(parse_order_type)
        .or_else(|| match fallback {
            Some(GrvtOrderFallback::Place(request)) => Some(request.order_type),
            _ => None,
        })
        .unwrap_or(OrderType::Limit);
    let quantity = string_or_number(value.get("size"))
        .or_else(|| string_or_number(value.get("quantity")))
        .or_else(|| string_or_number(value.get("order_size")))
        .or_else(|| string_or_number(value.get("new_size")))
        .or_else(|| match fallback {
            Some(GrvtOrderFallback::Place(request)) => Some(request.quantity.clone()),
            Some(GrvtOrderFallback::Amend(request)) => Some(request.new_quantity.clone()),
            _ => None,
        })
        .ok_or_else(|| parse_error(exchange_id.clone(), "GRVT order ack missing size", value))?;
    let status = value
        .get("status")
        .or_else(|| value.get("order_status"))
        .and_then(Value::as_str)
        .and_then(parse_order_status)
        .unwrap_or_else(|| {
            if matches!(fallback, Some(GrvtOrderFallback::Cancel(_))) {
                OrderStatus::Cancelled
            } else {
                OrderStatus::Open
            }
        });
    let client_order_id = string_or_number(value.get("client_order_id"))
        .or_else(|| string_or_number(value.get("clientOrderId")))
        .or_else(|| match fallback {
            Some(GrvtOrderFallback::Place(request)) => request.client_order_id.clone(),
            Some(GrvtOrderFallback::Amend(request)) => request
                .new_client_order_id
                .clone()
                .or_else(|| request.client_order_id.clone()),
            Some(GrvtOrderFallback::Cancel(request)) => request.client_order_id.clone(),
            None => None,
        });
    let exchange_order_id = string_or_number(value.get("order_id"))
        .or_else(|| string_or_number(value.get("id")))
        .or_else(|| match fallback {
            Some(GrvtOrderFallback::Amend(request)) => request.exchange_order_id.clone(),
            Some(GrvtOrderFallback::Cancel(request)) => request.exchange_order_id.clone(),
            _ => None,
        });

    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
        side,
        position_side: Some(PositionSide::None),
        order_type,
        time_in_force: None,
        status,
        quantity,
        price: string_or_number(value.get("limit_price"))
            .or_else(|| string_or_number(value.get("price")))
            .or_else(|| string_or_number(value.get("new_limit_price")))
            .or_else(|| match fallback {
                Some(GrvtOrderFallback::Place(request)) => request.price.clone(),
                _ => None,
            }),
        filled_quantity: string_or_number(value.get("filled_size"))
            .or_else(|| string_or_number(value.get("filled_quantity")))
            .or_else(|| string_or_number(value.get("executed_size")))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("average_fill_price"))
            .or_else(|| string_or_number(value.get("avg_fill_price"))),
        reduce_only: value
            .get("reduce_only")
            .and_then(Value::as_bool)
            .or_else(|| match fallback {
                Some(GrvtOrderFallback::Place(request)) => Some(request.reduce_only),
                _ => None,
            })
            .unwrap_or(false),
        post_only: value
            .get("post_only")
            .and_then(Value::as_bool)
            .or_else(|| match fallback {
                Some(GrvtOrderFallback::Place(request)) => Some(request.post_only),
                _ => None,
            })
            .unwrap_or(false),
        created_at: value.get("created_at").and_then(timestamp_any_value),
        updated_at: value
            .get("updated_at")
            .and_then(timestamp_any_value)
            .unwrap_or_else(Utc::now),
    })
}

fn data_payload(value: &Value) -> &Value {
    value
        .get("result")
        .or_else(|| value.get("data"))
        .unwrap_or(value)
}

fn first_order_like(value: &Value) -> ExchangeApiResult<&Value> {
    let data = data_payload(value);
    Ok(data
        .get("order")
        .or_else(|| data.get("result"))
        .or_else(|| data.as_array().and_then(|items| items.first()))
        .unwrap_or(data))
}

fn batch_items(value: &Value) -> Option<&[Value]> {
    let data = data_payload(value);
    data.as_array()
        .or_else(|| data.get("results").and_then(Value::as_array))
        .or_else(|| data.get("orders").and_then(Value::as_array))
        .or_else(|| data.get("items").and_then(Value::as_array))
        .map(Vec::as_slice)
}

fn fill_items(value: &Value) -> Option<&[Value]> {
    let data = data_payload(value);
    data.as_array()
        .or_else(|| data.get("fills").and_then(Value::as_array))
        .or_else(|| data.get("trades").and_then(Value::as_array))
        .or_else(|| data.get("items").and_then(Value::as_array))
        .or_else(|| data.get("results").and_then(Value::as_array))
        .map(Vec::as_slice)
}

fn ensure_success(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<()> {
    if value.get("success").and_then(Value::as_bool) == Some(false)
        || value
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| matches!(status.to_ascii_uppercase().as_str(), "ERROR" | "FAIL"))
    {
        return Err(parse_error(
            exchange_id.clone(),
            "GRVT response reported failure",
            value,
        ));
    }
    Ok(())
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
                    !matches!(
                        status.to_ascii_uppercase().as_str(),
                        "ERROR" | "FAILED" | "REJECTED"
                    )
                })
                .unwrap_or(true)
        })
}

fn item_error(exchange_id: &ExchangeId, value: &Value, message: &str) -> ExchangeError {
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::Unknown,
        value
            .get("message")
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or(message),
        Utc::now(),
    );
    error.raw = Some(value.clone());
    error
}

fn missing_item_error(exchange_id: &ExchangeId, message: &str) -> ExchangeError {
    ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::Unknown,
        message,
        Utc::now(),
    )
}

fn data_items(value: &Value) -> Option<&[Value]> {
    let data = data_payload(value);
    data.as_array().map(Vec::as_slice)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "GRVT order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "GRVT level must be array", level)
            })?;
            let price = array
                .first()
                .and_then(decimal_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = array
                .get(1)
                .and_then(decimal_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    fields: &[&str],
) -> ExchangeApiResult<&'a str> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(Value::as_str))
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                format!("GRVT payload missing one of {fields:?}"),
                value,
            )
        })
}

fn split_grvt_symbol(symbol: &str) -> Option<(String, String)> {
    let mut parts = symbol
        .trim()
        .split(['_', '/', '-'])
        .filter(|part| !part.is_empty());
    Some((
        parts.next()?.to_ascii_uppercase(),
        parts.next()?.to_ascii_uppercase(),
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

fn decimal_as_f64(value: &Value) -> Option<f64> {
    string_or_number(Some(value))?.parse().ok()
}

fn decimal_precision(value: String) -> Option<u32> {
    let trimmed = value.trim().trim_end_matches('0');
    trimmed
        .split_once('.')
        .map(|(_, fraction)| fraction.len() as u32)
        .or(Some(0))
}

fn parse_side(value: &str) -> Option<OrderSide> {
    match value.to_ascii_uppercase().as_str() {
        "BUY" | "BID" => Some(OrderSide::Buy),
        "SELL" | "ASK" => Some(OrderSide::Sell),
        _ => None,
    }
}

fn parse_order_type(value: &str) -> Option<OrderType> {
    match value.to_ascii_uppercase().as_str() {
        "MARKET" => Some(OrderType::Market),
        "LIMIT" | "POST_ONLY" | "POSTONLY" => Some(OrderType::Limit),
        _ => None,
    }
}

fn parse_order_status(value: &str) -> Option<OrderStatus> {
    match value.to_ascii_uppercase().as_str() {
        "NEW" | "PENDING" => Some(OrderStatus::New),
        "OPEN" | "ACTIVE" | "WORKING" => Some(OrderStatus::Open),
        "PARTIALLY_FILLED" | "PARTIAL_FILL" | "PARTIAL" => Some(OrderStatus::PartiallyFilled),
        "FILLED" | "DONE" => Some(OrderStatus::Filled),
        "PENDING_CANCEL" => Some(OrderStatus::PendingCancel),
        "CANCELLED" | "CANCELED" => Some(OrderStatus::Cancelled),
        "REJECTED" | "FAILED" => Some(OrderStatus::Rejected),
        "EXPIRED" => Some(OrderStatus::Expired),
        _ => None,
    }
}

fn parse_liquidity_role(value: &str) -> Option<LiquidityRole> {
    match value.to_ascii_uppercase().as_str() {
        "MAKER" | "M" => Some(LiquidityRole::Maker),
        "TAKER" | "T" => Some(LiquidityRole::Taker),
        _ => None,
    }
}

fn timestamp_any_value(value: &Value) -> Option<DateTime<Utc>> {
    value
        .as_str()
        .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp).ok())
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .or_else(|| {
            value.as_i64().and_then(|timestamp| match timestamp {
                millis if millis > 10_000_000_000 => DateTime::from_timestamp_millis(millis),
                seconds => DateTime::from_timestamp(seconds, 0),
            })
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
