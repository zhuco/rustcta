use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, BatchItemResult, BatchOperationReport, BatchPlaceOrdersRequest, ExchangeApiError,
    ExchangeApiResult, OrderState, ReconcilePlan, ReconcileTrigger, RetryReconcilePolicy,
    SymbolRules, SymbolScope, TenantId, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
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
    let symbols = value
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "ApolloX exchangeInfo missing symbols",
                value,
            )
        })?;
    symbols
        .iter()
        .filter(|symbol| {
            symbol
                .get("contractType")
                .and_then(Value::as_str)
                .unwrap_or("PERPETUAL")
                .eq_ignore_ascii_case("PERPETUAL")
        })
        .map(|symbol| parse_symbol_rule(exchange_id, symbol))
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(exchange_id, value.get("bids").or_else(|| value.get("b")))?;
    let asks = parse_levels(exchange_id, value.get("asks").or_else(|| value.get("a")))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "ApolloX DEX order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Perpetual,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = value
        .get("lastUpdateId")
        .or_else(|| value.get("u"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = value
        .get("E")
        .or_else(|| value.get("T"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn normalize_apollox_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "ApolloX DEX symbol must not be empty".to_string(),
        });
    }
    Ok(normalized)
}

pub fn normalize_depth(depth: u32) -> u32 {
    match depth {
        0..=5 => 5,
        6..=10 => 10,
        11..=20 => 20,
        21..=50 => 50,
        51..=100 => 100,
        101..=500 => 500,
        _ => 1000,
    }
}

pub fn parse_batch_place_orders_ack(
    exchange_id: &ExchangeId,
    request: &BatchPlaceOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<(Vec<OrderState>, BatchOperationReport)> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "ApolloX DEX batch place response is not an array",
            value,
        )
    })?;
    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(request.orders.len());
    for (index, order_request) in request.orders.iter().enumerate() {
        let Some(row) = rows.get(index) else {
            let error = batch_item_error(
                exchange_id,
                "missing ApolloX DEX batch place response item",
                None,
                order_request.client_order_id.clone(),
                None,
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
                    "ApolloX DEX did not return a batch place result for this request item",
                )),
            ));
            continue;
        };
        if let Some(error) =
            apollox_batch_item_error(exchange_id, row, order_request.client_order_id.clone())
        {
            let plan = error.requires_reconciliation().then(|| {
                ReconcilePlan::for_place_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchPlacePartialFailure,
                    order_request,
                    RetryReconcilePolicy::default(),
                    "ApolloX DEX batch place item failed and requires order readback",
                )
            });
            results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                None,
                error,
                plan,
            ));
            continue;
        }
        let order = parse_order_state(exchange_id, Some(&order_request.symbol), row)?;
        results.push(BatchItemResult::success(index, order.clone()));
        orders.push(order);
    }

    Ok((
        orders,
        BatchOperationReport {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            total_items: request.orders.len(),
            results,
        },
    ))
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let exchange_symbol_text = required_str(exchange_id, value, "symbol")
        .or_else(|_| required_str(exchange_id, value, "s"))?
        .to_ascii_uppercase();
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            exchange_symbol_text,
        )
        .map_err(validation_error)?
    };
    let market_type = symbol_hint
        .map(|symbol| symbol.market_type)
        .unwrap_or(exchange_symbol.market_type);
    let tif = value
        .get("timeInForce")
        .or_else(|| value.get("f"))
        .and_then(Value::as_str);
    let raw_type = value
        .get("type")
        .or_else(|| value.get("o"))
        .and_then(Value::as_str)
        .unwrap_or("LIMIT");
    let now = Utc::now();

    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone()),
        exchange_symbol,
        client_order_id: value_as_string(value.get("clientOrderId").or_else(|| value.get("c"))),
        exchange_order_id: value_as_string(value.get("orderId").or_else(|| value.get("i"))),
        side: parse_side(
            required_str(exchange_id, value, "side")
                .or_else(|_| required_str(exchange_id, value, "S"))?,
        )?,
        position_side: value
            .get("positionSide")
            .and_then(Value::as_str)
            .map(parse_position_side),
        order_type: parse_order_type(raw_type, tif),
        time_in_force: parse_time_in_force(tif),
        status: value
            .get("status")
            .or_else(|| value.get("X"))
            .and_then(Value::as_str)
            .map(parse_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(
            value
                .get("origQty")
                .or_else(|| value.get("q"))
                .or_else(|| value.get("quantity"))
                .or_else(|| value.get("qty")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: non_zero_string(
            string_or_number(value.get("price").or_else(|| value.get("p")))
                .unwrap_or_else(|| "0".to_string()),
        ),
        filled_quantity: string_or_number(value.get("executedQty").or_else(|| value.get("z")))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: value_as_string(value.get("avgPrice")).or_else(|| {
            non_zero_string(string_or_number(
                value
                    .get("avgPrice")
                    .or_else(|| value.get("ap"))
                    .or_else(|| value.get("averagePrice")),
            )?)
        }),
        reduce_only: value
            .get("reduceOnly")
            .or_else(|| value.get("R"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: raw_type.eq_ignore_ascii_case("LIMIT_MAKER")
            || tif.is_some_and(|value| value.eq_ignore_ascii_case("GTX")),
        created_at: first_timestamp_millis(value, &["transactTime", "time", "O", "E"]),
        updated_at: first_timestamp_millis(value, &["updateTime", "T", "E"]).unwrap_or(now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "ApolloX DEX open orders response is not an array",
            value,
        )
    })?;
    orders
        .iter()
        .map(|order| {
            if let Some(symbol_hint) = symbol_hint {
                parse_order_state(exchange_id, Some(symbol_hint), order)
            } else {
                let symbol_text = required_str(exchange_id, order, "symbol")?.to_ascii_uppercase();
                let symbol_scope = SymbolScope {
                    exchange: exchange_id.clone(),
                    market_type,
                    canonical_symbol: compact_symbol_assets(&symbol_text)
                        .and_then(|(base, quote)| CanonicalSymbol::new(base, quote).ok()),
                    exchange_symbol: ExchangeSymbol::new(
                        exchange_id.clone(),
                        market_type,
                        symbol_text,
                    )
                    .map_err(validation_error)?,
                };
                parse_order_state(exchange_id, Some(&symbol_scope), order)
            }
        })
        .collect()
}

pub fn parse_recent_fills(
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
                message: "ApolloX DEX recent fills request requires canonical_symbol".to_string(),
            })?;
    let fills = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "ApolloX DEX recent fills response is not an array",
            value,
        )
    })?;
    fills
        .iter()
        .map(|fill| {
            let is_buyer = fill
                .get("isBuyer")
                .or_else(|| fill.get("buyer"))
                .and_then(Value::as_bool)
                .unwrap_or_else(|| {
                    fill.get("side")
                        .and_then(Value::as_str)
                        .is_some_and(|side| side.eq_ignore_ascii_case("BUY"))
                });
            let is_maker = fill
                .get("isMaker")
                .or_else(|| fill.get("maker"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let price = decimal_value_to_f64(fill.get("price"))?.unwrap_or(0.0);
            let quantity = decimal_value_to_f64(fill.get("qty").or_else(|| fill.get("quantity")))?
                .unwrap_or(0.0);
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("orderId")),
                client_order_id: value_as_string(fill.get("clientOrderId")),
                fill_id: value_as_string(fill.get("id")),
                side: if is_buyer {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                },
                position_side: fill
                    .get("positionSide")
                    .and_then(Value::as_str)
                    .map(parse_position_side)
                    .unwrap_or(PositionSide::Net),
                status: FillStatus::Confirmed,
                liquidity_role: if is_maker {
                    LiquidityRole::Maker
                } else {
                    LiquidityRole::Taker
                },
                price,
                quantity,
                quote_quantity: decimal_value_to_f64(
                    fill.get("quoteQty").or_else(|| fill.get("quoteQuantity")),
                )?,
                fee_asset: value_as_string(fill.get("commissionAsset")),
                fee_amount: decimal_value_to_f64(fill.get("commission"))?,
                fee_rate: None,
                realized_pnl: decimal_value_to_f64(fill.get("realizedPnl"))?,
                filled_at: first_timestamp_millis(fill, &["time"]).unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let base_asset = required_str(exchange_id, value, "baseAsset")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "quoteAsset")?.to_ascii_uppercase();
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            exchange_symbol,
        )
        .map_err(validation_error)?,
    };
    let filters = value
        .get("filters")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let price_filter = find_filter(&filters, "PRICE_FILTER");
    let lot_filter = find_filter(&filters, "LOT_SIZE");
    let market_lot_filter = find_filter(&filters, "MARKET_LOT_SIZE");
    let notional_filter =
        find_filter(&filters, "MIN_NOTIONAL").or_else(|| find_filter(&filters, "NOTIONAL"));
    let order_types = value
        .get("orderTypes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let time_in_force = value
        .get("timeInForce")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let status = value
        .get("status")
        .or_else(|| value.get("contractStatus"))
        .and_then(Value::as_str)
        .unwrap_or("TRADING");
    let trading = status.eq_ignore_ascii_case("TRADING");
    let quantity_filter = lot_filter.or(market_lot_filter);

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(price_filter.and_then(|filter| filter.get("tickSize"))),
        quantity_increment: string_or_number(
            quantity_filter.and_then(|filter| filter.get("stepSize")),
        ),
        min_price: string_or_number(price_filter.and_then(|filter| filter.get("minPrice"))),
        max_price: string_or_number(price_filter.and_then(|filter| filter.get("maxPrice"))),
        min_quantity: string_or_number(quantity_filter.and_then(|filter| filter.get("minQty"))),
        max_quantity: string_or_number(quantity_filter.and_then(|filter| filter.get("maxQty"))),
        min_notional: string_or_number(
            notional_filter
                .and_then(|filter| filter.get("notional"))
                .or_else(|| notional_filter.and_then(|filter| filter.get("minNotional"))),
        ),
        max_notional: string_or_number(
            notional_filter.and_then(|filter| filter.get("maxNotional")),
        ),
        price_precision: precision_from_step_text(
            price_filter
                .and_then(|filter| filter.get("tickSize"))
                .and_then(text_from_value),
        )
        .or_else(|| value.get("pricePrecision").and_then(value_as_u32)),
        quantity_precision: precision_from_step_text(
            quantity_filter
                .and_then(|filter| filter.get("stepSize"))
                .and_then(text_from_value),
        )
        .or_else(|| value.get("quantityPrecision").and_then(value_as_u32)),
        supports_market_orders: trading && has_or_unlisted(&order_types, "MARKET"),
        supports_limit_orders: trading && has_or_unlisted(&order_types, "LIMIT"),
        supports_post_only: trading && has_or_unlisted(&time_in_force, "GTX"),
        supports_reduce_only: trading,
        updated_at: Utc::now(),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "ApolloX DEX order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let values = level.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid ApolloX DEX book level", level)
            })?;
            let price = values
                .first()
                .and_then(value_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = values
                .get(1)
                .and_then(value_as_f64)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn find_filter<'a>(filters: &'a [Value], filter_type: &str) -> Option<&'a Value> {
    filters.iter().find(|filter| {
        filter
            .get("filterType")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case(filter_type))
    })
}

fn has_or_unlisted(values: &[Value], expected: &str) -> bool {
    values.is_empty()
        || values.iter().any(|value| {
            value
                .as_str()
                .is_some_and(|value| value.eq_ignore_ascii_case(expected))
        })
}

fn apollox_batch_item_error(
    exchange_id: &ExchangeId,
    value: &Value,
    client_order_id: Option<String>,
) -> Option<ExchangeError> {
    let code = value.get("code").and_then(value_as_i64)?;
    if code == 0 {
        return None;
    }
    let message = value
        .get("msg")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("ApolloX DEX batch place item failed");
    Some(batch_item_error(
        exchange_id,
        message,
        Some(code.to_string()),
        value_as_string(value.get("clientOrderId").or_else(|| value.get("c"))).or(client_order_id),
        value_as_string(value.get("orderId").or_else(|| value.get("i"))),
        value.clone(),
    ))
}

fn batch_item_error(
    exchange_id: &ExchangeId,
    message: impl Into<String>,
    code: Option<String>,
    client_order_id: Option<String>,
    order_id: Option<String>,
    raw: Value,
) -> ExchangeError {
    let message = message.into();
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        classify_apollox_item_error(code.as_deref(), &message),
        message,
        Utc::now(),
    );
    error.code = code;
    error.client_order_id = client_order_id;
    error.order_id = order_id;
    error.raw = Some(raw);
    error
}

fn classify_apollox_item_error(code: Option<&str>, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let message = message.to_ascii_lowercase();
    if matches!(code, "-2010" | "-2018") || message.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if matches!(code, "-1121") || message.contains("invalid symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if matches!(code, "-1111" | "-1013")
        || message.contains("precision")
        || message.contains("filter failure")
    {
        ExchangeErrorClass::InvalidPrecision
    } else if matches!(code, "-1003" | "-1015") || message.contains("too many requests") {
        ExchangeErrorClass::RateLimited
    } else if matches!(code, "-1021" | "-1022" | "-2014" | "-2015")
        || message.contains("signature")
        || message.contains("api-key")
    {
        ExchangeErrorClass::Authentication
    } else if matches!(code, "-2011" | "-2013") || message.contains("order does not exist") {
        ExchangeErrorClass::OrderNotFound
    } else if code.is_empty() {
        ExchangeErrorClass::UnknownOrderState
    } else {
        ExchangeErrorClass::OrderRejected
    }
}

fn parse_side(value: &str) -> ExchangeApiResult<OrderSide> {
    match value.to_ascii_uppercase().as_str() {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported ApolloX DEX order side {value}"),
        }),
    }
}

fn parse_position_side(value: &str) -> PositionSide {
    match value.to_ascii_uppercase().as_str() {
        "LONG" => PositionSide::Long,
        "SHORT" => PositionSide::Short,
        "BOTH" | "NET" => PositionSide::Net,
        _ => PositionSide::None,
    }
}

fn parse_order_type(order_type: &str, tif: Option<&str>) -> OrderType {
    if order_type.eq_ignore_ascii_case("LIMIT_MAKER")
        || tif.is_some_and(|value| value.eq_ignore_ascii_case("GTX"))
    {
        OrderType::PostOnly
    } else if order_type.eq_ignore_ascii_case("MARKET") {
        OrderType::Market
    } else if tif.is_some_and(|value| value.eq_ignore_ascii_case("IOC")) {
        OrderType::IOC
    } else if tif.is_some_and(|value| value.eq_ignore_ascii_case("FOK")) {
        OrderType::FOK
    } else {
        OrderType::Limit
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value?.to_ascii_uppercase().as_str() {
        "GTC" => Some(TimeInForce::GTC),
        "IOC" => Some(TimeInForce::IOC),
        "FOK" => Some(TimeInForce::FOK),
        "GTX" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_order_status(value: &str) -> OrderStatus {
    match value.to_ascii_uppercase().as_str() {
        "NEW" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "PENDING_CANCEL" => OrderStatus::PendingCancel,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn compact_symbol_assets(symbol: &str) -> Option<(&str, &str)> {
    for quote in ["USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "BNB"] {
        if symbol.len() > quote.len() && symbol.ends_with(quote) {
            return Some(symbol.split_at(symbol.len() - quote.len()));
        }
    }
    None
}

fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => {
            number
                .as_f64()
                .map(Some)
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "ApolloX DEX numeric value cannot be represented as f64".to_string(),
                })
        }
        Some(Value::String(text)) if text.trim().is_empty() => Ok(None),
        Some(Value::String(text)) => {
            text.parse::<f64>()
                .map(Some)
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("ApolloX DEX numeric parse failed: {error}"),
                })
        }
        Some(_) => Err(ExchangeApiError::InvalidRequest {
            message: "ApolloX DEX numeric field has unsupported type".to_string(),
        }),
    }
}

fn non_zero_string(value: String) -> Option<String> {
    if value.trim().is_empty() || value == "0" || value == "0.0" || value == "0.00" {
        None
    } else {
        Some(value)
    }
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) if !value.trim().is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
    })
}

pub(super) fn time_in_force_value(tif: Option<TimeInForce>, post_only: bool) -> &'static str {
    if post_only {
        return "GTX";
    }
    match tif.unwrap_or(TimeInForce::GTC) {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTX",
    }
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            format!("ApolloX DEX exchangeInfo missing {field}"),
            value,
        )
    })
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn text_from_value(value: &Value) -> Option<&str> {
    value.as_str()
}

fn precision_from_step_text(value: Option<&str>) -> Option<u32> {
    let value = value?;
    let trimmed = value.trim_end_matches('0').trim_end_matches('.');
    trimmed
        .split('.')
        .nth(1)
        .map(|fraction| fraction.len() as u32)
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .or_else(|| value.as_str()?.parse().ok())
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
