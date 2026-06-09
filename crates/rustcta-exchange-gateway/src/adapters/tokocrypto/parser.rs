use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolRules, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
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
    let rows = value
        .get("data")
        .and_then(|data| data.get("list"))
        .and_then(Value::as_array)
        .or_else(|| value.get("symbols").and_then(Value::as_array))
        .ok_or_else(|| parse_error(exchange_id.clone(), "symbols list", value))?;
    rows.iter()
        .filter(|row| row.get("type").and_then(Value::as_i64).unwrap_or(1) == 1)
        .map(|row| parse_symbol_rule(exchange_id, row))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = normalize_tokocrypto_symbol(required_str(exchange_id, value, "symbol")?)?;
    let base_asset = required_str(exchange_id, value, "baseAsset")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "quoteAsset")?.to_ascii_uppercase();
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
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
    let notional_filter =
        find_filter(&filters, "MIN_NOTIONAL").or_else(|| find_filter(&filters, "NOTIONAL"));
    let order_types = value
        .get("orderTypes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let trading = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("TRADING")
        .eq_ignore_ascii_case("TRADING");

    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(price_filter.and_then(|filter| filter.get("tickSize"))),
        quantity_increment: string_or_number(lot_filter.and_then(|filter| filter.get("stepSize"))),
        min_price: string_or_number(price_filter.and_then(|filter| filter.get("minPrice"))),
        max_price: string_or_number(price_filter.and_then(|filter| filter.get("maxPrice"))),
        min_quantity: string_or_number(lot_filter.and_then(|filter| filter.get("minQty"))),
        max_quantity: string_or_number(lot_filter.and_then(|filter| filter.get("maxQty"))),
        min_notional: string_or_number(
            notional_filter
                .and_then(|filter| filter.get("minNotional"))
                .or_else(|| notional_filter.and_then(|filter| filter.get("notional"))),
        ),
        max_notional: string_or_number(
            notional_filter.and_then(|filter| filter.get("maxNotional")),
        ),
        price_precision: precision_from_step(
            price_filter
                .and_then(|filter| filter.get("tickSize"))
                .and_then(Value::as_str),
        )
        .or_else(|| value.get("pricePrecision").and_then(value_u32)),
        quantity_precision: precision_from_step(
            lot_filter
                .and_then(|filter| filter.get("stepSize"))
                .and_then(Value::as_str),
        )
        .or_else(|| {
            value
                .get("basePrecision")
                .or_else(|| value.get("baseAssetPrecision"))
                .and_then(value_u32)
        }),
        supports_market_orders: trading && has_or_unlisted(&order_types, "MARKET"),
        supports_limit_orders: trading && has_or_unlisted(&order_types, "LIMIT"),
        supports_post_only: trading && has_or_unlisted(&order_types, "LIMIT_MAKER"),
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("bids").or_else(|| data.get("b")))?;
    let asks = parse_levels(exchange_id, data.get("asks").or_else(|| data.get("a")))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "tokocrypto order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = data
        .get("lastUpdateId")
        .or_else(|| data.get("u"))
        .and_then(Value::as_u64);
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .or_else(|| data.get("E"))
        .and_then(value_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = first_payload_object(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "private order response missing order object",
            value,
        )
    })?;
    let symbol_text = required_str(exchange_id, order, "symbol")
        .or_else(|_| required_str(exchange_id, order, "s"))?
        .to_ascii_uppercase();
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            normalize_tokocrypto_symbol(&symbol_text)?,
        )
        .map_err(validation_error)?
    };
    let canonical_symbol = symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone());
    let raw_type = text_from_value(
        order
            .get("type")
            .or_else(|| order.get("orderType"))
            .or_else(|| order.get("o")),
    )
    .unwrap_or_else(|| "LIMIT".to_string());
    let tif_text = text_from_value(order.get("timeInForce").or_else(|| order.get("f")));
    let filled_quantity = string_or_number(
        order
            .get("executedQty")
            .or_else(|| order.get("executedQuantity"))
            .or_else(|| order.get("filledQty"))
            .or_else(|| order.get("filledQuantity"))
            .or_else(|| order.get("z")),
    )
    .unwrap_or_else(|| "0".to_string());
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol,
        client_order_id: value_as_string(
            order
                .get("clientOrderId")
                .or_else(|| order.get("client_order_id"))
                .or_else(|| order.get("c")),
        ),
        exchange_order_id: value_as_string(
            order
                .get("orderId")
                .or_else(|| order.get("order_id"))
                .or_else(|| order.get("i")),
        ),
        side: parse_side_value(
            order
                .get("side")
                .or_else(|| order.get("orderSide"))
                .or_else(|| order.get("S")),
        )?,
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(&raw_type),
        time_in_force: tif_text.as_deref().and_then(parse_time_in_force),
        status: text_from_value(order.get("status").or_else(|| order.get("X")))
            .as_deref()
            .map(map_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(
            order
                .get("origQty")
                .or_else(|| order.get("quantity"))
                .or_else(|| order.get("qty"))
                .or_else(|| order.get("q")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: non_zero_string(
            string_or_number(order.get("price").or_else(|| order.get("p")))
                .unwrap_or_else(|| "0".to_string()),
        ),
        filled_quantity,
        average_fill_price: value_as_string(order.get("avgPrice"))
            .or_else(|| average_price_text(order)),
        reduce_only: false,
        post_only: raw_type.eq_ignore_ascii_case("LIMIT_MAKER")
            || raw_type == "3"
            || tif_text
                .as_deref()
                .is_some_and(|tif| tif.eq_ignore_ascii_case("GTX")),
        created_at: first_timestamp_millis(
            order,
            &["transactTime", "createTime", "time", "O", "E"],
        ),
        updated_at: first_timestamp_millis(order, &["updateTime", "updatedTime", "T", "E"])
            .unwrap_or(now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = payload_array(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "open orders response is not an array",
            value,
        )
    })?;
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, symbol_hint, order))
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
                message: "tokocrypto recent fills request requires canonical_symbol".to_string(),
            })?;
    let fills = payload_array(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "recent fills response is not an array",
            value,
        )
    })?;
    fills
        .iter()
        .map(|fill| {
            let price =
                decimal_value_to_f64(fill.get("price").or_else(|| fill.get("p")))?.unwrap_or(0.0);
            let quantity = decimal_value_to_f64(
                fill.get("qty")
                    .or_else(|| fill.get("quantity"))
                    .or_else(|| fill.get("q")),
            )?
            .unwrap_or(0.0);
            let quote_quantity = decimal_value_to_f64(
                fill.get("quoteQty")
                    .or_else(|| fill.get("quoteQuantity"))
                    .or_else(|| fill.get("quote")),
            )?;
            let fee_amount = decimal_value_to_f64(
                fill.get("commission")
                    .or_else(|| fill.get("fee"))
                    .or_else(|| fill.get("feeAmount")),
            )?;
            let side = if let Some(side) = fill.get("side").or_else(|| fill.get("S")) {
                parse_side_value(Some(side))?
            } else if fill
                .get("isBuyer")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            let is_maker = fill
                .get("isMaker")
                .or_else(|| fill.get("maker"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(
                    fill.get("orderId")
                        .or_else(|| fill.get("order_id"))
                        .or_else(|| fill.get("i")),
                ),
                client_order_id: value_as_string(
                    fill.get("clientOrderId")
                        .or_else(|| fill.get("client_order_id")),
                ),
                fill_id: value_as_string(
                    fill.get("id")
                        .or_else(|| fill.get("tradeId"))
                        .or_else(|| fill.get("trade_id"))
                        .or_else(|| fill.get("t")),
                ),
                side,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: if is_maker {
                    LiquidityRole::Maker
                } else {
                    LiquidityRole::Taker
                },
                price,
                quantity,
                quote_quantity,
                fee_asset: value_as_string(
                    fill.get("commissionAsset")
                        .or_else(|| fill.get("feeAsset"))
                        .or_else(|| fill.get("fee_asset")),
                ),
                fee_amount,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp_millis(fill, &["time", "tradeTime", "createTime"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

pub fn normalize_tokocrypto_symbol(value: &str) -> ExchangeApiResult<String> {
    let normalized = value.trim().replace(['/', '-'], "_").to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "tokocrypto symbol must not be empty".to_string(),
        });
    }
    if normalized.contains('_') {
        Ok(normalized)
    } else {
        Ok(normalized)
    }
}

pub fn market_data_symbol(value: &str) -> ExchangeApiResult<String> {
    Ok(normalize_tokocrypto_symbol(value)?.replace('_', ""))
}

pub fn normalize_depth(depth: u32) -> u32 {
    match depth {
        0..=5 => 5,
        6..=10 => 10,
        11..=20 => 20,
        21..=50 => 50,
        51..=100 => 100,
        _ => 500,
    }
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let array = level.as_array().ok_or_else(|| {
                parse_error(exchange_id.clone(), "invalid order book level", level)
            })?;
            let price = array
                .first()
                .and_then(number_from_value)
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = array
                .get(1)
                .and_then(number_from_value)
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

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("missing field {field}"),
            value,
        )
    })
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    match value {
        Some(Value::String(text)) => {
            text.parse::<f64>()
                .map(Some)
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("invalid tokocrypto decimal {text}: {error}"),
                })
        }
        Some(Value::Number(number)) => Ok(number.as_f64()),
        Some(Value::Null) | None => Ok(None),
        Some(other) => Err(parse_error(
            ExchangeId::new("tokocrypto").map_err(validation_error)?,
            "decimal string or number",
            other,
        )),
    }
}

fn value_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn value_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|number| u32::try_from(number).ok())
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn first_timestamp_millis(value: &Value, keys: &[&str]) -> Option<DateTime<Utc>> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(value_i64))
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

fn precision_from_step(step: Option<&str>) -> Option<u32> {
    let step = step?;
    let decimal = step.split('.').nth(1)?;
    Some(decimal.trim_end_matches('0').len() as u32)
}

fn first_payload_object(value: &Value) -> Option<&Value> {
    value
        .get("data")
        .and_then(|data| {
            data.as_object().map(|_| data).or_else(|| {
                data.get("order")
                    .or_else(|| data.get("item"))
                    .or_else(|| data.get("detail"))
            })
        })
        .or_else(|| {
            value.as_object().and_then(|_| {
                if value.get("code").is_some() {
                    None
                } else {
                    Some(value)
                }
            })
        })
}

fn payload_array(value: &Value) -> Option<&[Value]> {
    value.as_array().map(Vec::as_slice).or_else(|| {
        value.get("data").and_then(|data| {
            data.as_array().map(Vec::as_slice).or_else(|| {
                data.get("list")
                    .or_else(|| data.get("items"))
                    .or_else(|| data.get("orders"))
                    .or_else(|| data.get("trades"))
                    .and_then(Value::as_array)
                    .map(Vec::as_slice)
            })
        })
    })
}

fn parse_side_value(value: Option<&Value>) -> ExchangeApiResult<OrderSide> {
    let text = text_from_value(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "tokocrypto order response missing side".to_string(),
    })?;
    match text.trim().to_ascii_uppercase().as_str() {
        "0" | "BUY" | "BID" => Ok(OrderSide::Buy),
        "1" | "SELL" | "ASK" => Ok(OrderSide::Sell),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported tokocrypto side {text}"),
        }),
    }
}

fn parse_order_type(value: &str) -> OrderType {
    match value.trim().to_ascii_uppercase().as_str() {
        "2" | "MARKET" => OrderType::Market,
        "3" | "LIMIT_MAKER" => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: &str) -> Option<rustcta_exchange_api::TimeInForce> {
    match value.trim().to_ascii_uppercase().as_str() {
        "1" | "GTC" => Some(rustcta_exchange_api::TimeInForce::GTC),
        "2" | "IOC" => Some(rustcta_exchange_api::TimeInForce::IOC),
        "3" | "FOK" => Some(rustcta_exchange_api::TimeInForce::FOK),
        "GTX" => Some(rustcta_exchange_api::TimeInForce::GTX),
        _ => None,
    }
}

fn map_order_status(status: &str) -> OrderStatus {
    match status.trim().to_ascii_uppercase().as_str() {
        "NEW" | "PENDING_NEW" => OrderStatus::New,
        "OPEN" => OrderStatus::Open,
        "PARTIALLY_FILLED" | "PARTIAL_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "PENDING_CANCEL" => OrderStatus::PendingCancel,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" | "EXPIRED_IN_MATCH" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn text_from_value(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn average_price_text(value: &Value) -> Option<String> {
    let cumulative_quote = decimal_value_to_f64(
        value
            .get("cummulativeQuoteQty")
            .or_else(|| value.get("cumulativeQuoteQty"))
            .or_else(|| value.get("quoteQty")),
    )
    .ok()
    .flatten()?;
    let executed = decimal_value_to_f64(
        value
            .get("executedQty")
            .or_else(|| value.get("executedQuantity"))
            .or_else(|| value.get("filledQty")),
    )
    .ok()
    .flatten()?;
    (executed > 0.0).then(|| (cumulative_quote / executed).to_string())
}

fn non_zero_string(value: String) -> Option<String> {
    value
        .parse::<f64>()
        .map(|number| number > 0.0)
        .unwrap_or_else(|_| !value.trim().is_empty())
        .then_some(value)
}

fn has_or_unlisted(order_types: &[Value], expected: &str) -> bool {
    order_types.is_empty()
        || order_types.iter().any(|value| {
            value
                .as_str()
                .is_some_and(|text| text.eq_ignore_ascii_case(expected))
        })
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Decode,
        format!("tokocrypto parser expected {message}"),
        Utc::now(),
    );
    error.raw = Some(value.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: rustcta_types::ValidationError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
