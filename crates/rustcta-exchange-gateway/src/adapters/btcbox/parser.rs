use chrono::{NaiveDateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderState, SymbolRules, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, OrderSide, OrderStatus, OrderType, PositionSide,
    TimeInForce,
};
use serde_json::Value;

pub fn normalize_btcbox_symbol(value: &str) -> ExchangeApiResult<String> {
    let symbol = value.trim().replace('-', "_").to_ascii_lowercase();
    if symbol.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "btcbox symbol must not be empty".to_string(),
        });
    }
    if symbol.contains('_') {
        let (base, quote) =
            symbol
                .split_once('_')
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("btcbox symbol {value} is not base_quote"),
                })?;
        if quote != "jpy" {
            return Err(ExchangeApiError::Unsupported {
                operation: "btcbox.non_jpy_quote",
            });
        }
        Ok(format!("{base}_jpy"))
    } else {
        Ok(format!("{symbol}_jpy"))
    }
}

pub fn coin_param_from_symbol(value: &str) -> ExchangeApiResult<String> {
    let symbol = normalize_btcbox_symbol(value)?;
    let (base, _) = symbol
        .split_once('_')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("btcbox symbol {value} is not base_quote"),
        })?;
    if !matches!(base, "btc" | "bch" | "ltc" | "eth") {
        return Err(ExchangeApiError::Unsupported {
            operation: "btcbox.order_book_unverified_for_ticker_only_symbol",
        });
    }
    Ok(base.to_string())
}

pub fn canonical_from_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let symbol = normalize_btcbox_symbol(symbol)?;
    let (base, quote) = symbol
        .split_once('_')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("btcbox symbol {symbol} is not base_quote"),
        })?;
    CanonicalSymbol::new(&base.to_ascii_uppercase(), &quote.to_ascii_uppercase())
        .map_err(validation_error)
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .as_object()
        .ok_or_else(|| parse_error(exchange_id, "tickers object", value))?;
    let mut symbols = rows
        .keys()
        .filter(|key| key.contains('_'))
        .map(|key| {
            let exchange_symbol = normalize_btcbox_symbol(key)?;
            let canonical = canonical_from_symbol(&exchange_symbol)?;
            let symbol =
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, &exchange_symbol)
                    .map_err(validation_error)?;
            Ok(SymbolRules {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: SymbolScope {
                    exchange: exchange_id.clone(),
                    market_type: MarketType::Spot,
                    canonical_symbol: Some(canonical.clone()),
                    exchange_symbol: symbol,
                },
                base_asset: canonical.base_asset().to_string(),
                quote_asset: canonical.quote_asset().to_string(),
                price_increment: None,
                quantity_increment: None,
                min_price: None,
                max_price: None,
                min_quantity: None,
                max_quantity: None,
                min_notional: None,
                max_notional: None,
                price_precision: None,
                quantity_precision: None,
                supports_market_orders: false,
                supports_limit_orders: true,
                supports_post_only: false,
                supports_reduce_only: false,
                updated_at: Utc::now(),
            })
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    symbols.sort_by(|left, right| {
        left.symbol
            .exchange_symbol
            .symbol
            .cmp(&right.symbol.exchange_symbol.symbol)
    });
    Ok(symbols)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let canonical =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "btcbox order book request requires canonical_symbol".to_string(),
            })?;
    let mut bids = parse_levels(exchange_id, value.get("bids"))?;
    let mut asks = parse_levels(exchange_id, value.get("asks"))?;
    bids.sort_by(|left, right| right.price.total_cmp(&left.price));
    asks.sort_by(|left, right| left.price.total_cmp(&right.price));
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    Ok(snapshot)
}

pub fn parse_btcbox_order_state(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let quantity = text_field(value, "amount_original")
        .ok_or_else(|| parse_error(exchange_id, "amount_original", value))?;
    let outstanding = text_field(value, "amount_outstanding").unwrap_or_else(|| "0".to_string());
    let filled_quantity = subtract_decimal_strings(&quantity, &outstanding);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: None,
        exchange_order_id: text_field(value, "id"),
        side: parse_side(value.get("type").and_then(Value::as_str))?,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: parse_order_status(
            value.get("status").and_then(Value::as_str),
            &filled_quantity,
        ),
        quantity,
        price: text_field(value, "price"),
        filled_quantity,
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: value
            .get("datetime")
            .and_then(Value::as_str)
            .and_then(parse_btcbox_datetime),
        updated_at: Utc::now(),
    })
}

pub fn parse_btcbox_open_orders(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let rows = value
        .as_array()
        .ok_or_else(|| parse_error(exchange_id, "trade_list array", value))?;
    rows.iter()
        .map(|row| {
            let mut order = parse_btcbox_order_state(exchange_id, symbol, row)?;
            if order.status == OrderStatus::Unknown {
                order.status = if order.filled_quantity == "0" {
                    OrderStatus::Open
                } else {
                    OrderStatus::PartiallyFilled
                };
            }
            Ok(order)
        })
        .collect()
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let rows = value
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id, "order book levels", &Value::Null))?;
    rows.iter()
        .map(|row| {
            let level = row
                .as_array()
                .ok_or_else(|| parse_error(exchange_id, "price/amount level", row))?;
            let price = level
                .first()
                .and_then(value_f64)
                .ok_or_else(|| parse_error(exchange_id, "price level value", row))?;
            let quantity = level
                .get(1)
                .and_then(value_f64)
                .ok_or_else(|| parse_error(exchange_id, "quantity level value", row))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn value_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn text_field(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|field| {
        field
            .as_str()
            .map(ToString::to_string)
            .or_else(|| field.as_i64().map(|number| number.to_string()))
            .or_else(|| field.as_u64().map(|number| number.to_string()))
            .or_else(|| field.as_f64().map(trim_float))
    })
}

fn trim_float(value: f64) -> String {
    let text = value.to_string();
    text.strip_suffix(".0").unwrap_or(&text).to_string()
}

fn subtract_decimal_strings(quantity: &str, outstanding: &str) -> String {
    let quantity = quantity.parse::<f64>().unwrap_or_default();
    let outstanding = outstanding.parse::<f64>().unwrap_or_default();
    trim_float((quantity - outstanding).max(0.0))
}

fn parse_side(value: Option<&str>) -> ExchangeApiResult<OrderSide> {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: "btcbox order row missing supported side".to_string(),
        }),
    }
}

fn parse_order_status(status: Option<&str>, filled_quantity: &str) -> OrderStatus {
    match status
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "wait" | "no" | "open" => OrderStatus::Open,
        "" if filled_quantity == "0" => OrderStatus::Open,
        "part" | "partial" | "partially_filled" => OrderStatus::PartiallyFilled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "all" | "filled" | "closed" => OrderStatus::Filled,
        _ => OrderStatus::Unknown,
    }
}

fn parse_btcbox_datetime(value: &str) -> Option<chrono::DateTime<Utc>> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|naive| Utc.from_utc_datetime(&naive))
}

fn parse_error(exchange_id: &ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::Decode,
        format!("btcbox parser expected {message}"),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: rustcta_types::ValidationError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
