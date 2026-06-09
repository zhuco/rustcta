use chrono::{TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolRules, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, Fill,
    FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

pub fn normalize_zaif_pair(value: &str) -> ExchangeApiResult<String> {
    let pair = value.trim().to_ascii_lowercase();
    if pair.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "zaif currency_pair must not be empty".to_string(),
        });
    }
    Ok(pair)
}

pub fn canonical_from_pair(pair: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let (base, quote) = pair
        .split_once('_')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("zaif currency_pair {pair} is not base_quote"),
        })?;
    CanonicalSymbol::new(&base.to_ascii_uppercase(), &quote.to_ascii_uppercase())
        .map_err(validation_error)
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = if let Some(rows) = value.as_array() {
        rows
    } else if let Some(rows) = value
        .get("data")
        .and_then(|data| data.get("currency_pairs"))
        .and_then(Value::as_array)
    {
        rows
    } else {
        return value
            .as_object()
            .map(|_| vec![parse_symbol_rule(exchange_id, value)])
            .unwrap_or_else(|| vec![Err(parse_error(exchange_id, "currency pair rows", value))])
            .into_iter()
            .collect();
    };
    rows.iter()
        .map(|row| parse_symbol_rule(exchange_id, row))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, row: &Value) -> ExchangeApiResult<SymbolRules> {
    let pair = row
        .get("currency_pair")
        .or_else(|| row.get("name"))
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id, "currency_pair", row))
        .and_then(normalize_zaif_pair)?;
    let canonical = canonical_from_pair(&pair)?;
    let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, &pair)
        .map_err(validation_error)?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical.clone()),
            exchange_symbol,
        },
        base_asset: canonical.base_asset().to_string(),
        quote_asset: canonical.quote_asset().to_string(),
        price_increment: string_number(row, "aux_unit_step"),
        quantity_increment: string_number(row, "item_unit_step"),
        min_price: string_number(row, "aux_unit_min"),
        max_price: None,
        min_quantity: string_number(row, "item_unit_min"),
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision: row
            .get("aux_unit_point")
            .and_then(value_u64)
            .map(|value| value as u32),
        quantity_precision: decimal_precision(row.get("item_unit_step")),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
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
                message: "zaif order book request requires canonical_symbol".to_string(),
            })?;
    let bids = parse_levels(exchange_id, value.get("bids"))?;
    let asks = parse_levels(exchange_id, value.get("asks"))?;
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
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .or_else(|| value.get("last_update_at"))
        .and_then(value_i64)
        .and_then(|seconds| Utc.timestamp_opt(seconds, 0).single());
    Ok(snapshot)
}

pub fn parse_zaif_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let rows = return_map(exchange_id, value, "active_orders")?;
    rows.iter()
        .map(|(order_id, row)| parse_zaif_open_order(exchange_id, symbol_hint, order_id, row))
        .collect()
}

pub fn parse_zaif_recent_fills(
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
                message: "zaif fills require canonical_symbol".to_string(),
            })?;
    let rows = return_map(exchange_id, value, "trade_history")?;
    rows.iter()
        .map(|(fill_id, row)| {
            let row = *row;
            let price = row
                .get("price")
                .and_then(value_f64)
                .ok_or_else(|| parse_error(exchange_id, "trade_history price", row))?;
            let quantity = row
                .get("amount")
                .and_then(value_f64)
                .ok_or_else(|| parse_error(exchange_id, "trade_history amount", row))?;
            let side = parse_side(exchange_id, row, row.get("action").and_then(Value::as_str))?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(row.get("order_id")),
                client_order_id: None,
                fill_id: Some(fill_id.clone()),
                side,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price,
                quantity,
                quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
                fee_asset: Some(canonical_symbol.quote_asset().to_string()),
                fee_amount: row.get("fee").and_then(value_f64),
                fee_rate: None,
                realized_pnl: None,
                filled_at: row
                    .get("timestamp")
                    .and_then(value_i64)
                    .and_then(|seconds| Utc.timestamp_opt(seconds, 0).single())
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_zaif_open_order(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    order_id: &str,
    row: &Value,
) -> ExchangeApiResult<OrderState> {
    let pair = row
        .get("currency_pair")
        .and_then(Value::as_str)
        .map(normalize_zaif_pair)
        .transpose()?;
    let symbol = if let Some(symbol) = symbol_hint {
        symbol.clone()
    } else {
        let pair =
            pair.ok_or_else(|| parse_error(exchange_id, "active_orders currency_pair", row))?;
        symbol_scope_from_pair(exchange_id, &pair)?
    };
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: None,
        exchange_order_id: Some(order_id.to_string()),
        side: parse_side(exchange_id, row, row.get("action").and_then(Value::as_str))?,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: OrderStatus::Open,
        quantity: value_as_string(row.get("amount")).unwrap_or_else(|| "0".to_string()),
        price: value_as_string(row.get("price")),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: row
            .get("timestamp")
            .and_then(value_i64)
            .and_then(|seconds| Utc.timestamp_opt(seconds, 0).single()),
        updated_at: Utc::now(),
    })
}

fn return_map<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    expected: &str,
) -> ExchangeApiResult<Vec<(String, &'a Value)>> {
    let object = value
        .get("return")
        .and_then(Value::as_object)
        .ok_or_else(|| parse_error(exchange_id, expected, value))?;
    let mut rows = object
        .iter()
        .map(|(key, row)| (key.clone(), row))
        .collect::<Vec<_>>();
    rows.sort_by(|(left, _), (right, _)| left.cmp(right));
    Ok(rows)
}

fn symbol_scope_from_pair(exchange_id: &ExchangeId, pair: &str) -> ExchangeApiResult<SymbolScope> {
    let canonical = canonical_from_pair(pair)?;
    let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, pair)
        .map_err(validation_error)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical),
        exchange_symbol,
    })
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

fn decimal_precision(value: Option<&Value>) -> Option<u32> {
    let text = value.and_then(string_value)?;
    let decimals = text
        .split_once('.')
        .map(|(_, decimals)| decimals.trim_end_matches('0'))?;
    Some(decimals.len() as u32)
}

fn string_number(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(string_value)
}

fn string_value(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(ToString::to_string)
        .or_else(|| value.as_i64().map(|number| number.to_string()))
        .or_else(|| value.as_f64().map(|number| trim_float(number)))
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    value.and_then(string_value)
}

fn parse_side(
    exchange_id: &ExchangeId,
    raw: &Value,
    value: Option<&str>,
) -> ExchangeApiResult<OrderSide> {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "bid" | "buy" => Ok(OrderSide::Buy),
        "ask" | "sell" => Ok(OrderSide::Sell),
        _ => Err(parse_error(exchange_id, "zaif order action", raw)),
    }
}

fn trim_float(value: f64) -> String {
    let text = format!("{value:.16}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn value_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn value_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn value_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn parse_error(exchange_id: &ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::Decode,
        format!("zaif parser expected {message}"),
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
