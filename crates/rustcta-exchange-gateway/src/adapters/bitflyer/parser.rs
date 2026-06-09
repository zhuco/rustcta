use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolRules, SymbolScope, TenantId,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, Fill,
    FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

pub fn parse_markets(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "bitFlyer markets response missing array",
            value,
        )
    })?;
    markets
        .iter()
        .map(|market| parse_market(exchange_id, market))
        .collect()
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    parse_markets(exchange_id, value)
}

fn parse_market(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let product_code = required_str(exchange_id, value, "product_code")?.to_ascii_uppercase();
    let market_type = market_type_from_product_code(&product_code);
    let (base_asset, quote_asset) = canonical_assets_from_product_code(&product_code)?;
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, product_code)
            .map_err(validation_error)?,
    };
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset: quote_asset.clone(),
        price_increment: Some(default_price_increment(&quote_asset).to_string()),
        quantity_increment: Some("0.00000001".to_string()),
        min_price: None,
        max_price: None,
        min_quantity: Some("0.001".to_string()),
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision: Some(precision_from_step(default_price_increment(&quote_asset))),
        quantity_precision: Some(8),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(exchange_id, value.get("bids"))?;
    let asks = parse_levels(exchange_id, value.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitFlyer order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_bitflyer_datetime);
    Ok(snapshot)
}

pub fn parse_public_board_snapshot_message(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let params = value.get("params").ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "bitFlyer public WS board message missing params",
            value,
        )
    })?;
    let channel = required_str(exchange_id, params, "channel")?;
    if !channel.starts_with("lightning_board_snapshot_") {
        return Err(parse_error(
            exchange_id.clone(),
            "bitFlyer public WS board snapshot requires lightning_board_snapshot channel",
            value,
        ));
    }
    let message = params.get("message").ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "bitFlyer public WS board snapshot missing params.message",
            value,
        )
    })?;
    parse_orderbook_snapshot(exchange_id, symbol, message)
}

pub fn parse_bitflyer_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = bitflyer_symbol_scope(exchange_id, symbol_hint, value)?;
    let quantity = value_as_string(value.get("size")).unwrap_or_else(|| "0".to_string());
    let filled_quantity =
        value_as_string(value.get("executed_size")).unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value_as_string(value.get("child_order_acceptance_id")),
        exchange_order_id: value_as_string(value.get("child_order_id").or_else(|| value.get("id"))),
        side: parse_side(value.get("side").and_then(Value::as_str)),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(value.get("child_order_type").and_then(Value::as_str)),
        time_in_force: parse_time_in_force(value.get("time_in_force").and_then(Value::as_str)),
        status: parse_order_status(value.get("child_order_state").and_then(Value::as_str)),
        quantity,
        price: value_as_string(value.get("price")),
        filled_quantity: filled_quantity.clone(),
        average_fill_price: value_as_string(value.get("average_price"))
            .filter(|value| value != "0" && value != "0.0"),
        reduce_only: false,
        post_only: false,
        created_at: value
            .get("child_order_date")
            .and_then(Value::as_str)
            .and_then(parse_bitflyer_datetime),
        updated_at: value
            .get("child_order_date")
            .and_then(Value::as_str)
            .and_then(parse_bitflyer_datetime)
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_bitflyer_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "bitFlyer child orders response missing array",
            value,
        )
    })?;
    rows.iter()
        .map(|row| parse_bitflyer_order_state(exchange_id, symbol_hint, row))
        .collect()
}

pub fn parse_bitflyer_fills(
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
                message: "bitflyer fills require canonical_symbol".to_string(),
            })?;
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "bitFlyer executions response missing array",
            value,
        )
    })?;
    rows.iter()
        .map(|row| {
            let price = decimal_value_to_f64(row.get("price"))?.unwrap_or(0.0);
            let quantity = decimal_value_to_f64(row.get("size"))?.unwrap_or(0.0);
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(row.get("child_order_id")),
                client_order_id: value_as_string(row.get("child_order_acceptance_id")),
                fill_id: value_as_string(row.get("id")),
                side: parse_side(row.get("side").and_then(Value::as_str)),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price,
                quantity,
                quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
                fee_asset: Some(canonical_symbol.base_asset().to_string()),
                fee_amount: decimal_value_to_f64(row.get("commission"))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: row
                    .get("exec_date")
                    .and_then(Value::as_str)
                    .and_then(parse_bitflyer_datetime)
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

pub fn normalize_product_code(symbol: &str) -> ExchangeApiResult<String> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitFlyer product_code must not be empty".to_string(),
        });
    }
    Ok(trimmed.replace(['/', '-'], "_").to_ascii_uppercase())
}

pub fn market_type_from_product_code(product_code: &str) -> MarketType {
    if product_code.to_ascii_uppercase().starts_with("FX_") {
        MarketType::Margin
    } else {
        MarketType::Spot
    }
}

pub fn canonical_assets_from_product_code(
    product_code: &str,
) -> ExchangeApiResult<(String, String)> {
    let normalized = normalize_product_code(product_code)?;
    let pair = normalized.strip_prefix("FX_").unwrap_or(&normalized);
    let (base, quote) = pair
        .split_once('_')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cannot infer bitFlyer canonical symbol from {product_code}"),
        })?;
    if base.trim().is_empty() || quote.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("invalid bitFlyer product_code {product_code}"),
        });
    }
    Ok((base.to_ascii_uppercase(), quote.to_ascii_uppercase()))
}

pub(super) fn parse_bitflyer_datetime(text: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .map(|value| value.with_timezone(&Utc))
        .ok()
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "bitFlyer order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| {
            let price = decimal_value_to_f64(level.get("price"))?
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = decimal_value_to_f64(level.get("size"))?
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level size", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

pub(super) fn required_str<'a>(
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

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub(super) fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .map(|value| {
            string_or_number(Some(value))
                .unwrap_or_else(|| value.to_string())
                .parse::<f64>()
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("invalid bitFlyer decimal value {value}: {error}"),
                })
        })
        .transpose()
}

pub(super) fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

fn bitflyer_symbol_scope(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let product_code = required_str(exchange_id, value, "product_code")?.to_ascii_uppercase();
    let market_type = market_type_from_product_code(&product_code);
    let (base_asset, quote_asset) = canonical_assets_from_product_code(&product_code)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(
            CanonicalSymbol::new(base_asset, quote_asset).map_err(validation_error)?,
        ),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, product_code)
            .map_err(validation_error)?,
    })
}

fn parse_side(value: Option<&str>) -> OrderSide {
    if value.unwrap_or_default().eq_ignore_ascii_case("SELL") {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value?.to_ascii_uppercase().as_str() {
        "GTC" => Some(TimeInForce::GTC),
        "IOC" => Some(TimeInForce::IOC),
        "FOK" => Some(TimeInForce::FOK),
        _ => None,
    }
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "ACTIVE" => OrderStatus::New,
        "COMPLETED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "EXPIRED" => OrderStatus::Expired,
        "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

pub(super) fn parse_error(
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

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn default_price_increment(quote_asset: &str) -> &'static str {
    if quote_asset.eq_ignore_ascii_case("JPY") {
        "1"
    } else {
        "0.01"
    }
}

fn precision_from_step(step: &str) -> u32 {
    step.trim_end_matches('0')
        .trim_end_matches('.')
        .split('.')
        .nth(1)
        .map(|fraction| fraction.len() as u32)
        .unwrap_or(0)
}
