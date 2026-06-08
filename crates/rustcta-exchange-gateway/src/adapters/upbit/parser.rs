use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, SymbolRules, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill,
    FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion, TenantId,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = value
        .as_array()
        .ok_or_else(|| parse_error("markets not array", value))?;
    markets
        .iter()
        .map(|market| {
            let market_code = required_str(market, "market")?;
            let (quote_asset, base_asset) = split_market(market_code)?;
            let symbol = symbol_scope(exchange_id, market_code, &base_asset, &quote_asset)?;
            Ok(SymbolRules {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol,
                base_asset,
                quote_asset,
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
                supports_market_orders: true,
                supports_limit_orders: true,
                supports_post_only: false,
                supports_reduce_only: false,
                updated_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let item = value
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(value);
    let units = item
        .get("orderbook_units")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error("orderbook_units missing", value))?;
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for unit in units {
        if let (Some(price), Some(quantity)) =
            (number(unit.get("bid_price")), number(unit.get("bid_size")))
        {
            bids.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
        }
        if let (Some(price), Some(quantity)) =
            (number(unit.get("ask_price")), number(unit.get("ask_size")))
        {
            asks.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
        }
    }
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "upbit order book request requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = item
        .get("timestamp")
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let accounts = value
        .as_array()
        .ok_or_else(|| parse_error("accounts not array", value))?;
    let mut balances = Vec::new();
    for account in accounts {
        let asset = required_str(account, "currency")?.to_ascii_uppercase();
        if !requested_assets.is_empty()
            && !requested_assets
                .iter()
                .any(|requested| requested.eq_ignore_ascii_case(&asset))
        {
            continue;
        }
        let available = number(account.get("balance")).unwrap_or(0.0);
        let locked = number(account.get("locked")).unwrap_or(0.0);
        balances.push(
            AssetBalance::new(asset, available + locked, available, locked)
                .map_err(validation_error)?,
        );
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
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> FeeRateSnapshot {
    FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        maker_rate: string_value(value.get("maker_bid_fee").or_else(|| value.get("bid_fee")))
            .unwrap_or_else(|| "0".to_string()),
        taker_rate: string_value(value.get("bid_fee").or_else(|| value.get("ask_fee")))
            .unwrap_or_else(|| "0".to_string()),
        source: Some(format!("{exchange_id}.orders_chance")),
        updated_at: Utc::now(),
    }
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<rustcta_exchange_api::OrderState> {
    let market = string_value(value.get("market"))
        .or_else(|| fallback_symbol.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| parse_error("order market missing", value))?;
    let (quote_asset, base_asset) = split_market(&market)?;
    let symbol = fallback_symbol.cloned().unwrap_or(symbol_scope(
        exchange_id,
        &market,
        &base_asset,
        &quote_asset,
    )?);
    Ok(rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_value(value.get("identifier")),
        exchange_order_id: string_value(value.get("uuid")),
        side: parse_side(string_value(value.get("side")).as_deref())?,
        position_side: None,
        order_type: parse_order_type(string_value(value.get("ord_type")).as_deref()),
        time_in_force: None,
        status: parse_order_status(string_value(value.get("state")).as_deref()),
        quantity: string_value(value.get("volume"))
            .or_else(|| string_value(value.get("remaining_volume")))
            .unwrap_or_else(|| "0".to_string()),
        price: string_value(value.get("price")),
        filled_quantity: string_value(value.get("executed_volume"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: average_price(value),
        reduce_only: false,
        post_only: false,
        created_at: parse_time(value.get("created_at")),
        updated_at: Utc::now(),
    })
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<rustcta_exchange_api::OrderState>> {
    value
        .as_array()
        .ok_or_else(|| parse_error("orders not array", value))?
        .iter()
        .map(|order| parse_order(exchange_id, fallback_symbol, order))
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let source = if let Some(trades) = value.get("trades").and_then(Value::as_array) {
        trades
    } else {
        value
            .as_array()
            .ok_or_else(|| parse_error("fills not array", value))?
    };
    source
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

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let market = string_value(value.get("market"))
        .or_else(|| fallback_symbol.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| parse_error("fill market missing", value))?;
    let (quote_asset, base_asset) = split_market(&market)?;
    let symbol = fallback_symbol.cloned().unwrap_or(symbol_scope(
        exchange_id,
        &market,
        &base_asset,
        &quote_asset,
    )?);
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .ok_or_else(|| parse_error("fill canonical symbol missing", value))?;
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_value(value.get("order_uuid")).or_else(|| string_value(value.get("uuid"))),
        client_order_id: string_value(value.get("identifier")),
        fill_id: string_value(value.get("uuid")).or_else(|| string_value(value.get("trade_uuid"))),
        side: parse_side(string_value(value.get("side")).as_deref())?,
        position_side: PositionSide::None,
        status: FillStatus::Confirmed,
        liquidity_role: LiquidityRole::Unknown,
        price: number(value.get("price"))
            .ok_or_else(|| parse_error("fill price missing", value))?,
        quantity: number(value.get("volume"))
            .or_else(|| number(value.get("executed_volume")))
            .ok_or_else(|| parse_error("fill quantity missing", value))?,
        quote_quantity: number(value.get("funds")),
        fee_asset: None,
        fee_amount: number(value.get("paid_fee")),
        fee_rate: None,
        realized_pnl: None,
        filled_at: parse_time(value.get("created_at")).unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

pub fn normalize_market_symbol(symbol: &str) -> ExchangeApiResult<String> {
    let normalized = symbol
        .trim()
        .replace('/', "-")
        .replace('_', "-")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "upbit symbol must not be empty".to_string(),
        });
    }
    if normalized.contains('-') {
        Ok(normalized)
    } else {
        Err(ExchangeApiError::InvalidRequest {
            message: format!("upbit symbol must use QUOTE-BASE format, got {normalized}"),
        })
    }
}

pub fn symbol_scope(
    exchange_id: &ExchangeId,
    market: &str,
    base_asset: &str,
    quote_asset: &str,
) -> ExchangeApiResult<SymbolScope> {
    let market = normalize_market_symbol(market)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(
            CanonicalSymbol::new(base_asset, quote_asset).map_err(validation_error)?,
        ),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, market)
            .map_err(validation_error)?,
    })
}

fn split_market(market: &str) -> ExchangeApiResult<(String, String)> {
    let market = normalize_market_symbol(market)?;
    let (quote, base) = market
        .split_once('-')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("invalid Upbit market {market}"),
        })?;
    Ok((quote.to_string(), base.to_string()))
}

fn parse_side(value: Option<&str>) -> ExchangeApiResult<OrderSide> {
    match value.unwrap_or("bid").to_ascii_lowercase().as_str() {
        "bid" | "buy" => Ok(OrderSide::Buy),
        "ask" | "sell" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unknown Upbit side {other}"),
        }),
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or("limit").to_ascii_lowercase().as_str() {
        "price" | "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or("wait").to_ascii_lowercase().as_str() {
        "wait" => OrderStatus::Open,
        "watch" => OrderStatus::New,
        "done" => OrderStatus::Filled,
        "cancel" | "cancelled" => OrderStatus::Cancelled,
        _ => OrderStatus::Unknown,
    }
}

fn average_price(value: &Value) -> Option<String> {
    let funds = number(value.get("executed_funds").or_else(|| value.get("funds")))?;
    let volume = number(value.get("executed_volume"))?;
    (volume > 0.0).then(|| format_float(funds / volume))
}

fn parse_time(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let value = value?;
    if let Some(ms) = value_as_i64(value) {
        return DateTime::<Utc>::from_timestamp_millis(ms);
    }
    value
        .as_str()
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|time| time.with_timezone(&Utc))
}

fn string_value(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn number(value: Option<&Value>) -> Option<f64> {
    match value? {
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

fn format_float(value: f64) -> String {
    let text = format!("{value:.12}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn required_str<'a>(value: &'a Value, field: &str) -> ExchangeApiResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(&format!("missing field {field}"), value))
}

fn parse_error(message: &str, value: &Value) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: format!("{message}: {value}"),
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
