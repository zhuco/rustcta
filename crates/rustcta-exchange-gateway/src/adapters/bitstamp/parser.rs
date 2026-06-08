use chrono::{DateTime, NaiveDateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, SymbolRules, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeError, ExchangeErrorClass, ExchangeId,
    ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot,
    OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "markets response is not an array",
            value,
        )
    })?;
    rows.iter()
        .filter(|row| {
            row.get("market_type")
                .and_then(Value::as_str)
                .unwrap_or("SPOT")
                .eq_ignore_ascii_case("SPOT")
        })
        .map(|row| parse_symbol_rule(exchange_id, row))
        .collect()
}

fn parse_symbol_rule(exchange_id: &ExchangeId, row: &Value) -> ExchangeApiResult<SymbolRules> {
    let exchange_symbol = required_str(exchange_id, row, "market_symbol")?.to_ascii_lowercase();
    let base_asset = required_str(exchange_id, row, "base_currency")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, row, "counter_currency")?.to_ascii_uppercase();
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
    let trading = row
        .get("trading")
        .and_then(Value::as_str)
        .unwrap_or("Enabled")
        .eq_ignore_ascii_case("Enabled");
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: decimals_to_increment(row.get("counter_decimals")),
        quantity_increment: decimals_to_increment(row.get("base_decimals")),
        min_price: None,
        max_price: None,
        min_quantity: None,
        max_quantity: None,
        min_notional: string_or_number(row.get("minimum_order_value")),
        max_notional: None,
        price_precision: row.get("counter_decimals").and_then(value_as_u32),
        quantity_precision: row.get("base_decimals").and_then(value_as_u32),
        supports_market_orders: trading,
        supports_limit_orders: trading,
        supports_post_only: trading,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    depth: Option<u32>,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let mut bids = parse_levels(exchange_id, value.get("bids"))?;
    let mut asks = parse_levels(exchange_id, value.get("asks"))?;
    if let Some(depth) = depth {
        bids.truncate(depth as usize);
        asks.truncate(depth as usize);
    }
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitstamp order book requires canonical_symbol".to_string(),
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
    snapshot.exchange_timestamp = value
        .get("microtimestamp")
        .and_then(value_as_i64)
        .and_then(|micros| DateTime::<Utc>::from_timestamp_micros(micros))
        .or_else(|| {
            value
                .get("timestamp")
                .and_then(value_as_i64)
                .and_then(|seconds| DateTime::<Utc>::from_timestamp(seconds, 0))
        });
    Ok(snapshot)
}

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: rustcta_exchange_api::TenantId,
    account_id: rustcta_exchange_api::AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "account_balances response is not an array",
            value,
        )
    })?;
    let requested = requested_assets
        .iter()
        .map(|asset| asset.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for row in rows {
        let asset = required_str(exchange_id, row, "currency")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let total = number_from_value(row.get("total")).unwrap_or(0.0);
        let available = number_from_value(row.get("available")).unwrap_or(0.0);
        let locked = number_from_value(row.get("reserved")).unwrap_or((total - available).max(0.0));
        balances
            .push(AssetBalance::new(asset, total, available, locked).map_err(validation_error)?);
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

pub fn parse_fees(
    _exchange_id: &ExchangeId,
    symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let rows = if let Some(array) = value.as_array() {
        array.clone()
    } else {
        vec![value.clone()]
    };
    rows.iter()
        .enumerate()
        .filter_map(|(index, row)| {
            let symbol = symbols.get(index).or_else(|| symbols.first())?.clone();
            Some((symbol, row))
        })
        .map(|(symbol, row)| {
            Ok(FeeRateSnapshot {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol,
                maker_rate: percent_to_rate(
                    string_or_number(row.get("maker"))
                        .or_else(|| string_or_number(row.get("maker_fee"))),
                ),
                taker_rate: percent_to_rate(
                    string_or_number(row.get("taker"))
                        .or_else(|| string_or_number(row.get("taker_fee"))),
                ),
                source: Some("bitstamp.fees.trading".to_string()),
                updated_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<rustcta_exchange_api::OrderState> {
    let exchange_symbol = symbol_hint
        .map(|symbol| symbol.exchange_symbol.clone())
        .or_else(|| {
            value
                .get("market")
                .or_else(|| value.get("currency_pair"))
                .and_then(Value::as_str)
                .and_then(|market| {
                    ExchangeSymbol::new(
                        exchange_id.clone(),
                        MarketType::Spot,
                        normalize_market_symbol(market),
                    )
                    .ok()
                })
        })
        .ok_or_else(|| parse_error(exchange_id.clone(), "order missing symbol", value))?;
    let side = parse_order_side(value);
    let order_type = parse_order_type(value);
    let status = parse_order_status(value);
    let quantity = string_or_number(value.get("amount"))
        .or_else(|| string_or_number(value.get("amount_remaining")))
        .unwrap_or_else(|| "0".to_string());
    let filled_quantity = string_or_number(value.get("amount_filled")).unwrap_or_else(|| {
        let amount = number_from_value(value.get("amount")).unwrap_or(0.0);
        let remaining = number_from_value(value.get("amount_remaining")).unwrap_or(amount);
        (amount - remaining).max(0.0).to_string()
    });
    let now = Utc::now();
    Ok(rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone()),
        exchange_symbol,
        client_order_id: value
            .get("client_order_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        exchange_order_id: value
            .get("id")
            .or_else(|| value.get("order_id"))
            .and_then(string_or_number_ref),
        side,
        position_side: None,
        order_type,
        time_in_force: None,
        status,
        quantity,
        price: string_or_number(value.get("price")),
        filled_quantity,
        average_fill_price: string_or_number(value.get("avg_execution_price")),
        reduce_only: false,
        post_only: value
            .get("moc_order")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: parse_bitstamp_datetime(value.get("datetime").and_then(Value::as_str)),
        updated_at: now,
    })
}

pub fn parse_order_list(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<rustcta_exchange_api::OrderState>> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "orders response is not an array",
            value,
        )
    })?;
    rows.iter()
        .map(|row| parse_order_state(exchange_id, symbol_hint, row))
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: rustcta_exchange_api::TenantId,
    account_id: rustcta_exchange_api::AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "user_transactions response is not an array",
            value,
        )
    })?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitstamp fills require canonical_symbol".to_string(),
            })?;
    rows.iter()
        .filter(|row| row.get("order_id").is_some())
        .map(|row| {
            let price = number_from_value(row.get("price")).unwrap_or(0.0);
            let quantity = number_from_value(row.get("amount")).unwrap_or(0.0).abs();
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: row.get("order_id").and_then(string_or_number_ref),
                client_order_id: row
                    .get("client_order_id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                fill_id: row.get("id").and_then(string_or_number_ref),
                side: parse_order_side(row),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price,
                quantity,
                quote_quantity: Some(price * quantity),
                fee_asset: string_or_number(row.get("fee_currency")),
                fee_amount: number_from_value(row.get("fee")).map(f64::abs),
                fee_rate: None,
                realized_pnl: None,
                filled_at: parse_bitstamp_datetime(row.get("datetime").and_then(Value::as_str))
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

pub fn normalize_market_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace('/', "")
        .replace('-', "")
        .replace('_', "")
        .to_ascii_lowercase()
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
            let price = number_from_value(array.first())
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level price", level))?;
            let quantity = number_from_value(array.get(1))
                .ok_or_else(|| parse_error(exchange_id.clone(), "invalid level quantity", level))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn parse_order_side(value: &Value) -> OrderSide {
    match value
        .get("type")
        .or_else(|| value.get("side"))
        .and_then(Value::as_str)
        .unwrap_or("0")
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "sell" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(value: &Value) -> OrderType {
    match value
        .get("subtype")
        .or_else(|| value.get("order_type"))
        .and_then(Value::as_str)
        .unwrap_or("limit")
        .to_ascii_lowercase()
        .as_str()
    {
        "market" => OrderType::Market,
        "ioc" => OrderType::IOC,
        "fok" => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_order_status(value: &Value) -> OrderStatus {
    match value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("open")
        .to_ascii_lowercase()
        .as_str()
    {
        "open" | "active" | "live" => OrderStatus::Open,
        "finished" | "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" | "cancel pending" => OrderStatus::Cancelled,
        "expired" => OrderStatus::Expired,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::New,
    }
}

fn parse_bitstamp_datetime(value: Option<&str>) -> Option<DateTime<Utc>> {
    let value = value?;
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .ok()
                .map(|dt| dt.and_utc())
        })
}

fn decimals_to_increment(value: Option<&Value>) -> Option<String> {
    let decimals = value.and_then(value_as_u32)?;
    if decimals == 0 {
        Some("1".to_string())
    } else {
        Some(format!(
            "0.{}1",
            "0".repeat(decimals.saturating_sub(1) as usize)
        ))
    }
}

fn percent_to_rate(value: Option<String>) -> String {
    value
        .and_then(|value| value.parse::<f64>().ok())
        .map(|value| value / 100.0)
        .unwrap_or(0.0)
        .to_string()
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(string_or_number_ref)
}

fn string_or_number_ref(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn number_from_value(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    })
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_i64(),
        _ => None,
    }
}

fn value_as_u32(value: &Value) -> Option<u32> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_u64().and_then(|value| u32::try_from(value).ok()),
        _ => None,
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
            &format!("missing field {field}"),
            value,
        )
    })
}

fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::InvalidRequest,
        message,
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
