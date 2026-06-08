#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolRules, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill,
    FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion, TenantId,
};
use serde_json::Value;

pub fn blockchaincom_symbol(symbol: &SymbolScope) -> String {
    symbol
        .exchange_symbol
        .symbol
        .trim()
        .replace(['/', '_'], "-")
        .to_ascii_uppercase()
}

pub fn parse_blockchaincom_symbol_rules(
    exchange_id: ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = symbol_rows(value)?;
    let requested_symbols = requested
        .iter()
        .map(blockchaincom_symbol)
        .collect::<Vec<_>>();
    let mut rules = Vec::new();
    for (symbol_name, row) in rows {
        let symbol_name = symbol_name.to_ascii_uppercase();
        if !requested_symbols.is_empty() && !requested_symbols.contains(&symbol_name) {
            continue;
        }
        let (base_asset, quote_asset) = parse_base_quote(&symbol_name, row)?;
        let symbol = symbol_scope(exchange_id.clone(), &base_asset, &quote_asset, &symbol_name)?;
        let status = row
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("open")
            .to_ascii_lowercase();
        let trading = matches!(status.as_str(), "open" | "trading" | "enabled");
        let price_scale = u32_field(row, "min_price_increment_scale")
            .or_else(|| u32_field(row, "counter_currency_scale"));
        let quantity_scale = u32_field(row, "base_currency_scale")
            .or_else(|| u32_field(row, "min_order_size_scale"));
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            base_asset,
            quote_asset,
            price_increment: scaled_value(
                row.get("min_price_increment"),
                row.get("min_price_increment_scale"),
            ),
            quantity_increment: scaled_value(row.get("lot_size"), row.get("base_currency_scale"))
                .or_else(|| decimal_increment(quantity_scale)),
            min_price: None,
            max_price: None,
            min_quantity: scaled_value(row.get("min_order_size"), row.get("min_order_size_scale")),
            max_quantity: scaled_value(row.get("max_order_size"), row.get("min_order_size_scale")),
            min_notional: None,
            max_notional: None,
            price_precision: price_scale,
            quantity_precision: quantity_scale,
            supports_market_orders: trading,
            supports_limit_orders: trading,
            supports_post_only: false,
            supports_reduce_only: false,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_blockchaincom_order_book(
    symbol: &SymbolScope,
    depth: Option<u32>,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let mut bids = parse_levels(value.get("bids"))?;
    let mut asks = parse_levels(value.get("asks"))?;
    bids.sort_by(|left, right| right.price.total_cmp(&left.price));
    asks.sort_by(|left, right| left.price.total_cmp(&right.price));
    if let Some(depth) = depth {
        bids.truncate(depth as usize);
        asks.truncate(depth as usize);
    }
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .or_else(|| {
            split_symbol(&symbol.exchange_symbol.symbol)
                .ok()
                .and_then(|(base, quote)| CanonicalSymbol::new(base, quote).ok())
        })
        .ok_or_else(|| invalid("blockchaincom order book requires canonical symbol"))?;
    let received_at = Utc::now();
    let mut snapshot = OrderBookSnapshot::new(
        symbol.exchange.clone(),
        MarketType::Spot,
        canonical_symbol,
        bids,
        asks,
        received_at,
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol.clone());
    snapshot.sequence = value.get("seqnum").and_then(value_as_u64);
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .or_else(|| value.get("last_update"))
        .and_then(parse_timestamp_value);
    Ok(snapshot)
}

pub fn parse_blockchaincom_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let requested = requested_assets
        .iter()
        .map(|asset| asset.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let rows = balance_rows(value)?;
    let mut balances = Vec::new();
    for row in rows {
        let asset = string_field(row, "currency")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let total = number_field(row, "balance").or_else(|| number_field(row, "total"));
        let available = number_field(row, "available").unwrap_or(total.unwrap_or(0.0));
        let locked = number_field(row, "locked").unwrap_or_else(|| {
            let total = total.unwrap_or(available);
            (total - available).max(0.0)
        });
        balances.push(
            AssetBalance::new(
                asset,
                total.unwrap_or(available + locked),
                available,
                locked,
            )
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

pub fn parse_blockchaincom_fees(
    exchange_id: &ExchangeId,
    symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let maker = percent_to_rate(string_or_number(value.get("makerRate")).or_else(|| {
        string_or_number(value.get("maker")).or_else(|| string_or_number(value.get("maker_rate")))
    }));
    let taker = percent_to_rate(string_or_number(value.get("takerRate")).or_else(|| {
        string_or_number(value.get("taker")).or_else(|| string_or_number(value.get("taker_rate")))
    }));
    let mut fees = Vec::new();
    for symbol in symbols {
        fees.push(FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: symbol.clone(),
            maker_rate: maker.clone(),
            taker_rate: taker.clone(),
            source: Some(format!("{exchange_id}.fees")),
            updated_at: Utc::now(),
        });
    }
    Ok(fees)
}

pub fn parse_blockchaincom_order_list(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let rows = value
        .as_array()
        .or_else(|| value.get("orders").and_then(Value::as_array))
        .ok_or_else(|| invalid("blockchaincom orders response must be an array"))?;
    rows.iter()
        .map(|row| parse_blockchaincom_order(exchange_id, symbol_hint, row))
        .collect()
}

pub fn parse_blockchaincom_order(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let raw_symbol = value
        .get("symbol")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.to_ascii_uppercase()))
        .ok_or_else(|| invalid("blockchaincom order missing symbol"))?;
    let (base, quote) = split_symbol(&raw_symbol)?;
    let exchange_symbol = ExchangeSymbol::new(
        exchange_id.clone(),
        MarketType::Spot,
        raw_symbol.to_ascii_uppercase(),
    )
    .map_err(validation_error)?;
    let canonical = CanonicalSymbol::new(base, quote).map_err(validation_error)?;
    let remaining = number_field(value, "leavesQty")
        .or_else(|| number_field(value, "remainingQty"))
        .unwrap_or(0.0);
    let filled = number_field(value, "cumQty")
        .or_else(|| number_field(value, "filledQty"))
        .unwrap_or(0.0);
    let quantity = number_field(value, "orderQty")
        .or_else(|| number_field(value, "qty"))
        .unwrap_or(remaining + filled);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical),
        exchange_symbol,
        client_order_id: value
            .get("clOrdId")
            .or_else(|| value.get("clientOrderId"))
            .and_then(Value::as_str)
            .map(str::to_string),
        exchange_order_id: value
            .get("exOrdId")
            .or_else(|| value.get("orderId"))
            .and_then(string_or_number_ref),
        side: parse_side(value),
        position_side: None,
        order_type: parse_order_type(value),
        time_in_force: None,
        status: parse_order_status(value),
        quantity: quantity.to_string(),
        price: string_or_number(value.get("price")).or_else(|| string_or_number(value.get("px"))),
        filled_quantity: filled.to_string(),
        average_fill_price: string_or_number(value.get("avgPx")),
        reduce_only: false,
        post_only: false,
        created_at: value
            .get("timestamp")
            .or_else(|| value.get("createdTime"))
            .and_then(parse_timestamp_value),
        updated_at: Utc::now(),
    })
}

pub fn parse_blockchaincom_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let rows = value
        .as_array()
        .or_else(|| value.get("fills").and_then(Value::as_array))
        .or_else(|| value.get("trades").and_then(Value::as_array))
        .ok_or_else(|| invalid("blockchaincom fills response must be an array"))?;
    rows.iter()
        .map(|row| {
            let raw_symbol = row
                .get("symbol")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| {
                    symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.to_ascii_uppercase())
                })
                .ok_or_else(|| invalid("blockchaincom fill missing symbol"))?;
            let (base, quote) = split_symbol(&raw_symbol)?;
            let canonical = CanonicalSymbol::new(base, quote).map_err(validation_error)?;
            let price = number_field(row, "price")
                .or_else(|| number_field(row, "px"))
                .ok_or_else(|| invalid("blockchaincom fill missing price"))?;
            let quantity = number_field(row, "qty")
                .or_else(|| number_field(row, "quantity"))
                .ok_or_else(|| invalid("blockchaincom fill missing quantity"))?;
            let exchange_symbol = ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                raw_symbol.to_ascii_uppercase(),
            )
            .map_err(validation_error)?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical,
                exchange_symbol: Some(exchange_symbol),
                order_id: row
                    .get("exOrdId")
                    .or_else(|| row.get("orderId"))
                    .and_then(string_or_number_ref),
                client_order_id: row
                    .get("clOrdId")
                    .or_else(|| row.get("clientOrderId"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                fill_id: row
                    .get("tradeId")
                    .or_else(|| row.get("execId"))
                    .and_then(string_or_number_ref),
                side: parse_side(row),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity(row),
                price,
                quantity,
                quote_quantity: Some(price * quantity),
                fee_asset: row
                    .get("feeCurrency")
                    .or_else(|| row.get("fee_currency"))
                    .and_then(Value::as_str)
                    .map(|asset| asset.to_ascii_uppercase()),
                fee_amount: number_field(row, "fee").or_else(|| number_field(row, "feeAmount")),
                fee_rate: None,
                realized_pnl: None,
                filled_at: row
                    .get("timestamp")
                    .or_else(|| row.get("tradeTime"))
                    .and_then(parse_timestamp_value)
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn symbol_rows(value: &Value) -> ExchangeApiResult<Vec<(String, &Value)>> {
    if let Some(object) = value.as_object() {
        return Ok(object
            .iter()
            .map(|(symbol, row)| (symbol.clone(), row))
            .collect());
    }
    if let Some(array) = value.as_array() {
        return array
            .iter()
            .map(|row| {
                let symbol = row
                    .get("symbol")
                    .or_else(|| row.get("base_currency"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("blockchaincom symbol row missing symbol"))?;
                Ok((symbol.to_string(), row))
            })
            .collect();
    }
    Err(invalid(
        "blockchaincom symbols response must be object or array",
    ))
}

fn parse_base_quote(symbol_name: &str, row: &Value) -> ExchangeApiResult<(String, String)> {
    let split = split_symbol(symbol_name)?;
    let base = row
        .get("base_currency")
        .and_then(Value::as_str)
        .filter(|value| !value.contains('-'))
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or(split.0);
    let quote = row
        .get("counter_currency")
        .or_else(|| row.get("quote_currency"))
        .and_then(Value::as_str)
        .filter(|value| !value.contains('-'))
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or(split.1);
    Ok((base, quote))
}

fn split_symbol(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let symbol = symbol.trim().replace(['/', '_'], "-").to_ascii_uppercase();
    let Some((base, quote)) = symbol.split_once('-') else {
        return Err(invalid(format!(
            "blockchaincom unsupported symbol format {symbol}"
        )));
    };
    Ok((base.to_string(), quote.to_string()))
}

fn symbol_scope(
    exchange_id: ExchangeId,
    base: &str,
    quote: &str,
    raw_symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let canonical = CanonicalSymbol::new(base, quote).map_err(validation_error)?;
    let exchange_symbol = ExchangeSymbol::new(
        exchange_id.clone(),
        MarketType::Spot,
        raw_symbol.to_ascii_uppercase(),
    )
    .map_err(validation_error)?;
    Ok(SymbolScope {
        exchange: exchange_id,
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical),
        exchange_symbol,
    })
}

fn parse_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("blockchaincom order book side must be an array"))?;
    let mut output = Vec::new();
    for level in levels {
        let price = number_field(level, "px")
            .or_else(|| number_field(level, "price"))
            .ok_or_else(|| invalid("blockchaincom level missing price"))?;
        let quantity = number_field(level, "qty")
            .or_else(|| number_field(level, "quantity"))
            .ok_or_else(|| invalid("blockchaincom level missing quantity"))?;
        if quantity <= 0.0 {
            continue;
        }
        output.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
    }
    Ok(output)
}

fn balance_rows(value: &Value) -> ExchangeApiResult<Vec<&Value>> {
    if let Some(array) = value.as_array() {
        return Ok(array.iter().collect());
    }
    let Some(object) = value.as_object() else {
        return Err(invalid(
            "blockchaincom accounts response must be object or array",
        ));
    };
    if let Some(primary) = object.get("primary").and_then(Value::as_array) {
        return Ok(primary.iter().collect());
    }
    Ok(object
        .values()
        .filter_map(Value::as_array)
        .flatten()
        .collect())
}

fn parse_side(value: &Value) -> OrderSide {
    match value
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or("BUY")
        .to_ascii_uppercase()
        .as_str()
    {
        "SELL" | "ASK" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(value: &Value) -> OrderType {
    match value
        .get("ordType")
        .or_else(|| value.get("orderType"))
        .and_then(Value::as_str)
        .unwrap_or("LIMIT")
        .to_ascii_uppercase()
        .as_str()
    {
        "MARKET" => OrderType::Market,
        "STOP" => OrderType::StopMarket,
        "STOPLIMIT" | "STOP_LIMIT" => OrderType::StopLimit,
        _ => OrderType::Limit,
    }
}

fn parse_order_status(value: &Value) -> OrderStatus {
    match value
        .get("ordStatus")
        .or_else(|| value.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_ascii_uppercase()
        .as_str()
    {
        "NEW" | "PENDING" => OrderStatus::New,
        "OPEN" => OrderStatus::Open,
        "PARTIALLY_FILLED" | "PART_FILLED" | "PARTIAL" => OrderStatus::PartiallyFilled,
        "FILLED" | "TRADED" => OrderStatus::Filled,
        "PENDING_CANCEL" => OrderStatus::PendingCancel,
        "CANCELED" | "CANCELLED" | "EXPIRED_CANCELLED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity(value: &Value) -> LiquidityRole {
    match value
        .get("liquidity")
        .or_else(|| value.get("liquidityInd"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_uppercase()
        .as_str()
    {
        "M" | "MAKER" => LiquidityRole::Maker,
        "T" | "TAKER" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn scaled_value(value: Option<&Value>, scale: Option<&Value>) -> Option<String> {
    let value = value_to_i128(value?)?;
    if value == 0 {
        return None;
    }
    let scale = scale.and_then(|value| value_as_u32(value)).unwrap_or(0);
    Some(format_scaled(value, scale))
}

fn decimal_increment(scale: Option<u32>) -> Option<String> {
    let scale = scale?;
    Some(format_scaled(1, scale))
}

fn format_scaled(value: i128, scale: u32) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let sign = if value < 0 { "-" } else { "" };
    let digits = value.abs().to_string();
    let scale = scale as usize;
    let text = if digits.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    let trimmed = text.trim_end_matches('0').trim_end_matches('.');
    format!("{sign}{trimmed}")
}

fn percent_to_rate(value: Option<String>) -> String {
    value
        .and_then(|raw| raw.parse::<f64>().ok())
        .map(|percent| {
            if percent.abs() > 1.0 {
                (percent / 100.0).to_string()
            } else {
                percent.to_string()
            }
        })
        .unwrap_or_else(|| "0".to_string())
}

fn u32_field(value: &Value, field: &str) -> Option<u32> {
    value.get(field).and_then(value_as_u32)
}

fn value_as_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|number| u32::try_from(number).ok())
        .or_else(|| value.as_str()?.parse::<u32>().ok())
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse::<u64>().ok())
}

fn value_to_i128(value: &Value) -> Option<i128> {
    value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(i128::from))
        .or_else(|| value.as_str()?.parse::<i128>().ok())
}

fn number_field(value: &Value, field: &str) -> Option<f64> {
    value.get(field).and_then(number_from_value)
}

fn number_from_value(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())
}

fn string_field(value: &Value, field: &str) -> ExchangeApiResult<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| invalid(format!("blockchaincom missing string field {field}")))
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(string_or_number_ref)
}

fn string_or_number_ref(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_string)
        .or_else(|| value.as_i64().map(|number| number.to_string()))
        .or_else(|| value.as_u64().map(|number| number.to_string()))
        .or_else(|| value.as_f64().map(|number| number.to_string()))
}

fn parse_timestamp_value(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(text) = value.as_str() {
        if let Ok(datetime) = DateTime::parse_from_rfc3339(text) {
            return Some(datetime.with_timezone(&Utc));
        }
        if let Ok(millis) = text.parse::<i64>() {
            return DateTime::<Utc>::from_timestamp_millis(millis);
        }
    }
    value
        .as_i64()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

fn invalid(message: impl Into<String>) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: message.into(),
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
