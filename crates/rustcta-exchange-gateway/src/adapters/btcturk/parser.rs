#![cfg_attr(not(test), allow(dead_code))]

use chrono::{TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolRules, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType,
    OrderBookLevel, OrderBookSnapshot, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion,
};
use serde_json::Value;

pub fn btcturk_symbol(symbol: &SymbolScope) -> String {
    symbol
        .exchange_symbol
        .symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase()
}

pub fn parse_btcturk_symbol_rules(
    exchange_id: ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let symbols = value
        .get("data")
        .and_then(|data| data.get("symbols"))
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("btcturk exchangeinfo missing data.symbols"))?;
    let mut rules = Vec::new();
    for entry in symbols {
        let raw_symbol = string_field(entry, "name")
            .or_else(|_| string_field(entry, "symbol"))
            .or_else(|_| string_field(entry, "pair"))?;
        if !requested.is_empty()
            && !requested
                .iter()
                .any(|symbol| btcturk_symbol(symbol) == raw_symbol.to_ascii_uppercase())
        {
            continue;
        }
        let base =
            string_field(entry, "numerator").or_else(|_| string_field(entry, "numeratorSymbol"))?;
        let quote = string_field(entry, "denominator")
            .or_else(|_| string_field(entry, "denominatorSymbol"))?;
        let scope = symbol_scope(exchange_id.clone(), &base, &quote, &raw_symbol)?;
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: scope,
            base_asset: base.to_ascii_uppercase(),
            quote_asset: quote.to_ascii_uppercase(),
            price_increment: decimal_increment(entry.get("denominatorScale")),
            quantity_increment: decimal_increment(entry.get("numeratorScale")),
            min_price: optional_string(entry, "minLimitOrderPrice"),
            max_price: optional_string(entry, "maxLimitOrderPrice"),
            min_quantity: optional_string(entry, "minAmount"),
            max_quantity: optional_string(entry, "maxAmount"),
            min_notional: optional_string(entry, "minExchangeValue"),
            max_notional: None,
            price_precision: entry
                .get("denominatorScale")
                .and_then(Value::as_u64)
                .map(|value| value as u32),
            quantity_precision: entry
                .get("numeratorScale")
                .and_then(Value::as_u64)
                .map(|value| value as u32),
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: false,
            supports_reduce_only: false,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_btcturk_order_book(
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value
        .get("data")
        .ok_or_else(|| invalid("btcturk orderbook missing data"))?;
    let bids = parse_levels(data.get("bids"))?;
    let asks = parse_levels(data.get("asks"))?;
    let canonical = symbol
        .canonical_symbol
        .clone()
        .ok_or_else(|| invalid("btcturk orderbook requires canonical symbol"))?;
    let received_at = Utc::now();
    let mut snapshot = OrderBookSnapshot::new(
        symbol.exchange.clone(),
        MarketType::Spot,
        canonical,
        bids,
        asks,
        received_at,
    )
    .map_err(|error| invalid(format!("invalid btcturk orderbook: {error}")))?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol.clone());
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .and_then(Value::as_i64)
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single());
    Ok(snapshot)
}

pub fn parse_btcturk_order_state(
    exchange_id: &ExchangeId,
    symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = first_data_object(value)?;
    let exchange_order_id = string_field_any(order, &["id", "orderId"])?;
    let pair = string_field_any_optional(order, &["pairSymbol", "symbol"])
        .or_else(|| symbol.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| invalid("btcturk order missing pairSymbol"))?;
    let scope = symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| scope_from_pair(exchange_id.clone(), &pair))?;
    let quantity =
        string_field_any_optional(order, &["quantity", "amount"]).unwrap_or_else(|| "0".into());
    let filled_quantity = string_field_any_optional(
        order,
        &[
            "filledQuantity",
            "filledAmount",
            "executedQuantity",
            "executedAmount",
        ],
    )
    .unwrap_or_else(|| {
        if parse_order_status(order) == OrderStatus::Filled {
            quantity.clone()
        } else {
            "0".to_string()
        }
    });
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: scope.canonical_symbol.clone(),
        exchange_symbol: scope.exchange_symbol,
        client_order_id: string_field_any_optional(
            order,
            &["orderClientId", "clientOrderId", "newOrderClientId"],
        ),
        exchange_order_id: Some(exchange_order_id),
        side: parse_order_side(order)?,
        position_side: None,
        order_type: parse_order_type(order),
        time_in_force: None,
        status: parse_order_status(order),
        quantity,
        price: string_field_any_optional(order, &["price", "limitPrice"]),
        filled_quantity: filled_quantity.clone(),
        average_fill_price: if filled_quantity == "0" {
            None
        } else {
            string_field_any_optional(order, &["avgPrice", "averagePrice", "price"])
        },
        reduce_only: false,
        post_only: false,
        created_at: timestamp_millis_any(order, &["date", "createdAt", "createdDate"]),
        updated_at: timestamp_millis_any(order, &["updatedAt", "updateDate"])
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_btcturk_open_orders(
    exchange_id: &ExchangeId,
    symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.get("orders").and_then(Value::as_array))
        .ok_or_else(|| invalid("btcturk open orders response missing data array"))?;
    orders
        .iter()
        .map(|order| parse_btcturk_order_state(exchange_id, symbol, order))
        .collect()
}

pub fn parse_btcturk_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let trades = value
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| value.get("trades").and_then(Value::as_array))
        .ok_or_else(|| invalid("btcturk recent fills response missing data array"))?;
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .ok_or_else(|| invalid("btcturk fills require canonical symbol"))?;
    trades
        .iter()
        .map(|trade| {
            let price = parse_f64(
                trade.get("price").or_else(|| trade.get("rate")),
                "fill price",
            )?;
            let quantity = parse_f64(
                trade.get("amount").or_else(|| trade.get("quantity")),
                "fill quantity",
            )?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: string_field_any_optional(trade, &["orderId", "order_id"]),
                client_order_id: string_field_any_optional(
                    trade,
                    &["orderClientId", "clientOrderId"],
                ),
                fill_id: string_field_any_optional(trade, &["id", "tradeId", "transactionId"]),
                side: parse_order_side(trade)?,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price,
                quantity,
                quote_quantity: Some(price * quantity),
                fee_asset: symbol
                    .canonical_symbol
                    .as_ref()
                    .map(|canonical| canonical.quote_asset().to_string()),
                fee_amount: numeric_field(trade, "fee"),
                fee_rate: None,
                realized_pnl: None,
                filled_at: timestamp_millis_any(trade, &["date", "timestamp", "createdAt"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn symbol_scope(
    exchange_id: ExchangeId,
    base: &str,
    quote: &str,
    raw_symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let canonical = CanonicalSymbol::new(base.to_ascii_uppercase(), quote.to_ascii_uppercase())
        .map_err(|error| invalid(format!("invalid btcturk canonical symbol: {error}")))?;
    let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, raw_symbol)
        .map_err(|error| invalid(format!("invalid btcturk exchange symbol: {error}")))?;
    Ok(SymbolScope {
        exchange: exchange_id,
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical),
        exchange_symbol,
    })
}

fn scope_from_pair(exchange_id: ExchangeId, pair: &str) -> ExchangeApiResult<SymbolScope> {
    let normalized = pair
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase();
    for quote in ["TRY", "USDT", "USDC", "EUR", "BTC", "ETH"] {
        if normalized.ends_with(quote) && normalized.len() > quote.len() {
            let base = &normalized[..normalized.len() - quote.len()];
            return symbol_scope(exchange_id, base, quote, &normalized);
        }
    }
    Err(invalid(format!("unsupported btcturk pair format {pair}")))
}

fn first_data_object(value: &Value) -> ExchangeApiResult<&Value> {
    if let Some(data) = value.get("data") {
        if let Some(array) = data.as_array() {
            return array
                .first()
                .ok_or_else(|| invalid("btcturk response data array is empty"));
        }
        return Ok(data);
    }
    if let Some(order) = value.get("order") {
        return Ok(order);
    }
    Ok(value)
}

fn parse_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = value
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("btcturk orderbook level side must be an array"))?;
    levels
        .iter()
        .map(|level| {
            let tuple = level
                .as_array()
                .ok_or_else(|| invalid("btcturk orderbook level must be [price, quantity]"))?;
            let price = parse_f64(tuple.first(), "price")?;
            let quantity = parse_f64(tuple.get(1), "quantity")?;
            OrderBookLevel::new(price, quantity)
                .map_err(|error| invalid(format!("invalid btcturk level: {error}")))
        })
        .collect()
}

fn decimal_increment(value: Option<&Value>) -> Option<String> {
    let scale = value.and_then(Value::as_u64)?;
    if scale == 0 {
        return Some("1".to_string());
    }
    Some(format!(
        "0.{}1",
        "0".repeat(scale.saturating_sub(1) as usize)
    ))
}

fn string_field(value: &Value, field: &str) -> ExchangeApiResult<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| invalid(format!("btcturk missing string field {field}")))
}

fn string_field_any(value: &Value, fields: &[&str]) -> ExchangeApiResult<String> {
    string_field_any_optional(value, fields).ok_or_else(|| {
        invalid(format!(
            "btcturk missing string field {}",
            fields.first().copied().unwrap_or("unknown")
        ))
    })
}

fn string_field_any_optional(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        value.get(field).and_then(|value| {
            value
                .as_str()
                .map(str::to_string)
                .or_else(|| value.as_i64().map(|number| number.to_string()))
                .or_else(|| value.as_u64().map(|number| number.to_string()))
                .or_else(|| value.as_f64().map(|number| number.to_string()))
        })
    })
}

fn optional_string(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(|field_value| {
        field_value
            .as_str()
            .map(str::to_string)
            .or_else(|| field_value.as_f64().map(|number| number.to_string()))
    })
}

fn parse_f64(value: Option<&Value>, field: &str) -> ExchangeApiResult<f64> {
    value
        .and_then(|value| {
            value
                .as_str()
                .and_then(|text| text.parse::<f64>().ok())
                .or_else(|| value.as_f64())
        })
        .ok_or_else(|| invalid(format!("btcturk invalid numeric {field}")))
}

fn numeric_field(value: &Value, field: &str) -> Option<f64> {
    value.get(field).and_then(|value| {
        value
            .as_str()
            .and_then(|text| text.parse::<f64>().ok())
            .or_else(|| value.as_f64())
    })
}

fn timestamp_millis_any(value: &Value, fields: &[&str]) -> Option<chrono::DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value
            .get(field)
            .and_then(|value| {
                value
                    .as_i64()
                    .or_else(|| value.as_str().and_then(|text| text.parse::<i64>().ok()))
            })
            .and_then(|millis| Utc.timestamp_millis_opt(millis).single())
    })
}

fn parse_order_side(value: &Value) -> ExchangeApiResult<OrderSide> {
    match string_field_any_optional(value, &["type", "side", "orderType"])
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "buy" | "bid" => Ok(OrderSide::Buy),
        "sell" | "ask" => Ok(OrderSide::Sell),
        other => Err(invalid(format!("unsupported btcturk order side {other}"))),
    }
}

fn parse_order_type(value: &Value) -> OrderType {
    match string_field_any_optional(value, &["method", "orderMethod"])
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_order_status(value: &Value) -> OrderStatus {
    match string_field_any_optional(value, &["status", "orderStatus"])
        .unwrap_or_default()
        .trim()
        .replace(['_', '-'], "")
        .to_ascii_lowercase()
        .as_str()
    {
        "new" | "untouched" | "open" | "active" | "" => OrderStatus::Open,
        "partial" | "partiallyfilled" | "partialfilled" => OrderStatus::PartiallyFilled,
        "filled" | "matched" | "closed" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn invalid(message: impl Into<String>) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: message.into(),
    }
}
