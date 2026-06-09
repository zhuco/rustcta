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

pub fn luno_symbol(symbol: &SymbolScope) -> String {
    symbol
        .exchange_symbol
        .symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase()
}

pub fn parse_luno_symbol_rules(
    exchange_id: ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let tickers = value
        .get("tickers")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("luno tickers response missing tickers"))?;
    let mut rules = Vec::new();
    for ticker in tickers {
        let pair = string_field(ticker, "pair")?;
        if !requested.is_empty()
            && !requested
                .iter()
                .any(|symbol| luno_symbol(symbol) == pair.to_ascii_uppercase())
        {
            continue;
        }
        let (base, quote) = split_luno_pair(&pair)?;
        let scope = symbol_scope(exchange_id.clone(), &base, &quote, &pair)?;
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: scope,
            base_asset: base,
            quote_asset: quote,
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
        });
    }
    Ok(rules)
}

pub fn parse_luno_order_book(
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(value.get("bids"))?;
    let asks = parse_levels(value.get("asks"))?;
    let canonical = symbol
        .canonical_symbol
        .clone()
        .ok_or_else(|| invalid("luno orderbook requires canonical symbol"))?;
    let received_at = Utc::now();
    let mut snapshot = OrderBookSnapshot::new(
        symbol.exchange.clone(),
        MarketType::Spot,
        canonical,
        bids,
        asks,
        received_at,
    )
    .map_err(|error| invalid(format!("invalid luno orderbook: {error}")))?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol.clone());
    Ok(snapshot)
}

pub fn parse_luno_order_state(
    exchange_id: &ExchangeId,
    symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = value
        .get("order")
        .unwrap_or(value)
        .get("orders")
        .and_then(Value::as_array)
        .and_then(|orders| orders.first())
        .unwrap_or_else(|| value.get("order").unwrap_or(value));
    let exchange_order_id = string_field_any(order, &["order_id", "id"])?;
    let pair = string_field_any_optional(order, &["pair"])
        .or_else(|| symbol.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| invalid("luno order missing pair"))?;
    let scope = symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| scope_from_pair(exchange_id.clone(), &pair))?;
    let quantity = string_field_any_optional(order, &["limit_volume", "volume", "base"])
        .unwrap_or_else(|| "0".to_string());
    let filled_quantity = string_field_any_optional(order, &["base"])
        .or_else(|| {
            if order_status(order) == OrderStatus::Filled {
                Some(quantity.clone())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "0".to_string());
    let average_fill_price = average_price(order);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: scope.canonical_symbol.clone(),
        exchange_symbol: scope.exchange_symbol,
        client_order_id: string_field_any_optional(order, &["client_order_id"]),
        exchange_order_id: Some(exchange_order_id),
        side: order_side(order)?,
        position_side: None,
        order_type: if order.get("limit_price").is_some() || order.get("price").is_some() {
            OrderType::Limit
        } else {
            OrderType::Market
        },
        time_in_force: None,
        status: order_status(order),
        quantity,
        price: string_field_any_optional(order, &["limit_price", "price"]),
        filled_quantity,
        average_fill_price,
        reduce_only: false,
        post_only: false,
        created_at: timestamp_millis(order, "creation_timestamp"),
        updated_at: timestamp_millis(order, "completed_timestamp").unwrap_or_else(Utc::now),
    })
}

pub fn parse_luno_open_orders(
    exchange_id: &ExchangeId,
    symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value
        .get("orders")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("luno listorders response missing orders"))?;
    orders
        .iter()
        .map(|order| parse_luno_order_state(exchange_id, symbol, order))
        .collect()
}

pub fn parse_luno_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let trades = value
        .get("trades")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("luno listtrades response missing trades"))?;
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .ok_or_else(|| invalid("luno fills require canonical symbol"))?;
    trades
        .iter()
        .map(|trade| {
            let price = field_f64(trade, "price")?;
            let quantity = field_f64(trade, "base")?;
            let quote_quantity = numeric_field(trade, "counter");
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: string_field_any_optional(trade, &["order_id"]),
                client_order_id: string_field_any_optional(trade, &["client_order_id"]),
                fill_id: string_field_any_optional(trade, &["sequence", "trade_id", "id"]),
                side: order_side(trade)?,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price,
                quantity,
                quote_quantity,
                fee_asset: fee_asset(trade),
                fee_amount: fee_amount(trade),
                fee_rate: None,
                realized_pnl: None,
                filled_at: timestamp_millis(trade, "timestamp").unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn split_luno_pair(pair: &str) -> ExchangeApiResult<(String, String)> {
    let pair = pair.trim().to_ascii_uppercase();
    for base in ["XBT", "BTC", "ETH", "LTC", "XRP", "BCH", "USDC"] {
        if pair.starts_with(base) && pair.len() > base.len() {
            let quote = &pair[base.len()..];
            let base = if base == "XBT" { "BTC" } else { base };
            return Ok((base.to_string(), quote.to_string()));
        }
    }
    Err(invalid(format!("unsupported luno pair format {pair}")))
}

fn scope_from_pair(exchange_id: ExchangeId, pair: &str) -> ExchangeApiResult<SymbolScope> {
    let (base, quote) = split_luno_pair(pair)?;
    symbol_scope(exchange_id, &base, &quote, pair)
}

fn symbol_scope(
    exchange_id: ExchangeId,
    base: &str,
    quote: &str,
    raw_symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let canonical = CanonicalSymbol::new(base, quote)
        .map_err(|error| invalid(format!("invalid luno canonical symbol: {error}")))?;
    let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, raw_symbol)
        .map_err(|error| invalid(format!("invalid luno exchange symbol: {error}")))?;
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
        .ok_or_else(|| invalid("luno orderbook level side must be an array"))?;
    levels
        .iter()
        .map(|level| {
            let price = field_f64(level, "price")?;
            let quantity = field_f64(level, "volume")?;
            OrderBookLevel::new(price, quantity)
                .map_err(|error| invalid(format!("invalid luno level: {error}")))
        })
        .collect()
}

fn string_field(value: &Value, field: &str) -> ExchangeApiResult<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| invalid(format!("luno missing string field {field}")))
}

fn string_field_any(value: &Value, fields: &[&str]) -> ExchangeApiResult<String> {
    string_field_any_optional(value, fields).ok_or_else(|| {
        invalid(format!(
            "luno missing string field {}",
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
        })
    })
}

fn field_f64(value: &Value, field: &str) -> ExchangeApiResult<f64> {
    value
        .get(field)
        .and_then(|value| {
            value
                .as_str()
                .and_then(|text| text.parse::<f64>().ok())
                .or_else(|| value.as_f64())
        })
        .ok_or_else(|| invalid(format!("luno invalid numeric {field}")))
}

fn numeric_field(value: &Value, field: &str) -> Option<f64> {
    value.get(field).and_then(|value| {
        value
            .as_str()
            .and_then(|text| text.parse::<f64>().ok())
            .or_else(|| value.as_f64())
    })
}

fn timestamp_millis(value: &Value, field: &str) -> Option<chrono::DateTime<Utc>> {
    value
        .get(field)
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|text| text.parse::<i64>().ok()))
        })
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single())
}

fn order_side(value: &Value) -> ExchangeApiResult<OrderSide> {
    if let Some(is_buy) = value.get("is_buy").and_then(Value::as_bool) {
        return Ok(if is_buy {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        });
    }
    match string_field_any_optional(value, &["type", "side"])
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase()
        .as_str()
    {
        "BID" | "BUY" => Ok(OrderSide::Buy),
        "ASK" | "SELL" => Ok(OrderSide::Sell),
        other => Err(invalid(format!("unsupported luno order side {other}"))),
    }
}

fn order_status(value: &Value) -> OrderStatus {
    match string_field_any_optional(value, &["state", "status"])
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase()
        .as_str()
    {
        "PENDING" => OrderStatus::New,
        "ACTIVE" | "OPEN" => OrderStatus::Open,
        "COMPLETE" | "COMPLETED" | "FILLED" => OrderStatus::Filled,
        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
        "EXPIRED" => OrderStatus::Expired,
        "" => OrderStatus::Unknown,
        _ => OrderStatus::Unknown,
    }
}

fn average_price(value: &Value) -> Option<String> {
    if let Some(price) = string_field_any_optional(value, &["average_price", "avg_price"]) {
        return Some(price);
    }
    let base = numeric_field(value, "base")?;
    let counter = numeric_field(value, "counter")?;
    if base > 0.0 {
        Some((counter / base).to_string())
    } else {
        None
    }
}

fn fee_amount(value: &Value) -> Option<f64> {
    numeric_field(value, "fee_base")
        .filter(|fee| *fee > 0.0)
        .or_else(|| numeric_field(value, "fee_counter").filter(|fee| *fee > 0.0))
}

fn fee_asset(value: &Value) -> Option<String> {
    if numeric_field(value, "fee_base").is_some_and(|fee| fee > 0.0) {
        return Some("base".to_string());
    }
    if numeric_field(value, "fee_counter").is_some_and(|fee| fee > 0.0) {
        return Some("counter".to_string());
    }
    None
}

fn invalid(message: impl Into<String>) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: message.into(),
    }
}
