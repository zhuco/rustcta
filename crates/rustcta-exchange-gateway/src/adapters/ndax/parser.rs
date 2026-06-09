#![cfg_attr(not(test), allow(dead_code))]

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

const KNOWN_QUOTES: &[&str] = &["CAD", "USD", "USDT", "BTC", "ETH"];

pub fn ndax_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['/', '-', '_'], "")
        .to_ascii_uppercase()
}

pub fn ndax_canonical_pair(symbol: &str) -> ExchangeApiResult<(String, String)> {
    let trimmed = symbol.trim().to_ascii_uppercase();
    for separator in ['/', '-', '_'] {
        if let Some((base, quote)) = trimmed.split_once(separator) {
            if !base.is_empty() && !quote.is_empty() {
                return Ok((base.to_string(), quote.to_string()));
            }
        }
    }
    let normalized = ndax_symbol(symbol);
    for quote in KNOWN_QUOTES {
        if normalized.ends_with(quote) && normalized.len() > quote.len() {
            return Ok((
                normalized[..normalized.len() - quote.len()].to_string(),
                (*quote).to_string(),
            ));
        }
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("NDAX symbol {symbol} must include a known quote asset"),
    })
}

pub fn ensure_ndax_spot_market(market_type: MarketType) -> ExchangeApiResult<()> {
    if market_type != MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "ndax.unsupported_market_type",
        });
    }
    Ok(())
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "NDAX GetInstruments fixture missing array",
            value,
        )
    })?;
    let mut rules = Vec::new();
    for row in rows {
        let symbol_text = required_text(exchange_id, row, &["symbol", "Symbol"])?;
        let exchange_symbol = ndax_symbol(&symbol_text);
        if !requested_symbols.is_empty()
            && !requested_symbols
                .iter()
                .any(|symbol| ndax_symbol(&symbol.exchange_symbol.symbol) == exchange_symbol)
        {
            continue;
        }
        let (fallback_base, fallback_quote) = ndax_canonical_pair(&exchange_symbol)?;
        let base_asset = text_any(row, &["product1Symbol", "Product1Symbol"])
            .unwrap_or(fallback_base)
            .to_ascii_uppercase();
        let quote_asset = text_any(row, &["product2Symbol", "Product2Symbol"])
            .unwrap_or(fallback_quote)
            .to_ascii_uppercase();
        let status = value_as_i64_any(row, &["sessionStatus", "SessionStatus"]).unwrap_or(1);
        if status != 1 {
            continue;
        }
        let symbol = SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(
                CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?,
            ),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Spot,
                &exchange_symbol,
            )
            .map_err(validation_error)?,
        };
        let price_increment = decimal_text_any(row, &["priceIncrement", "PriceIncrement"]);
        let quantity_increment = decimal_text_any(row, &["quantityIncrement", "QuantityIncrement"]);
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            base_asset,
            quote_asset,
            price_precision: price_increment.as_deref().and_then(decimal_precision),
            quantity_precision: quantity_increment.as_deref().and_then(decimal_precision),
            price_increment,
            quantity_increment,
            min_price: None,
            max_price: None,
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            max_notional: None,
            supports_market_orders: false,
            supports_limit_orders: true,
            supports_post_only: false,
            supports_reduce_only: false,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_order_book_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    depth: Option<u32>,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "NDAX GetL2Snapshot fixture missing array",
            value,
        )
    })?;
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for row in rows {
        let side = l2_side(row)
            .ok_or_else(|| parse_error(exchange_id.clone(), "NDAX L2 row missing side", row))?;
        let level = OrderBookLevel::new(
            l2_price(row).ok_or_else(|| {
                parse_error(exchange_id.clone(), "NDAX L2 row missing price", row)
            })?,
            l2_quantity(row).ok_or_else(|| {
                parse_error(exchange_id.clone(), "NDAX L2 row missing quantity", row)
            })?,
        )
        .map_err(validation_error)?;
        match side {
            L2Side::Bid => bids.push(level),
            L2Side::Ask => asks.push(level),
        }
    }
    if let Some(depth) = depth.map(|value| value as usize) {
        bids.truncate(depth);
        asks.truncate(depth);
    }
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "NDAX order book requires canonical_symbol".to_string(),
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
    Ok(snapshot)
}

pub fn parse_order_book_shape(value: &Value) -> ExchangeApiResult<(usize, usize)> {
    let rows = value
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "NDAX L2 fixture missing array".to_string(),
        })?;
    let mut bids = 0;
    let mut asks = 0;
    for row in rows {
        match l2_side(row) {
            Some(L2Side::Bid) => bids += 1,
            Some(L2Side::Ask) => asks += 1,
            None => {}
        }
    }
    Ok((bids, asks))
}

pub fn ndax_instrument_id(symbol: &str) -> Option<i64> {
    match ndax_symbol(symbol).as_str() {
        "BTCCAD" => Some(1),
        "ETHCAD" => Some(2),
        _ => None,
    }
}

pub fn parse_query_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    if let Some(order) = first_order(value) {
        return Ok(Some(parse_order_state(
            exchange_id,
            Some(symbol_hint),
            order,
        )?));
    }
    match value {
        Value::Object(object) if !object.is_empty() => Ok(Some(parse_order_state(
            exchange_id,
            Some(symbol_hint),
            value,
        )?)),
        _ => Ok(None),
    }
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = first_order(value).unwrap_or(value);
    let symbol = ndax_symbol_scope(exchange_id, symbol_hint, order)?;
    let quantity = text_any(
        order,
        &[
            "quantity",
            "Quantity",
            "origQuantity",
            "OrigQuantity",
            "orderQty",
            "OrderQty",
        ],
    )
    .unwrap_or_else(|| "0".to_string());
    let filled_quantity = text_any(
        order,
        &[
            "quantityExecuted",
            "QuantityExecuted",
            "filledQuantity",
            "FilledQuantity",
            "executedQuantity",
            "executedQty",
            "cumQty",
        ],
    )
    .unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: text_any(
            order,
            &[
                "clientOrderId",
                "ClientOrderId",
                "client_order_id",
                "externalId",
            ],
        ),
        exchange_order_id: text_any(order, &["orderId", "OrderId", "id", "Id"]),
        side: parse_side(order),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(order),
        time_in_force: parse_time_in_force(order),
        status: parse_order_status(order, &quantity, &filled_quantity),
        quantity,
        price: text_any(
            order,
            &[
                "limitPrice",
                "LimitPrice",
                "price",
                "Price",
                "avgPrice",
                "AvgPrice",
            ],
        )
        .filter(|price| !is_zero_decimal(price)),
        filled_quantity: filled_quantity.clone(),
        average_fill_price: text_any(
            order,
            &[
                "avgPrice",
                "AvgPrice",
                "averagePrice",
                "AveragePrice",
                "averageFillPrice",
            ],
        )
        .filter(|price| !is_zero_decimal(price)),
        reduce_only: false,
        post_only: false,
        created_at: timestamp_any(
            order,
            &[
                "receiveTime",
                "ReceiveTime",
                "createdAt",
                "CreatedAt",
                "time",
                "Time",
            ],
        ),
        updated_at: timestamp_any(
            order,
            &[
                "lastUpdatedTime",
                "LastUpdatedTime",
                "updatedAt",
                "UpdatedAt",
                "receiveTime",
                "ReceiveTime",
            ],
        )
        .unwrap_or_else(Utc::now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let mut orders = Vec::new();
    for order in order_rows(value)? {
        let parsed = parse_order_state(exchange_id, symbol_hint, order)?;
        if symbol_hint.is_none()
            || symbol_hint.is_some_and(|symbol| same_symbol(symbol, &parsed.exchange_symbol.symbol))
        {
            orders.push(parsed);
        }
    }
    Ok(orders)
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let mut fills = Vec::new();
    for row in fill_rows(value)? {
        let symbol = ndax_symbol_scope(exchange_id, Some(symbol_hint), row)?;
        let canonical_symbol =
            symbol
                .canonical_symbol
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "NDAX fill requires canonical_symbol".to_string(),
                })?;
        let price = decimal_any(row, &["price", "Price"])?
            .ok_or_else(|| parse_error(exchange_id.clone(), "NDAX fill missing price", row))?;
        let quantity = decimal_any(row, &["quantity", "Quantity", "qty", "Qty"])?
            .ok_or_else(|| parse_error(exchange_id.clone(), "NDAX fill missing quantity", row))?;
        fills.push(Fill {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol,
            exchange_symbol: Some(symbol.exchange_symbol),
            order_id: text_any(row, &["orderId", "OrderId"]),
            client_order_id: text_any(row, &["clientOrderId", "ClientOrderId"]),
            fill_id: text_any(row, &["tradeId", "TradeId", "id", "Id"]),
            side: parse_side(row),
            position_side: PositionSide::None,
            status: FillStatus::Confirmed,
            liquidity_role: parse_liquidity_role(row),
            price,
            quantity,
            quote_quantity: decimal_any(row, &["value", "Value", "notional", "Notional"])?
                .or_else(|| Some(price * quantity)),
            fee_asset: text_any(row, &["feeCurrency", "FeeCurrency", "feeAsset", "FeeAsset"]),
            fee_amount: decimal_any(row, &["fee", "Fee", "feeAmount", "FeeAmount"])?,
            fee_rate: None,
            realized_pnl: None,
            filled_at: timestamp_any(
                row,
                &[
                    "tradeTime",
                    "TradeTime",
                    "createdAt",
                    "CreatedAt",
                    "time",
                    "Time",
                ],
            )
            .unwrap_or_else(Utc::now),
            received_at: Utc::now(),
        });
    }
    Ok(fills)
}

#[derive(Clone, Copy)]
enum L2Side {
    Bid,
    Ask,
}

fn l2_side(value: &Value) -> Option<L2Side> {
    if let Some(side) = value_as_i64_any(value, &["side", "Side"]).or_else(|| {
        value
            .as_array()
            .and_then(|items| items.get(9))
            .and_then(value_as_i64)
    }) {
        return match side {
            0 => Some(L2Side::Bid),
            1 => Some(L2Side::Ask),
            _ => None,
        };
    }
    match text_any(value, &["side", "Side"])?
        .to_ascii_lowercase()
        .as_str()
    {
        "buy" | "bid" | "bids" => Some(L2Side::Bid),
        "sell" | "ask" | "asks" => Some(L2Side::Ask),
        _ => None,
    }
}

fn l2_price(value: &Value) -> Option<f64> {
    value_as_f64_any(value, &["price", "Price"]).or_else(|| {
        value
            .as_array()
            .and_then(|items| items.get(4))
            .and_then(value_as_f64)
    })
}

fn l2_quantity(value: &Value) -> Option<f64> {
    value_as_f64_any(value, &["quantity", "Quantity"]).or_else(|| {
        value
            .as_array()
            .and_then(|items| items.get(8))
            .and_then(value_as_f64)
    })
}

fn order_rows(value: &Value) -> ExchangeApiResult<&[Value]> {
    value
        .as_array()
        .or_else(|| value.get("orders").and_then(Value::as_array))
        .or_else(|| value.get("Orders").and_then(Value::as_array))
        .or_else(|| value.get("data").and_then(Value::as_array))
        .or_else(|| value.get("Data").and_then(Value::as_array))
        .or_else(|| value.get("result").and_then(Value::as_array))
        .or_else(|| value.get("Result").and_then(Value::as_array))
        .map(Vec::as_slice)
        .ok_or_else(|| {
            parse_error(
                ExchangeId::unchecked("ndax"),
                "NDAX orders missing array",
                value,
            )
        })
}

fn fill_rows(value: &Value) -> ExchangeApiResult<&[Value]> {
    value
        .as_array()
        .or_else(|| value.get("fills").and_then(Value::as_array))
        .or_else(|| value.get("Fills").and_then(Value::as_array))
        .or_else(|| value.get("trades").and_then(Value::as_array))
        .or_else(|| value.get("Trades").and_then(Value::as_array))
        .or_else(|| value.get("data").and_then(Value::as_array))
        .or_else(|| value.get("Data").and_then(Value::as_array))
        .or_else(|| value.get("result").and_then(Value::as_array))
        .or_else(|| value.get("Result").and_then(Value::as_array))
        .map(Vec::as_slice)
        .ok_or_else(|| {
            parse_error(
                ExchangeId::unchecked("ndax"),
                "NDAX fills missing array",
                value,
            )
        })
}

fn first_order(value: &Value) -> Option<&Value> {
    value
        .get("order")
        .or_else(|| value.get("Order"))
        .or_else(|| value.get("data").filter(|value| value.is_object()))
        .or_else(|| value.get("Data").filter(|value| value.is_object()))
        .or_else(|| value.get("result").filter(|value| value.is_object()))
        .or_else(|| value.get("Result").filter(|value| value.is_object()))
        .or_else(|| value.as_array().and_then(|orders| orders.first()))
        .or_else(|| {
            value
                .get("orders")
                .or_else(|| value.get("Orders"))
                .or_else(|| value.get("data"))
                .or_else(|| value.get("Data"))
                .or_else(|| value.get("result"))
                .or_else(|| value.get("Result"))
                .and_then(Value::as_array)
                .and_then(|orders| orders.first())
        })
}

fn ndax_symbol_scope(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    row: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let native_symbol = text_any(row, &["symbol", "Symbol", "instrument", "Instrument"])
        .or_else(|| {
            value_as_i64_any(row, &["instrumentId", "InstrumentId"]).and_then(ndax_symbol_for_id)
        })
        .ok_or_else(|| parse_error(exchange_id.clone(), "NDAX row missing symbol", row))?;
    let normalized = ndax_symbol(&native_symbol);
    let (base, quote) = ndax_canonical_pair(&normalized)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, normalized)
            .map_err(validation_error)?,
    })
}

fn ndax_symbol_for_id(instrument_id: i64) -> Option<String> {
    match instrument_id {
        1 => Some("BTCCAD".to_string()),
        2 => Some("ETHCAD".to_string()),
        _ => None,
    }
}

fn same_symbol(symbol: &SymbolScope, native_symbol: &str) -> bool {
    ndax_symbol(&symbol.exchange_symbol.symbol) == ndax_symbol(native_symbol)
}

fn parse_side(value: &Value) -> OrderSide {
    match value_as_i64_any(value, &["side", "Side"]) {
        Some(1) => return OrderSide::Sell,
        Some(0) => return OrderSide::Buy,
        _ => {}
    }
    match text_any(value, &["side", "Side"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "sell" | "ask" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(value: &Value) -> OrderType {
    match value_as_i64_any(value, &["orderType", "OrderType"]) {
        Some(1) => return OrderType::Market,
        Some(2) => return OrderType::Limit,
        _ => {}
    }
    match text_any(value, &["orderType", "OrderType", "type", "Type"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "market" => OrderType::Market,
        "post_only" | "post-only" => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: &Value) -> Option<TimeInForce> {
    match value_as_i64_any(value, &["timeInForce", "TimeInForce"]) {
        Some(1) => return Some(TimeInForce::GTC),
        Some(3) => return Some(TimeInForce::IOC),
        Some(4) => return Some(TimeInForce::FOK),
        _ => {}
    }
    match text_any(value, &["timeInForce", "TimeInForce"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "gtc" | "goodtillcancelled" => Some(TimeInForce::GTC),
        "ioc" | "immediateorcancel" => Some(TimeInForce::IOC),
        "fok" | "fillorkill" => Some(TimeInForce::FOK),
        _ => Some(TimeInForce::GTC),
    }
}

fn parse_order_status(value: &Value, quantity: &str, filled_quantity: &str) -> OrderStatus {
    if let Some(status) = value_as_i64_any(value, &["orderState", "OrderState", "status", "Status"])
    {
        return match status {
            0 => OrderStatus::New,
            1 => OrderStatus::Open,
            2 => OrderStatus::Filled,
            3 => OrderStatus::Cancelled,
            4 => OrderStatus::Expired,
            5 => OrderStatus::Rejected,
            _ => OrderStatus::Unknown,
        };
    }
    match text_any(
        value,
        &[
            "orderState",
            "OrderState",
            "status",
            "Status",
            "state",
            "State",
        ],
    )
    .unwrap_or_default()
    .to_ascii_lowercase()
    .as_str()
    {
        "new" | "accepted" => OrderStatus::New,
        "working" | "open" | "active" => OrderStatus::Open,
        "partiallyfilled" | "partially_filled" | "partial" => OrderStatus::PartiallyFilled,
        "filled" | "complete" | "completed" => OrderStatus::Filled,
        "pendingcancel" | "pending_cancel" => OrderStatus::PendingCancel,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => {
            let qty = quantity.parse::<f64>().unwrap_or(0.0);
            let filled = filled_quantity.parse::<f64>().unwrap_or(0.0);
            if qty > 0.0 && filled >= qty {
                OrderStatus::Filled
            } else if filled > 0.0 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Unknown
            }
        }
    }
}

fn parse_liquidity_role(value: &Value) -> LiquidityRole {
    match text_any(
        value,
        &["liquidity", "Liquidity", "liquidityRole", "LiquidityRole"],
    )
    .unwrap_or_default()
    .to_ascii_lowercase()
    .as_str()
    {
        "maker" | "m" => LiquidityRole::Maker,
        "taker" | "t" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn decimal_any(value: &Value, keys: &[&str]) -> ExchangeApiResult<Option<f64>> {
    keys.iter()
        .find_map(|key| value.get(*key))
        .map(|value| {
            value_as_f64(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("NDAX decimal field {} is not numeric", keys.join("/")),
            })
        })
        .transpose()
}

fn timestamp_any(value: &Value, keys: &[&str]) -> Option<DateTime<Utc>> {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(parse_timestamp)
}

fn parse_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(text) = value.as_str() {
        if let Ok(timestamp) = DateTime::parse_from_rfc3339(text) {
            return Some(timestamp.with_timezone(&Utc));
        }
        if let Ok(number) = text.parse::<i64>() {
            return timestamp_from_i64(number);
        }
    }
    value_as_i64(value).and_then(timestamp_from_i64)
}

fn timestamp_from_i64(value: i64) -> Option<DateTime<Utc>> {
    if value > 10_000_000_000 {
        DateTime::from_timestamp_millis(value)
    } else {
        DateTime::from_timestamp(value, 0)
    }
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|number| number == 0.0)
}

fn required_text(
    exchange_id: &ExchangeId,
    value: &Value,
    keys: &[&str],
) -> ExchangeApiResult<String> {
    text_any(value, keys).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("missing field {}", keys.join("/")),
            value,
        )
    })
}

fn text_any(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| text(value.get(*key)))
        .filter(|value| !value.trim().is_empty())
}

fn decimal_text_any(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(|value| text(Some(value)).or_else(|| value_as_f64(value).map(trim_float)))
}

fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.trim().to_string()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_i64_any(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(value_as_i64)
}

fn value_as_f64_any(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(value_as_f64)
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn trim_float(value: f64) -> String {
    let formatted = format!("{value:.12}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn decimal_precision(value: &str) -> Option<u32> {
    value
        .split_once('.')
        .map(|(_, fraction)| fraction.trim_end_matches('0').len() as u32)
        .filter(|precision| *precision > 0)
        .or(Some(0))
}

fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
