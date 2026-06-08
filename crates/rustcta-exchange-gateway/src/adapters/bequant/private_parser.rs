use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill, FillStatus, LiquidityRole,
    MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

use super::parser::{
    decimal_value_to_f64, first_timestamp, normalize_bequant_symbol, parse_error, required_str,
    string_or_number, validation_error, value_as_string,
};

pub fn parse_account_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "spot balance response missing array",
            value,
        )
    })?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for row in rows {
        let asset = required_str(exchange_id, row, "currency")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_value_to_f64(
            row.get("available")
                .or_else(|| row.get("available_balance"))
                .or_else(|| row.get("cash")),
        )?
        .unwrap_or(0.0);
        let reserved =
            decimal_value_to_f64(row.get("reserved").or_else(|| row.get("reserved_balance")))?
                .unwrap_or(0.0);
        let total = decimal_value_to_f64(row.get("total"))?.unwrap_or(available + reserved);
        if total > 0.0 || !requested.is_empty() {
            balances.push(
                AssetBalance::new(asset, total, available, reserved).map_err(validation_error)?,
            );
        }
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

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_scope(exchange_id, symbol_hint, value)?;
    let quantity = string_or_number(value.get("quantity")).unwrap_or_else(|| "0".to_string());
    let cum_quantity = string_or_number(
        value
            .get("cum_quantity")
            .or_else(|| value.get("filled_quantity")),
    )
    .unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value_as_string(
            value
                .get("client_order_id")
                .or_else(|| value.get("clientOrderId")),
        ),
        exchange_order_id: value_as_string(value.get("id").or_else(|| value.get("order_id"))),
        side: parse_side(value.get("side").and_then(Value::as_str)),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(value.get("type").and_then(Value::as_str)),
        time_in_force: parse_time_in_force(value.get("time_in_force").and_then(Value::as_str)),
        status: parse_status(value.get("status").and_then(Value::as_str)),
        quantity,
        price: string_or_number(value.get("price")),
        filled_quantity: cum_quantity.clone(),
        average_fill_price: average_price(value, &cum_quantity),
        reduce_only: false,
        post_only: value
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: first_timestamp(value, &["created_at", "createdAt", "timestamp"]),
        updated_at: first_timestamp(value, &["updated_at", "updatedAt", "timestamp"])
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    value
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|order| parse_order_state(exchange_id, symbol_hint, order))
        .collect()
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let rows = if let Some(array) = value.as_array() {
        array.clone()
    } else {
        vec![value.clone()]
    };
    let mut fees = Vec::new();
    for row in rows {
        let symbol = symbol_scope_from_fee(exchange_id, requested_symbols, &row)?;
        fees.push(FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: string_or_number(row.get("make_rate").or_else(|| row.get("maker_rate")))
                .unwrap_or_else(|| "0".to_string()),
            taker_rate: string_or_number(row.get("take_rate").or_else(|| row.get("taker_rate")))
                .unwrap_or_else(|| "0".to_string()),
            source: Some("bequant.spot_fee".to_string()),
            updated_at: Utc::now(),
        });
    }
    Ok(fees)
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
                message: "bequant fills require canonical_symbol".to_string(),
            })?;
    let trades = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "spot trade history response missing array",
            value,
        )
    })?;
    trades
        .iter()
        .map(|trade| {
            let price = decimal_value_to_f64(trade.get("price"))?.unwrap_or(0.0);
            let quantity = decimal_value_to_f64(trade.get("quantity"))?.unwrap_or(0.0);
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(trade.get("order_id")),
                client_order_id: value_as_string(
                    trade
                        .get("client_order_id")
                        .or_else(|| trade.get("clientOrderId")),
                ),
                fill_id: value_as_string(trade.get("id").or_else(|| trade.get("trade_id"))),
                side: parse_side(trade.get("side").and_then(Value::as_str)),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity(
                    trade
                        .get("liquidity")
                        .or_else(|| trade.get("liquidity_indicator"))
                        .and_then(Value::as_str),
                ),
                price,
                quantity,
                quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
                fee_asset: value_as_string(trade.get("fee_currency"))
                    .or_else(|| Some(canonical_symbol.quote_asset().to_string())),
                fee_amount: decimal_value_to_f64(trade.get("fee"))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp(trade, &["timestamp", "created_at"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn symbol_scope(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let raw_symbol = required_str(exchange_id, value, "symbol")?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: canonical_from_symbol(raw_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            normalize_bequant_symbol(raw_symbol)?,
        )
        .map_err(validation_error)?,
    })
}

fn symbol_scope_from_fee(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = value.get("symbol").and_then(Value::as_str).and_then(|raw| {
        requested_symbols
            .iter()
            .find(|symbol| symbol.exchange_symbol.symbol.eq_ignore_ascii_case(raw))
    }) {
        return Ok(symbol.clone());
    }
    if let Some(symbol) = requested_symbols.first() {
        return Ok(symbol.clone());
    }
    let raw_symbol = value
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or("BTCUSDT");
    symbol_scope(
        exchange_id,
        None,
        &serde_json::json!({ "symbol": raw_symbol }),
    )
}

fn canonical_from_symbol(symbol: &str) -> Option<rustcta_types::CanonicalSymbol> {
    let normalized = normalize_bequant_symbol(symbol).ok()?;
    ["USDT", "USD", "BTC", "ETH", "EUR"]
        .iter()
        .find_map(|quote| {
            normalized
                .strip_suffix(quote)
                .filter(|base| !base.is_empty())
                .and_then(|base| rustcta_types::CanonicalSymbol::new(base, quote).ok())
        })
}

fn parse_side(value: Option<&str>) -> OrderSide {
    if value.unwrap_or_default().eq_ignore_ascii_case("sell") {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        "post_only" => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value?.to_ascii_uppercase().as_str() {
        "GTC" => Some(TimeInForce::GTC),
        "IOC" => Some(TimeInForce::IOC),
        "FOK" => Some(TimeInForce::FOK),
        "GTX" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "new" => OrderStatus::New,
        "partiallyfilled" | "partially_filled" | "part_filled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "expired" => OrderStatus::Expired,
        "suspended" | "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "M" | "MAKER" => LiquidityRole::Maker,
        "T" | "TAKER" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn average_price(value: &Value, filled_quantity: &str) -> Option<String> {
    string_or_number(
        value
            .get("average_price")
            .or_else(|| value.get("avg_price"))
            .or_else(|| value.get("trade_price")),
    )
    .or_else(|| {
        let cum_quote = decimal_value_to_f64(
            value
                .get("cum_quote_quantity")
                .or_else(|| value.get("cum_quote_amount")),
        )
        .ok()
        .flatten()?;
        let filled = filled_quantity.parse::<f64>().ok()?;
        (filled > 0.0).then(|| (cum_quote / filled).to_string())
    })
}
