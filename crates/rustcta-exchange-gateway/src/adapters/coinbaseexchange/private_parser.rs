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
    decimal_value_to_f64, first_timestamp_millis, normalize_coinbaseexchange_symbol, parse_error,
    required_str, string_or_number, validation_error, value_as_string,
};

pub fn parse_account_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let accounts = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "accounts response missing array",
            value,
        )
    })?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for account in accounts {
        let asset = required_str(exchange_id, account, "currency")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let total = decimal_value_to_f64(account.get("balance"))?.unwrap_or(0.0);
        let available = decimal_value_to_f64(account.get("available"))?.unwrap_or(0.0);
        let locked =
            decimal_value_to_f64(account.get("hold"))?.unwrap_or((total - available).max(0.0));
        if total > 0.0 || !requested.is_empty() {
            balances.push(
                AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
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
    let size = string_or_number(value.get("size")).unwrap_or_else(|| "0".to_string());
    let filled = string_or_number(value.get("filled_size")).unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value_as_string(value.get("client_oid")),
        exchange_order_id: value_as_string(value.get("id")),
        side: parse_side(value.get("side").and_then(Value::as_str)),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(value.get("type").and_then(Value::as_str)),
        time_in_force: parse_time_in_force(value.get("time_in_force").and_then(Value::as_str)),
        status: parse_status(value),
        quantity: size,
        price: string_or_number(value.get("price")),
        filled_quantity: filled.clone(),
        average_fill_price: average_price(value, &filled),
        reduce_only: false,
        post_only: value
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: first_timestamp_millis(value, &["created_at"]),
        updated_at: first_timestamp_millis(value, &["done_at"]).unwrap_or_else(Utc::now),
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
) -> Vec<FeeRateSnapshot> {
    let symbol = requested_symbols
        .first()
        .cloned()
        .unwrap_or_else(|| SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: None,
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, "BTC-USD")
                .expect("static symbol"),
        });
    vec![FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        maker_rate: string_or_number(value.get("maker_fee_rate"))
            .unwrap_or_else(|| "0".to_string()),
        taker_rate: string_or_number(value.get("taker_fee_rate"))
            .unwrap_or_else(|| "0".to_string()),
        source: Some("coinbaseexchange.fees".to_string()),
        updated_at: Utc::now(),
    }]
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
                message: "coinbaseexchange fills require canonical_symbol".to_string(),
            })?;
    value
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|fill| {
            let price = decimal_value_to_f64(fill.get("price"))?.unwrap_or(0.0);
            let quantity = decimal_value_to_f64(fill.get("size"))?.unwrap_or(0.0);
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("order_id")),
                client_order_id: None,
                fill_id: value_as_string(fill.get("trade_id")),
                side: parse_side(fill.get("side").and_then(Value::as_str)),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity(fill.get("liquidity").and_then(Value::as_str)),
                price,
                quantity,
                quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
                fee_asset: symbol
                    .canonical_symbol
                    .as_ref()
                    .map(|canonical| canonical.quote_asset().to_string()),
                fee_amount: decimal_value_to_f64(fill.get("fee"))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp_millis(fill, &["created_at"]).unwrap_or_else(Utc::now),
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
    let product_id = required_str(exchange_id, value, "product_id")?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: None,
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            normalize_coinbaseexchange_symbol(product_id)?,
        )
        .map_err(validation_error)?,
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

fn parse_status(value: &Value) -> OrderStatus {
    match value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "open" | "pending" | "active" => OrderStatus::New,
        "done" => {
            if value
                .get("done_reason")
                .and_then(Value::as_str)
                .is_some_and(|reason| reason.eq_ignore_ascii_case("canceled"))
            {
                OrderStatus::Cancelled
            } else {
                OrderStatus::Filled
            }
        }
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity(value: Option<&str>) -> LiquidityRole {
    if value.unwrap_or_default().eq_ignore_ascii_case("M") {
        LiquidityRole::Maker
    } else {
        LiquidityRole::Taker
    }
}

fn average_price(value: &Value, filled: &str) -> Option<String> {
    let executed = decimal_value_to_f64(value.get("executed_value"))
        .ok()
        .flatten()?;
    let filled = filled.parse::<f64>().ok()?;
    (filled > 0.0).then(|| (executed / filled).to_string())
}
