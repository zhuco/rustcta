use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    normalize_cryptocom_symbol, parse_error, required_str, string_or_number, validation_error,
    value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let data = value.get("data").unwrap_or(value);
    let items = data.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "balance response is not an array",
            value,
        )
    })?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for item in items {
        if let Some(position_balances) = item.get("position_balances").and_then(Value::as_array) {
            for position in position_balances {
                push_balance(&mut balances, &requested, position)?;
            }
        } else {
            push_balance(&mut balances, &requested, item)?;
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

fn push_balance(
    balances: &mut Vec<AssetBalance>,
    requested: &[String],
    item: &Value,
) -> ExchangeApiResult<()> {
    let asset = item
        .get("instrument_name")
        .or_else(|| item.get("currency"))
        .or_else(|| item.get("ccy"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    if asset.is_empty() || (!requested.is_empty() && !requested.contains(&asset)) {
        return Ok(());
    }
    let total = decimal_value_to_f64(
        item.get("quantity")
            .or_else(|| item.get("total_cash_balance"))
            .or_else(|| item.get("balance")),
    )?
    .unwrap_or(0.0);
    let locked = decimal_value_to_f64(
        item.get("reserved_qty")
            .or_else(|| item.get("reserved_quantity"))
            .or_else(|| item.get("locked")),
    )?
    .unwrap_or(0.0);
    let available = decimal_value_to_f64(
        item.get("max_withdrawal_balance")
            .or_else(|| item.get("available"))
            .or_else(|| item.get("total_available_balance")),
    )?
    .unwrap_or_else(|| (total - locked).max(0.0));
    if total > 0.0 || available > 0.0 || locked > 0.0 || !requested.is_empty() {
        balances
            .push(AssetBalance::new(asset, total, available, locked).map_err(validation_error)?);
    }
    Ok(())
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbols: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let data = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "positions response is not an array",
                value,
            )
        })?;
    let requested = symbols
        .iter()
        .map(|symbol| symbol.symbol.as_str())
        .collect::<Vec<_>>();
    let mut positions = Vec::new();
    for row in data {
        let instrument = row
            .get("instrument_name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if instrument.is_empty() {
            continue;
        }
        if !requested.is_empty() && !requested.contains(&instrument) {
            continue;
        }
        let signed_quantity = decimal_value_to_f64(row.get("quantity"))?.unwrap_or(0.0);
        let quantity = signed_quantity.abs();
        if quantity == 0.0 && requested.is_empty() {
            continue;
        }
        let market_type = infer_market_type_from_instrument(instrument);
        let canonical_symbol = canonical_from_instrument(instrument)?;
        let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), market_type, instrument)
            .map_err(validation_error)?;
        positions.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type,
            canonical_symbol,
            exchange_symbol: Some(exchange_symbol),
            side: if signed_quantity < 0.0 {
                PositionSide::Short
            } else if signed_quantity > 0.0 {
                PositionSide::Long
            } else {
                PositionSide::Net
            },
            quantity,
            entry_price: None,
            mark_price: None,
            liquidation_price: None,
            unrealized_pnl: decimal_value_to_f64(
                row.get("open_position_pnl")
                    .or_else(|| row.get("unrealized_pnl")),
            )?,
            leverage: None,
            observed_at: row
                .get("update_timestamp_ms")
                .and_then(value_as_i64)
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .unwrap_or_else(Utc::now),
        });
    }
    Ok(positions)
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let value = value.get("order_info").unwrap_or(value);
    let instrument = value
        .get("instrument_name")
        .and_then(Value::as_str)
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.as_str()))
        .unwrap_or("UNKNOWN");
    let market_type = symbol_hint
        .map(|symbol| symbol.market_type)
        .unwrap_or_else(|| infer_market_type_from_instrument(instrument));
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(exchange_id.clone(), market_type, instrument)
            .map_err(validation_error)?
    };
    let order_type_text = value
        .get("order_type")
        .or_else(|| value.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("LIMIT");
    let tif = value.get("time_in_force").and_then(Value::as_str);
    let exec_inst = value.get("exec_inst");
    let filled_quantity = string_or_number(
        value
            .get("cumulative_quantity")
            .or_else(|| value.get("filled_quantity")),
    )
    .unwrap_or_else(|| "0".to_string());
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone()),
        exchange_symbol,
        client_order_id: value_as_string(value.get("client_oid")),
        exchange_order_id: value_as_string(value.get("order_id")),
        side: value
            .get("side")
            .and_then(Value::as_str)
            .map(parse_side)
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(order_type_text, tif, exec_inst),
        time_in_force: parse_time_in_force(tif),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(|status| map_order_status(status, &filled_quantity))
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(value.get("quantity")).unwrap_or_else(|| "0".to_string()),
        price: non_zero_string(
            string_or_number(value.get("limit_price").or_else(|| value.get("price")))
                .unwrap_or_else(|| "0".to_string()),
        ),
        filled_quantity,
        average_fill_price: non_zero_string(
            string_or_number(value.get("avg_price")).unwrap_or_else(|| "0".to_string()),
        ),
        reduce_only: exec_inst_contains(exec_inst, "REDUCE_ONLY"),
        post_only: exec_inst_contains(exec_inst, "POST_ONLY"),
        created_at: first_timestamp_millis(value, &["create_time", "created_at"]),
        updated_at: first_timestamp_millis(value, &["update_time", "updated_at"]).unwrap_or(now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "open orders response is not an array",
                value,
            )
        })?;
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, symbol_hint, order))
        .collect()
}

pub fn parse_fee_snapshots(
    symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let data = value.get("data").unwrap_or(value);
    let maker = parse_fee_rate(
        data.get("effective_spot_maker_rate_bps"),
        data.get("maker_fee_rate").or_else(|| data.get("maker")),
        "maker",
    )?;
    let taker = parse_fee_rate(
        data.get("effective_spot_taker_rate_bps"),
        data.get("taker_fee_rate").or_else(|| data.get("taker")),
        "taker",
    )?;
    Ok(symbols
        .iter()
        .cloned()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: maker.clone(),
            taker_rate: taker.clone(),
            source: Some("cryptocom.private_get_fee_rate".to_string()),
            updated_at: Utc::now(),
        })
        .collect())
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
                message: "cryptocom get_recent_fills requires canonical_symbol".to_string(),
            })?;
    let fills = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id.clone(), "fills response is not an array", value))?;
    fills
        .iter()
        .map(|fill| {
            let side = parse_side(required_str(exchange_id, fill, "side")?)?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("order_id")),
                client_order_id: value_as_string(fill.get("client_oid")),
                fill_id: value_as_string(fill.get("trade_id").or_else(|| fill.get("exec_id"))),
                side,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity_role(fill, side),
                price: decimal_value_to_f64(
                    fill.get("traded_price").or_else(|| fill.get("price")),
                )?
                .unwrap_or(0.0),
                quantity: decimal_value_to_f64(
                    fill.get("traded_quantity").or_else(|| fill.get("quantity")),
                )?
                .unwrap_or(0.0),
                quote_quantity: decimal_value_to_f64(fill.get("traded_value"))?,
                fee_asset: value_as_string(
                    fill.get("fee_instrument_name")
                        .or_else(|| fill.get("fee_currency")),
                ),
                fee_amount: decimal_value_to_f64(fill.get("fees").or_else(|| fill.get("fee")))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp_millis(fill, &["create_time", "traded_at"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

pub fn cancelled_order_state(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: OrderStatus::Cancelled,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    }
}

pub fn parse_side(side: &str) -> ExchangeApiResult<OrderSide> {
    match side.trim().to_ascii_uppercase().as_str() {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported cryptocom order side {other}"),
        }),
    }
}

fn parse_order_type(order_type: &str, tif: Option<&str>, exec_inst: Option<&Value>) -> OrderType {
    if exec_inst_contains(exec_inst, "POST_ONLY") {
        return OrderType::PostOnly;
    }
    match (
        order_type.trim().to_ascii_uppercase().as_str(),
        tif.map(|text| text.trim().to_ascii_uppercase()),
    ) {
        ("MARKET", _) => OrderType::Market,
        (_, Some(tif)) if tif == "IMMEDIATE_OR_CANCEL" => OrderType::IOC,
        (_, Some(tif)) if tif == "FILL_OR_KILL" => OrderType::FOK,
        ("STOP_LOSS", _) => OrderType::StopMarket,
        ("STOP_LIMIT" | "TAKE_PROFIT_LIMIT", _) => OrderType::StopLimit,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(tif: Option<&str>) -> Option<TimeInForce> {
    match tif?.trim().to_ascii_uppercase().as_str() {
        "GOOD_TILL_CANCEL" => Some(TimeInForce::GTC),
        "IMMEDIATE_OR_CANCEL" => Some(TimeInForce::IOC),
        "FILL_OR_KILL" => Some(TimeInForce::FOK),
        _ => Some(TimeInForce::GTC),
    }
}

fn map_order_status(status: &str, filled_quantity: &str) -> OrderStatus {
    match status.trim().to_ascii_uppercase().as_str() {
        "ACTIVE" | "PENDING" | "OPEN" if decimal_is_positive(filled_quantity) => {
            OrderStatus::PartiallyFilled
        }
        "ACTIVE" | "PENDING" | "OPEN" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "EXPIRED" => OrderStatus::Expired,
        "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity_role(fill: &Value, side: OrderSide) -> LiquidityRole {
    let liquidity_text = fill
        .get("liquidity_indicator")
        .or_else(|| fill.get("taker_side"))
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase);
    match liquidity_text.as_deref() {
        Some(value) if value == "MAKER" || value == "M" => LiquidityRole::Maker,
        Some(value) if value == "TAKER" || value == "T" => LiquidityRole::Taker,
        _ => match fill
            .get("taker_side")
            .and_then(Value::as_str)
            .map(str::to_ascii_uppercase)
        {
            Some(value)
                if matches!(
                    (value.as_str(), side),
                    ("BUY", OrderSide::Buy) | ("SELL", OrderSide::Sell)
                ) =>
            {
                LiquidityRole::Taker
            }
            Some(value) if value == "BUY" || value == "SELL" => LiquidityRole::Maker,
            _ => LiquidityRole::Unknown,
        },
    }
}

fn decimal_is_positive(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|value| value > 0.0)
}

fn infer_market_type_from_instrument(instrument: &str) -> MarketType {
    let instrument = instrument.to_ascii_uppercase();
    if instrument.contains("-PERP") || instrument.contains("PERP") {
        MarketType::Perpetual
    } else {
        MarketType::Spot
    }
}

fn canonical_from_instrument(instrument: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let normalized = instrument.trim().to_ascii_uppercase().replace('/', "_");
    let without_suffix = normalized
        .strip_suffix("-PERP")
        .or_else(|| normalized.strip_suffix("_PERP"))
        .unwrap_or(&normalized);
    if let Some((base, quote)) = without_suffix.split_once('_') {
        return CanonicalSymbol::new(base, quote).map_err(validation_error);
    }
    for quote in ["USDT", "USDC", "USD", "BTC", "ETH", "CRO"] {
        if let Some(base) = without_suffix.strip_suffix(quote) {
            if !base.is_empty() {
                return CanonicalSymbol::new(base, quote).map_err(validation_error);
            }
        }
    }
    Err(ExchangeApiError::InvalidRequest {
        message: format!("cannot infer cryptocom canonical symbol from {instrument}"),
    })
}

fn exec_inst_contains(value: Option<&Value>, needle: &str) -> bool {
    match value {
        Some(Value::String(text)) => text.eq_ignore_ascii_case(needle),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .any(|item| item.eq_ignore_ascii_case(needle)),
        _ => false,
    }
}

fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
    })
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn non_zero_string(value: String) -> Option<String> {
    if value.trim().is_empty() || value.trim() == "0" || value.trim() == "0.0" {
        None
    } else {
        Some(value)
    }
}

fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let text = string_or_number(Some(value)).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: format!("invalid decimal value {value}"),
    })?;
    text.parse::<f64>()
        .map(Some)
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid decimal value {text}: {error}"),
        })
}

fn parse_fee_rate(
    bps_value: Option<&Value>,
    decimal_value: Option<&Value>,
    side: &str,
) -> ExchangeApiResult<String> {
    if let Some(value) = string_or_number(bps_value) {
        return bps_to_rate(value);
    }
    string_or_number(decimal_value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: format!("cryptocom fee response missing {side} rate"),
    })
}

fn bps_to_rate(value: String) -> ExchangeApiResult<String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid cryptocom bps fee rate {value}: {error}"),
        })?;
    Ok(format!("{:.8}", parsed / 10_000.0)
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string())
}

#[allow(dead_code)]
fn _symbol_for_params(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    normalize_cryptocom_symbol(symbol)
}
