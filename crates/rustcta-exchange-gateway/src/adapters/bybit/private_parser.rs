use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion,
};
use serde_json::Value;

use super::parser::{
    decimal_value_to_f64, normalize_bybit_symbol, parse_error, required_str, string_or_number,
    validation_error, value_as_i64,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let coins = value
        .get("result")
        .and_then(|result| result.get("list"))
        .and_then(Value::as_array)
        .and_then(|list| list.first())
        .and_then(|account| account.get("coin"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Bybit wallet response missing coin rows",
                value,
            )
        })?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for coin in coins {
        let asset = required_str(exchange_id, coin, "coin")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let free = decimal_value_to_f64(coin.get("free"))?;
        let locked_field = decimal_value_to_f64(coin.get("locked"))?;
        let total = first_decimal_field(coin, &["walletBalance", "equity"])?
            .or_else(|| free.zip(locked_field).map(|(free, locked)| free + locked))
            .or(free)
            .unwrap_or(0.0);
        let available =
            first_decimal_field(coin, &["availableToWithdraw", "availableBalance", "free"])?
                .unwrap_or(total);
        let locked = locked_field.unwrap_or_else(|| (total - available).max(0.0));
        balances
            .push(AssetBalance::new(asset, total, available, locked).map_err(validation_error)?);
    }
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        balances,
        observed_at: Utc::now(),
    }])
}

fn first_decimal_field(value: &Value, fields: &[&str]) -> ExchangeApiResult<Option<f64>> {
    for field in fields {
        if let Some(parsed) = decimal_value_to_f64(value.get(*field))? {
            return Ok(Some(parsed));
        }
    }
    Ok(None)
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let rows = result_list(
        exchange_id,
        value,
        "Bybit positions response missing result.list",
    )?;
    let requested = requested
        .iter()
        .map(|symbol| normalize_bybit_symbol(&symbol.symbol))
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    let mut positions = Vec::new();
    for row in rows {
        let symbol_text = required_str(exchange_id, row, "symbol")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&symbol_text) {
            continue;
        }
        let quantity = decimal_value_to_f64(row.get("size"))?.unwrap_or(0.0);
        if quantity == 0.0 {
            continue;
        }
        let (base, quote) = split_symbol(&symbol_text);
        let market_type = MarketType::Perpetual;
        positions.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type,
            canonical_symbol: CanonicalSymbol::new(base, quote).map_err(validation_error)?,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), market_type, symbol_text)
                    .map_err(validation_error)?,
            ),
            side: parse_position_side(row.get("side").and_then(Value::as_str)),
            quantity,
            entry_price: decimal_value_to_f64(row.get("avgPrice"))?,
            mark_price: decimal_value_to_f64(row.get("markPrice"))?,
            liquidation_price: decimal_value_to_f64(row.get("liqPrice"))?,
            unrealized_pnl: decimal_value_to_f64(row.get("unrealisedPnl"))?,
            leverage: decimal_value_to_f64(row.get("leverage"))?,
            observed_at: Utc::now(),
        });
    }
    Ok(positions)
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let row = value.get("result").unwrap_or(value);
    let symbol_text = required_str(exchange_id, row, "symbol")
        .or_else(|_| {
            symbol_hint
                .map(|symbol| symbol.exchange_symbol.symbol.as_str())
                .ok_or_else(|| parse_error(exchange_id.clone(), "Bybit order missing symbol", row))
        })?
        .to_ascii_uppercase();
    let market_type = symbol_hint
        .map(|symbol| symbol.market_type)
        .unwrap_or(MarketType::Perpetual);
    let exchange_symbol = symbol_hint
        .map(|symbol| symbol.exchange_symbol.clone())
        .unwrap_or(
            ExchangeSymbol::new(exchange_id.clone(), market_type, symbol_text)
                .map_err(validation_error)?,
        );
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone()),
        exchange_symbol,
        client_order_id: string_or_number(row.get("orderLinkId")),
        exchange_order_id: string_or_number(row.get("orderId")),
        side: parse_side(row.get("side").and_then(Value::as_str).unwrap_or("Buy"))?,
        position_side: Some(parse_position_side(
            row.get("positionSide").and_then(Value::as_str),
        )),
        order_type: parse_order_type(row.get("orderType").and_then(Value::as_str)),
        time_in_force: None,
        status: row
            .get("orderStatus")
            .and_then(Value::as_str)
            .map(parse_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(row.get("qty")).unwrap_or_else(|| "0".to_string()),
        price: string_or_number(row.get("price")),
        filled_quantity: string_or_number(row.get("cumExecQty")).unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(row.get("avgPrice")),
        reduce_only: row
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: row
            .get("timeInForce")
            .and_then(Value::as_str)
            .is_some_and(|tif| tif.eq_ignore_ascii_case("PostOnly")),
        created_at: row
            .get("createdTime")
            .and_then(value_as_i64)
            .and_then(chrono::DateTime::<Utc>::from_timestamp_millis),
        updated_at: row
            .get("updatedTime")
            .and_then(value_as_i64)
            .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    result_list(
        exchange_id,
        value,
        "Bybit orders response missing result.list",
    )?
    .iter()
    .map(|row| parse_order_state(exchange_id, symbol_hint, row))
    .collect()
}

pub fn parse_fills(
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
                message: "bybit fills request requires canonical_symbol".to_string(),
            })?;
    result_list(
        exchange_id,
        value,
        "Bybit executions response missing result.list",
    )?
    .iter()
    .map(|fill| {
        Ok(Fill {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: symbol.market_type,
            canonical_symbol: canonical_symbol.clone(),
            exchange_symbol: Some(symbol.exchange_symbol.clone()),
            order_id: string_or_number(fill.get("orderId")),
            client_order_id: string_or_number(fill.get("orderLinkId")),
            fill_id: string_or_number(fill.get("execId")),
            side: parse_side(fill.get("side").and_then(Value::as_str).unwrap_or("Buy"))?,
            position_side: PositionSide::None,
            status: FillStatus::Confirmed,
            liquidity_role: match fill.get("isMaker").and_then(Value::as_bool) {
                Some(true) => LiquidityRole::Maker,
                _ => LiquidityRole::Taker,
            },
            price: decimal_value_to_f64(fill.get("execPrice"))?.unwrap_or(0.0),
            quantity: decimal_value_to_f64(fill.get("execQty"))?.unwrap_or(0.0),
            quote_quantity: decimal_value_to_f64(fill.get("execValue"))?,
            fee_asset: string_or_number(fill.get("feeCurrency")),
            fee_amount: decimal_value_to_f64(fill.get("execFee"))?,
            fee_rate: None,
            realized_pnl: None,
            filled_at: fill
                .get("execTime")
                .and_then(value_as_i64)
                .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
                .unwrap_or_else(Utc::now),
            received_at: Utc::now(),
        })
    })
    .collect()
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let rows = result_list(
        exchange_id,
        value,
        "Bybit fee-rate response missing result.list",
    )?;
    let mut fees = Vec::new();
    for symbol in symbols {
        let normalized_symbol = normalize_bybit_symbol(&symbol.exchange_symbol.symbol)?;
        let row = rows
            .iter()
            .find(|row| {
                row.get("symbol")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.eq_ignore_ascii_case(&normalized_symbol))
            })
            .or_else(|| rows.first())
            .ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Bybit fee-rate response is empty",
                    value,
                )
            })?;
        fees.push(FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: symbol.clone(),
            maker_rate: string_or_number(row.get("makerFeeRate"))
                .unwrap_or_else(|| "0".to_string()),
            taker_rate: string_or_number(row.get("takerFeeRate"))
                .unwrap_or_else(|| "0".to_string()),
            source: Some("bybit.v5.account_fee_rate".to_string()),
            updated_at: Utc::now(),
        });
    }
    Ok(fees)
}

fn result_list<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    message: &str,
) -> ExchangeApiResult<&'a Vec<Value>> {
    value
        .get("result")
        .and_then(|result| result.get("list"))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), message, value))
}

fn parse_side(side: &str) -> ExchangeApiResult<OrderSide> {
    match side.to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported Bybit side {side}"),
        }),
    }
}

fn parse_position_side(side: Option<&str>) -> PositionSide {
    match side.map(str::to_ascii_lowercase).as_deref() {
        Some("buy") | Some("long") => PositionSide::Long,
        Some("sell") | Some("short") => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn parse_order_type(order_type: Option<&str>) -> OrderType {
    match order_type.map(str::to_ascii_lowercase).as_deref() {
        Some("market") => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_lowercase().as_str() {
        "new" | "created" => OrderStatus::New,
        "partiallyfilled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn split_symbol(symbol: &str) -> (&str, &str) {
    for quote in ["USDT", "USDC", "USD"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            return (base, quote);
        }
    }
    (symbol, "USDT")
}
