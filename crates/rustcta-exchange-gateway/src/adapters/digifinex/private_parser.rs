use chrono::Utc;
use rustcta_exchange_api::{FeeRateSnapshot, OrderState, SymbolScope, EXCHANGE_API_SCHEMA_VERSION};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition,
    ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType,
    PositionSide, SchemaVersion, TenantId,
};
use serde_json::{json, Value};

use super::parser::{
    decimal, normalize_symbol, split_symbol, text_from_fields, timestamp_from_value,
    validation_error, value_as_string,
};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let data = value.get("data").unwrap_or(value);
    let rows = data
        .get("list")
        .or_else(|| data.get("balances"))
        .or_else(|| data.get("margin"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| {
            data.as_object()
                .map(|map| {
                    map.iter()
                        .map(|(asset, row)| {
                            let mut row = row.clone();
                            if let Value::Object(object) = &mut row {
                                object
                                    .entry("currency".to_string())
                                    .or_insert_with(|| json!(asset));
                            }
                            row
                        })
                        .collect()
                })
                .unwrap_or_default()
        });
    let wanted = assets
        .iter()
        .map(|asset| asset.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for row in rows {
        let asset = text_from_fields(&row, &["currency", "asset", "symbol", "margin_currency"])
            .unwrap_or_default()
            .to_ascii_uppercase();
        if asset.is_empty() || !wanted.is_empty() && !wanted.contains(&asset) {
            continue;
        }
        let available = decimal(
            row.get("available")
                .or_else(|| row.get("free"))
                .or_else(|| row.get("balance_available")),
        )
        .unwrap_or(0.0);
        let locked = decimal(
            row.get("frozen")
                .or_else(|| row.get("locked"))
                .or_else(|| row.get("balance_frozen")),
        )
        .unwrap_or(0.0);
        let total =
            decimal(row.get("total").or_else(|| row.get("balance"))).unwrap_or(available + locked);
        balances.push(ExchangeBalance {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type,
            balances: vec![
                AssetBalance::new(asset, total, available, locked).map_err(validation_error)?
            ],
            observed_at: Utc::now(),
        });
    }
    Ok(balances)
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let rows = value
        .get("data")
        .or_else(|| value.get("positions"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut positions = Vec::new();
    for row in rows {
        let symbol = text_from_fields(&row, &["symbol", "instrument_id", "contract_code"])
            .unwrap_or_default();
        if symbol.is_empty() {
            continue;
        }
        let normalized = normalize_symbol(&symbol, MarketType::Perpetual)?;
        let (base, quote) = split_symbol(&normalized)
            .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
        let quantity = decimal(
            row.get("amount")
                .or_else(|| row.get("quantity"))
                .or_else(|| row.get("position")),
        )
        .unwrap_or(0.0)
        .abs();
        if quantity == 0.0 {
            continue;
        }
        positions.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: CanonicalSymbol::new(base, quote).map_err(validation_error)?,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, normalized)
                    .map_err(validation_error)?,
            ),
            side: parse_position_side(
                text_from_fields(&row, &["side", "position_side", "direction"]).as_deref(),
            ),
            quantity,
            entry_price: decimal(row.get("entry_price").or_else(|| row.get("avg_price"))),
            mark_price: decimal(row.get("mark_price")),
            liquidation_price: decimal(row.get("liquidation_price")),
            unrealized_pnl: decimal(row.get("unrealized_pnl").or_else(|| row.get("pnl"))),
            leverage: decimal(row.get("leverage")),
            observed_at: Utc::now(),
        });
    }
    Ok(positions)
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let data = value.get("data").unwrap_or(value);
    let maker = value_as_string(
        data.get("maker_fee_rate")
            .or_else(|| data.get("maker"))
            .or_else(|| data.get("makerFeeRate")),
    )
    .unwrap_or_else(|| "0.001".to_string());
    let taker = value_as_string(
        data.get("taker_fee_rate")
            .or_else(|| data.get("taker"))
            .or_else(|| data.get("takerFeeRate")),
    )
    .unwrap_or_else(|| "0.001".to_string());
    Ok(symbols
        .iter()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: symbol.clone(),
            maker_rate: maker.clone(),
            taker_rate: taker.clone(),
            source: Some(format!("{exchange_id}.fee_metadata")),
            updated_at: Utc::now(),
        })
        .collect())
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let data = value.get("data").unwrap_or(value);
    if data.is_null() {
        return Ok(None);
    }
    Ok(Some(parse_order_state(
        exchange_id,
        symbol_hint,
        market_type,
        data,
    )?))
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let rows = value
        .get("data")
        .or_else(|| value.get("orders"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    rows.iter()
        .map(|row| parse_order_state(exchange_id, symbol_hint, market_type, row))
        .collect()
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_hint
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_from_value(exchange_id, market_type, value))?;
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: text_from_fields(
            value,
            &["client_oid", "client_order_id", "clientOrderId"],
        ),
        exchange_order_id: text_from_fields(value, &["order_id", "id", "orderId"]),
        side: parse_side(text_from_fields(value, &["side", "type", "direction"]).as_deref()),
        position_side: Some(parse_position_side(
            text_from_fields(value, &["position_side", "direction"]).as_deref(),
        )),
        order_type: parse_order_type(text_from_fields(value, &["order_type", "type"]).as_deref()),
        time_in_force: None,
        status: parse_status(text_from_fields(value, &["status", "state"]).as_deref()),
        quantity: text_from_fields(value, &["amount", "quantity", "size"])
            .unwrap_or_else(|| "0".to_string()),
        price: text_from_fields(value, &["price"]),
        filled_quantity: text_from_fields(
            value,
            &["executed_amount", "filled_amount", "filled", "deal_amount"],
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: text_from_fields(value, &["avg_price", "average_price"]),
        reduce_only: value
            .get("reduce_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: text_from_fields(value, &["time_in_force", "tif"])
            .is_some_and(|tif| tif.eq_ignore_ascii_case("post_only")),
        created_at: value
            .get("created_date")
            .or_else(|| value.get("created_at"))
            .and_then(timestamp_from_value),
        updated_at: value
            .get("updated_date")
            .or_else(|| value.get("finished_date"))
            .and_then(timestamp_from_value)
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let rows = value
        .get("data")
        .or_else(|| value.get("fills"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    rows.iter()
        .map(|row| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                symbol_hint,
                market_type,
                row,
            )
        })
        .collect()
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = symbol_hint
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_from_value(exchange_id, market_type, value))?;
    let price = decimal(value.get("price")).unwrap_or(0.0);
    let quantity = decimal(value.get("amount").or_else(|| value.get("quantity"))).unwrap_or(0.0);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol.canonical_symbol.ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "digifinex fill requires canonical_symbol".to_string(),
            }
        })?,
        exchange_symbol: Some(symbol.exchange_symbol),
        fill_id: Some(text_from_fields(value, &["trade_id", "id", "exec_id"]).unwrap_or_default()),
        order_id: text_from_fields(value, &["order_id", "orderId"]),
        client_order_id: text_from_fields(value, &["client_oid", "client_order_id"]),
        side: parse_side(text_from_fields(value, &["side", "type"]).as_deref()),
        position_side: PositionSide::Net,
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity(text_from_fields(value, &["role", "liquidity"]).as_deref()),
        price,
        quantity,
        quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
        fee_asset: text_from_fields(value, &["fee_currency", "fee_asset"]),
        fee_amount: decimal(value.get("fee")),
        fee_rate: None,
        realized_pnl: decimal(value.get("realized_pnl")),
        filled_at: value
            .get("created_date")
            .or_else(|| value.get("time"))
            .and_then(timestamp_from_value)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn symbol_from_value(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let raw = text_from_fields(
        value,
        &["symbol", "market", "instrument_id", "contract_code"],
    )
    .ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: format!("DigiFinex order missing symbol: {value}"),
    })?;
    let normalized = normalize_symbol(&raw, market_type)?;
    let (base, quote) =
        split_symbol(&normalized).unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, normalized)
            .map_err(validation_error)?,
    })
}

fn parse_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "buy" | "bid" | "open_long" | "close_short" => OrderSide::Buy,
        _ => OrderSide::Sell,
    }
}

fn parse_position_side(value: Option<&str>) -> PositionSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "long" | "open_long" | "close_long" => PositionSide::Long,
        "short" | "open_short" | "close_short" => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        "post_only" => OrderType::PostOnly,
        "ioc" => OrderType::IOC,
        "fok" => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "0" | "submitted" | "open" | "new" => OrderStatus::Open,
        "1" | "partial_filled" | "partial-filled" | "partially_filled" => {
            OrderStatus::PartiallyFilled
        }
        "2" | "filled" | "done" => OrderStatus::Filled,
        "3" | "canceled" | "cancelled" => OrderStatus::Cancelled,
        "4" | "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" => LiquidityRole::Maker,
        "taker" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}
