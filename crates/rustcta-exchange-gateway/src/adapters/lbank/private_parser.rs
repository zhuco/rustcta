use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    data_array, decimal_text_to_f64, normalize_spot_symbol, parse_error, string_or_number,
    validation_error, value_as_i64, value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let data = value.get("data").unwrap_or(value);
    let rows = data
        .get("balances")
        .or_else(|| data.get("list"))
        .or_else(|| data.get("data"))
        .and_then(Value::as_array)
        .or_else(|| data.as_array())
        .ok_or_else(|| parse_error(exchange_id.clone(), "balance response missing rows", value))?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for row in rows {
        let asset = row
            .get("assetCode")
            .or_else(|| row.get("asset"))
            .or_else(|| row.get("coin"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_uppercase();
        if asset.is_empty() || (!requested.is_empty() && !requested.contains(&asset)) {
            continue;
        }
        let available = string_or_number(
            row.get("usableAmt")
                .or_else(|| row.get("free"))
                .or_else(|| row.get("available")),
        )
        .unwrap_or_else(|| "0".to_string());
        let locked = string_or_number(row.get("locked")).unwrap_or_else(|| "0".to_string());
        let total = string_or_number(
            row.get("assetAmt")
                .or_else(|| row.get("total"))
                .or_else(|| row.get("balance")),
        );
        let available_number = decimal_text_to_f64(&available)?;
        let locked_number = decimal_text_to_f64(&locked)?;
        let total_number = match total {
            Some(total) => decimal_text_to_f64(&total)?,
            None => available_number + locked_number,
        };
        if total_number > 0.0 || available_number > 0.0 || !requested.is_empty() {
            balances.push(
                AssetBalance::new(asset, total_number, available_number, locked_number)
                    .map_err(validation_error)?,
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

pub fn parse_contract_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let data = value.get("data").unwrap_or(value);
    let rows = data
        .get("accounts")
        .or_else(|| data.get("balances"))
        .or_else(|| data.get("list"))
        .and_then(Value::as_array);
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    if let Some(rows) = rows {
        for row in rows {
            push_contract_balance(exchange_id, &mut balances, &requested, row)?;
        }
    } else {
        push_contract_balance(exchange_id, &mut balances, &requested, data)?;
    }
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        balances,
        observed_at: Utc::now(),
    }])
}

fn push_contract_balance(
    _exchange_id: &ExchangeId,
    balances: &mut Vec<AssetBalance>,
    requested: &[String],
    row: &Value,
) -> ExchangeApiResult<()> {
    let asset = row
        .get("asset")
        .or_else(|| row.get("currency"))
        .or_else(|| row.get("clearCurrency"))
        .and_then(Value::as_str)
        .unwrap_or("USDT")
        .to_ascii_uppercase();
    if !requested.is_empty() && !requested.contains(&asset) {
        return Ok(());
    }
    let available = string_or_number(
        row.get("available")
            .or_else(|| row.get("availableBalance"))
            .or_else(|| row.get("free")),
    )
    .unwrap_or_else(|| "0".to_string());
    let total = string_or_number(
        row.get("balance")
            .or_else(|| row.get("walletBalance"))
            .or_else(|| row.get("equity")),
    )
    .unwrap_or_else(|| available.clone());
    let available_number = decimal_text_to_f64(&available)?;
    let total_number = decimal_text_to_f64(&total)?;
    let locked_number = (total_number - available_number).max(0.0);
    if total_number > 0.0 || available_number > 0.0 || !requested.is_empty() {
        balances.push(
            AssetBalance::new(asset, total_number, available_number, locked_number)
                .map_err(validation_error)?,
        );
    }
    Ok(())
}

pub fn parse_contract_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_symbols: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let data = value.get("data").unwrap_or(value);
    let Some(rows) = data
        .get("positions")
        .or_else(|| data.get("positionList"))
        .or_else(|| data.get("list"))
        .and_then(Value::as_array)
    else {
        return Ok(Vec::new());
    };
    rows.iter()
        .filter(|row| {
            if requested_symbols.is_empty() {
                return true;
            }
            row.get("symbol")
                .and_then(Value::as_str)
                .is_some_and(|symbol| {
                    requested_symbols
                        .iter()
                        .any(|requested| requested.symbol.eq_ignore_ascii_case(symbol))
                })
        })
        .map(|row| {
            let raw_symbol = row
                .get("symbol")
                .and_then(Value::as_str)
                .ok_or_else(|| parse_error(exchange_id.clone(), "position missing symbol", row))?
                .to_ascii_uppercase();
            let canonical_symbol = canonical_from_contract_symbol(&raw_symbol)?;
            let quantity = string_or_number(
                row.get("volume")
                    .or_else(|| row.get("position"))
                    .or_else(|| row.get("quantity")),
            )
            .unwrap_or_else(|| "0".to_string());
            let position = ExchangePosition {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol,
                exchange_symbol: Some(
                    ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, raw_symbol)
                        .map_err(validation_error)?,
                ),
                side: parse_position_side(row.get("side").and_then(Value::as_str)),
                quantity: decimal_text_to_f64(&quantity)?.abs(),
                entry_price: optional_f64(row, &["entryPrice", "openPrice", "avgPrice"])?,
                mark_price: optional_f64(row, &["markPrice", "markedPrice"])?,
                liquidation_price: optional_f64(row, &["liquidationPrice", "liqPrice"])?,
                unrealized_pnl: optional_f64(row, &["unrealizedPnl", "unrealizedProfit"])?,
                leverage: optional_f64(row, &["leverage"])?,
                observed_at: Utc::now(),
            };
            position.validate().map_err(validation_error)?;
            Ok(position)
        })
        .collect()
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let rows = data_array(exchange_id, value, "fee response missing data")?;
    rows.iter()
        .map(|row| {
            let symbol_text = row
                .get("symbol")
                .and_then(Value::as_str)
                .map(normalize_spot_symbol)
                .transpose()?;
            let symbol = symbol_text
                .as_deref()
                .and_then(|text| {
                    requested_symbols
                        .iter()
                        .find(|symbol| symbol.exchange_symbol.symbol.eq_ignore_ascii_case(text))
                })
                .cloned()
                .or_else(|| requested_symbols.first().cloned())
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "fee response cannot map symbol", row)
                })?;
            Ok(FeeRateSnapshot {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol,
                maker_rate: normalize_percent_rate(
                    string_or_number(row.get("makerCommission"))
                        .or_else(|| string_or_number(row.get("maker"))),
                ),
                taker_rate: normalize_percent_rate(
                    string_or_number(row.get("takerCommission"))
                        .or_else(|| string_or_number(row.get("taker"))),
                ),
                source: Some("lbank.customer_trade_fee".to_string()),
                updated_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let data = value
        .get("data")
        .or_else(|| value.get("orders"))
        .and_then(|data| data.as_array().and_then(|rows| rows.first()).or(Some(data)))
        .unwrap_or(value);
    let raw_symbol = data
        .get("symbol")
        .and_then(Value::as_str)
        .map(normalize_spot_symbol)
        .transpose()?
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| parse_error(exchange_id.clone(), "order response missing symbol", data))?;
    let exchange_symbol = symbol_hint
        .map(|symbol| symbol.exchange_symbol.clone())
        .unwrap_or(
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, raw_symbol)
                .map_err(validation_error)?,
        );
    let canonical_symbol = symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone());
    let order_type_text = data
        .get("type")
        .or_else(|| data.get("tradeType"))
        .and_then(Value::as_str)
        .unwrap_or("buy");
    let (side, order_type, tif, post_only) = parse_lbank_order_type(order_type_text)?;
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol,
        client_order_id: value_as_string(
            data.get("clientOrderId")
                .or_else(|| data.get("custom_id"))
                .or_else(|| data.get("origClientOrderId")),
        ),
        exchange_order_id: value_as_string(data.get("order_id").or_else(|| data.get("orderId"))),
        side,
        position_side: Some(PositionSide::None),
        order_type,
        time_in_force: tif,
        status: data
            .get("status")
            .and_then(value_as_i64)
            .map(map_lbank_order_status)
            .or_else(|| {
                data.get("status")
                    .and_then(Value::as_str)
                    .map(map_text_order_status)
            })
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(data.get("origQty").or_else(|| data.get("amount")))
            .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(data.get("price")).filter(|value| value != "0"),
        filled_quantity: string_or_number(
            data.get("executedQty").or_else(|| data.get("deal_amount")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(
            data.get("avgPrice").or_else(|| data.get("avg_price")),
        ),
        reduce_only: false,
        post_only,
        created_at: data
            .get("time")
            .or_else(|| data.get("create_time"))
            .or_else(|| data.get("transactTime"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        updated_at: data
            .get("updateTime")
            .or_else(|| data.get("time"))
            .or_else(|| data.get("create_time"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or(now),
    })
}

pub fn parse_contract_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let data = value
        .get("data")
        .and_then(|data| data.as_array().and_then(|rows| rows.first()).or(Some(data)))
        .unwrap_or(value);
    let raw_symbol = data
        .get("symbol")
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "contract order response missing symbol",
                data,
            )
        })?;
    let exchange_symbol = symbol_hint
        .map(|symbol| symbol.exchange_symbol.clone())
        .unwrap_or(
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, raw_symbol)
                .map_err(validation_error)?,
        );
    let canonical_symbol = symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone());
    let side_text = data.get("side").and_then(Value::as_str).unwrap_or("BUY");
    let raw_order_type = data
        .get("orderPriceType")
        .or_else(|| data.get("type"))
        .and_then(|value| string_or_number(Some(value)))
        .unwrap_or_else(|| "4".to_string());
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol,
        exchange_symbol,
        client_order_id: value_as_string(data.get("clientOrderId")),
        exchange_order_id: value_as_string(data.get("orderId").or_else(|| data.get("order_id"))),
        side: parse_side(side_text),
        position_side: Some(parse_position_side(
            data.get("positionSide").and_then(Value::as_str),
        )),
        order_type: if raw_order_type == "5" {
            OrderType::Market
        } else {
            OrderType::Limit
        },
        time_in_force: Some(TimeInForce::GTC),
        status: data
            .get("status")
            .and_then(value_as_i64)
            .map(map_contract_order_status)
            .or_else(|| {
                data.get("status")
                    .and_then(Value::as_str)
                    .map(map_text_order_status)
            })
            .unwrap_or(OrderStatus::Open),
        quantity: string_or_number(data.get("volume").or_else(|| data.get("quantity")))
            .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(data.get("price")).filter(|value| value != "0"),
        filled_quantity: string_or_number(data.get("dealVolume").or_else(|| data.get("filled")))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(data.get("avgPrice")),
        reduce_only: data
            .get("offsetFlag")
            .and_then(value_as_i64)
            .is_some_and(|flag| flag != 0),
        post_only: false,
        created_at: data
            .get("time")
            .or_else(|| data.get("createTime"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        updated_at: data
            .get("updateTime")
            .or_else(|| data.get("time"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or(now),
    })
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let rows = value
        .get("data")
        .or_else(|| value.get("transaction"))
        .or_else(|| value.get("transactions"))
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "transaction history response missing rows",
                value,
            )
        })?;
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .ok_or_else(|| parse_error(exchange_id.clone(), "fill missing canonical symbol", value))?;
    rows.iter()
        .map(|row| {
            let price = string_or_number(row.get("price").or_else(|| row.get("dealPrice")))
                .unwrap_or_else(|| "0".to_string());
            let quantity = string_or_number(
                row.get("amount")
                    .or_else(|| row.get("qty"))
                    .or_else(|| row.get("quantity"))
                    .or_else(|| row.get("dealQuantity")),
            )
            .unwrap_or_else(|| "0".to_string());
            let quote_quantity = string_or_number(
                row.get("volume")
                    .or_else(|| row.get("quoteQty"))
                    .or_else(|| row.get("quoteQuantity"))
                    .or_else(|| row.get("dealVolumePrice")),
            )
            .map(|value| decimal_text_to_f64(&value))
            .transpose()?;
            let fill = Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(
                    row.get("orderId")
                        .or_else(|| row.get("order_id"))
                        .or_else(|| row.get("orderUuid")),
                ),
                client_order_id: value_as_string(row.get("clientOrderId")),
                fill_id: value_as_string(
                    row.get("id")
                        .or_else(|| row.get("tradeId"))
                        .or_else(|| row.get("txUuid")),
                ),
                side: row
                    .get("tradeType")
                    .or_else(|| row.get("side"))
                    .and_then(Value::as_str)
                    .map(parse_side)
                    .unwrap_or(OrderSide::Buy),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity_role(row),
                price: decimal_text_to_f64(&price)?,
                quantity: decimal_text_to_f64(&quantity)?,
                quote_quantity,
                fee_asset: row
                    .get("tradeFeeAsset")
                    .or_else(|| row.get("feeAsset"))
                    .and_then(Value::as_str)
                    .map(str::to_ascii_uppercase),
                fee_amount: string_or_number(row.get("tradeFee").or_else(|| row.get("fee")))
                    .map(|value| decimal_text_to_f64(&value))
                    .transpose()?,
                fee_rate: string_or_number(row.get("tradeFeeRate"))
                    .map(|value| decimal_text_to_f64(&value))
                    .transpose()?,
                realized_pnl: None,
                filled_at: row
                    .get("time")
                    .or_else(|| row.get("timestamp"))
                    .or_else(|| row.get("dealTime"))
                    .and_then(value_as_i64)
                    .and_then(DateTime::<Utc>::from_timestamp_millis)
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            };
            fill.validate().map_err(validation_error)?;
            Ok(fill)
        })
        .collect()
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let rows = value
        .get("data")
        .or_else(|| value.get("orders"))
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "open orders response missing data",
                value,
            )
        })?;
    rows.iter()
        .map(|row| parse_order_state(exchange_id, symbol_hint, row))
        .collect()
}

pub fn parse_order_states(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    if let Some(rows) = value
        .get("data")
        .or_else(|| value.get("orders"))
        .and_then(Value::as_array)
        .or_else(|| value.as_array())
    {
        return rows
            .iter()
            .map(|row| parse_order_state(exchange_id, symbol_hint, row))
            .collect();
    }
    Ok(vec![parse_order_state(exchange_id, symbol_hint, value)?])
}

fn parse_lbank_order_type(
    raw: &str,
) -> ExchangeApiResult<(OrderSide, OrderType, Option<TimeInForce>, bool)> {
    let raw = raw.to_ascii_lowercase();
    let side = if raw.starts_with("sell") {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    };
    let order_type = if raw.contains("market") {
        OrderType::Market
    } else if raw.contains("maker") {
        OrderType::PostOnly
    } else if raw.contains("ioc") {
        OrderType::IOC
    } else if raw.contains("fok") {
        OrderType::FOK
    } else {
        OrderType::Limit
    };
    let tif = match order_type {
        OrderType::IOC => Some(TimeInForce::IOC),
        OrderType::FOK => Some(TimeInForce::FOK),
        OrderType::PostOnly => Some(TimeInForce::GTX),
        OrderType::Limit => Some(TimeInForce::GTC),
        _ => None,
    };
    Ok((side, order_type, tif, order_type == OrderType::PostOnly))
}

fn map_lbank_order_status(status: i64) -> OrderStatus {
    match status {
        -1 | 3 => OrderStatus::Cancelled,
        0 => OrderStatus::Open,
        1 => OrderStatus::PartiallyFilled,
        2 => OrderStatus::Filled,
        4 => OrderStatus::PendingCancel,
        _ => OrderStatus::Unknown,
    }
}

fn map_text_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_lowercase().as_str() {
        "new" | "open" => OrderStatus::Open,
        "partially_filled" | "partial" => OrderStatus::PartiallyFilled,
        "filled" | "done" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        _ => OrderStatus::Unknown,
    }
}

fn map_contract_order_status(status: i64) -> OrderStatus {
    match status {
        0 => OrderStatus::Open,
        1 => OrderStatus::PartiallyFilled,
        2 => OrderStatus::Filled,
        3 | 4 => OrderStatus::Cancelled,
        _ => OrderStatus::Unknown,
    }
}

fn parse_side(side: &str) -> OrderSide {
    if side.eq_ignore_ascii_case("sell") {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
}

fn parse_position_side(side: Option<&str>) -> PositionSide {
    match side.unwrap_or_default().to_ascii_lowercase().as_str() {
        "long" | "buy" => PositionSide::Long,
        "short" | "sell" => PositionSide::Short,
        "net" => PositionSide::Net,
        _ => PositionSide::Net,
    }
}

fn parse_liquidity_role(row: &Value) -> LiquidityRole {
    if row
        .get("isMaker")
        .or_else(|| row.get("maker"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        LiquidityRole::Maker
    } else {
        LiquidityRole::Taker
    }
}

fn optional_f64(row: &Value, fields: &[&str]) -> ExchangeApiResult<Option<f64>> {
    fields
        .iter()
        .find_map(|field| string_or_number(row.get(*field)))
        .map(|value| decimal_text_to_f64(&value))
        .transpose()
}

fn canonical_from_contract_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    if let Some(base) = symbol.strip_suffix("USDT").filter(|base| !base.is_empty()) {
        return CanonicalSymbol::new(base, "USDT").map_err(validation_error);
    }
    Err(parse_error(
        ExchangeId::new("lbank").map_err(validation_error)?,
        "cannot infer contract canonical symbol",
        &Value::String(symbol.to_string()),
    ))
}

fn normalize_percent_rate(value: Option<String>) -> String {
    let Some(value) = value else {
        return "0".to_string();
    };
    let Ok(number) = value.parse::<f64>() else {
        return value;
    };
    if number.abs() >= 1.0 {
        (number / 100.0).to_string()
    } else {
        value
    }
}
