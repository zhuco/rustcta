use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol, Fill, FillStatus,
    LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
    TimeInForce,
};
use serde_json::Value;

use super::parser::{
    bitmex_symbol_key, market_type_from_instrument, parse_error, required_str, string_or_number,
    validation_error,
};

pub fn parse_margin_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let items = as_array_or_single(value);
    let requested = assets
        .iter()
        .map(|asset| normalize_asset(asset))
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for item in items {
        let asset = item
            .get("currency")
            .or_else(|| item.get("settlCurrency"))
            .and_then(Value::as_str)
            .map(normalize_asset)
            .unwrap_or_else(|| "BTC".to_string());
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let raw_total = decimal_value_to_f64(
            item.get("marginBalance")
                .or_else(|| item.get("walletBalance"))
                .or_else(|| item.get("amount")),
        )?
        .unwrap_or(0.0);
        let raw_available = decimal_value_to_f64(
            item.get("availableMargin")
                .or_else(|| item.get("availableBalance")),
        )?
        .unwrap_or(raw_total);
        let total = bitmex_asset_amount(&asset, raw_total);
        let available = bitmex_asset_amount(&asset, raw_available);
        let locked = (total - available).max(0.0);
        if total > 0.0 || available > 0.0 || !requested.is_empty() {
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
        market_type: MarketType::Perpetual,
        balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let positions = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BitMEX position response is not an array",
            value,
        )
    })?;
    positions
        .iter()
        .filter(|position| {
            position
                .get("isOpen")
                .and_then(Value::as_bool)
                .unwrap_or(true)
        })
        .map(|position| {
            let symbol_text = required_str(exchange_id, position, "symbol")?.to_ascii_uppercase();
            let canonical_symbol = canonical_from_symbol(&symbol_text, position);
            let quantity = decimal_value_to_f64(
                position
                    .get("currentQty")
                    .or_else(|| position.get("homeNotional")),
            )?
            .unwrap_or(0.0);
            let market_type = if position.get("expiry").is_some_and(Value::is_null) {
                MarketType::Perpetual
            } else {
                MarketType::Perpetual
            };
            let parsed = ExchangePosition {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type,
                canonical_symbol,
                exchange_symbol: Some(
                    ExchangeSymbol::new(exchange_id.clone(), market_type, symbol_text)
                        .map_err(validation_error)?,
                ),
                side: if quantity > 0.0 {
                    PositionSide::Long
                } else if quantity < 0.0 {
                    PositionSide::Short
                } else {
                    PositionSide::None
                },
                quantity: quantity.abs(),
                entry_price: decimal_value_to_f64(position.get("avgEntryPrice"))?,
                mark_price: decimal_value_to_f64(position.get("markPrice"))?,
                liquidation_price: decimal_value_to_f64(position.get("liquidationPrice"))?,
                unrealized_pnl: decimal_value_to_f64(position.get("unrealisedPnl"))?,
                leverage: decimal_value_to_f64(position.get("leverage"))?,
                observed_at: Utc::now(),
            };
            parsed.validate().map_err(validation_error)?;
            Ok(parsed)
        })
        .collect()
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let instruments = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BitMEX instrument fee response is not an array",
            value,
        )
    })?;
    let mut fees = Vec::new();
    for symbol in symbols {
        let requested = bitmex_symbol_key(&symbol.exchange_symbol.symbol)?;
        let instrument = instruments
            .iter()
            .find(|instrument| {
                instrument
                    .get("symbol")
                    .and_then(Value::as_str)
                    .and_then(|value| bitmex_symbol_key(value).ok())
                    .is_some_and(|value| value == requested)
            })
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("BitMEX fee response missing symbol {requested}"),
            })?;
        fees.push(FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: symbol.clone(),
            maker_rate: string_or_number(instrument.get("makerFee"))
                .unwrap_or_else(|| "0".to_string()),
            taker_rate: string_or_number(instrument.get("takerFee"))
                .unwrap_or_else(|| "0".to_string()),
            source: Some("bitmex.instrument.active".to_string()),
            updated_at: Utc::now(),
        });
    }
    Ok(fees)
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = if let Some(array) = value.as_array() {
        array.first().ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "BitMEX order response is an empty array",
                value,
            )
        })?
    } else {
        value
    };
    let symbol_text = required_str(exchange_id, order, "symbol")
        .or_else(|_| {
            symbol_hint
                .map(|symbol| symbol.exchange_symbol.symbol.as_str())
                .ok_or_else(|| {
                    parse_error(
                        exchange_id.clone(),
                        "BitMEX order response missing symbol",
                        order,
                    )
                })
        })?
        .to_ascii_uppercase();
    let market_type = symbol_hint
        .map(|symbol| symbol.market_type)
        .unwrap_or(MarketType::Perpetual);
    let exchange_symbol = symbol_hint
        .map(|symbol| Ok(symbol.exchange_symbol.clone()))
        .unwrap_or_else(|| {
            ExchangeSymbol::new(exchange_id.clone(), market_type, symbol_text)
                .map_err(validation_error)
        })?;
    let order_type = order
        .get("ordType")
        .and_then(Value::as_str)
        .map(parse_order_type)
        .unwrap_or(OrderType::Limit);
    let exec_inst = order
        .get("execInst")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone()),
        exchange_symbol,
        client_order_id: value_as_string(order.get("clOrdID")),
        exchange_order_id: value_as_string(order.get("orderID")),
        side: order
            .get("side")
            .and_then(Value::as_str)
            .map(parse_side)
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        position_side: Some(PositionSide::Net),
        order_type,
        time_in_force: order
            .get("timeInForce")
            .and_then(Value::as_str)
            .map(parse_time_in_force),
        status: order
            .get("ordStatus")
            .and_then(Value::as_str)
            .map(parse_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(order.get("orderQty").or_else(|| order.get("cumQty")))
            .unwrap_or_else(|| "0".to_string()),
        price: non_zero_string(string_or_number(order.get("price")).unwrap_or_default()),
        filled_quantity: string_or_number(order.get("cumQty")).unwrap_or_else(|| "0".to_string()),
        average_fill_price: non_zero_string(
            string_or_number(order.get("avgPx")).unwrap_or_default(),
        ),
        reduce_only: exec_inst
            .split(',')
            .any(|item| item.trim().eq_ignore_ascii_case("ReduceOnly")),
        post_only: exec_inst
            .split(',')
            .any(|item| item.trim().eq_ignore_ascii_case("ParticipateDoNotInitiate")),
        created_at: order.get("timestamp").and_then(value_as_timestamp),
        updated_at: order
            .get("transactTime")
            .or_else(|| order.get("timestamp"))
            .and_then(value_as_timestamp)
            .unwrap_or(now),
    })
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BitMEX orders response is not an array",
            value,
        )
    })?;
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, symbol_hint, order))
        .collect()
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
                message: "bitmex.get_recent_fills requires canonical_symbol".to_string(),
            })?;
    let fills = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "BitMEX execution response is not an array",
            value,
        )
    })?;
    fills
        .iter()
        .filter(|fill| {
            fill.get("execType")
                .and_then(Value::as_str)
                .map(|value| value.eq_ignore_ascii_case("Trade"))
                .unwrap_or(true)
        })
        .map(|fill| {
            let price = decimal_value_to_f64(fill.get("lastPx").or_else(|| fill.get("price")))?
                .unwrap_or(0.0);
            let quantity =
                decimal_value_to_f64(fill.get("lastQty").or_else(|| fill.get("orderQty")))?
                    .unwrap_or(0.0);
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("orderID")),
                client_order_id: value_as_string(fill.get("clOrdID")),
                fill_id: value_as_string(fill.get("execID").or_else(|| fill.get("trdMatchID"))),
                side: fill
                    .get("side")
                    .and_then(Value::as_str)
                    .map(parse_side)
                    .transpose()?
                    .unwrap_or(OrderSide::Buy),
                position_side: PositionSide::Net,
                status: FillStatus::Confirmed,
                liquidity_role: fill
                    .get("lastLiquidityInd")
                    .and_then(Value::as_str)
                    .map(parse_liquidity)
                    .unwrap_or(LiquidityRole::Unknown),
                price,
                quantity,
                quote_quantity: Some(price * quantity),
                fee_asset: value_as_string(
                    fill.get("settlCurrency").or_else(|| fill.get("currency")),
                )
                .map(|asset| normalize_asset(&asset)),
                fee_amount: decimal_value_to_f64(fill.get("execComm").or_else(|| fill.get("fee")))?
                    .map(|amount| {
                        fill.get("settlCurrency")
                            .or_else(|| fill.get("currency"))
                            .and_then(Value::as_str)
                            .map(normalize_asset)
                            .map(|asset| bitmex_asset_amount(&asset, amount))
                            .unwrap_or(amount)
                    }),
                fee_rate: decimal_value_to_f64(fill.get("commission"))?,
                realized_pnl: decimal_value_to_f64(fill.get("realisedPnl"))?,
                filled_at: fill
                    .get("timestamp")
                    .and_then(value_as_timestamp)
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn as_array_or_single(value: &Value) -> Vec<&Value> {
    value
        .as_array()
        .map(|items| items.iter().collect())
        .unwrap_or_else(|| vec![value])
}

fn canonical_from_symbol(symbol: &str, value: &Value) -> rustcta_types::CanonicalSymbol {
    let base = value
        .get("underlying")
        .or_else(|| value.get("rootSymbol"))
        .and_then(Value::as_str)
        .map(normalize_asset)
        .unwrap_or_else(|| split_symbol_assets(symbol).0);
    let quote = value
        .get("quoteCurrency")
        .or_else(|| value.get("settlCurrency"))
        .and_then(Value::as_str)
        .map(normalize_asset)
        .unwrap_or_else(|| split_symbol_assets(symbol).1);
    rustcta_types::CanonicalSymbol::new(base, quote).expect("normalized BitMEX assets")
}

fn split_symbol_assets(symbol: &str) -> (String, String) {
    for quote in ["USDT", "USDC", "USD", "XBT", "BTC", "ETH"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return (normalize_asset(base), normalize_asset(quote));
            }
        }
    }
    (normalize_asset(symbol), "USD".to_string())
}

fn parse_order_type(value: &str) -> OrderType {
    match value.to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        "stop" => OrderType::StopMarket,
        "stoplimit" | "limitiftouched" | "limitif-touched" => OrderType::StopLimit,
        "marketiftouched" | "marketif-touched" => OrderType::StopMarket,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: &str) -> TimeInForce {
    match value.to_ascii_lowercase().as_str() {
        "immediateorcancel" | "ioc" => TimeInForce::IOC,
        "fillorkill" | "fok" => TimeInForce::FOK,
        _ => TimeInForce::GTC,
    }
}

fn parse_order_status(value: &str) -> OrderStatus {
    match value.to_ascii_lowercase().as_str() {
        "new" => OrderStatus::New,
        "partiallyfilled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_side(value: &str) -> ExchangeApiResult<OrderSide> {
    match value.to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported BitMEX side {other}"),
        }),
    }
}

fn parse_liquidity(value: &str) -> LiquidityRole {
    match value.to_ascii_lowercase().as_str() {
        "addedliquidity" | "maker" => LiquidityRole::Maker,
        "removedliquidity" | "taker" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn normalize_asset(asset: &str) -> String {
    match asset.trim().to_ascii_uppercase().as_str() {
        "XBT" | "XBTUSD" => "BTC".to_string(),
        other => other.to_string(),
    }
}

fn bitmex_asset_amount(asset: &str, amount: f64) -> f64 {
    if asset.eq_ignore_ascii_case("BTC") {
        amount / 100_000_000.0
    } else {
        amount
    }
}

fn non_zero_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.trim_matches('0').trim_matches('.').is_empty() {
        None
    } else {
        Some(value)
    }
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .map(|value| {
            let text = match value {
                Value::String(text) => text.clone(),
                Value::Number(number) => number.to_string(),
                _ => "0".to_string(),
            };
            text.parse::<f64>()
                .map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("invalid BitMEX decimal value {text}: {error}"),
                })
        })
        .transpose()
}

fn value_as_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(text) = value.as_str() {
        return DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|timestamp| timestamp.with_timezone(&Utc));
    }
    value
        .as_i64()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

#[allow(dead_code)]
fn _market_type_from_instrument_for_private(value: &Value) -> MarketType {
    market_type_from_instrument(value)
}
