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
    normalize_whitebit_symbol, parse_error, required_str, string_or_number, validation_error,
    value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    if let Some(map) = value.get("data").unwrap_or(value).as_object() {
        for (asset, item) in map {
            let asset = asset.to_ascii_uppercase();
            if !requested.is_empty() && !requested.contains(&asset) {
                continue;
            }
            let available = decimal_value_to_f64(
                item.get("available")
                    .or_else(|| item.get("available_balance"))
                    .or_else(|| item.get("balance")),
            )?
            .unwrap_or(0.0);
            let locked = decimal_value_to_f64(
                item.get("freeze")
                    .or_else(|| item.get("frozen"))
                    .or_else(|| item.get("locked")),
            )?
            .unwrap_or(0.0);
            let total = decimal_value_to_f64(item.get("total"))?.unwrap_or(available + locked);
            if total > 0.0 || available > 0.0 || locked > 0.0 || !requested.is_empty() {
                balances.push(
                    AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
                );
            }
        }
    } else {
        let items = array_items(value).ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "balance response is not an array or object",
                value,
            )
        })?;
        for item in items {
            let asset = required_str(exchange_id, item, "ticker")
                .or_else(|_| required_str(exchange_id, item, "ccy"))
                .or_else(|_| required_str(exchange_id, item, "asset"))?
                .to_ascii_uppercase();
            if !requested.is_empty() && !requested.contains(&asset) {
                continue;
            }
            let available = decimal_value_to_f64(
                item.get("available")
                    .or_else(|| item.get("available_balance"))
                    .or_else(|| item.get("balance")),
            )?
            .unwrap_or(0.0);
            let locked = decimal_value_to_f64(
                item.get("freeze")
                    .or_else(|| item.get("frozen"))
                    .or_else(|| item.get("locked")),
            )?
            .unwrap_or(0.0);
            let total = decimal_value_to_f64(item.get("total"))?.unwrap_or(available + locked);
            if total > 0.0 || available > 0.0 || locked > 0.0 || !requested.is_empty() {
                balances.push(
                    AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
                );
            }
        }
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

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let market = required_str(exchange_id, value, "market")
        .or_else(|_| required_str(exchange_id, value, "symbol"))
        .unwrap_or("UNKNOWN");
    let normalized_market = normalize_whitebit_symbol(market)?;
    let market_type = symbol_hint
        .map(|symbol| symbol.market_type)
        .unwrap_or_else(|| {
            if normalized_market.ends_with("_PERP") {
                MarketType::Perpetual
            } else {
                MarketType::Spot
            }
        });
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(exchange_id.clone(), market_type, normalized_market)
            .map_err(validation_error)?
    };
    let canonical_symbol = symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone());
    let option = value
        .get("option")
        .or_else(|| value.get("time_in_force"))
        .and_then(Value::as_str);
    let order_type_text = value.get("type").and_then(Value::as_str).unwrap_or("limit");
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol,
        exchange_symbol,
        client_order_id: value_as_string(
            value
                .get("clientOrderId")
                .or_else(|| value.get("client_id")),
        ),
        exchange_order_id: value_as_string(
            value
                .get("orderId")
                .or_else(|| value.get("order_id"))
                .or_else(|| value.get("id")),
        ),
        side: value
            .get("side")
            .and_then(Value::as_str)
            .map(parse_side)
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        position_side: Some(parse_position_side(
            value.get("positionSide").and_then(Value::as_str),
            market_type,
        )),
        order_type: parse_order_type(order_type_text, option),
        time_in_force: parse_time_in_force(option),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(map_whitebit_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(value.get("amount").or_else(|| value.get("quantity")))
            .unwrap_or_else(|| "0".to_string()),
        price: non_zero_string(
            string_or_number(value.get("price")).unwrap_or_else(|| "0".to_string()),
        ),
        filled_quantity: string_or_number(
            value
                .get("dealStock")
                .or_else(|| value.get("filled_amount"))
                .or_else(|| value.get("deal_amount")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: non_zero_string(
            string_or_number(value.get("avgPrice").or_else(|| value.get("avg_price")))
                .unwrap_or_else(|| "0".to_string()),
        ),
        reduce_only: value
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: value
            .get("postOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || order_type_text.eq_ignore_ascii_case("maker_only")
            || option.is_some_and(|text| text.eq_ignore_ascii_case("maker_only")),
        created_at: first_timestamp(value, &["timestamp", "created_at", "create_time"]),
        updated_at: first_timestamp(value, &["updated_at", "finished_at", "timestamp"])
            .unwrap_or(now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = array_items(value).ok_or_else(|| {
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
    exchange_id: &ExchangeId,
    symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let data = value.get("data").unwrap_or(value);
    if let Some(items) = data.as_array() {
        return items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let symbol = symbols
                    .get(index)
                    .cloned()
                    .or_else(|| symbols.first().cloned())
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "whitebit get_fees requires at least one symbol".to_string(),
                    })?;
                parse_fee_snapshot(exchange_id, symbol, item)
            })
            .collect();
    }
    let symbol = symbols
        .first()
        .cloned()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "whitebit get_fees requires at least one symbol".to_string(),
        })?;
    Ok(vec![parse_fee_snapshot(exchange_id, symbol, data)?])
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
                message: "whitebit get_recent_fills requires canonical_symbol".to_string(),
            })?;
    let fills = array_items(value)
        .ok_or_else(|| parse_error(exchange_id.clone(), "fills response is not an array", value))?;
    fills
        .iter()
        .map(|fill| {
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("orderId").or_else(|| fill.get("order_id"))),
                client_order_id: value_as_string(
                    fill.get("clientOrderId").or_else(|| fill.get("client_id")),
                ),
                fill_id: value_as_string(
                    fill.get("tradeId")
                        .or_else(|| fill.get("deal_id"))
                        .or_else(|| fill.get("id")),
                ),
                side: parse_side(required_str(exchange_id, fill, "side")?)?,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price: decimal_value_to_f64(fill.get("price"))?.unwrap_or(0.0),
                quantity: decimal_value_to_f64(
                    fill.get("amount")
                        .or_else(|| fill.get("dealStock"))
                        .or_else(|| fill.get("quantity")),
                )?
                .unwrap_or(0.0),
                quote_quantity: decimal_value_to_f64(fill.get("dealMoney"))?,
                fee_asset: value_as_string(
                    fill.get("feeCurrency")
                        .or_else(|| fill.get("fee_ccy"))
                        .or_else(|| fill.get("fee_asset")),
                ),
                fee_amount: decimal_value_to_f64(fill.get("fee").or_else(|| fill.get("dealFee")))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp(fill, &["timestamp", "created_at", "time"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<&ExchangeSymbol>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let positions = array_items(value).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "positions response is not an array",
            value,
        )
    })?;
    positions
        .iter()
        .filter_map(|position| {
            let result = parse_position(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                symbol_hint,
                position,
            );
            match result {
                Ok(Some(position)) => Some(Ok(position)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            }
        })
        .collect()
}

fn parse_fee_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<FeeRateSnapshot> {
    let maker_field = if symbol.market_type == MarketType::Perpetual {
        value
            .get("futures_maker")
            .or_else(|| value.get("maker"))
            .or_else(|| value.get("makerFee"))
            .or_else(|| value.get("maker_fee_rate"))
            .or_else(|| value.get("maker_fee"))
    } else {
        value
            .get("maker")
            .or_else(|| value.get("makerFee"))
            .or_else(|| value.get("maker_fee_rate"))
            .or_else(|| value.get("maker_fee"))
    };
    let maker = string_or_number(maker_field).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "fee response missing maker rate",
            value,
        )
    })?;
    let taker_field = if symbol.market_type == MarketType::Perpetual {
        value
            .get("futures_taker")
            .or_else(|| value.get("taker"))
            .or_else(|| value.get("takerFee"))
            .or_else(|| value.get("taker_fee_rate"))
            .or_else(|| value.get("taker_fee"))
    } else {
        value
            .get("taker")
            .or_else(|| value.get("takerFee"))
            .or_else(|| value.get("taker_fee_rate"))
            .or_else(|| value.get("taker_fee"))
    };
    let taker = string_or_number(taker_field).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "fee response missing taker rate",
            value,
        )
    })?;
    Ok(FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        maker_rate: maker,
        taker_rate: taker,
        source: Some("whitebit.market_fee".to_string()),
        updated_at: Utc::now(),
    })
}

fn parse_side(side: &str) -> ExchangeApiResult<OrderSide> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported whitebit order side {other}"),
        }),
    }
}

fn parse_order_type(order_type: &str, option: Option<&str>) -> OrderType {
    match (
        order_type.trim().to_ascii_lowercase().as_str(),
        option.map(|text| text.trim().to_ascii_lowercase()),
    ) {
        ("market", _) | ("stock market", _) => OrderType::Market,
        ("stop market", _) | ("stop_market", _) => OrderType::StopMarket,
        ("stop limit", _) | ("stop_limit", _) => OrderType::StopLimit,
        ("maker_only", _) => OrderType::PostOnly,
        (_, Some(option)) if option == "maker_only" => OrderType::PostOnly,
        (_, Some(option)) if option == "ioc" => OrderType::IOC,
        (_, Some(option)) if option == "fok" => OrderType::FOK,
        ("ioc", _) => OrderType::IOC,
        ("fok", _) => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(option: Option<&str>) -> Option<TimeInForce> {
    match option?.trim().to_ascii_lowercase().as_str() {
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        "maker_only" => Some(TimeInForce::GTX),
        _ => Some(TimeInForce::GTC),
    }
}

fn map_whitebit_order_status(status: &str) -> OrderStatus {
    match status.trim().to_ascii_lowercase().as_str() {
        "new" | "open" | "not_deal" | "pending" => OrderStatus::New,
        "part_deal" | "partial_filled" | "partially_filled" => OrderStatus::PartiallyFilled,
        "done" | "filled" | "finished" => OrderStatus::Filled,
        "cancel" | "canceled" | "cancelled" | "partial_canceled" => OrderStatus::Cancelled,
        "expired" => OrderStatus::Expired,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(value_as_f64)
            .and_then(timestamp_to_utc)
    })
}

fn non_zero_string(value: String) -> Option<String> {
    if value.trim().is_empty() || value.trim() == "0" || value.trim() == "0.0" {
        None
    } else {
        Some(value)
    }
}

fn array_items(value: &Value) -> Option<&Vec<Value>> {
    value
        .get("data")
        .or_else(|| value.get("result"))
        .or_else(|| value.get("records"))
        .unwrap_or(value)
        .as_array()
}

fn parse_position(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<&ExchangeSymbol>,
    value: &Value,
) -> ExchangeApiResult<Option<ExchangePosition>> {
    let market = value
        .get("market")
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .or_else(|| symbol_hint.map(|symbol| symbol.symbol.as_str()))
        .ok_or_else(|| parse_error(exchange_id.clone(), "position missing market", value))?;
    let exchange_symbol = ExchangeSymbol::new(
        exchange_id.clone(),
        MarketType::Perpetual,
        normalize_whitebit_symbol(market)?,
    )
    .map_err(validation_error)?;
    let (base, quote) = exchange_symbol
        .symbol
        .strip_suffix("_PERP")
        .map(|base| (base.to_string(), "USDT".to_string()))
        .or_else(|| {
            exchange_symbol
                .symbol
                .split_once('_')
                .map(|(base, quote)| (base.to_string(), quote.to_string()))
        })
        .ok_or_else(|| parse_error(exchange_id.clone(), "position market missing assets", value))?;
    let quantity = decimal_value_to_f64(
        value
            .get("amount")
            .or_else(|| value.get("quantity"))
            .or_else(|| value.get("positionAmt")),
    )?
    .unwrap_or(0.0)
    .abs();
    if quantity == 0.0 {
        return Ok(None);
    }
    let side = parse_position_side(
        value.get("side").and_then(Value::as_str),
        MarketType::Perpetual,
    );
    let position = ExchangePosition {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: CanonicalSymbol::new(base, quote).map_err(validation_error)?,
        exchange_symbol: Some(exchange_symbol),
        side,
        quantity,
        entry_price: decimal_value_to_f64(
            value
                .get("basePrice")
                .or_else(|| value.get("entryPrice"))
                .or_else(|| value.get("entry_price")),
        )?,
        mark_price: decimal_value_to_f64(
            value
                .get("marketPrice")
                .or_else(|| value.get("markPrice"))
                .or_else(|| value.get("mark_price")),
        )?,
        liquidation_price: decimal_value_to_f64(
            value
                .get("liquidationPrice")
                .or_else(|| value.get("liquidation_price")),
        )?,
        unrealized_pnl: decimal_value_to_f64(
            value
                .get("unrealizedPnl")
                .or_else(|| value.get("unrealized_pnl"))
                .or_else(|| value.get("pnl")),
        )?,
        leverage: decimal_value_to_f64(value.get("leverage"))?,
        observed_at: Utc::now(),
    };
    position.validate().map_err(validation_error)?;
    Ok(Some(position))
}

fn parse_position_side(side: Option<&str>, market_type: MarketType) -> PositionSide {
    match side
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "long" | "buy" => PositionSide::Long,
        "short" | "sell" => PositionSide::Short,
        "both" if market_type == MarketType::Perpetual => PositionSide::Net,
        _ if market_type == MarketType::Spot => PositionSide::None,
        _ => PositionSide::Net,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|value| value as f64))
        .or_else(|| value.as_str()?.parse().ok())
}

fn timestamp_to_utc(timestamp: f64) -> Option<DateTime<Utc>> {
    if timestamp > 10_000_000_000.0 {
        DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
    } else {
        let seconds = timestamp.trunc() as i64;
        let nanos = ((timestamp.fract()) * 1_000_000_000.0) as u32;
        DateTime::<Utc>::from_timestamp(seconds, nanos)
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
