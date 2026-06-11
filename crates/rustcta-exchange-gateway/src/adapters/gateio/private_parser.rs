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

use super::parser::{normalize_gateio_symbol, split_gateio_pair};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let items = value
        .as_array()
        .map(|items| items.iter().collect::<Vec<_>>())
        .unwrap_or_else(|| vec![value]);
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for item in items {
        let asset = item
            .get("currency")
            .or_else(|| item.get("asset"))
            .and_then(Value::as_str)
            .unwrap_or("USDT")
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_value_to_f64(
            item.get("available")
                .or_else(|| item.get("available_balance"))
                .or_else(|| item.get("cross_available")),
        )?
        .unwrap_or(0.0);
        let total = decimal_value_to_f64(
            item.get("total")
                .or_else(|| item.get("total_balance"))
                .or_else(|| item.get("cross_margin_balance")),
        )?
        .unwrap_or_else(|| {
            let locked = decimal_value_to_f64(item.get("locked"))
                .ok()
                .flatten()
                .unwrap_or(0.0);
            available + locked
        });
        let locked = decimal_value_to_f64(item.get("locked"))?
            .or_else(|| {
                let position_margin = decimal_value_to_f64(item.get("position_margin"))
                    .ok()
                    .flatten()
                    .unwrap_or(0.0);
                let order_margin = decimal_value_to_f64(item.get("order_margin"))
                    .ok()
                    .flatten()
                    .unwrap_or(0.0);
                (position_margin + order_margin > 0.0).then_some(position_margin + order_margin)
            })
            .unwrap_or_else(|| (total - available).max(0.0));
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
        market_type,
        balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    parse_order_state(exchange_id, fallback_symbol, market_type, value)
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let items = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Gate.io open orders response is not an array",
            value,
        )
    })?;
    let mut orders = Vec::new();
    for item in items {
        if let Some(nested) = item.get("orders").and_then(Value::as_array) {
            for order in nested {
                orders.push(parse_order_state(
                    exchange_id,
                    fallback_symbol,
                    market_type,
                    order,
                )?);
            }
        } else {
            orders.push(parse_order_state(
                exchange_id,
                fallback_symbol,
                market_type,
                item,
            )?);
        }
    }
    Ok(orders)
}

pub fn parse_fees(
    exchange_id: &ExchangeId,
    fallback_symbol: &SymbolScope,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<FeeRateSnapshot> {
    let (maker_key, taker_key, source) = match market_type {
        MarketType::Perpetual => (
            "maker_fee_rate",
            "taker_fee_rate",
            "gateio.futures.contract",
        ),
        _ => ("maker_fee", "taker_fee", "gateio.spot.fee"),
    };
    let maker_rate = string_or_number(value.get(maker_key)).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("Gate.io fee response missing {maker_key}"),
            value,
        )
    })?;
    let taker_rate = string_or_number(value.get(taker_key)).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("Gate.io fee response missing {taker_key}"),
            value,
        )
    })?;
    Ok(FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: fallback_symbol.clone(),
        maker_rate,
        taker_rate,
        source: Some(source.to_string()),
        updated_at: Utc::now(),
    })
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let fills = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Gate.io fills response is not an array",
            value,
        )
    })?;
    fills
        .iter()
        .map(|fill| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                fallback_symbol,
                market_type,
                fill,
            )
        })
        .collect()
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let items = value
        .as_array()
        .map(|items| items.iter().collect::<Vec<_>>())
        .unwrap_or_else(|| vec![value]);
    let mut positions = Vec::new();
    for item in items {
        let symbol_text = required_str(exchange_id, item, "contract")?.to_ascii_uppercase();
        let (base, quote) = split_gateio_pair(&symbol_text)?;
        let canonical_symbol = CanonicalSymbol::new(base, quote).map_err(validation_error)?;
        let signed_size = gateio_signed_size(item);
        let quantity = signed_size.abs();
        if quantity == 0.0 {
            continue;
        }
        let side = gateio_side_from_signed(item, signed_size);
        positions.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol_text)
                    .map_err(validation_error)?,
            ),
            side: gateio_position_side(item, signed_size, side),
            quantity,
            entry_price: decimal_value_to_f64(item.get("entry_price"))?,
            mark_price: decimal_value_to_f64(item.get("mark_price"))?,
            liquidation_price: decimal_value_to_f64(item.get("liq_price"))?,
            unrealized_pnl: decimal_value_to_f64(
                item.get("unrealised_pnl")
                    .or_else(|| item.get("unrealized_pnl")),
            )?,
            leverage: decimal_value_to_f64(item.get("leverage"))?,
            observed_at: Utc::now(),
        });
    }
    Ok(positions)
}

fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_scope(exchange_id, fallback_symbol, market_type, value)?;
    let signed_size = gateio_signed_size(value);
    let quantity = match market_type {
        MarketType::Perpetual => decimal_to_string(signed_size.abs()),
        _ => string_or_number(value.get("amount")).unwrap_or_else(|| "0".to_string()),
    };
    let left = decimal_value_to_f64(value.get("left"))?.map(|value| value.abs());
    let quantity_number = decimal_text_to_f64(&quantity)?;
    let filled_quantity = left
        .map(|left| (quantity_number - left).max(0.0).to_string())
        .or_else(|| string_or_number(value.get("filled_total")))
        .unwrap_or_else(|| "0".to_string());
    let side = value
        .get("side")
        .and_then(Value::as_str)
        .map(|side| parse_side(exchange_id, side))
        .transpose()?
        .unwrap_or_else(|| gateio_side_from_signed(value, signed_size));
    let order_type = parse_order_type(
        market_type,
        value.get("type").and_then(Value::as_str).unwrap_or("limit"),
        value
            .get("time_in_force")
            .or_else(|| value.get("tif"))
            .and_then(Value::as_str),
        value.get("price"),
    );

    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value_as_string(value.get("text")).filter(|value| !value.is_empty()),
        exchange_order_id: value_as_string(value.get("id")).filter(|value| !value.is_empty()),
        side,
        position_side: Some(match market_type {
            MarketType::Perpetual => gateio_position_side(value, signed_size, side),
            _ => PositionSide::None,
        }),
        order_type,
        time_in_force: parse_time_in_force(
            value
                .get("time_in_force")
                .or_else(|| value.get("tif"))
                .and_then(Value::as_str),
        ),
        status: value
            .get("finish_as")
            .or_else(|| value.get("status"))
            .and_then(Value::as_str)
            .map(|status| map_gateio_order_status(status, left, &filled_quantity))
            .unwrap_or(OrderStatus::Unknown),
        quantity,
        price: string_or_number(value.get("price")).filter(|value| !is_zero_decimal(value)),
        filled_quantity,
        average_fill_price: string_or_number(
            value
                .get("avg_deal_price")
                .or_else(|| value.get("fill_price")),
        )
        .filter(|value| !is_zero_decimal(value)),
        reduce_only: bool_from_value(
            value
                .get("is_reduce_only")
                .or_else(|| value.get("reduce_only")),
        ),
        post_only: matches!(order_type, OrderType::PostOnly),
        created_at: first_timestamp(value, &["create_time_ms", "create_time"]),
        updated_at: first_timestamp(value, &["update_time_ms", "update_time"])
            .unwrap_or_else(Utc::now),
    })
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = symbol_scope(exchange_id, fallback_symbol, market_type, value)?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "gateio recent fills require canonical_symbol".to_string(),
            })?;
    let price = decimal_value_to_f64(value.get("price"))?.unwrap_or(0.0);
    let signed_size = gateio_signed_size(value);
    let quantity = match market_type {
        MarketType::Perpetual => signed_size.abs(),
        _ => decimal_value_to_f64(value.get("amount"))?.unwrap_or(0.0),
    };
    let quote_quantity = (price > 0.0 && quantity > 0.0).then_some(price * quantity);
    let side = value
        .get("side")
        .and_then(Value::as_str)
        .map(|side| parse_side(exchange_id, side))
        .transpose()?
        .unwrap_or_else(|| gateio_side_from_signed(value, signed_size));
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: value_as_string(value.get("order_id").or_else(|| value.get("order")))
            .filter(|value| !value.is_empty()),
        client_order_id: value_as_string(value.get("text")).filter(|value| !value.is_empty()),
        fill_id: value_as_string(value.get("trade_id").or_else(|| value.get("id")))
            .filter(|value| !value.is_empty()),
        side,
        position_side: match market_type {
            MarketType::Perpetual => gateio_position_side(value, signed_size, side),
            _ => PositionSide::None,
        },
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity_role(value.get("role").and_then(Value::as_str)),
        price,
        quantity,
        quote_quantity,
        fee_asset: value_as_string(value.get("fee_currency")).filter(|value| !value.is_empty()),
        fee_amount: decimal_value_to_f64(value.get("fee"))?,
        fee_rate: None,
        realized_pnl: decimal_value_to_f64(
            value
                .get("realised_pnl")
                .or_else(|| value.get("realized_pnl")),
        )?,
        filled_at: first_timestamp(value, &["create_time_ms", "create_time"])
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn symbol_scope(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = fallback_symbol {
        return Ok(symbol.clone());
    }
    let symbol = match market_type {
        MarketType::Perpetual => required_str(exchange_id, value, "contract")
            .or_else(|_| required_str(exchange_id, value, "currency_pair"))
            .or_else(|_| required_str(exchange_id, value, "symbol"))?,
        _ => required_str(exchange_id, value, "currency_pair")
            .or_else(|_| required_str(exchange_id, value, "symbol"))
            .or_else(|_| required_str(exchange_id, value, "contract"))?,
    };
    let normalized = normalize_gateio_symbol(symbol)?;
    let canonical_symbol = split_gateio_pair(&normalized)
        .ok()
        .and_then(|(base, quote)| CanonicalSymbol::new(base, quote).ok());
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol,
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, normalized)
            .map_err(validation_error)?,
    })
}

fn gateio_signed_size(value: &Value) -> f64 {
    let quantity = decimal_value_to_f64(value.get("size").or_else(|| value.get("contracts")))
        .ok()
        .flatten()
        .unwrap_or_default();
    if quantity < 0.0 {
        return quantity;
    }
    match value.get("side").and_then(Value::as_str) {
        Some(side) if side.eq_ignore_ascii_case("sell") => -quantity.abs(),
        _ => quantity,
    }
}

fn gateio_side_from_signed(value: &Value, signed_size: f64) -> OrderSide {
    value
        .get("side")
        .and_then(Value::as_str)
        .and_then(|side| match side.to_ascii_lowercase().as_str() {
            "buy" => Some(OrderSide::Buy),
            "sell" => Some(OrderSide::Sell),
            _ => None,
        })
        .unwrap_or_else(|| {
            if signed_size < 0.0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            }
        })
}

fn gateio_position_side(value: &Value, signed_size: f64, side: OrderSide) -> PositionSide {
    value
        .get("position_side")
        .or_else(|| value.get("pos_side"))
        .and_then(Value::as_str)
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "long" | "buy" => Some(PositionSide::Long),
            "short" | "sell" => Some(PositionSide::Short),
            "net" | "both" => Some(PositionSide::Net),
            _ => None,
        })
        .filter(|position_side| *position_side != PositionSide::Net)
        .unwrap_or_else(|| {
            if signed_size < 0.0 || side == OrderSide::Sell {
                PositionSide::Short
            } else {
                PositionSide::Long
            }
        })
}

fn parse_side(exchange_id: &ExchangeId, value: &str) -> ExchangeApiResult<OrderSide> {
    match value.to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(parse_error(
            exchange_id.clone(),
            "invalid Gate.io order side",
            &Value::String(value.to_string()),
        )),
    }
}

fn parse_order_type(
    market_type: MarketType,
    order_type: &str,
    tif: Option<&str>,
    price: Option<&Value>,
) -> OrderType {
    if market_type == MarketType::Perpetual
        && price
            .and_then(|value| string_or_number(Some(value)))
            .is_some_and(|price| is_zero_decimal(&price))
    {
        return OrderType::Market;
    }
    match (
        order_type.to_ascii_lowercase().as_str(),
        tif.map(str::to_ascii_lowercase),
    ) {
        ("market", _) => OrderType::Market,
        (_, Some(tif)) if tif == "ioc" => OrderType::IOC,
        (_, Some(tif)) if tif == "fok" => OrderType::FOK,
        (_, Some(tif)) if tif == "poc" => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value?.to_ascii_lowercase().as_str() {
        "gtc" => Some(TimeInForce::GTC),
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        "poc" | "gtx" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn map_gateio_order_status(status: &str, left: Option<f64>, filled_quantity: &str) -> OrderStatus {
    let has_fill = !is_zero_decimal(filled_quantity);
    match status.to_ascii_lowercase().as_str() {
        "open" => {
            if has_fill {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::New
            }
        }
        "closed" | "finished" | "filled" => {
            if left.is_some_and(|left| left > 0.0) {
                OrderStatus::Cancelled
            } else {
                OrderStatus::Filled
            }
        }
        "cancelled" | "canceled" | "cancel" | "ioc" | "poc" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity_role(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" => LiquidityRole::Maker,
        "taker" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("missing Gate.io field {field}"),
            value,
        )
    })
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    value
        .and_then(|value| string_or_number(Some(value)))
        .map(|value| decimal_text_to_f64(&value).map(Some))
        .unwrap_or(Ok(None))
}

fn decimal_text_to_f64(value: &str) -> ExchangeApiResult<f64> {
    value
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid Gate.io decimal `{value}`: {error}"),
        })
}

fn decimal_to_string(value: f64) -> String {
    if value.fract() == 0.0 {
        return format!("{value:.0}");
    }
    let text = format!("{value:.12}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn bool_from_value(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(value)) => *value,
        Some(Value::String(value)) => matches!(value.to_ascii_lowercase().as_str(), "1" | "true"),
        Some(Value::Number(value)) => value.as_i64().is_some_and(|value| value != 0),
        _ => false,
    }
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|number| number == 0.0)
}

fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(gateio_timestamp))
}

fn gateio_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    let raw = match value {
        Value::String(text) => text.parse::<f64>().ok()?,
        Value::Number(number) => number.as_f64()?,
        _ => return None,
    };
    if raw > 1_000_000_000_000.0 {
        DateTime::<Utc>::from_timestamp_millis(raw as i64)
    } else {
        DateTime::<Utc>::from_timestamp_millis((raw * 1000.0) as i64)
    }
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    ExchangeApiError::Exchange(rustcta_types::ExchangeError {
        schema_version: SchemaVersion::current(),
        exchange_id,
        class: rustcta_types::ExchangeErrorClass::Decode,
        code: None,
        message: format!("{message}: {value}"),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    })
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
