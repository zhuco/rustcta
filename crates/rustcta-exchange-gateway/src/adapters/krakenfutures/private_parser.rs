use chrono::Utc;
use rustcta_exchange_api::{FeeRateSnapshot, OrderState, SymbolScope, EXCHANGE_API_SCHEMA_VERSION};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, Fill,
    FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    TenantId,
};
use serde_json::Value;

use super::parser::{
    canonical_from_futures_symbol, canonical_from_pair, f64_from_value, normalize_asset,
    parse_error, reject_krakenfutures_error_response, string_or_number, timestamp_from_value,
    validation_error,
};
use rustcta_exchange_api::ExchangeApiResult;

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    assets_filter: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    reject_krakenfutures_error_response(exchange_id, value)?;
    let object = value
        .get("accounts")
        .or_else(|| value.get("balances"))
        .unwrap_or(value)
        .as_object()
        .ok_or_else(|| parse_error(exchange_id.clone(), "Kraken balance missing object", value))?;
    let wanted = assets_filter
        .iter()
        .map(|asset| asset.to_ascii_uppercase())
        .collect::<std::collections::HashSet<_>>();
    let balances = object
        .iter()
        .filter_map(|(asset, row)| {
            let asset = normalize_asset(asset);
            if !wanted.is_empty() && !wanted.contains(&asset) {
                return None;
            }
            parse_asset_balance(&asset, row).transpose()
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(vec![ExchangeBalance {
        schema_version: rustcta_types::SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        balances,
        observed_at: Utc::now(),
    }])
}

fn parse_asset_balance(asset: &str, value: &Value) -> ExchangeApiResult<Option<AssetBalance>> {
    if let Some(total) = value.as_str().and_then(|text| text.parse::<f64>().ok()) {
        return Ok(Some(
            AssetBalance::new(asset, total, total, 0.0).map_err(validation_error)?,
        ));
    }
    let total = value
        .get("balance")
        .or_else(|| value.get("total"))
        .and_then(f64_from_value)
        .unwrap_or(0.0);
    let locked = value
        .get("hold_trade")
        .or_else(|| value.get("hold"))
        .and_then(f64_from_value)
        .unwrap_or(0.0);
    if total == 0.0 && locked == 0.0 {
        return Ok(None);
    }
    Ok(Some(
        AssetBalance::new(asset, total, (total - locked).max(0.0), locked)
            .map_err(validation_error)?,
    ))
}

pub fn parse_fee_snapshots(
    _exchange_id: &ExchangeId,
    symbols: &[SymbolScope],
    value: &Value,
) -> Vec<FeeRateSnapshot> {
    symbols
        .iter()
        .map(|symbol| {
            let pair_key = symbol.exchange_symbol.symbol.as_str();
            let fee_row = value
                .get("fees")
                .and_then(Value::as_object)
                .and_then(|fees| fees.get(pair_key).or_else(|| fees.values().next()));
            let maker = fee_row
                .and_then(|row| row.get("fee_maker").or_else(|| row.get("maker")))
                .and_then(f64_from_value)
                .map(|value| value / 100.0)
                .unwrap_or(0.0016);
            let taker = fee_row
                .and_then(|row| row.get("fee").or_else(|| row.get("taker")))
                .and_then(f64_from_value)
                .map(|value| value / 100.0)
                .unwrap_or(0.0026);
            FeeRateSnapshot {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: symbol.clone(),
                maker_rate: maker.to_string(),
                taker_rate: taker.to_string(),
                source: Some(if fee_row.is_some() {
                    "krakenfutures.trade_volume".to_string()
                } else {
                    "krakenfutures.default_fallback".to_string()
                }),
                updated_at: Utc::now(),
            }
        })
        .collect()
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    order_id: Option<&str>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    reject_krakenfutures_error_response(exchange_id, value)?;
    let row = if let Some(object) = value.as_object() {
        if object.contains_key("descr") || object.contains_key("status") {
            value
        } else {
            object.iter().next().map(|(_, row)| row).ok_or_else(|| {
                parse_error(exchange_id.clone(), "Kraken order missing row", value)
            })?
        }
    } else {
        return Err(parse_error(
            exchange_id.clone(),
            "Kraken order response is not object",
            value,
        ));
    };
    let descr = row.get("descr").unwrap_or(row);
    let symbol = descr
        .get("pair")
        .or_else(|| row.get("symbol"))
        .and_then(Value::as_str)
        .and_then(|pair| {
            if fallback_symbol.is_some_and(|symbol| symbol.market_type == MarketType::Perpetual) {
                canonical_from_futures_symbol(pair)
            } else {
                canonical_from_pair(pair).ok()
            }
        })
        .or_else(|| fallback_symbol.and_then(|symbol| symbol.canonical_symbol.clone()))
        .unwrap_or_else(|| CanonicalSymbol::new("BTC", "USD").expect("fallback canonical"));
    let market_type = fallback_symbol
        .map(|symbol| symbol.market_type)
        .unwrap_or(MarketType::Spot);
    let exchange_symbol = fallback_symbol
        .map(|symbol| symbol.exchange_symbol.clone())
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Kraken order parser requires fallback symbol for exchange_symbol",
                value,
            )
        })?;
    let side = parse_side(
        descr
            .get("type")
            .or_else(|| row.get("side"))
            .and_then(Value::as_str),
    );
    let order_type = parse_order_type(
        descr
            .get("ordertype")
            .or_else(|| row.get("orderType"))
            .and_then(Value::as_str),
    );
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(symbol),
        exchange_symbol,
        client_order_id: string_or_number(row.get("cl_ord_id").or_else(|| row.get("cliOrdId"))),
        exchange_order_id: order_id
            .map(str::to_string)
            .or_else(|| string_or_number(row.get("txid").or_else(|| row.get("order_id")))),
        side,
        position_side: Some(PositionSide::None),
        order_type,
        time_in_force: None,
        status: parse_status(row.get("status").and_then(Value::as_str)),
        quantity: string_or_number(row.get("vol").or_else(|| row.get("size")))
            .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(
            descr
                .get("price")
                .or_else(|| row.get("limitPrice"))
                .or_else(|| row.get("price")),
        ),
        filled_quantity: string_or_number(row.get("vol_exec").or_else(|| row.get("filled")))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(row.get("price").or_else(|| row.get("avgPrice"))),
        reduce_only: row
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: matches!(order_type, OrderType::PostOnly),
        created_at: row
            .get("opentm")
            .or_else(|| row.get("receivedTime"))
            .and_then(timestamp_from_value),
        updated_at: row
            .get("closetm")
            .or_else(|| row.get("lastUpdateTime"))
            .and_then(timestamp_from_value)
            .unwrap_or_else(Utc::now),
    })
}

pub fn ack_order_state(
    exchange_id: &ExchangeId,
    request: &rustcta_exchange_api::PlaceOrderRequest,
    exchange_order_id: Option<String>,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id,
        side: request.side,
        position_side: request.position_side,
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only || request.order_type == OrderType::PostOnly,
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
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
        time_in_force: None,
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

pub fn parse_order_list(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    reject_krakenfutures_error_response(exchange_id, value)?;
    let object = value
        .get("open")
        .or_else(|| value.get("orders"))
        .or_else(|| value.get("elements"))
        .unwrap_or(value);
    if let Some(map) = object.as_object() {
        return map
            .iter()
            .map(|(order_id, row)| {
                parse_order_state(exchange_id, fallback_symbol, Some(order_id), row)
            })
            .collect();
    }
    let rows = object.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Kraken order list missing object or array",
            value,
        )
    })?;
    rows.iter()
        .map(|row| {
            let order_id = string_or_number(row.get("order_id").or_else(|| row.get("orderId")));
            parse_order_state(exchange_id, fallback_symbol, order_id.as_deref(), row)
        })
        .collect()
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    reject_krakenfutures_error_response(exchange_id, value)?;
    let object = value
        .get("trades")
        .or_else(|| value.get("fills"))
        .or_else(|| value.get("elements"))
        .unwrap_or(value);
    if let Some(map) = object.as_object() {
        return map
            .iter()
            .filter_map(|(fill_id, row)| {
                parse_fill(
                    exchange_id,
                    tenant_id.clone(),
                    account_id.clone(),
                    fallback_symbol,
                    Some(fill_id),
                    row,
                )
                .transpose()
            })
            .collect();
    }
    let rows = object.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Kraken fills missing object or array",
            value,
        )
    })?;
    rows.iter()
        .filter_map(|row| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                fallback_symbol,
                None,
                row,
            )
            .transpose()
        })
        .collect()
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    fill_id: Option<&str>,
    row: &Value,
) -> ExchangeApiResult<Option<Fill>> {
    let market_type = fallback_symbol
        .map(|symbol| symbol.market_type)
        .unwrap_or(MarketType::Spot);
    let canonical = row
        .get("pair")
        .or_else(|| row.get("symbol"))
        .and_then(Value::as_str)
        .and_then(|symbol| {
            if market_type == MarketType::Perpetual {
                canonical_from_futures_symbol(symbol).or_else(|| canonical_from_pair(symbol).ok())
            } else {
                canonical_from_pair(symbol)
                    .ok()
                    .or_else(|| canonical_from_futures_symbol(symbol))
            }
        })
        .or_else(|| fallback_symbol.and_then(|symbol| symbol.canonical_symbol.clone()));
    let Some(canonical) = canonical else {
        return Ok(None);
    };
    let price = row.get("price").and_then(f64_from_value).unwrap_or(0.0);
    let quantity = row
        .get("vol")
        .or_else(|| row.get("size"))
        .or_else(|| row.get("quantity"))
        .and_then(f64_from_value)
        .unwrap_or(0.0);
    let fill = Fill {
        schema_version: rustcta_types::SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        canonical_symbol: canonical,
        exchange_symbol: fallback_symbol.map(|symbol| symbol.exchange_symbol.clone()),
        order_id: string_or_number(row.get("ordertxid").or_else(|| row.get("order_id"))),
        client_order_id: string_or_number(row.get("cl_ord_id").or_else(|| row.get("cliOrdId"))),
        fill_id: fill_id
            .map(str::to_string)
            .or_else(|| string_or_number(row.get("trade_id").or_else(|| row.get("fill_id")))),
        side: parse_side(
            row.get("type")
                .or_else(|| row.get("side"))
                .and_then(Value::as_str),
        ),
        position_side: PositionSide::None,
        status: FillStatus::Confirmed,
        liquidity_role: match row.get("maker").and_then(Value::as_bool) {
            Some(true) => LiquidityRole::Maker,
            Some(false) => LiquidityRole::Taker,
            None => LiquidityRole::Unknown,
        },
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: None,
        fee_amount: row.get("fee").and_then(f64_from_value),
        fee_rate: None,
        realized_pnl: row.get("pnl").and_then(f64_from_value),
        filled_at: row
            .get("time")
            .or_else(|| row.get("fillTime"))
            .and_then(timestamp_from_value)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    };
    fill.validate().map_err(validation_error)?;
    Ok(Some(fill))
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbols_filter: &[rustcta_types::ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    reject_krakenfutures_error_response(exchange_id, value)?;
    let rows = value
        .get("openPositions")
        .or_else(|| value.get("positions"))
        .or_else(|| value.get("elements"))
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "Kraken positions missing array", value))?;
    let wanted = symbols_filter
        .iter()
        .map(|symbol| symbol.symbol.to_ascii_uppercase())
        .collect::<std::collections::HashSet<_>>();
    rows.iter()
        .filter_map(|row| {
            let symbol_text = row
                .get("symbol")
                .or_else(|| row.get("instrument"))
                .and_then(Value::as_str)?;
            if !wanted.is_empty() && !wanted.contains(&symbol_text.to_ascii_uppercase()) {
                return None;
            }
            let canonical = canonical_from_futures_symbol(symbol_text)?;
            let quantity = row
                .get("size")
                .or_else(|| row.get("quantity"))
                .and_then(f64_from_value)
                .unwrap_or(0.0)
                .abs();
            let position = ExchangePosition {
                schema_version: rustcta_types::SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol: canonical,
                exchange_symbol: rustcta_types::ExchangeSymbol::new(
                    exchange_id.clone(),
                    MarketType::Perpetual,
                    symbol_text,
                )
                .ok(),
                side: parse_position_side(row),
                quantity,
                entry_price: row
                    .get("price")
                    .or_else(|| row.get("entryPrice"))
                    .and_then(f64_from_value),
                mark_price: row.get("markPrice").and_then(f64_from_value),
                liquidation_price: row.get("liquidationPrice").and_then(f64_from_value),
                unrealized_pnl: row
                    .get("unrealizedFunding")
                    .or_else(|| row.get("pnl"))
                    .and_then(f64_from_value),
                leverage: row.get("leverage").and_then(f64_from_value),
                observed_at: Utc::now(),
            };
            Some(position)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|position| {
            position.validate().map_err(validation_error)?;
            Ok(position)
        })
        .collect()
}

fn parse_position_side(row: &Value) -> PositionSide {
    if let Some(side) = row.get("side").and_then(Value::as_str) {
        return match side.to_ascii_lowercase().as_str() {
            "long" | "buy" => PositionSide::Long,
            "short" | "sell" => PositionSide::Short,
            _ => PositionSide::Net,
        };
    }
    match row
        .get("size")
        .or_else(|| row.get("quantity"))
        .and_then(f64_from_value)
        .unwrap_or(0.0)
        .total_cmp(&0.0)
    {
        std::cmp::Ordering::Less => PositionSide::Short,
        std::cmp::Ordering::Greater => PositionSide::Long,
        std::cmp::Ordering::Equal => PositionSide::Net,
    }
}

fn parse_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or("buy").to_ascii_lowercase().as_str() {
        "sell" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or("limit").to_ascii_lowercase().as_str() {
        "market" | "mkt" => OrderType::Market,
        "post" => OrderType::PostOnly,
        "ioc" => OrderType::IOC,
        "fok" => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or("unknown").to_ascii_lowercase().as_str() {
        "pending" => OrderStatus::New,
        "open" | "untouched" => OrderStatus::Open,
        "partial" | "partially_filled" => OrderStatus::PartiallyFilled,
        "closed" | "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "expired" => OrderStatus::Expired,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}
