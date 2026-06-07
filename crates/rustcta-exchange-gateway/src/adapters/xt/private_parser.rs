use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderStatus, OrderType, SchemaVersion,
    TimeInForce,
};
use serde_json::Value;

use super::parser::{
    data_payload, decimal_as_f64, first_timestamp_millis, parse_error, parse_position_side,
    parse_side, required_str, split_xt_symbol, string_or_number, validation_error, value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let data = data_payload(value);
    let items = data
        .get("assets")
        .or_else(|| data.get("balances"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .unwrap_or_else(|| std::slice::from_ref(data));
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for item in items {
        let asset = item
            .get("currency")
            .or_else(|| item.get("coin"))
            .and_then(Value::as_str)
            .unwrap_or("USDT")
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_as_f64(
            item.get("availableAmount")
                .or_else(|| item.get("availableBalance")),
        )
        .unwrap_or(0.0);
        let locked = decimal_as_f64(
            item.get("frozenAmount")
                .or_else(|| item.get("openOrderMarginFrozen"))
                .or_else(|| item.get("isolatedMargin")),
        )
        .unwrap_or(0.0);
        let total = decimal_as_f64(
            item.get("totalAmount")
                .or_else(|| item.get("walletBalance"))
                .or_else(|| item.get("balance")),
        )
        .unwrap_or(available + locked);
        if total > 0.0 || available > 0.0 || locked > 0.0 || !requested.is_empty() {
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

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let data = data_payload(value);
    let positions = data
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(data));
    let mut output = Vec::new();
    for position in positions {
        let symbol = required_str(exchange_id, position, "symbol")?.to_ascii_lowercase();
        let (base, quote) =
            split_xt_symbol(&symbol).unwrap_or_else(|| ("unknown".to_string(), "usdt".to_string()));
        let quantity = decimal_as_f64(
            position
                .get("positionSize")
                .or_else(|| position.get("availableCloseSize")),
        )
        .unwrap_or(0.0)
        .abs();
        if quantity == 0.0 {
            continue;
        }
        let canonical_symbol =
            CanonicalSymbol::new(base.to_ascii_uppercase(), quote.to_ascii_uppercase())
                .map_err(validation_error)?;
        let exchange_symbol =
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol)
                .map_err(validation_error)?;
        output.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol,
            exchange_symbol: Some(exchange_symbol),
            side: parse_position_side(position.get("positionSide").and_then(Value::as_str)),
            quantity,
            entry_price: decimal_as_f64(position.get("entryPrice")),
            mark_price: decimal_as_f64(position.get("markPrice")),
            liquidation_price: None,
            unrealized_pnl: decimal_as_f64(
                position
                    .get("unrealizedProfit")
                    .or_else(|| position.get("realizedProfit")),
            ),
            leverage: decimal_as_f64(position.get("leverage")),
            observed_at: Utc::now(),
        });
    }
    Ok(output)
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    symbols: &[SymbolScope],
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let data = data_payload(value);
    let items = data
        .get("symbols")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .unwrap_or_else(|| std::slice::from_ref(data));
    let requested = symbols
        .iter()
        .map(|symbol| {
            (
                symbol.exchange_symbol.symbol.to_ascii_lowercase(),
                symbol.clone(),
            )
        })
        .collect::<Vec<_>>();
    let mut fees = Vec::new();
    for item in items {
        let symbol = if let Some(symbol) = item.get("symbol").and_then(Value::as_str) {
            requested
                .iter()
                .find(|(requested, _)| requested == &symbol.to_ascii_lowercase())
                .map(|(_, scope)| scope.clone())
                .or_else(|| symbol_from_payload(exchange_id, market_type, item).ok())
        } else {
            symbols.first().cloned()
        };
        let Some(symbol) = symbol else {
            continue;
        };
        fees.push(FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: string_or_number(
                item.get("makerFeeRate")
                    .or_else(|| item.get("makerFee"))
                    .or_else(|| data.get("makerFee")),
            )
            .unwrap_or_else(|| "0".to_string()),
            taker_rate: string_or_number(
                item.get("takerFeeRate")
                    .or_else(|| item.get("takerFee"))
                    .or_else(|| data.get("takerFee")),
            )
            .unwrap_or_else(|| "0".to_string()),
            source: Some(if market_type == MarketType::Spot {
                "xt.spot.symbol_fee".to_string()
            } else {
                "xt.futures.symbol_fee".to_string()
            }),
            updated_at: Utc::now(),
        });
        if !requested.is_empty() && fees.len() == requested.len() {
            break;
        }
    }
    Ok(fees)
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let data = data_payload(value);
    let order = data
        .get("order")
        .unwrap_or(data)
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(data);
    if order.is_null() {
        return Ok(None);
    }
    Ok(Some(parse_order_state(
        exchange_id,
        fallback_symbol,
        market_type,
        order,
    )?))
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let data = data_payload(value);
    let orders = data
        .get("items")
        .or_else(|| data.get("orders"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "XT orders response missing list",
                value,
            )
        })?;
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, fallback_symbol, market_type, order))
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let data = data_payload(value);
    let fills = data
        .get("items")
        .or_else(|| data.get("fills"))
        .or_else(|| data.get("trades"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .ok_or_else(|| parse_error(exchange_id.clone(), "XT fills response missing list", value))?;
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

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_from_payload(exchange_id, market_type, value))?;
    let raw_type = value
        .get("type")
        .or_else(|| value.get("orderType"))
        .and_then(Value::as_str)
        .unwrap_or("LIMIT");
    let tif = value
        .get("timeInForce")
        .and_then(Value::as_str)
        .map(parse_time_in_force);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value_as_string(value.get("clientOrderId")),
        exchange_order_id: value_as_string(value.get("orderId").or_else(|| value.get("id"))),
        side: parse_side(
            exchange_id,
            required_side(exchange_id, value, &["side", "orderSide"])?,
        )?,
        position_side: Some(parse_position_side(
            value.get("positionSide").and_then(Value::as_str),
        )),
        order_type: parse_order_type(raw_type, tif),
        time_in_force: tif,
        status: value
            .get("state")
            .and_then(Value::as_str)
            .map(map_xt_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(
            value
                .get("origQty")
                .or_else(|| value.get("quantity"))
                .or_else(|| value.get("executedQty")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")).filter(|value| !is_zero_decimal(value)),
        filled_quantity: string_or_number(value.get("executedQty"))
            .or_else(|| string_or_number(value.get("tradeBase")))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("avgPrice"))
            .filter(|value| !is_zero_decimal(value)),
        reduce_only: false,
        post_only: matches!(tif, Some(TimeInForce::GTX)),
        created_at: first_timestamp_millis(value, &["time", "createdTime", "createTime"]),
        updated_at: first_timestamp_millis(value, &["updatedTime", "updateTime", "time"])
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
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_from_payload(exchange_id, market_type, value))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "XT fill requires canonical_symbol".to_string(),
            })?;
    let price = decimal_as_f64(value.get("price")).unwrap_or(0.0);
    let quantity =
        decimal_as_f64(value.get("quantity").or_else(|| value.get("qty"))).unwrap_or(0.0);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: value_as_string(value.get("orderId")),
        client_order_id: value_as_string(value.get("clientOrderId")),
        fill_id: value_as_string(value.get("tradeId").or_else(|| value.get("execId"))),
        side: parse_side(
            exchange_id,
            required_side(exchange_id, value, &["orderSide", "side"])?,
        )?,
        position_side: parse_position_side(value.get("positionSide").and_then(Value::as_str)),
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity(value.get("takerMaker")),
        price,
        quantity,
        quote_quantity: decimal_as_f64(value.get("quoteQty").or_else(|| value.get("tradeQuote")))
            .or_else(|| (price > 0.0 && quantity > 0.0).then_some(price * quantity)),
        fee_asset: value_as_string(value.get("feeCurrency").or_else(|| value.get("feeCoin"))),
        fee_amount: decimal_as_f64(value.get("fee")),
        fee_rate: None,
        realized_pnl: decimal_as_f64(value.get("realizedPnl")),
        filled_at: first_timestamp_millis(value, &["time", "timestamp"]).unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn symbol_from_payload(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let exchange_symbol = required_str(exchange_id, value, "symbol")?.to_ascii_lowercase();
    let (base, quote) = split_xt_symbol(&exchange_symbol)
        .unwrap_or_else(|| ("unknown".to_string(), "usdt".to_string()));
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(
            CanonicalSymbol::new(base.to_ascii_uppercase(), quote.to_ascii_uppercase())
                .map_err(validation_error)?,
        ),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    })
}

fn required_side<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    fields: &[&str],
) -> ExchangeApiResult<&'a str> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(Value::as_str))
        .ok_or_else(|| parse_error(exchange_id.clone(), "XT order missing side", value))
}

fn parse_order_type(raw_type: &str, tif: Option<TimeInForce>) -> OrderType {
    match raw_type.to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        "STOP_MARKET" | "TAKE_PROFIT_MARKET" | "TRIGGER_MARKET" => OrderType::StopMarket,
        "STOP" | "TAKE_PROFIT" | "TRIGGER_LIMIT" => OrderType::StopLimit,
        _ => match tif {
            Some(TimeInForce::IOC) => OrderType::IOC,
            Some(TimeInForce::FOK) => OrderType::FOK,
            Some(TimeInForce::GTX) => OrderType::PostOnly,
            _ => OrderType::Limit,
        },
    }
}

fn parse_time_in_force(value: &str) -> TimeInForce {
    match value.to_ascii_uppercase().as_str() {
        "IOC" => TimeInForce::IOC,
        "FOK" => TimeInForce::FOK,
        "GTX" | "POSTONLY" | "POST_ONLY" => TimeInForce::GTX,
        _ => TimeInForce::GTC,
    }
}

fn map_xt_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_uppercase().as_str() {
        "NEW" | "UNFINISHED" => OrderStatus::New,
        "PARTIALLY_FILLED" | "PARTIALLY_CANCELED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" | "USER_REVOCATION" => OrderStatus::Cancelled,
        "FAILED" | "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity(value: Option<&Value>) -> LiquidityRole {
    match value
        .and_then(Value::as_str)
        .map(str::to_ascii_uppercase)
        .as_deref()
    {
        Some("MAKER") => LiquidityRole::Maker,
        Some("TAKER") => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn is_zero_decimal(value: &str) -> bool {
    value.trim().trim_matches('0').trim_matches('.').is_empty()
}
