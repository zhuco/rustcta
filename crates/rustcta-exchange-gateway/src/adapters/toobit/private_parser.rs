use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, ExchangePosition, Fill, FillStatus, LiquidityRole,
    MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    data_payload, decimal_as_f64, first_timestamp_millis, parse_error,
    symbol_scope_from_exchange_symbol, symbol_scope_from_payload, validation_error,
    value_as_string,
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
        .get("balances")
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
            .get("coin")
            .or_else(|| item.get("asset"))
            .or_else(|| item.get("marginCoin"))
            .and_then(Value::as_str)
            .unwrap_or("USDT")
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_as_f64(
            item.get("free")
                .or_else(|| item.get("availableBalance"))
                .or_else(|| item.get("available")),
        )
        .unwrap_or(0.0);
        let locked =
            decimal_as_f64(item.get("locked").or_else(|| item.get("frozen"))).unwrap_or(0.0);
        let total = decimal_as_f64(
            item.get("total")
                .or_else(|| item.get("balance"))
                .or_else(|| item.get("walletBalance")),
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
    let items = data
        .get("positions")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .unwrap_or_else(|| std::slice::from_ref(data));
    let mut output = Vec::new();
    for position in items {
        let symbol = position
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN")
            .to_ascii_uppercase();
        let scope = symbol_scope_from_exchange_symbol(exchange_id, MarketType::Perpetual, &symbol)?;
        let quantity = decimal_as_f64(
            position
                .get("positionAmt")
                .or_else(|| position.get("quantity"))
                .or_else(|| position.get("size"))
                .or_else(|| position.get("available")),
        )
        .unwrap_or(0.0)
        .abs();
        if quantity == 0.0 {
            continue;
        }
        let canonical_symbol =
            scope
                .canonical_symbol
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "toobit position requires canonical_symbol".to_string(),
                })?;
        output.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol,
            exchange_symbol: Some(scope.exchange_symbol),
            side: parse_position_side(position.get("positionSide").and_then(Value::as_str)),
            quantity,
            entry_price: decimal_as_f64(
                position
                    .get("entryPrice")
                    .or_else(|| position.get("avgPrice")),
            ),
            mark_price: decimal_as_f64(position.get("markPrice")),
            liquidation_price: decimal_as_f64(position.get("liquidationPrice")),
            unrealized_pnl: decimal_as_f64(
                position
                    .get("unrealizedProfit")
                    .or_else(|| position.get("unrealizedPnl")),
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
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(data));
    let mut fees = Vec::new();
    for item in items {
        let symbol = symbols
            .first()
            .cloned()
            .or_else(|| symbol_scope_from_payload(exchange_id, market_type, item).ok())
            .ok_or_else(|| parse_error(exchange_id.clone(), "fee response missing symbol", item))?;
        fees.push(FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: super::parser::string_or_number(
                item.get("makerCommission")
                    .or_else(|| item.get("makerFeeRate"))
                    .or_else(|| item.get("makerFee"))
                    .or_else(|| item.get("maker")),
            )
            .unwrap_or_else(|| "0".to_string()),
            taker_rate: super::parser::string_or_number(
                item.get("takerCommission")
                    .or_else(|| item.get("takerFeeRate"))
                    .or_else(|| item.get("takerFee"))
                    .or_else(|| item.get("taker")),
            )
            .unwrap_or_else(|| "0".to_string()),
            source: Some(if market_type == MarketType::Spot {
                "toobit.config_override".to_string()
            } else {
                "toobit.futures.commission_rate".to_string()
            }),
            updated_at: Utc::now(),
        });
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
        .get("orders")
        .or_else(|| data.get("success"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .ok_or_else(|| parse_error(exchange_id.clone(), "orders response missing list", value))?;
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
        .get("fills")
        .or_else(|| data.get("trades"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .ok_or_else(|| parse_error(exchange_id.clone(), "fills response missing list", value))?;
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
        .unwrap_or_else(|| symbol_scope_from_payload(exchange_id, market_type, value))?;
    let raw_type = value.get("type").and_then(Value::as_str).unwrap_or("LIMIT");
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
        client_order_id: value_as_string(
            value
                .get("clientOrderId")
                .or_else(|| value.get("origClientOrderId"))
                .or_else(|| value.get("c")),
        ),
        exchange_order_id: value_as_string(value.get("orderId").or_else(|| value.get("id"))),
        side: value
            .get("side")
            .or_else(|| value.get("S"))
            .and_then(Value::as_str)
            .map(|side| parse_side(exchange_id, side))
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        position_side: Some(parse_position_side(
            value.get("positionSide").and_then(Value::as_str),
        )),
        order_type: parse_order_type(raw_type, tif),
        time_in_force: tif,
        status: value
            .get("status")
            .or_else(|| value.get("X"))
            .and_then(Value::as_str)
            .map(map_toobit_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: super::parser::string_or_number(
            value
                .get("origQty")
                .or_else(|| value.get("quantity"))
                .or_else(|| value.get("q")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: super::parser::string_or_number(value.get("price").or_else(|| value.get("p")))
            .filter(|value| !is_zero_decimal(value)),
        filled_quantity: super::parser::string_or_number(
            value.get("executedQty").or_else(|| value.get("z")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: average_fill_price(value),
        reduce_only: value
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: matches!(tif, Some(TimeInForce::GTX))
            || raw_type.eq_ignore_ascii_case("LIMIT_MAKER"),
        created_at: first_timestamp_millis(value, &["time", "transactTime", "O"]),
        updated_at: first_timestamp_millis(value, &["updateTime", "E", "time"])
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
        .unwrap_or_else(|| symbol_scope_from_payload(exchange_id, market_type, value))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "toobit fill requires canonical_symbol".to_string(),
            })?;
    let price = decimal_as_f64(value.get("price").or_else(|| value.get("p"))).unwrap_or(0.0);
    let quantity = decimal_as_f64(
        value
            .get("qty")
            .or_else(|| value.get("quantity"))
            .or_else(|| value.get("q")),
    )
    .unwrap_or(0.0);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: value_as_string(value.get("orderId").or_else(|| value.get("o"))),
        client_order_id: value_as_string(value.get("clientOrderId").or_else(|| value.get("c"))),
        fill_id: value_as_string(
            value
                .get("id")
                .or_else(|| value.get("ticketId"))
                .or_else(|| value.get("T")),
        ),
        side: value
            .get("side")
            .or_else(|| value.get("S"))
            .and_then(Value::as_str)
            .map(|side| parse_side(exchange_id, side))
            .transpose()?
            .or_else(|| {
                value
                    .get("isBuyer")
                    .and_then(Value::as_bool)
                    .map(|is_buyer| {
                        if is_buyer {
                            OrderSide::Buy
                        } else {
                            OrderSide::Sell
                        }
                    })
            })
            .unwrap_or(OrderSide::Buy),
        position_side: parse_position_side(value.get("positionSide").and_then(Value::as_str)),
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity(value.get("isMaker").or_else(|| value.get("m"))),
        price,
        quantity,
        quote_quantity: decimal_as_f64(value.get("quoteQty"))
            .or_else(|| (price > 0.0 && quantity > 0.0).then_some(price * quantity)),
        fee_asset: value_as_string(
            value
                .get("commissionAsset")
                .or_else(|| value.get("feeCoinId")),
        ),
        fee_amount: decimal_as_f64(value.get("commission").or_else(|| value.get("feeAmount"))),
        fee_rate: None,
        realized_pnl: decimal_as_f64(value.get("realizedPnl")),
        filled_at: first_timestamp_millis(value, &["time", "t"]).unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn parse_side(exchange_id: &ExchangeId, value: &str) -> ExchangeApiResult<OrderSide> {
    match value.to_ascii_uppercase().as_str() {
        "BUY" | "BUY_OPEN" | "BUY_CLOSE" => Ok(OrderSide::Buy),
        "SELL" | "SELL_OPEN" | "SELL_CLOSE" => Ok(OrderSide::Sell),
        _ => Err(parse_error(
            exchange_id.clone(),
            "invalid side",
            &Value::String(value.to_string()),
        )),
    }
}

pub(super) fn parse_position_side(value: Option<&str>) -> PositionSide {
    match value.unwrap_or("NET").to_ascii_uppercase().as_str() {
        "LONG" => PositionSide::Long,
        "SHORT" => PositionSide::Short,
        "BOTH" | "NET" => PositionSide::Net,
        _ => PositionSide::Net,
    }
}

fn parse_order_type(raw_type: &str, tif: Option<TimeInForce>) -> OrderType {
    match raw_type.to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        "LIMIT_MAKER" => OrderType::PostOnly,
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
        "POST_ONLY" | "GTX" => TimeInForce::GTX,
        _ => TimeInForce::GTC,
    }
}

fn map_toobit_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_uppercase().as_str() {
        "PENDING_NEW" | "NEW" | "ORDER_NEW" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" | "ORDER_FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" | "ORDER_CANCELED" => OrderStatus::Cancelled,
        "REJECTED" | "ORDER_REJECTED" | "ORDER_FAILED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity(value: Option<&Value>) -> LiquidityRole {
    match value {
        Some(Value::Bool(true)) => LiquidityRole::Maker,
        Some(Value::Bool(false)) => LiquidityRole::Taker,
        Some(Value::String(text)) if text.eq_ignore_ascii_case("maker") => LiquidityRole::Maker,
        Some(Value::String(text)) if text.eq_ignore_ascii_case("taker") => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn average_fill_price(value: &Value) -> Option<String> {
    super::parser::string_or_number(value.get("avgPrice").or_else(|| value.get("averagePrice")))
        .filter(|value| !is_zero_decimal(value))
}

fn is_zero_decimal(value: &str) -> bool {
    value.trim().trim_matches('0').trim_matches('.').is_empty()
}
