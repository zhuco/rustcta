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
    parse_side, split_hashkey_global_symbol, string_or_number, validation_error, value_as_string,
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
        .or_else(|| data.get("account"))
        .or_else(|| data.get("balance"))
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
            .get("asset")
            .or_else(|| item.get("coin"))
            .or_else(|| item.get("marginCoin"))
            .and_then(Value::as_str)
            .unwrap_or("USDT")
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_as_f64(
            item.get("free")
                .or_else(|| item.get("accountNormal"))
                .or_else(|| item.get("availableBalance"))
                .or_else(|| item.get("availableMargin")),
        )
        .unwrap_or(0.0);
        let locked = decimal_as_f64(
            item.get("locked")
                .or_else(|| item.get("accountLock"))
                .or_else(|| item.get("frozen")),
        )
        .unwrap_or(0.0);
        let total = decimal_as_f64(
            item.get("balance")
                .or_else(|| item.get("equity"))
                .or_else(|| item.get("totalEquity"))
                .or_else(|| item.get("total")),
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
    let mut nested_positions = Vec::new();
    for account in data
        .get("account")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for position_group in account
            .get("positionVos")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let group_symbol = position_group
                .get("symbol")
                .or_else(|| position_group.get("contractSymbol"))
                .cloned();
            for position in position_group
                .get("positions")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let mut position = position.clone();
                if let (Some(symbol), Value::Object(fields)) = (&group_symbol, &mut position) {
                    fields
                        .entry("symbol".to_string())
                        .or_insert_with(|| symbol.clone());
                }
                nested_positions.push(position);
            }
        }
    }
    let fallback_positions;
    let positions = if nested_positions.is_empty() {
        fallback_positions = data
            .as_array()
            .map(|items| items.iter().collect::<Vec<_>>())
            .unwrap_or_else(|| vec![data]);
        fallback_positions
    } else {
        nested_positions.iter().collect::<Vec<_>>()
    };
    let mut output = Vec::new();
    for position in positions {
        let symbol = position
            .get("symbol")
            .or_else(|| position.get("symbol"))
            .and_then(Value::as_str)
            .ok_or_else(|| parse_error(exchange_id.clone(), "position missing symbol", position))?
            .to_ascii_uppercase();
        let (base, quote) = split_hashkey_global_symbol(&symbol)
            .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
        let quantity = decimal_as_f64(
            position
                .get("positionAmt")
                .or_else(|| position.get("volume"))
                .or_else(|| position.get("availableAmt"))
                .or_else(|| position.get("positionQty"))
                .or_else(|| position.get("quantity")),
        )
        .unwrap_or(0.0)
        .abs();
        if quantity == 0.0 {
            continue;
        }
        let canonical_symbol = CanonicalSymbol::new(base, quote).map_err(validation_error)?;
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
            side: parse_position_side(
                position
                    .get("positionSide")
                    .or_else(|| position.get("side"))
                    .and_then(Value::as_str),
            ),
            quantity,
            entry_price: decimal_as_f64(
                position
                    .get("avgPrice")
                    .or_else(|| position.get("entryPrice"))
                    .or_else(|| position.get("openPrice")),
            ),
            mark_price: decimal_as_f64(
                position
                    .get("markPrice")
                    .or_else(|| position.get("indexPrice")),
            ),
            liquidation_price: decimal_as_f64(
                position
                    .get("liquidationPrice")
                    .or_else(|| position.get("reducePrice")),
            ),
            unrealized_pnl: decimal_as_f64(
                position
                    .get("unrealizedProfit")
                    .or_else(|| position.get("unRealizedAmount"))
                    .or_else(|| position.get("unrealizedPnl")),
            ),
            leverage: decimal_as_f64(
                position
                    .get("leverage")
                    .or_else(|| position.get("leverageLevel")),
            ),
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
    let items = data.as_array().map(Vec::as_slice);
    let mut fees = Vec::new();
    if let Some(items) = items {
        for item in items {
            let symbol = symbols
                .first()
                .cloned()
                .or_else(|| symbol_from_payload(exchange_id, market_type, item).ok())
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "fee response missing symbol", item)
                })?;
            fees.push(parse_fee_snapshot(symbol, market_type, item));
        }
    } else {
        let fee_symbols = if symbols.is_empty() {
            vec![symbol_from_payload(exchange_id, market_type, data)?]
        } else {
            symbols.to_vec()
        };
        for symbol in fee_symbols {
            fees.push(parse_fee_snapshot(symbol, market_type, data));
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
        .unwrap_or_else(|| data.get("data").unwrap_or(data));
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
        .or_else(|| data.get("orders"))
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
        .unwrap_or_else(|| symbol_from_payload(exchange_id, market_type, value))?;
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
                .or_else(|| value.get("clientOrderID")),
        ),
        exchange_order_id: value_as_string(value.get("orderId")),
        side: fill_side(exchange_id, value)?,
        position_side: Some(parse_position_side(
            value.get("positionSide").and_then(Value::as_str),
        )),
        order_type: parse_order_type(raw_type, tif),
        time_in_force: tif,
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(map_hashkey_global_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(
            value
                .get("origQty")
                .or_else(|| value.get("volume"))
                .or_else(|| value.get("quantity"))
                .or_else(|| value.get("executedQty")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")).filter(|value| !is_zero_decimal(value)),
        filled_quantity: string_or_number(value.get("executedQty"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("avgPrice"))
            .filter(|value| !is_zero_decimal(value)),
        reduce_only: value
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or_else(|| {
                value
                    .get("action")
                    .and_then(Value::as_str)
                    .is_some_and(|action| action.eq_ignore_ascii_case("CLOSE"))
            }),
        post_only: matches!(tif, Some(TimeInForce::GTX))
            || raw_type.eq_ignore_ascii_case("POST_ONLY"),
        created_at: first_timestamp_millis(value, &["time", "transactTime", "createTime"]),
        updated_at: first_timestamp_millis(value, &["updateTime", "time"]).unwrap_or_else(Utc::now),
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
                message: "hashkey_global fill requires canonical_symbol".to_string(),
            })?;
    let price = decimal_as_f64(value.get("price")).unwrap_or(0.0);
    let quantity = decimal_as_f64(
        value
            .get("qty")
            .or_else(|| value.get("volume"))
            .or_else(|| value.get("quantity"))
            .or_else(|| value.get("executedQty")),
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
        order_id: value_as_string(value.get("orderId")),
        client_order_id: value_as_string(value.get("clientOrderId")),
        fill_id: value_as_string(value.get("id").or_else(|| value.get("tradeId"))),
        side: fill_side(exchange_id, value)?,
        position_side: parse_position_side(value.get("positionSide").and_then(Value::as_str)),
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity(value.get("maker").or_else(|| value.get("isMaker"))),
        price,
        quantity,
        quote_quantity: decimal_as_f64(
            value
                .get("quoteQty")
                .or_else(|| value.get("amount"))
                .or_else(|| value.get("cumQuote")),
        )
        .or_else(|| (price > 0.0 && quantity > 0.0).then_some(price * quantity)),
        fee_asset: value_as_string(
            value
                .get("commissionAsset")
                .or_else(|| value.get("feeAsset"))
                .or_else(|| value.get("feeCoin")),
        ),
        fee_amount: decimal_as_f64(value.get("commission").or_else(|| value.get("fee"))),
        fee_rate: None,
        realized_pnl: decimal_as_f64(value.get("profit").or_else(|| value.get("realizedPnl"))),
        filled_at: first_timestamp_millis(value, &["time", "tradeTime", "updateTime"])
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn symbol_from_payload(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let exchange_symbol = value
        .get("symbol")
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id.clone(), "payload missing symbol", value))?
        .to_ascii_uppercase();
    let (base, quote) = split_hashkey_global_symbol(&exchange_symbol)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    })
}

fn parse_fee_snapshot(
    symbol: SymbolScope,
    market_type: MarketType,
    item: &Value,
) -> FeeRateSnapshot {
    FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        maker_rate: fee_rate_text(
            item.get("makerCommissionRate")
                .or_else(|| item.get("makerFeeRate"))
                .or_else(|| item.get("openMakerFee"))
                .or_else(|| item.get("closeMakerFee")),
            item.get("makerCommission"),
        ),
        taker_rate: fee_rate_text(
            item.get("takerCommissionRate")
                .or_else(|| item.get("takerFeeRate"))
                .or_else(|| item.get("openTakerFee"))
                .or_else(|| item.get("closeTakerFee")),
            item.get("takerCommission"),
        ),
        source: Some(if market_type == MarketType::Spot {
            "hashkey_global.spot.account_commission".to_string()
        } else {
            "hashkey_global.futures.commission_rate".to_string()
        }),
        updated_at: Utc::now(),
    }
}

fn fee_rate_text(rate_value: Option<&Value>, commission_bps_value: Option<&Value>) -> String {
    if let Some(rate) = string_or_number(rate_value) {
        return rate;
    }
    let Some(raw) = string_or_number(commission_bps_value) else {
        return "0".to_string();
    };
    let Ok(number) = raw.parse::<f64>() else {
        return raw;
    };
    if number.abs() >= 1.0 {
        trim_decimal(number / 10_000.0)
    } else {
        raw
    }
}

fn trim_decimal(value: f64) -> String {
    let text = format!("{value:.10}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn fill_side(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<rustcta_types::OrderSide> {
    if let Some(side) = value.get("side").and_then(Value::as_str) {
        return parse_side(exchange_id, side);
    }
    if let Some(is_buyer) = value.get("isBuyer").and_then(Value::as_bool) {
        return Ok(if is_buyer {
            rustcta_types::OrderSide::Buy
        } else {
            rustcta_types::OrderSide::Sell
        });
    }
    Err(parse_error(
        exchange_id.clone(),
        "fill missing side/isBuyer",
        value,
    ))
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
        "POSTONLY" | "POST_ONLY" | "GTX" => TimeInForce::GTX,
        _ => TimeInForce::GTC,
    }
}

fn map_hashkey_global_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_uppercase().as_str() {
        "NEW" | "PENDING" | "INIT" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "FAILED" | "REJECTED" => OrderStatus::Rejected,
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

fn is_zero_decimal(value: &str) -> bool {
    value.trim().trim_matches('0').trim_matches('.').is_empty()
}
