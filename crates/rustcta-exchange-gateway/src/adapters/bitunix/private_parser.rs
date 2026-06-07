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
    parse_side, required_str, split_bitunix_symbol, string_or_number, validation_error,
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
        .or_else(|| data.get("balance"))
        .or_else(|| data.get("assets"))
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
            item.get("available")
                .or_else(|| item.get("availableBalance"))
                .or_else(|| item.get("free")),
        )
        .unwrap_or(0.0);
        let locked = decimal_as_f64(
            item.get("frozen")
                .or_else(|| item.get("locked"))
                .or_else(|| item.get("margin")),
        )
        .unwrap_or(0.0);
        let total = decimal_as_f64(
            item.get("balance")
                .or_else(|| item.get("total"))
                .or_else(|| item.get("equity")),
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
        .get("positionList")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .unwrap_or_else(|| std::slice::from_ref(data));
    let mut output = Vec::new();
    for position in positions {
        let symbol = required_str(exchange_id, position, "symbol")?.to_ascii_uppercase();
        let (base, quote) = split_bitunix_symbol(&symbol)
            .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
        let quantity = decimal_as_f64(
            position
                .get("qty")
                .or_else(|| position.get("positionQty"))
                .or_else(|| position.get("maxQty")),
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
            side: parse_position_side(position.get("side").and_then(Value::as_str)),
            quantity,
            entry_price: decimal_as_f64(
                position
                    .get("avgOpenPrice")
                    .or_else(|| position.get("entryPrice")),
            ),
            mark_price: None,
            liquidation_price: decimal_as_f64(position.get("liqPrice")),
            unrealized_pnl: decimal_as_f64(
                position
                    .get("unrealizedPNL")
                    .or_else(|| position.get("unrealizedPnl")),
            ),
            leverage: decimal_as_f64(position.get("leverage")),
            observed_at: Utc::now(),
        });
    }
    Ok(output)
}

pub fn parse_fee_snapshots(
    symbols: &[SymbolScope],
    market_type: MarketType,
) -> Vec<FeeRateSnapshot> {
    symbols
        .iter()
        .cloned()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: "0".to_string(),
            taker_rate: "0".to_string(),
            source: Some(format!("bitunix.{market_type:?}.fee_unsupported_default")),
            updated_at: Utc::now(),
        })
        .collect()
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
        .get("orderList")
        .or_else(|| data.get("successList"))
        .or_else(|| data.get("orders"))
        .or_else(|| data.get("list"))
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
        .get("tradeList")
        .or_else(|| data.get("dealList"))
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
        .unwrap_or_else(|| symbol_from_payload(exchange_id, market_type, value))?;
    let raw_type = value
        .get("orderType")
        .or_else(|| value.get("type"))
        .and_then(|value| string_or_number(Some(value)))
        .unwrap_or_else(|| "LIMIT".to_string());
    let tif = value
        .get("effect")
        .or_else(|| value.get("timeInForce"))
        .and_then(Value::as_str)
        .map(parse_time_in_force);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value_as_string(
            value.get("clientId").or_else(|| value.get("clientOrderId")),
        ),
        exchange_order_id: value_as_string(value.get("orderId").or_else(|| value.get("id"))),
        side: parse_side(
            exchange_id,
            string_or_number(value.get("side"))
                .as_deref()
                .unwrap_or("BUY"),
        )?,
        position_side: Some(parse_position_side(
            value
                .get("tradeSide")
                .or_else(|| value.get("positionSide"))
                .and_then(Value::as_str),
        )),
        order_type: parse_order_type(&raw_type, tif),
        time_in_force: tif,
        status: value
            .get("status")
            .or_else(|| value.get("orderStatus"))
            .and_then(|value| string_or_number(Some(value)))
            .as_deref()
            .map(map_bitunix_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(
            value
                .get("qty")
                .or_else(|| value.get("quantity"))
                .or_else(|| value.get("volume")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")).filter(|value| !is_zero_decimal(value)),
        filled_quantity: string_or_number(
            value
                .get("tradeQty")
                .or_else(|| value.get("dealVolume"))
                .or_else(|| value.get("executedQty")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(
            value.get("avgPrice").or_else(|| value.get("averagePrice")),
        )
        .filter(|value| !is_zero_decimal(value)),
        reduce_only: value
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: matches!(tif, Some(TimeInForce::GTX))
            || raw_type.eq_ignore_ascii_case("POST_ONLY"),
        created_at: first_timestamp_millis(value, &["ctime", "time", "createTime"]),
        updated_at: first_timestamp_millis(value, &["mtime", "updateTime", "ctime"])
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
                message: "bitunix fill requires canonical_symbol".to_string(),
            })?;
    let price = decimal_as_f64(value.get("price")).unwrap_or(0.0);
    let quantity = decimal_as_f64(
        value
            .get("tradeQty")
            .or_else(|| value.get("executedQty"))
            .or_else(|| value.get("dealVolume"))
            .or_else(|| value.get("qty"))
            .or_else(|| value.get("quantity")),
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
        client_order_id: value_as_string(
            value.get("clientId").or_else(|| value.get("clientOrderId")),
        ),
        fill_id: value_as_string(
            value
                .get("tradeId")
                .or_else(|| value.get("dealId"))
                .or_else(|| value.get("fillId"))
                .or_else(|| value.get("id")),
        ),
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: value
            .get("feeCoin")
            .or_else(|| value.get("feeAsset"))
            .and_then(Value::as_str)
            .map(str::to_ascii_uppercase),
        side: parse_side(
            exchange_id,
            required_str(exchange_id, value, "side").unwrap_or("BUY"),
        )?,
        position_side: parse_position_side(
            value
                .get("tradeSide")
                .or_else(|| value.get("positionSide"))
                .and_then(Value::as_str),
        ),
        status: FillStatus::Confirmed,
        liquidity_role: value
            .get("roleType")
            .or_else(|| value.get("role"))
            .and_then(Value::as_str)
            .map(|role| {
                if role.eq_ignore_ascii_case("MAKER") {
                    LiquidityRole::Maker
                } else {
                    LiquidityRole::Taker
                }
            })
            .unwrap_or(LiquidityRole::Unknown),
        fee_amount: decimal_as_f64(value.get("fee")).map(f64::abs),
        fee_rate: None,
        realized_pnl: decimal_as_f64(
            value
                .get("realizedPNL")
                .or_else(|| value.get("realizedPnl")),
        ),
        filled_at: first_timestamp_millis(value, &["ctime", "time", "createTime"])
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn symbol_from_payload(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let symbol = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let (base, quote) = split_bitunix_symbol(&symbol)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol)
            .map_err(validation_error)?,
    })
}

fn parse_order_type(raw_type: &str, tif: Option<TimeInForce>) -> OrderType {
    match raw_type.to_ascii_uppercase().as_str() {
        "MARKET" | "2" => OrderType::Market,
        "POST_ONLY" => OrderType::PostOnly,
        "LIMIT" | "1" if matches!(tif, Some(TimeInForce::IOC)) => OrderType::IOC,
        "LIMIT" | "1" if matches!(tif, Some(TimeInForce::FOK)) => OrderType::FOK,
        "LIMIT" | "1" if matches!(tif, Some(TimeInForce::GTX)) => OrderType::PostOnly,
        _ => OrderType::Limit,
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

fn map_bitunix_order_status(value: &str) -> OrderStatus {
    match value.to_ascii_uppercase().as_str() {
        "INIT" | "NEW" | "1" => OrderStatus::New,
        "PART_FILLED" | "PARTIALLY_FILLED" | "3" => OrderStatus::PartiallyFilled,
        "FILLED" | "2" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" | "PART_FILLED_CANCELED" | "4" | "7" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|value| value == 0.0)
}
