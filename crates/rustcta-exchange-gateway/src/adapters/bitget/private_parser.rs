use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    Balance, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId,
    ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType,
    PositionSide, SchemaVersion, TenantId, TimeInForce,
};
use serde_json::Value;

use super::parser::normalize_bitget_symbol;

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<Balance>> {
    let assets = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "assets response is not an array",
                value,
            )
        })?;
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for asset in assets {
        let asset_name = required_str(exchange_id, asset, "coin")
            .or_else(|_| required_str(exchange_id, asset, "coinName"))
            .or_else(|_| required_str(exchange_id, asset, "marginCoin"))?
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset_name) {
            continue;
        }
        let available = decimal_as_f64(
            asset
                .get("available")
                .or_else(|| asset.get("availableAmount"))
                .or_else(|| asset.get("availableBalance")),
        )
        .unwrap_or(0.0);
        let locked =
            decimal_as_f64(asset.get("frozen").or_else(|| asset.get("locked"))).unwrap_or(0.0);
        let total = decimal_as_f64(asset.get("equity").or_else(|| asset.get("totalAmount")))
            .unwrap_or(available + locked);
        if total > 0.0 || available > 0.0 || locked > 0.0 || !requested.is_empty() {
            balances.push(
                AssetBalance::new(asset_name, total, available, locked)
                    .map_err(validation_error)?,
            );
        }
    }
    Ok(vec![Balance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let data = value.get("data").unwrap_or(value);
    let order = data
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(data);
    if order.is_null() {
        return Ok(None);
    }
    Ok(Some(parse_order_state(
        exchange_id,
        fallback_symbol,
        order,
    )?))
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "open orders response is not an array",
                value,
            )
        })?;
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, fallback_symbol, order))
        .collect()
}

pub fn parse_fees(
    _exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let item = value.get("data").unwrap_or(value);
    Ok(vec![FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: symbol.clone(),
        maker_rate: string_or_number(item.get("makerFeeRate")).unwrap_or_else(|| "0".to_string()),
        taker_rate: string_or_number(item.get("takerFeeRate")).unwrap_or_else(|| "0".to_string()),
        source: Some("bitget.account.fee-rate".to_string()),
        updated_at: Utc::now(),
    }])
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let data = value.get("data").unwrap_or(value);
    let fills = data
        .get("fillList")
        .unwrap_or(data)
        .as_array()
        .ok_or_else(|| parse_error(exchange_id.clone(), "fills response is not an array", value))?;
    fills
        .iter()
        .map(|fill| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                fallback_symbol,
                fill,
            )
        })
        .collect()
}

fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let exchange_symbol_text = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_scope_from_exchange_symbol(exchange_id, &exchange_symbol_text))?;
    let force = value.get("force").and_then(Value::as_str);
    let order_type = parse_order_type(
        value
            .get("orderType")
            .and_then(Value::as_str)
            .unwrap_or("limit"),
        force,
    );
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(value.get("clientOid")).filter(|value| !value.is_empty()),
        exchange_order_id: string_or_number(value.get("orderId")).filter(|value| !value.is_empty()),
        side: parse_side(exchange_id, required_str(exchange_id, value, "side")?)?,
        position_side: Some(PositionSide::None),
        order_type,
        time_in_force: parse_time_in_force(force),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(map_bitget_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(value.get("size").or_else(|| value.get("quantity")))
            .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")).filter(|value| !is_zero_decimal(value)),
        filled_quantity: string_or_number(
            value
                .get("filledSize")
                .or_else(|| value.get("baseVolume"))
                .or_else(|| value.get("fillSz")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(
            value.get("priceAvg").or_else(|| value.get("avgPrice")),
        )
        .filter(|value| !is_zero_decimal(value)),
        reduce_only: false,
        post_only: matches!(order_type, OrderType::PostOnly),
        created_at: first_timestamp_millis(value, &["cTime", "createTime"]),
        updated_at: first_timestamp_millis(value, &["uTime", "updateTime"])
            .unwrap_or_else(Utc::now),
    })
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let exchange_symbol_text = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_scope_from_exchange_symbol(exchange_id, &exchange_symbol_text))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitget fill requires canonical_symbol".to_string(),
            })?;
    let price = decimal_as_f64(value.get("price")).unwrap_or(0.0);
    let quantity = decimal_as_f64(value.get("size")).unwrap_or(0.0);
    let quote_quantity = (price > 0.0 && quantity > 0.0).then_some(price * quantity);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("orderId")).filter(|value| !value.is_empty()),
        client_order_id: string_or_number(value.get("clientOid")).filter(|value| !value.is_empty()),
        fill_id: string_or_number(value.get("tradeId").or_else(|| value.get("fillId")))
            .filter(|value| !value.is_empty()),
        side: parse_side(exchange_id, required_str(exchange_id, value, "side")?)?,
        position_side: PositionSide::None,
        status: FillStatus::Confirmed,
        liquidity_role: match value
            .get("tradeScope")
            .or_else(|| value.get("execType"))
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "maker" => LiquidityRole::Maker,
            "taker" => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        },
        price,
        quantity,
        quote_quantity,
        fee_asset: string_or_number(value.get("feeCcy")).filter(|value| !value.is_empty()),
        fee_amount: decimal_as_f64(value.get("fee")).map(f64::abs),
        fee_rate: None,
        realized_pnl: None,
        filled_at: first_timestamp_millis(value, &["cTime", "uTime"]).unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn symbol_scope_from_exchange_symbol(
    exchange_id: &ExchangeId,
    symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let normalized = normalize_bitget_symbol(symbol)?;
    let (base, quote) = split_compact_symbol(&normalized)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, normalized)
            .map_err(validation_error)?,
    })
}

fn split_compact_symbol(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 9] = [
        "USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "EUR", "TRY", "BNB",
    ];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}

fn parse_side(exchange_id: &ExchangeId, side: &str) -> ExchangeApiResult<OrderSide> {
    match side.to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(parse_error(
            exchange_id.clone(),
            "unsupported order side",
            &Value::String(side.to_string()),
        )),
    }
}

fn parse_order_type(order_type: &str, force: Option<&str>) -> OrderType {
    match (
        order_type.to_ascii_lowercase().as_str(),
        force.map(str::to_ascii_lowercase),
    ) {
        ("market", _) => OrderType::Market,
        (_, Some(force)) if force == "post_only" || force == "gtx" => OrderType::PostOnly,
        (_, Some(force)) if force == "ioc" => OrderType::IOC,
        (_, Some(force)) if force == "fok" => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(force: Option<&str>) -> Option<TimeInForce> {
    match force?.to_ascii_lowercase().as_str() {
        "gtc" => Some(TimeInForce::GTC),
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        "post_only" | "gtx" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn map_bitget_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_lowercase().as_str() {
        "live" | "new" => OrderStatus::New,
        "partially_filled" | "partial-fill" => OrderStatus::PartiallyFilled,
        "filled" | "full-fill" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn first_timestamp_millis(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_i64))
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|number| number == 0.0)
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("missing field {field}"),
            value,
        )
    })
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn parse_error(exchange_id: ExchangeId, message: &str, value: &Value) -> ExchangeApiError {
    ExchangeApiError::Exchange(ExchangeError {
        schema_version: SchemaVersion::current(),
        exchange_id,
        class: ExchangeErrorClass::Decode,
        code: None,
        message: format!("{message}: {value}"),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    })
}
