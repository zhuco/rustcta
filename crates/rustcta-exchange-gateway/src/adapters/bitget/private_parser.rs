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
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Balance>> {
    let assets = bitget_balance_assets(exchange_id, value)?;
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    if let Some(total) = bitget_account_equity(value) {
        if requested.is_empty() || requested.contains(&"USDT".to_string()) {
            balances.push(AssetBalance::new("USDT", total, total, 0.0).map_err(validation_error)?);
            return Ok(vec![Balance {
                schema_version: SchemaVersion::current(),
                tenant_id,
                account_id,
                exchange_id: exchange_id.clone(),
                market_type,
                balances,
                observed_at: Utc::now(),
            }]);
        }
    }
    for asset in assets {
        let asset_name = required_str(exchange_id, asset, "coin")
            .or_else(|_| required_str(exchange_id, asset, "coinName"))
            .or_else(|_| required_str(exchange_id, asset, "marginCoin"))
            .or_else(|_| required_str(exchange_id, asset, "asset"))
            .or_else(|_| required_str(exchange_id, asset, "currency"))?
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset_name) {
            continue;
        }
        let available = decimal_as_f64(
            asset
                .get("available")
                .or_else(|| asset.get("availableAmount"))
                .or_else(|| asset.get("availableBalance"))
                .or_else(|| asset.get("free")),
        )
        .unwrap_or(0.0);
        let locked =
            decimal_as_f64(asset.get("frozen").or_else(|| asset.get("locked"))).unwrap_or(0.0);
        let total = decimal_as_f64(
            asset
                .get("equity")
                .or_else(|| asset.get("totalAmount"))
                .or_else(|| asset.get("accountEquity"))
                .or_else(|| asset.get("balance")),
        )
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
        market_type,
        balances,
        observed_at: Utc::now(),
    }])
}

fn bitget_account_equity(value: &Value) -> Option<f64> {
    let data = value.get("data").unwrap_or(value);
    if !data.is_object() {
        return None;
    }
    decimal_as_f64(
        data.get("usdtEquity")
            .or_else(|| data.get("accountEquity"))
            .or_else(|| data.get("totalEquity")),
    )
}

fn bitget_balance_assets<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
) -> ExchangeApiResult<Vec<&'a Value>> {
    let data = value.get("data").unwrap_or(value);
    if let Some(items) = data.as_array() {
        return Ok(items.iter().collect());
    }
    for field in ["assets", "list"] {
        if let Some(items) = data.get(field).and_then(Value::as_array) {
            return Ok(items.iter().collect());
        }
    }
    if data.is_object()
        && ["coin", "coinName", "marginCoin", "asset", "currency"]
            .iter()
            .any(|field| data.get(*field).is_some())
    {
        return Ok(vec![data]);
    }
    Err(parse_error(
        exchange_id.clone(),
        "assets response does not contain asset rows",
        value,
    ))
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
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
    let data = value.get("data").unwrap_or(value);
    let orders_value = data
        .get("entrustedList")
        .or_else(|| data.get("orderList"))
        .or_else(|| data.get("list"))
        .unwrap_or(data);
    let Some(orders) = orders_value.as_array() else {
        if orders_value.is_null() {
            return Ok(Vec::new());
        }
        return Err(parse_error(
            exchange_id.clone(),
            "open orders response is not an array",
            value,
        ));
    };
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, fallback_symbol, market_type, order))
        .collect()
}

pub fn parse_fees(
    _exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let data = value.get("data").unwrap_or(value);
    let item = data
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(data);
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
    market_type: MarketType,
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
    requested_symbols: &[rustcta_types::ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<rustcta_types::ExchangePosition>> {
    let requested = requested_symbols
        .iter()
        .map(|symbol| normalize_bitget_symbol(&symbol.symbol))
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    let positions = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "positions response is not an array",
                value,
            )
        })?;
    positions
        .iter()
        .filter_map(|position| {
            let symbol_text = required_str(exchange_id, position, "symbol")
                .or_else(|_| required_str(exchange_id, position, "instId"))
                .ok()?
                .to_ascii_uppercase();
            if !requested.is_empty() && !requested.contains(&symbol_text) {
                return None;
            }
            let quantity = decimal_as_f64(
                position
                    .get("total")
                    .or_else(|| position.get("size"))
                    .or_else(|| position.get("available")),
            )
            .unwrap_or(0.0)
            .abs();
            if quantity == 0.0 {
                return None;
            }
            let (base, quote) = split_compact_symbol(&symbol_text)
                .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
            let canonical_symbol = CanonicalSymbol::new(base, quote).ok()?;
            let exchange_symbol =
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol_text)
                    .ok()?;
            Some(Ok(rustcta_types::ExchangePosition {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol,
                exchange_symbol: Some(exchange_symbol),
                side: parse_position_side(
                    string_or_number(
                        position
                            .get("holdSide")
                            .or_else(|| position.get("posSide"))
                            .or_else(|| position.get("side")),
                    )
                    .as_deref(),
                ),
                quantity,
                entry_price: decimal_as_f64(
                    position
                        .get("openPriceAvg")
                        .or_else(|| position.get("averageOpenPrice"))
                        .or_else(|| position.get("entryPrice")),
                ),
                mark_price: decimal_as_f64(position.get("markPrice")),
                liquidation_price: decimal_as_f64(position.get("liquidationPrice")),
                unrealized_pnl: decimal_as_f64(
                    position
                        .get("unrealizedPL")
                        .or_else(|| position.get("unrealizedPnl")),
                ),
                leverage: decimal_as_f64(position.get("leverage")),
                observed_at: Utc::now(),
            }))
        })
        .collect()
}

fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let exchange_symbol_text = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let symbol = fallback_symbol.cloned().map(Ok).unwrap_or_else(|| {
        symbol_scope_from_exchange_symbol(exchange_id, market_type, &exchange_symbol_text)
    })?;
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
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(value.get("clientOid")).filter(|value| !value.is_empty()),
        exchange_order_id: string_or_number(value.get("orderId")).filter(|value| !value.is_empty()),
        side: parse_side(exchange_id, required_str(exchange_id, value, "side")?)?,
        position_side: Some(position_side_from_order(value)),
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
                .or_else(|| value.get("accBaseVolume"))
                .or_else(|| value.get("baseVolume"))
                .or_else(|| value.get("fillSz"))
                .or_else(|| value.get("fillSize"))
                .or_else(|| value.get("filledQty"))
                .or_else(|| value.get("fillQuantity"))
                .or_else(|| value.get("filledAmount"))
                .or_else(|| value.get("execQty"))
                .or_else(|| value.get("executedQty")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(
            value
                .get("priceAvg")
                .or_else(|| value.get("avgPrice"))
                .or_else(|| value.get("averagePrice"))
                .or_else(|| value.get("average_fill_price"))
                .or_else(|| value.get("fillPrice"))
                .or_else(|| value.get("avgPx")),
        )
        .filter(|value| !is_zero_decimal(value)),
        reduce_only: is_reduce_only(value),
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
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let exchange_symbol_text = required_str(exchange_id, value, "symbol")?.to_ascii_uppercase();
    let symbol = fallback_symbol.cloned().map(Ok).unwrap_or_else(|| {
        symbol_scope_from_exchange_symbol(exchange_id, market_type, &exchange_symbol_text)
    })?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitget fill requires canonical_symbol".to_string(),
            })?;
    let price = decimal_as_f64(
        value
            .get("price")
            .or_else(|| value.get("fillPrice"))
            .or_else(|| value.get("execPrice"))
            .or_else(|| value.get("px")),
    )
    .unwrap_or(0.0);
    let quantity = decimal_as_f64(
        value
            .get("size")
            .or_else(|| value.get("fillSize"))
            .or_else(|| value.get("baseVolume"))
            .or_else(|| value.get("fillSz"))
            .or_else(|| value.get("qty"))
            .or_else(|| value.get("execQty")),
    )
    .unwrap_or(0.0);
    let quote_quantity = (price > 0.0 && quantity > 0.0).then_some(price * quantity);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("orderId")).filter(|value| !value.is_empty()),
        client_order_id: string_or_number(value.get("clientOid")).filter(|value| !value.is_empty()),
        fill_id: string_or_number(value.get("tradeId").or_else(|| value.get("fillId")))
            .filter(|value| !value.is_empty()),
        side: parse_side(exchange_id, required_str(exchange_id, value, "side")?)?,
        position_side: position_side_from_order(value),
        status: FillStatus::Confirmed,
        liquidity_role: match value
            .get("tradeScope")
            .or_else(|| value.get("execType"))
            .or_else(|| value.get("role"))
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "maker" => LiquidityRole::Maker,
            "taker" => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        },
        price,
        quantity,
        quote_quantity: decimal_as_f64(value.get("quoteVolume")).or(quote_quantity),
        fee_asset: string_or_number(value.get("feeCcy").or_else(|| value.get("feeCoin")))
            .or_else(|| fee_detail(value).and_then(|detail| detail.1))
            .filter(|value| !value.is_empty()),
        fee_amount: decimal_as_f64(value.get("fee"))
            .or_else(|| fee_detail(value).and_then(|detail| detail.0))
            .map(f64::abs),
        fee_rate: None,
        realized_pnl: decimal_as_f64(value.get("profit").or_else(|| value.get("realizedPnl"))),
        filled_at: first_timestamp_millis(value, &["tradeTime", "cTime", "uTime"])
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn symbol_scope_from_exchange_symbol(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: &str,
) -> ExchangeApiResult<SymbolScope> {
    let normalized = normalize_bitget_symbol(symbol)?;
    let (base, quote) = split_compact_symbol(&normalized)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, normalized)
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
    match status
        .trim()
        .to_ascii_lowercase()
        .replace(['-', ' '], "_")
        .as_str()
    {
        "live" | "new" | "open" | "init" | "not_trigger" | "not_triggered" => OrderStatus::New,
        "partially_filled" | "partial_fill" | "partially_fill" => OrderStatus::PartiallyFilled,
        "filled" | "full_fill" | "fullfilled" | "complete" | "completed" | "done" | "success" => {
            OrderStatus::Filled
        }
        "cancelled" | "canceled" | "cancel" | "partial_cancelled" | "partial_canceled" => {
            OrderStatus::Cancelled
        }
        "rejected" | "reject" | "failed" | "fail" => OrderStatus::Rejected,
        "expired" | "expire" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_position_side(value: Option<&str>) -> PositionSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "long" => PositionSide::Long,
        "short" => PositionSide::Short,
        "net" | "both" => PositionSide::Net,
        _ => PositionSide::None,
    }
}

fn position_side_from_order(value: &Value) -> PositionSide {
    if let Some(side) = string_or_number(
        value
            .get("holdSide")
            .or_else(|| value.get("posSide"))
            .or_else(|| value.get("positionSide")),
    ) {
        return parse_position_side(Some(&side));
    }
    let trade_side = value
        .get("tradeSide")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let side = value
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match (
        trade_side.to_ascii_lowercase().as_str(),
        side.to_ascii_lowercase().as_str(),
    ) {
        ("open", "buy") | ("close", "sell") => PositionSide::Long,
        ("open", "sell") | ("close", "buy") => PositionSide::Short,
        _ => PositionSide::None,
    }
}

fn is_reduce_only(value: &Value) -> bool {
    value
        .get("tradeSide")
        .and_then(Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("close"))
        || string_or_number(value.get("reduceOnly")).is_some_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "true" | "yes" | "y" | "1"
            )
        })
}

fn fee_detail(value: &Value) -> Option<(Option<f64>, Option<String>)> {
    let detail = value.get("feeDetail")?.as_array()?.first()?;
    Some((
        decimal_as_f64(
            detail
                .get("totalFee")
                .or_else(|| detail.get("fee"))
                .or_else(|| detail.get("totalDeductionFee")),
        ),
        string_or_number(detail.get("feeCoin").or_else(|| detail.get("feeCcy"))),
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use rustcta_exchange_api::SymbolScope;
    use serde_json::json;

    #[test]
    fn bitget_order_status_aliases_should_map_to_terminal_states() {
        assert_eq!(
            map_bitget_order_status("partial-fill"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(
            map_bitget_order_status("partial_fill"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(map_bitget_order_status("full-fill"), OrderStatus::Filled);
        assert_eq!(map_bitget_order_status("completed"), OrderStatus::Filled);
        assert_eq!(map_bitget_order_status("expired"), OrderStatus::Expired);
    }

    #[test]
    fn bitget_order_parser_should_read_alternate_fill_fields() {
        let exchange_id = ExchangeId::new("bitget").expect("exchange id");
        let symbol = SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: Some(CanonicalSymbol::new("ESPORTS", "USDT").expect("symbol")),
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Perpetual,
                "ESPORTSUSDT",
            )
            .expect("exchange symbol"),
        };
        let order = parse_order_state(
            &exchange_id,
            Some(&symbol),
            MarketType::Perpetual,
            &json!({
                "symbol": "ESPORTSUSDT",
                "side": "sell",
                "orderType": "limit",
                "force": "gtc",
                "status": "full-fill",
                "size": "66",
                "filledQty": "66",
                "priceAvg": "0.08399",
                "clientOid": "ca-os-bitget",
                "orderId": "123456",
            }),
        )
        .expect("order state");

        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.filled_quantity, "66");
        assert_eq!(order.average_fill_price.as_deref(), Some("0.08399"));
    }
}
