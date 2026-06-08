use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let contracts = value
        .get("data")
        .and_then(|data| data.get("contractConfig"))
        .and_then(|config| config.get("perpetualContract"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "ApeX symbols response missing data.contractConfig.perpetualContract",
                value,
            )
        })?;
    let mut rules = Vec::new();
    for contract in contracts {
        let public_symbol = required_str(exchange_id, contract, "symbol")?;
        let trade_symbol = contract
            .get("crossSymbolName")
            .and_then(Value::as_str)
            .map(apex_trade_symbol)
            .unwrap_or_else(|| apex_trade_symbol(public_symbol));
        if !requested.is_empty()
            && !requested.iter().any(|scope| {
                scope.market_type == MarketType::Perpetual
                    && (scope
                        .exchange_symbol
                        .symbol
                        .eq_ignore_ascii_case(public_symbol)
                        || scope
                            .exchange_symbol
                            .symbol
                            .eq_ignore_ascii_case(&trade_symbol))
            })
        {
            continue;
        }
        rules.push(parse_symbol_rule(
            exchange_id,
            contract,
            public_symbol,
            &trade_symbol,
        )?);
    }
    Ok(rules)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("b"), "bids")?;
    let asks = parse_levels(exchange_id, data.get("a"), "asks")?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "apex order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Perpetual,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = data.get("u").and_then(number_u64);
    Ok(snapshot)
}

pub fn apex_public_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '/', '_'], "")
        .to_ascii_uppercase()
}

pub fn apex_trade_symbol(symbol: &str) -> String {
    let upper = apex_public_symbol(symbol);
    if upper.ends_with("USDT") && upper.len() > 4 {
        format!("{}-USDT", &upper[..upper.len() - 4])
    } else {
        upper
    }
}

fn parse_symbol_rule(
    exchange_id: &ExchangeId,
    value: &Value,
    public_symbol: &str,
    trade_symbol: &str,
) -> ExchangeApiResult<SymbolRules> {
    let (base_asset, quote_asset) = split_symbol_assets(trade_symbol);
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let symbol = SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            public_symbol.to_string(),
        )
        .map_err(validation_error)?,
    };
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(value.get("tickSize")),
        quantity_increment: string_or_number(value.get("stepSize")),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("minOrderSize")),
        max_quantity: string_or_number(value.get("maxOrderSize")),
        min_notional: None,
        max_notional: None,
        price_precision: string_or_number(value.get("tickSize"))
            .and_then(|value| precision(&value)),
        quantity_precision: string_or_number(value.get("stepSize"))
            .and_then(|value| precision(&value)),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
    side: &str,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let rows = value.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("ApeX order book missing {side}"),
            value.unwrap_or(&Value::Null),
        )
    })?;
    let mut levels = Vec::with_capacity(rows.len());
    for row in rows {
        let values = row.as_array().ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "ApeX order book level is not an array",
                row,
            )
        })?;
        let price = number_from_value(values.first())
            .ok_or_else(|| parse_error(exchange_id.clone(), "ApeX level missing price", row))?;
        let quantity = number_from_value(values.get(1))
            .ok_or_else(|| parse_error(exchange_id.clone(), "ApeX level missing quantity", row))?;
        levels.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
    }
    if side == "bids" {
        levels.sort_by(|left, right| right.price.total_cmp(&left.price));
    } else {
        levels.sort_by(|left, right| left.price.total_cmp(&right.price));
    }
    Ok(levels)
}

fn split_symbol_assets(symbol: &str) -> (String, String) {
    let clean = symbol.replace('-', "");
    for quote in ["USDT", "USDC", "USD"] {
        if clean.ends_with(quote) && clean.len() > quote.len() {
            return (
                clean[..clean.len() - quote.len()].to_string(),
                quote.to_string(),
            );
        }
    }
    (clean, "USDT".to_string())
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("ApeX payload missing {field}"),
            value,
        )
    })
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn number_from_value(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn number_u64(value: &Value) -> Option<u64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_u64(),
        _ => None,
    }
}

fn precision(value: &str) -> Option<u32> {
    value
        .trim_end_matches('0')
        .split_once('.')
        .map(|(_, fraction)| fraction.len() as u32)
}

fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
