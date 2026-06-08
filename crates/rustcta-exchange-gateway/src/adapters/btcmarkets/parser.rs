use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn normalize_market_id(symbol: &str) -> ExchangeApiResult<String> {
    let symbol = symbol.trim().replace(['/', '_'], "-").to_ascii_uppercase();
    if !symbol.contains('-') {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("btcmarkets marketId must use BASE-AUD form: {symbol}"),
        });
    }
    Ok(symbol)
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = array_data(value)?;
    let mut rules = markets
        .iter()
        .filter(|market| {
            market
                .get("quoteAsset")
                .and_then(Value::as_str)
                .is_some_and(|quote| quote.eq_ignore_ascii_case("AUD"))
        })
        .map(|market| parse_symbol_rule(exchange_id, market))
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    if !requested.is_empty() {
        let wanted = requested
            .iter()
            .map(|symbol| normalize_market_id(&symbol.exchange_symbol.symbol))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        rules.retain(|rule| wanted.contains(&rule.symbol.exchange_symbol.symbol));
    }
    Ok(rules)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value.get("data").unwrap_or(value);
    let bids = parse_levels(exchange_id, data.get("bids"))?;
    let asks = parse_levels(exchange_id, data.get("asks"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "btcmarkets order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = data.get("snapshotId").and_then(value_as_u64);
    snapshot.exchange_timestamp = data
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_time);
    Ok(snapshot)
}

pub(super) fn parse_market_id(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<(String, String, String)> {
    let market_id = required_str(exchange_id, value, "marketId")?.to_ascii_uppercase();
    let (base, quote) = market_id.split_once('-').ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "btcmarkets marketId missing '-' separator",
            value,
        )
    })?;
    Ok((market_id.clone(), base.to_string(), quote.to_string()))
}

pub(super) fn symbol_from_market_id(
    exchange_id: &ExchangeId,
    market_id: &str,
) -> ExchangeApiResult<SymbolScope> {
    let market_id = normalize_market_id(market_id)?;
    let (base, quote) =
        market_id
            .split_once('-')
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("invalid btcmarkets marketId {market_id}"),
            })?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, market_id)
            .map_err(validation_error)?,
    })
}

pub(super) fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("btcmarkets response missing {field}"),
            value,
        )
    })
}

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

pub(super) fn value_as_f64(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    })
}

pub(super) fn parse_time(text: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|time| time.with_timezone(&Utc))
}

pub(super) fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let (market_id, base_asset, quote_asset) = parse_market_id(exchange_id, value)?;
    let symbol = symbol_from_market_id(exchange_id, &market_id)?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: decimal_increment(value.get("priceDecimals")),
        quantity_increment: decimal_increment(value.get("amountDecimals")),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(value.get("minOrderAmount")),
        max_quantity: string_or_number(value.get("maxOrderAmount")),
        min_notional: None,
        max_notional: None,
        price_precision: value.get("priceDecimals").and_then(value_as_u32),
        quantity_precision: value.get("amountDecimals").and_then(value_as_u32),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: true,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "btcmarkets orderbook missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .filter(|level| level.as_array().is_some_and(|row| row.len() >= 2))
        .map(|level| {
            let row = level.as_array().expect("checked row");
            let price = value_as_f64(row.first()).ok_or_else(|| {
                parse_error(exchange_id.clone(), "btcmarkets level missing price", level)
            })?;
            let quantity = value_as_f64(row.get(1)).ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "btcmarkets level missing quantity",
                    level,
                )
            })?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn array_data(value: &Value) -> ExchangeApiResult<&Vec<Value>> {
    value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "btcmarkets response is not an array".to_string(),
        })
}

fn decimal_increment(value: Option<&Value>) -> Option<String> {
    let precision = value.and_then(value_as_u32)?;
    Some(if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(precision.saturating_sub(1) as usize))
    })
}

fn value_as_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .map(|number| number as u32)
        .or_else(|| value.as_str()?.parse().ok())
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

#[cfg(test)]
mod tests {
    use rustcta_types::{ExchangeId, MarketType};
    use serde_json::Value;

    use super::{parse_orderbook_snapshot, parse_symbol_rules, symbol_from_market_id};

    fn fixture(name: &str) -> Value {
        let text = match name {
            "markets.json" => {
                include_str!("../../../../../tests/fixtures/exchanges/btcmarkets/markets.json")
            }
            "orderbook.json" => {
                include_str!("../../../../../tests/fixtures/exchanges/btcmarkets/orderbook.json")
            }
            _ => panic!("unknown btcmarkets fixture {name}"),
        };
        serde_json::from_str(text).expect("btcmarkets fixture")
    }

    #[test]
    fn symbol_rules_should_keep_aud_spot_markets() {
        let exchange = ExchangeId::new("btcmarkets").unwrap();
        let rules = parse_symbol_rules(&exchange, &[], &fixture("markets.json")).unwrap();

        assert_eq!(rules.len(), 2);
        assert!(rules.iter().all(|rule| rule.quote_asset == "AUD"));
        assert_eq!(rules[0].symbol.market_type, MarketType::Spot);
        assert_eq!(rules[0].price_increment.as_deref(), Some("0.01"));
    }

    #[test]
    fn orderbook_should_parse_snapshot_id_and_levels() {
        let exchange = ExchangeId::new("btcmarkets").unwrap();
        let symbol = symbol_from_market_id(&exchange, "BTC-AUD").unwrap();
        let book = parse_orderbook_snapshot(&exchange, symbol, &fixture("orderbook.json")).unwrap();

        assert_eq!(book.sequence, Some(1573067029187000));
        assert_eq!(book.bids[0].price, 100000.0);
        assert_eq!(book.asks[0].quantity, 0.12);
    }
}
