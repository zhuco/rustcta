use chrono::{TimeZone, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

const FIAT_QUOTES: [&str; 3] = ["AUD", "SGD", "USD"];

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let mut rules = Vec::new();
    for row in market_rows(value) {
        let primary = row
            .get("primaryCurrencyCode")
            .or_else(|| row.get("PrimaryCurrencyCode"))
            .or_else(|| row.get("primary_currency_code"))
            .or_else(|| row.get("primary"))
            .and_then(Value::as_str)
            .map(normalize_asset)
            .ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Independent Reserve market missing primaryCurrencyCode",
                    row,
                )
            })?;
        let secondary = row
            .get("secondaryCurrencyCode")
            .or_else(|| row.get("SecondaryCurrencyCode"))
            .or_else(|| row.get("secondary_currency_code"))
            .or_else(|| row.get("secondary"))
            .and_then(Value::as_str)
            .map(normalize_asset)
            .ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "Independent Reserve market missing secondaryCurrencyCode",
                    row,
                )
            })?;
        if !FIAT_QUOTES.contains(&secondary.as_str()) || is_inactive(row) {
            continue;
        }
        let symbol_text = independentreserve_symbol(&primary, &secondary);
        if !requested.is_empty()
            && !requested
                .iter()
                .any(|scope| symbol_matches(scope, &symbol_text, &primary, &secondary))
        {
            continue;
        }
        rules.push(symbol_rule(exchange_id, row, &primary, &secondary)?);
    }
    Ok(rules)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let data = value
        .get("Data")
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let bids = parse_levels(
        exchange_id,
        data.get("BuyOrders")
            .or_else(|| data.get("buyOrders"))
            .or_else(|| data.get("bids")),
        "bids",
    )?;
    let asks = parse_levels(
        exchange_id,
        data.get("SellOrders")
            .or_else(|| data.get("sellOrders"))
            .or_else(|| data.get("asks")),
        "asks",
    )?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "independentreserve order book request requires canonical_symbol"
                    .to_string(),
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
    snapshot.sequence = string_or_number(
        data.get("nonce")
            .or_else(|| data.get("Nonce"))
            .or_else(|| data.get("sequence")),
    )
    .and_then(|value| value.parse().ok());
    snapshot.exchange_timestamp = data
        .get("CreatedTimestampUtc")
        .or_else(|| data.get("createdTimestampUtc"))
        .or_else(|| data.get("timestamp"))
        .and_then(parse_timestamp_value);
    Ok(snapshot)
}

pub fn symbol_scope(exchange_id: &ExchangeId, symbol: &str) -> ExchangeApiResult<SymbolScope> {
    let (base, quote) = split_symbol_assets(symbol);
    let canonical_symbol = CanonicalSymbol::new(&base, &quote).map_err(validation_error)?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            independentreserve_symbol(&base, &quote),
        )
        .map_err(validation_error)?,
    })
}

pub fn independentreserve_symbol(primary: &str, secondary: &str) -> String {
    format!(
        "{}_{}",
        normalize_asset(primary),
        normalize_asset(secondary)
    )
}

pub fn split_symbol_assets(symbol: &str) -> (String, String) {
    let clean = normalize_symbol_text(symbol);
    if let Some((base, quote)) = clean.split_once('_') {
        return (normalize_asset(base), normalize_asset(quote));
    }
    for quote in FIAT_QUOTES {
        if clean.ends_with(quote) && clean.len() > quote.len() {
            return (
                normalize_asset(&clean[..clean.len() - quote.len()]),
                quote.to_string(),
            );
        }
    }
    for quote in ["USDT", "USDC", "BTC", "ETH"] {
        if clean.ends_with(quote) && clean.len() > quote.len() {
            return (
                normalize_asset(&clean[..clean.len() - quote.len()]),
                quote.to_string(),
            );
        }
    }
    (normalize_asset(&clean), "AUD".to_string())
}

pub fn normalize_asset(value: &str) -> String {
    match value.trim().to_ascii_uppercase().as_str() {
        "XBT" => "BTC".to_string(),
        other => other.to_string(),
    }
}

pub fn independentreserve_asset(value: &str) -> String {
    match normalize_asset(value).as_str() {
        "BTC" => "Xbt".to_string(),
        "AUD" => "Aud".to_string(),
        "SGD" => "Sgd".to_string(),
        "USD" => "Usd".to_string(),
        "ETH" => "Eth".to_string(),
        "XRP" => "Xrp".to_string(),
        "LTC" => "Ltc".to_string(),
        "BCH" => "Bch".to_string(),
        "USDT" => "Usdt".to_string(),
        "USDC" => "Usdc".to_string(),
        other => title_code(other),
    }
}

pub fn normalize_symbol_text(value: &str) -> String {
    value
        .trim()
        .replace(['-', '/', ':'], "_")
        .to_ascii_uppercase()
}

pub fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    string_or_number(value)?.parse().ok()
}

pub fn parse_timestamp_value(value: &Value) -> Option<chrono::DateTime<Utc>> {
    match value {
        Value::String(text) => chrono::DateTime::parse_from_rfc3339(text)
            .map(|timestamp| timestamp.with_timezone(&Utc))
            .ok()
            .or_else(|| {
                chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
                    .ok()
                    .map(|timestamp| timestamp.and_utc())
            }),
        Value::Number(number) => {
            let value = number.as_i64()?;
            if value > 10_000_000_000 {
                Utc.timestamp_millis_opt(value).single()
            } else {
                Utc.timestamp_opt(value, 0).single()
            }
        }
        _ => None,
    }
}

pub fn parse_error(exchange_id: ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error =
        ExchangeError::new(exchange_id, ExchangeErrorClass::Decode, message, Utc::now());
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

pub fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

pub fn market_rows(value: &Value) -> Vec<&Value> {
    value
        .get("Data")
        .and_then(Value::as_array)
        .or_else(|| value.get("data").and_then(Value::as_array))
        .or_else(|| value.get("markets").and_then(Value::as_array))
        .or_else(|| value.as_array())
        .map(|rows| rows.iter().collect())
        .unwrap_or_else(|| vec![value])
}

fn symbol_rule(
    exchange_id: &ExchangeId,
    row: &Value,
    primary: &str,
    secondary: &str,
) -> ExchangeApiResult<SymbolRules> {
    let scope = symbol_scope(exchange_id, &independentreserve_symbol(primary, secondary))?;
    let price_increment = string_or_number(
        row.get("minimumPriceIncrement")
            .or_else(|| row.get("MinimumPriceIncrement"))
            .or_else(|| row.get("priceIncrement")),
    );
    let quantity_increment = string_or_number(
        row.get("minimumVolumeIncrement")
            .or_else(|| row.get("MinimumVolumeIncrement"))
            .or_else(|| row.get("volumeIncrement")),
    );
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: scope,
        base_asset: primary.to_string(),
        quote_asset: secondary.to_string(),
        price_increment: price_increment.clone(),
        quantity_increment: quantity_increment.clone(),
        min_price: string_or_number(row.get("minimumPrice").or_else(|| row.get("MinimumPrice"))),
        max_price: None,
        min_quantity: string_or_number(
            row.get("minimumOrderVolume")
                .or_else(|| row.get("MinimumOrderVolume"))
                .or_else(|| row.get("minimumVolume")),
        ),
        max_quantity: string_or_number(
            row.get("maximumOrderVolume")
                .or_else(|| row.get("MaximumOrderVolume")),
        ),
        min_notional: string_or_number(
            row.get("minimumOrderValue")
                .or_else(|| row.get("MinimumOrderValue")),
        ),
        max_notional: None,
        price_precision: price_increment.as_deref().and_then(precision),
        quantity_precision: quantity_increment.as_deref().and_then(precision),
        supports_market_orders: true,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
    side: &str,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let value = value.ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("Independent Reserve order book missing {side}"),
            &Value::Null,
        )
    })?;
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Independent Reserve order book side must be an array",
            value,
        )
    })?;
    let mut levels = Vec::new();
    for row in rows {
        let price = decimal_as_f64(
            row.get("Price")
                .or_else(|| row.get("price"))
                .or_else(|| row.get(0)),
        )
        .ok_or_else(|| parse_error(exchange_id.clone(), "IR level missing price", row))?;
        let quantity = decimal_as_f64(
            row.get("Volume")
                .or_else(|| row.get("volume"))
                .or_else(|| row.get("quantity"))
                .or_else(|| row.get(1)),
        )
        .ok_or_else(|| parse_error(exchange_id.clone(), "IR level missing volume", row))?;
        levels.push(OrderBookLevel::new(price, quantity).map_err(validation_error)?);
    }
    if side == "bids" {
        levels.sort_by(|left, right| right.price.total_cmp(&left.price));
    } else {
        levels.sort_by(|left, right| left.price.total_cmp(&right.price));
    }
    Ok(levels)
}

fn symbol_matches(scope: &SymbolScope, symbol_text: &str, primary: &str, secondary: &str) -> bool {
    if scope.market_type != MarketType::Spot {
        return false;
    }
    let requested = normalize_symbol_text(&scope.exchange_symbol.symbol);
    requested == symbol_text
        || scope.canonical_symbol.as_ref().is_some_and(|canonical| {
            canonical.base_asset() == primary && canonical.quote_asset() == secondary
        })
}

fn is_inactive(row: &Value) -> bool {
    row.get("isActive")
        .or_else(|| row.get("IsActive"))
        .and_then(Value::as_bool)
        .is_some_and(|active| !active)
        || row
            .get("status")
            .or_else(|| row.get("Status"))
            .and_then(Value::as_str)
            .is_some_and(|status| {
                matches!(
                    status.to_ascii_lowercase().as_str(),
                    "inactive" | "disabled" | "offline"
                )
            })
}

fn precision(value: &str) -> Option<u32> {
    let text = value.trim().trim_end_matches('0');
    text.split_once('.')
        .map(|(_, decimals)| decimals.len() as u32)
}

fn title_code(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => {
            let mut text = first.to_ascii_uppercase().to_string();
            text.push_str(&chars.as_str().to_ascii_lowercase());
            text
        }
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use rustcta_types::ExchangeId;

    use super::{parse_orderbook_snapshot, parse_symbol_rules, symbol_scope};

    #[test]
    fn independentreserve_symbol_rules_should_keep_aud_sgd_usd_spot_markets() {
        let exchange = ExchangeId::new("independentreserve").expect("exchange id");
        let value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/independentreserve/rest/markets.json"
        ))
        .expect("markets fixture");

        let rules = parse_symbol_rules(&exchange, &[], &value).expect("rules");

        assert_eq!(rules.len(), 3);
        assert!(rules.iter().any(|rule| rule.base_asset == "BTC"
            && rule.quote_asset == "AUD"
            && rule.symbol.exchange_symbol.symbol == "BTC_AUD"));
        assert!(rules.iter().any(|rule| rule.quote_asset == "SGD"));
        assert!(rules.iter().any(|rule| rule.quote_asset == "USD"));
    }

    #[test]
    fn independentreserve_orderbook_fixture_should_parse_levels() {
        let exchange = ExchangeId::new("independentreserve").expect("exchange id");
        let symbol = symbol_scope(&exchange, "BTC_AUD").expect("symbol");
        let value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/independentreserve/rest/orderbook_btc_aud.json"
        ))
        .expect("orderbook fixture");

        let book = parse_orderbook_snapshot(&exchange, symbol, &value).expect("book");

        assert_eq!(book.bids[0].price, 100_000.0);
        assert_eq!(book.asks[0].price, 100_010.0);
        assert_eq!(book.sequence, Some(42));
    }
}
