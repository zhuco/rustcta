use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, SymbolRules, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot, SchemaVersion,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeltaFundingRate {
    pub symbol: SymbolScope,
    pub funding_rate: String,
    pub next_funding_at: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeltaOptionContract {
    pub symbol: SymbolScope,
    pub product_id: Option<u64>,
    pub option_kind: DeltaOptionKind,
    pub underlying_asset: String,
    pub quote_asset: String,
    pub settlement_asset: Option<String>,
    pub strike_price: Option<String>,
    pub expiry_at: Option<DateTime<Utc>>,
    pub contract_value: Option<String>,
    pub state: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaOptionKind {
    Call,
    Put,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeltaGreeksSnapshot {
    pub symbol: SymbolScope,
    pub mark_price: Option<String>,
    pub spot_price: Option<String>,
    pub delta: Option<String>,
    pub gamma: Option<String>,
    pub theta: Option<String>,
    pub vega: Option<String>,
    pub rho: Option<String>,
    pub implied_volatility: Option<String>,
    pub open_interest: Option<String>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeltaOptionChainSnapshot {
    pub underlying_asset: String,
    pub quote_asset: Option<String>,
    pub contracts: Vec<DeltaOptionContract>,
    pub greeks: Vec<DeltaGreeksSnapshot>,
    pub observed_at: DateTime<Utc>,
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    product_rows(value)?
        .iter()
        .map(|product| parse_symbol_rule(exchange_id, product))
        .collect()
}

pub fn parse_option_chain(
    exchange_id: &ExchangeId,
    underlying_asset: &str,
    value: &Value,
) -> ExchangeApiResult<DeltaOptionChainSnapshot> {
    let rows = product_rows(value)?;
    let mut contracts = Vec::new();
    let mut greeks = Vec::new();
    for row in rows {
        let contract = parse_option_contract(exchange_id, row)?;
        greeks.push(parse_greeks_snapshot(
            exchange_id,
            contract.symbol.clone(),
            row,
        )?);
        contracts.push(contract);
    }
    let quote_asset = contracts
        .first()
        .map(|contract| contract.quote_asset.clone());
    Ok(DeltaOptionChainSnapshot {
        underlying_asset: underlying_asset.trim().to_ascii_uppercase(),
        quote_asset,
        contracts,
        greeks,
        observed_at: Utc::now(),
    })
}

pub fn parse_funding_rates(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<DeltaFundingRate>> {
    let rows = response_result(value);
    let rows = rows.as_array().map(Vec::as_slice).unwrap_or_else(|| {
        rows.get("tickers")
            .or_else(|| rows.get("rows"))
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    });
    rows.iter()
        .filter(|row| {
            string_or_number(
                row.get("funding_rate")
                    .or_else(|| row.get("fundingRate"))
                    .or_else(|| row.get("predicted_funding_rate")),
            )
            .is_some()
        })
        .map(|row| {
            let symbol = required_str(exchange_id, row, "symbol")?;
            let scope = symbol_scope_from_product(exchange_id, row, Some(symbol))?;
            Ok(DeltaFundingRate {
                symbol: scope,
                funding_rate: string_or_number(
                    row.get("funding_rate")
                        .or_else(|| row.get("fundingRate"))
                        .or_else(|| row.get("predicted_funding_rate")),
                )
                .ok_or_else(|| parse_error(exchange_id.clone(), "funding row missing rate", row))?,
                next_funding_at: first_timestamp(row, &["next_funding_time", "funding_time"]),
                observed_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_fees_from_products(
    exchange_id: &ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let requested_symbols = requested
        .iter()
        .map(|symbol| symbol.exchange_symbol.symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    product_rows(value)?
        .iter()
        .filter(|product| {
            requested_symbols.is_empty()
                || product
                    .get("symbol")
                    .and_then(Value::as_str)
                    .is_some_and(|symbol| requested_symbols.contains(&symbol.to_ascii_uppercase()))
        })
        .map(|product| {
            let scope = symbol_scope_from_product(exchange_id, product, None)?;
            Ok(FeeRateSnapshot {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: scope,
                maker_rate: string_or_number(
                    product
                        .get("maker_commission_rate")
                        .or_else(|| product.get("maker_fee"))
                        .or_else(|| product.get("maker_fee_rate")),
                )
                .unwrap_or_else(|| "0".to_string()),
                taker_rate: string_or_number(
                    product
                        .get("taker_commission_rate")
                        .or_else(|| product.get("taker_fee"))
                        .or_else(|| product.get("taker_fee_rate")),
                )
                .unwrap_or_else(|| "0".to_string()),
                source: Some("delta.products".to_string()),
                updated_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let result = response_result(value);
    let bids = parse_levels(
        exchange_id,
        result
            .get("buy")
            .or_else(|| result.get("bids"))
            .or_else(|| result.get("buy_book")),
    )?;
    let asks = parse_levels(
        exchange_id,
        result
            .get("sell")
            .or_else(|| result.get("asks"))
            .or_else(|| result.get("sell_book")),
    )?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "delta order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = result
        .get("last_sequence_no")
        .or_else(|| result.get("sequence_no"))
        .or_else(|| result.get("sequence"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = first_timestamp(result, &["timestamp", "updated_at"]);
    Ok(snapshot)
}

pub fn normalize_delta_symbol(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    let raw = symbol.exchange_symbol.symbol.trim();
    if !raw.is_empty() {
        return Ok(raw.to_ascii_uppercase());
    }
    symbol
        .canonical_symbol
        .as_ref()
        .map(|canonical| format!("{}{}", canonical.base_asset(), canonical.quote_asset()))
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "delta symbol requires exchange_symbol or canonical_symbol".to_string(),
        })
}

pub fn normalize_depth(depth: u32) -> u32 {
    depth.clamp(1, 100)
}

pub(super) fn response_result(value: &Value) -> &Value {
    value.get("result").unwrap_or(value)
}

pub(super) fn product_rows(value: &Value) -> ExchangeApiResult<&[Value]> {
    response_result(value)
        .as_array()
        .map(Vec::as_slice)
        .or_else(|| {
            response_result(value)
                .get("products")
                .or_else(|| response_result(value).get("rows"))
                .and_then(Value::as_array)
                .map(Vec::as_slice)
        })
        .ok_or_else(|| {
            parse_error(
                ExchangeId::new("delta").unwrap_or_else(|_| ExchangeId::unchecked("delta")),
                "delta products response is not an array",
                value,
            )
        })
}

pub(super) fn parse_option_contract(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<DeltaOptionContract> {
    let symbol = symbol_scope_from_product(exchange_id, value, None)?;
    let contract_type = value
        .get("contract_type")
        .or_else(|| value.get("contractType"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let option_kind = if contract_type.contains("call") {
        DeltaOptionKind::Call
    } else if contract_type.contains("put") {
        DeltaOptionKind::Put
    } else {
        DeltaOptionKind::Unknown
    };
    Ok(DeltaOptionContract {
        symbol,
        product_id: value.get("id").and_then(value_as_u64),
        option_kind,
        underlying_asset: asset_code(value.get("underlying_asset"))
            .or_else(|| string_or_number(value.get("underlying_asset_symbol")))
            .unwrap_or_else(|| "UNKNOWN".to_string()),
        quote_asset: asset_code(value.get("quoting_asset"))
            .or_else(|| asset_code(value.get("quote_asset")))
            .unwrap_or_else(|| "USDT".to_string()),
        settlement_asset: asset_code(value.get("settling_asset")),
        strike_price: string_or_number(value.get("strike_price").or_else(|| value.get("strike"))),
        expiry_at: first_timestamp(value, &["settlement_time", "expiry_time", "expires_at"]),
        contract_value: string_or_number(
            value
                .get("contract_value")
                .or_else(|| value.get("contractValue")),
        ),
        state: value
            .get("state")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

pub(super) fn parse_greeks_snapshot(
    _exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<DeltaGreeksSnapshot> {
    let greeks = value
        .get("greeks")
        .or_else(|| value.get("g"))
        .unwrap_or(value);
    Ok(DeltaGreeksSnapshot {
        symbol,
        mark_price: string_or_number(value.get("mark_price").or_else(|| value.get("markPrice"))),
        spot_price: string_or_number(value.get("spot_price").or_else(|| value.get("spotPrice"))),
        delta: greek_value(greeks, "delta", "d"),
        gamma: greek_value(greeks, "gamma", "g"),
        theta: greek_value(greeks, "theta", "t"),
        vega: greek_value(greeks, "vega", "v"),
        rho: greek_value(greeks, "rho", "r"),
        implied_volatility: string_or_number(
            value
                .get("implied_volatility")
                .or_else(|| value.get("iv"))
                .or_else(|| greeks.get("iv")),
        ),
        open_interest: string_or_number(
            value
                .get("oi")
                .or_else(|| value.get("open_interest"))
                .or_else(|| value.get("openInterest")),
        ),
        observed_at: first_timestamp(value, &["timestamp", "updated_at"]).unwrap_or_else(Utc::now),
    })
}

pub(super) fn symbol_scope_from_product(
    exchange_id: &ExchangeId,
    value: &Value,
    symbol_hint: Option<&str>,
) -> ExchangeApiResult<SymbolScope> {
    let symbol_text = symbol_hint
        .map(str::to_string)
        .or_else(|| {
            value
                .get("symbol")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .ok_or_else(|| parse_error(exchange_id.clone(), "delta product missing symbol", value))?
        .to_ascii_uppercase();
    let contract_type = value
        .get("contract_type")
        .or_else(|| value.get("contractType"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let market_type = if contract_type.contains("option")
        || contract_type.contains("call")
        || contract_type.contains("put")
    {
        MarketType::Option
    } else if contract_type.contains("future") && !contract_type.contains("perpetual") {
        MarketType::Futures
    } else {
        MarketType::Perpetual
    };
    let base = asset_code(value.get("underlying_asset"))
        .or_else(|| string_or_number(value.get("underlying_asset_symbol")))
        .or_else(|| split_symbol_guess(&symbol_text).map(|(base, _)| base))
        .unwrap_or_else(|| "UNKNOWN".to_string());
    let quote = asset_code(value.get("quoting_asset"))
        .or_else(|| asset_code(value.get("quote_asset")))
        .or_else(|| split_symbol_guess(&symbol_text).map(|(_, quote)| quote))
        .unwrap_or_else(|| "USDT".to_string());
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol_text)
            .map_err(validation_error)?,
    })
}

fn parse_symbol_rule(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let symbol = symbol_scope_from_product(exchange_id, value, None)?;
    let market_type = symbol.market_type;
    let base_asset = symbol
        .canonical_symbol
        .as_ref()
        .map(|canonical| canonical.base_asset().to_string())
        .unwrap_or_else(|| "UNKNOWN".to_string());
    let quote_asset = symbol
        .canonical_symbol
        .as_ref()
        .map(|canonical| canonical.quote_asset().to_string())
        .unwrap_or_else(|| "USDT".to_string());
    let tradable = value
        .get("state")
        .and_then(Value::as_str)
        .map(|state| matches!(state, "live" | "online" | "trading"))
        .unwrap_or(true);
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        base_asset,
        quote_asset,
        price_increment: string_or_number(
            value
                .get("tick_size")
                .or_else(|| value.get("price_increment"))
                .or_else(|| value.pointer("/product_specs/tick_size")),
        ),
        quantity_increment: string_or_number(
            value
                .get("contract_value")
                .or_else(|| value.get("size_increment"))
                .or_else(|| value.pointer("/product_specs/contract_value")),
        ),
        min_price: None,
        max_price: None,
        min_quantity: string_or_number(
            value
                .get("min_order_size")
                .or_else(|| value.pointer("/product_specs/min_order_size")),
        ),
        max_quantity: string_or_number(
            value
                .get("max_order_size")
                .or_else(|| value.pointer("/product_specs/max_order_size")),
        ),
        min_notional: None,
        max_notional: None,
        price_precision: precision_from_increment(
            string_or_number(
                value
                    .get("tick_size")
                    .or_else(|| value.pointer("/product_specs/tick_size")),
            )
            .as_deref(),
        ),
        quantity_precision: precision_from_increment(
            string_or_number(
                value
                    .get("contract_value")
                    .or_else(|| value.pointer("/product_specs/contract_value")),
            )
            .as_deref(),
        ),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: !matches!(market_type, MarketType::Spot),
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
            "delta order book missing levels",
            &Value::Null,
        )
    })?;
    levels
        .iter()
        .map(|level| match level {
            Value::Array(array) => {
                let price = array.first().and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid delta level price", level)
                })?;
                let quantity = array.get(1).and_then(number_from_value).ok_or_else(|| {
                    parse_error(exchange_id.clone(), "invalid delta level quantity", level)
                })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            Value::Object(map) => {
                let price = map
                    .get("price")
                    .and_then(number_from_value)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid delta level price", level)
                    })?;
                let quantity = map
                    .get("size")
                    .or_else(|| map.get("quantity"))
                    .and_then(number_from_value)
                    .ok_or_else(|| {
                        parse_error(exchange_id.clone(), "invalid delta level quantity", level)
                    })?;
                OrderBookLevel::new(price, quantity).map_err(validation_error)
            }
            _ => Err(parse_error(
                exchange_id.clone(),
                "invalid delta order book level",
                level,
            )),
        })
        .collect()
}

pub(super) fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            &format!("delta payload missing field {field}"),
            value,
        )
    })
}

pub(super) fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.trim().is_empty() && text != "null" => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub(super) fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    string_or_number(value)?.parse().ok()
}

pub(super) fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

pub(super) fn first_timestamp(value: &Value, keys: &[&str]) -> Option<DateTime<Utc>> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        if let Some(number) = value_as_i64(value) {
            if number > 1_000_000_000_000_000 {
                DateTime::<Utc>::from_timestamp_micros(number)
            } else if number > 1_000_000_000 {
                DateTime::<Utc>::from_timestamp_millis(number)
            } else {
                DateTime::<Utc>::from_timestamp(number, 0)
            }
        } else {
            value.as_str()?.parse::<DateTime<Utc>>().ok()
        }
    })
}

fn asset_code(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.to_ascii_uppercase()),
        Value::Object(map) => map
            .get("symbol")
            .or_else(|| map.get("name"))
            .and_then(Value::as_str)
            .map(str::to_ascii_uppercase),
        _ => None,
    }
}

fn split_symbol_guess(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 7] = ["USDT", "USDC", "USD", "BTC", "ETH", "INR", "EUR"];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), (*quote).to_string()))
    })
}

fn greek_value(value: &Value, long: &str, short: &str) -> Option<String> {
    string_or_number(value.get(long).or_else(|| value.get(short)))
}

fn precision_from_increment(increment: Option<&str>) -> Option<u32> {
    let decimals = increment?.split_once('.')?.1.trim_end_matches('0');
    Some(decimals.len() as u32)
}

pub(super) fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

pub(super) fn parse_error(
    exchange_id: ExchangeId,
    message: &str,
    value: &Value,
) -> ExchangeApiError {
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
