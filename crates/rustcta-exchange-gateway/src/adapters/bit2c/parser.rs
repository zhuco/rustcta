use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bit2cPair {
    pub venue: &'static str,
    pub base: &'static str,
    pub quote: &'static str,
}

pub const SUPPORTED_PAIRS: &[Bit2cPair] = &[
    Bit2cPair {
        venue: "BtcNis",
        base: "BTC",
        quote: "NIS",
    },
    Bit2cPair {
        venue: "EthNis",
        base: "ETH",
        quote: "NIS",
    },
    Bit2cPair {
        venue: "LtcNis",
        base: "LTC",
        quote: "NIS",
    },
    Bit2cPair {
        venue: "UsdcNis",
        base: "USDC",
        quote: "NIS",
    },
];

pub fn symbol_rules(
    exchange_id: &ExchangeId,
    requested: &[SymbolScope],
) -> ExchangeApiResult<Vec<SymbolRules>> {
    if requested.is_empty() {
        return SUPPORTED_PAIRS
            .iter()
            .map(|pair| pair_to_rules(exchange_id, pair))
            .collect();
    }

    requested
        .iter()
        .map(|symbol| {
            let pair = pair_from_symbol(&symbol.exchange_symbol.symbol)?;
            pair_to_rules(exchange_id, &pair)
        })
        .collect()
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    depth: u32,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let pair = pair_from_symbol(&symbol.exchange_symbol.symbol)?;
    let mut bids = parse_levels(exchange_id, value.get("bids"))?;
    let mut asks = parse_levels(exchange_id, value.get("asks"))?;
    let max_depth = depth as usize;
    bids.truncate(max_depth);
    asks.truncate(max_depth);
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .unwrap_or(CanonicalSymbol::new(pair.base, pair.quote).map_err(validation_error)?);
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
    Ok(snapshot)
}

pub fn pair_from_symbol(symbol: &str) -> ExchangeApiResult<Bit2cPair> {
    let normalized = symbol
        .trim()
        .replace('-', "/")
        .replace('_', "/")
        .to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "Bit2C symbol must not be empty".to_string(),
        });
    }

    for pair in SUPPORTED_PAIRS {
        if normalized == pair.venue.to_ascii_uppercase()
            || normalized == format!("{}/{}", pair.base, pair.quote)
            || normalized == format!("{}{}", pair.base, pair.quote)
        {
            return Ok(*pair);
        }
    }

    Err(ExchangeApiError::Unsupported {
        operation: "bit2c.unsupported_symbol",
    })
}

pub fn normalize_depth(depth: Option<u32>) -> u32 {
    depth.unwrap_or(50).clamp(1, 200)
}

pub fn classify_bit2c_error(message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if msg.contains("apikey") || msg.contains("api key") || msg.contains("signature") {
        ExchangeErrorClass::Authentication
    } else if msg.contains("nonce") {
        ExchangeErrorClass::InvalidRequest
    } else if msg.contains("no order") || msg.contains("not found") {
        ExchangeErrorClass::OrderNotFound
    } else if msg.contains("balance") || msg.contains("fund") {
        ExchangeErrorClass::InsufficientBalance
    } else if msg.contains("pair") || msg.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if msg.contains("rate") || msg.contains("limit") {
        ExchangeErrorClass::RateLimited
    } else {
        ExchangeErrorClass::Unknown
    }
}

pub fn exchange_error(
    exchange_id: ExchangeId,
    message: impl Into<String>,
    raw: Value,
) -> ExchangeApiError {
    let message = message.into();
    let mut error = ExchangeError::new(
        exchange_id,
        classify_bit2c_error(&message),
        message,
        Utc::now(),
    );
    error.raw = Some(raw);
    ExchangeApiError::Exchange(error)
}

fn pair_to_rules(exchange_id: &ExchangeId, pair: &Bit2cPair) -> ExchangeApiResult<SymbolRules> {
    let canonical_symbol = CanonicalSymbol::new(pair.base, pair.quote).map_err(validation_error)?;
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, pair.venue)
                .map_err(validation_error)?,
        },
        base_asset: pair.base.to_string(),
        quote_asset: pair.quote.to_string(),
        price_increment: None,
        quantity_increment: None,
        min_price: None,
        max_price: None,
        min_quantity: None,
        max_quantity: None,
        min_notional: None,
        max_notional: None,
        price_precision: None,
        quantity_precision: None,
        supports_market_orders: false,
        supports_limit_orders: true,
        supports_post_only: false,
        supports_reduce_only: false,
        updated_at: Utc::now(),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    levels: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = levels.and_then(Value::as_array).ok_or_else(|| {
        ExchangeApiError::Exchange(ExchangeError::new(
            exchange_id.clone(),
            ExchangeErrorClass::Decode,
            "Bit2C order book missing bids/asks array",
            Utc::now(),
        ))
    })?;
    levels
        .iter()
        .map(|level| {
            let values = level.as_array().ok_or_else(|| {
                ExchangeApiError::Exchange(ExchangeError::new(
                    exchange_id.clone(),
                    ExchangeErrorClass::Decode,
                    "Bit2C order book level must be [price, amount]",
                    Utc::now(),
                ))
            })?;
            let price = values.first().and_then(number_from_value).ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: format!("invalid Bit2C order book price: {level}"),
                }
            })?;
            let quantity = values.get(1).and_then(number_from_value).ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: format!("invalid Bit2C order book quantity: {level}"),
                }
            })?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
