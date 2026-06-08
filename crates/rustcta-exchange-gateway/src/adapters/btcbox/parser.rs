use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, SymbolRules, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeError, ExchangeErrorClass, ExchangeId, ExchangeSymbol, MarketType,
    OrderBookLevel, OrderBookSnapshot,
};
use serde_json::Value;

pub fn normalize_btcbox_symbol(value: &str) -> ExchangeApiResult<String> {
    let symbol = value.trim().replace('-', "_").to_ascii_lowercase();
    if symbol.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "btcbox symbol must not be empty".to_string(),
        });
    }
    if symbol.contains('_') {
        let (base, quote) =
            symbol
                .split_once('_')
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("btcbox symbol {value} is not base_quote"),
                })?;
        if quote != "jpy" {
            return Err(ExchangeApiError::Unsupported {
                operation: "btcbox.non_jpy_quote",
            });
        }
        Ok(format!("{base}_jpy"))
    } else {
        Ok(format!("{symbol}_jpy"))
    }
}

pub fn coin_param_from_symbol(value: &str) -> ExchangeApiResult<String> {
    let symbol = normalize_btcbox_symbol(value)?;
    let (base, _) = symbol
        .split_once('_')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("btcbox symbol {value} is not base_quote"),
        })?;
    if !matches!(base, "btc" | "bch" | "ltc" | "eth") {
        return Err(ExchangeApiError::Unsupported {
            operation: "btcbox.order_book_unverified_for_ticker_only_symbol",
        });
    }
    Ok(base.to_string())
}

pub fn canonical_from_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let symbol = normalize_btcbox_symbol(symbol)?;
    let (base, quote) = symbol
        .split_once('_')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("btcbox symbol {symbol} is not base_quote"),
        })?;
    CanonicalSymbol::new(&base.to_ascii_uppercase(), &quote.to_ascii_uppercase())
        .map_err(validation_error)
}

pub fn parse_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let rows = value
        .as_object()
        .ok_or_else(|| parse_error(exchange_id, "tickers object", value))?;
    let mut symbols = rows
        .keys()
        .filter(|key| key.contains('_'))
        .map(|key| {
            let exchange_symbol = normalize_btcbox_symbol(key)?;
            let canonical = canonical_from_symbol(&exchange_symbol)?;
            let symbol =
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, &exchange_symbol)
                    .map_err(validation_error)?;
            Ok(SymbolRules {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: SymbolScope {
                    exchange: exchange_id.clone(),
                    market_type: MarketType::Spot,
                    canonical_symbol: Some(canonical.clone()),
                    exchange_symbol: symbol,
                },
                base_asset: canonical.base_asset().to_string(),
                quote_asset: canonical.quote_asset().to_string(),
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
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    symbols.sort_by(|left, right| {
        left.symbol
            .exchange_symbol
            .symbol
            .cmp(&right.symbol.exchange_symbol.symbol)
    });
    Ok(symbols)
}

pub fn parse_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let canonical =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "btcbox order book request requires canonical_symbol".to_string(),
            })?;
    let mut bids = parse_levels(exchange_id, value.get("bids"))?;
    let mut asks = parse_levels(exchange_id, value.get("asks"))?;
    bids.sort_by(|left, right| right.price.total_cmp(&left.price));
    asks.sort_by(|left, right| left.price.total_cmp(&right.price));
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        MarketType::Spot,
        canonical,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    Ok(snapshot)
}

fn parse_levels(
    exchange_id: &ExchangeId,
    value: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let rows = value
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id, "order book levels", &Value::Null))?;
    rows.iter()
        .map(|row| {
            let level = row
                .as_array()
                .ok_or_else(|| parse_error(exchange_id, "price/amount level", row))?;
            let price = level
                .first()
                .and_then(value_f64)
                .ok_or_else(|| parse_error(exchange_id, "price level value", row))?;
            let quantity = level
                .get(1)
                .and_then(value_f64)
                .ok_or_else(|| parse_error(exchange_id, "quantity level value", row))?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn value_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn parse_error(exchange_id: &ExchangeId, message: &str, raw: &Value) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::Decode,
        format!("btcbox parser expected {message}"),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: rustcta_types::ValidationError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
