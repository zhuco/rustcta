use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OrderState, SymbolRules, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeError, ExchangeErrorClass,
    ExchangeId, ExchangePosition, ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType,
    OrderBookLevel, OrderBookSnapshot, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion, TenantId, TimeInForce,
};
use serde_json::Value;

pub fn parse_markets(
    exchange_id: &ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let markets = value
        .get("markets")
        .and_then(Value::as_object)
        .ok_or_else(|| parse_error("dydx perpetualMarkets response missing markets"))?;
    let mut rules = Vec::new();
    for (ticker, market) in markets {
        if !requested.is_empty()
            && !requested.iter().any(|requested| {
                requested.market_type == MarketType::Perpetual
                    && requested
                        .exchange_symbol
                        .symbol
                        .eq_ignore_ascii_case(ticker)
            })
        {
            continue;
        }
        let (base, quote) = split_market(ticker);
        rules.push(SymbolRules {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: SymbolScope {
                exchange: exchange_id.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol: Some(
                    CanonicalSymbol::new(&base, &quote).map_err(validation_error)?,
                ),
                exchange_symbol: ExchangeSymbol::new(
                    exchange_id.clone(),
                    MarketType::Perpetual,
                    ticker,
                )
                .map_err(validation_error)?,
            },
            base_asset: base,
            quote_asset: quote,
            price_increment: value_as_string(market.get("tickSize")),
            quantity_increment: value_as_string(market.get("stepSize")),
            min_price: None,
            max_price: None,
            min_quantity: value_as_string(market.get("stepSize")),
            max_quantity: None,
            min_notional: None,
            max_notional: None,
            price_precision: None,
            quantity_precision: None,
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: true,
            supports_reduce_only: true,
            updated_at: Utc::now(),
        });
    }
    Ok(rules)
}

pub fn parse_orderbook(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(value.get("bids"), "bids")?;
    let asks = parse_levels(value.get("asks"), "asks")?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "dydx order book request requires canonical_symbol".to_string(),
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
    Ok(snapshot)
}

pub fn parse_subaccount_balance(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<ExchangeBalance> {
    let subaccount = value.get("subaccount").unwrap_or(value);
    let equity = number_like(subaccount.get("equity")).unwrap_or(0.0);
    let free_collateral = number_like(subaccount.get("freeCollateral")).unwrap_or(equity);
    let usdc = AssetBalance::new(
        "USDC",
        equity,
        free_collateral,
        (equity - free_collateral).max(0.0),
    )
    .map_err(validation_error)?;
    Ok(ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        balances: vec![usdc],
        observed_at: Utc::now(),
    })
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let positions = value
        .get("positions")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error("dydx positions response missing positions"))?;
    positions
        .iter()
        .filter(|position| {
            number_like(position.get("size"))
                .or_else(|| number_like(position.get("netSize")))
                .unwrap_or(0.0)
                != 0.0
        })
        .map(|position| {
            let ticker = required_str(position, "market")?;
            let (base, quote) = split_market(ticker);
            let size = number_like(position.get("size"))
                .or_else(|| number_like(position.get("netSize")))
                .unwrap_or(0.0);
            let parsed = ExchangePosition {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol: CanonicalSymbol::new(base, quote).map_err(validation_error)?,
                exchange_symbol: Some(
                    ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, ticker)
                        .map_err(validation_error)?,
                ),
                side: if size >= 0.0 {
                    PositionSide::Long
                } else {
                    PositionSide::Short
                },
                quantity: size.abs(),
                entry_price: number_like(position.get("entryPrice")),
                mark_price: None,
                liquidation_price: None,
                unrealized_pnl: number_like(position.get("unrealizedPnl")),
                leverage: None,
                observed_at: Utc::now(),
            };
            parsed.validate().map_err(validation_error)?;
            Ok(parsed)
        })
        .collect()
}

pub fn parse_orders(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value
        .get("orders")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error("dydx orders response missing orders"))?;
    orders
        .iter()
        .map(|order| parse_order(exchange_id, order))
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let fills = value
        .get("fills")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error("dydx fills response missing fills"))?;
    fills
        .iter()
        .map(|fill| parse_fill(exchange_id, &tenant_id, &account_id, fill))
        .collect()
}

fn parse_order(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<OrderState> {
    let ticker = required_str(value, "ticker").or_else(|_| required_str(value, "market"))?;
    let (base, quote) = split_market(ticker);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, ticker)
            .map_err(validation_error)?,
        client_order_id: value_as_string(value.get("clientMetadata")),
        exchange_order_id: value_as_string(value.get("id")),
        side: parse_side(value.get("side")),
        position_side: Some(PositionSide::Net),
        order_type: parse_order_type(value.get("type")),
        time_in_force: Some(TimeInForce::GTC),
        status: parse_status(value.get("status")),
        quantity: value_as_string(value.get("size")).unwrap_or_else(|| "0".to_string()),
        price: value_as_string(value.get("price")),
        filled_quantity: value_as_string(value.get("totalFilled"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: None,
        reduce_only: value
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: value
            .get("postOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: parse_time(value.get("createdAt")),
        updated_at: parse_time(value.get("updatedAt")).unwrap_or_else(Utc::now),
    })
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: &TenantId,
    account_id: &AccountId,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let ticker = required_str(value, "ticker").or_else(|_| required_str(value, "market"))?;
    let (base, quote) = split_market(ticker);
    let price =
        number_like(value.get("price")).ok_or_else(|| parse_error("dydx fill missing price"))?;
    let quantity =
        number_like(value.get("size")).ok_or_else(|| parse_error("dydx fill missing size"))?;
    let fill = Fill {
        schema_version: SchemaVersion::current(),
        tenant_id: tenant_id.clone(),
        account_id: account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: CanonicalSymbol::new(base, quote).map_err(validation_error)?,
        exchange_symbol: Some(
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, ticker)
                .map_err(validation_error)?,
        ),
        order_id: value_as_string(value.get("orderId")),
        client_order_id: None,
        fill_id: value_as_string(value.get("id")),
        side: parse_side(value.get("side")),
        position_side: PositionSide::Net,
        status: FillStatus::Confirmed,
        liquidity_role: match value.get("liquidity").and_then(Value::as_str) {
            Some("MAKER") | Some("maker") => LiquidityRole::Maker,
            Some("TAKER") | Some("taker") => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        },
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: Some("USDC".to_string()),
        fee_amount: number_like(value.get("fee")),
        fee_rate: None,
        realized_pnl: None,
        filled_at: parse_time(value.get("createdAt")).unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    };
    fill.validate().map_err(validation_error)?;
    Ok(fill)
}

fn parse_levels(value: Option<&Value>, field: &str) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(format!("dydx order book missing {field}")))?
        .iter()
        .map(|level| {
            let price = number_like(level.get("price").or_else(|| level.get(0)))
                .ok_or_else(|| parse_error("dydx level missing price"))?;
            let size = number_like(level.get("size").or_else(|| level.get(1)))
                .ok_or_else(|| parse_error("dydx level missing size"))?;
            OrderBookLevel::new(price, size).map_err(validation_error)
        })
        .collect()
}

pub fn split_market(ticker: &str) -> (String, String) {
    let normalized = ticker.trim().to_ascii_uppercase();
    if let Some((base, quote)) = normalized.split_once('-') {
        (base.to_string(), quote.to_string())
    } else {
        (normalized, "USD".to_string())
    }
}

fn parse_side(value: Option<&Value>) -> OrderSide {
    match value
        .and_then(Value::as_str)
        .unwrap_or("BUY")
        .to_ascii_uppercase()
        .as_str()
    {
        "SELL" | "ASK" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(value: Option<&Value>) -> OrderType {
    match value
        .and_then(Value::as_str)
        .unwrap_or("LIMIT")
        .to_ascii_uppercase()
        .as_str()
    {
        "MARKET" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_status(value: Option<&Value>) -> OrderStatus {
    match value
        .and_then(Value::as_str)
        .unwrap_or("OPEN")
        .to_ascii_uppercase()
        .as_str()
    {
        "OPEN" | "BEST_EFFORT_OPENED" => OrderStatus::Open,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "BEST_EFFORT_CANCELED" => OrderStatus::Cancelled,
        "UNTRIGGERED" | "PENDING" => OrderStatus::New,
        _ => OrderStatus::Unknown,
    }
}

fn parse_time(value: Option<&Value>) -> Option<DateTime<Utc>> {
    value
        .and_then(Value::as_str)
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|time| time.with_timezone(&Utc))
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn number_like(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn required_str<'a>(value: &'a Value, field: &str) -> ExchangeApiResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| parse_error(format!("dydx response missing {field}")))
}

fn parse_error(message: impl Into<String>) -> ExchangeApiError {
    ExchangeApiError::Exchange(ExchangeError::new(
        ExchangeId::new("dydx").expect("valid exchange id"),
        ExchangeErrorClass::Decode,
        message.into(),
        Utc::now(),
    ))
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
