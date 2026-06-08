use chrono::Utc;
use rustcta_exchange_api::{
    Balance, ExchangeApiResult, OrderState, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeId, ExchangePosition, ExchangeSymbol, Fill,
    FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion, TenantId, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    decimal_as_f64, first_timestamp, parse_error, required_str, response_result, string_or_number,
    symbol_scope_from_product, validation_error,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Balance>> {
    let rows = response_result(value).as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "delta balances response is not an array",
            value,
        )
    })?;
    let requested = requested_assets
        .iter()
        .map(|asset| asset.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for row in rows {
        let asset = row
            .get("asset_symbol")
            .or_else(|| row.get("asset"))
            .and_then(|asset| {
                asset.as_str().map(str::to_string).or_else(|| {
                    asset
                        .get("symbol")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
            })
            .unwrap_or_else(|| "UNKNOWN".to_string())
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_as_f64(
            row.get("available_balance")
                .or_else(|| row.get("available")),
        )
        .unwrap_or(0.0);
        let total =
            decimal_as_f64(row.get("balance").or_else(|| row.get("total"))).unwrap_or(available);
        let locked = (total - available).max(0.0);
        balances
            .push(AssetBalance::new(asset, total, available, locked).map_err(validation_error)?);
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

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_symbols: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let rows = response_result(value).as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "delta positions response is not an array",
            value,
        )
    })?;
    let requested = requested_symbols
        .iter()
        .map(|symbol| symbol.symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    rows.iter()
        .filter(|row| {
            requested.is_empty()
                || row
                    .get("product_symbol")
                    .or_else(|| row.get("symbol"))
                    .and_then(Value::as_str)
                    .is_some_and(|symbol| requested.contains(&symbol.to_ascii_uppercase()))
        })
        .map(|row| parse_position(exchange_id, tenant_id.clone(), account_id.clone(), row))
        .collect()
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let result = response_result(value);
    if result.is_null() {
        return Ok(None);
    }
    Ok(Some(parse_order_state(
        exchange_id,
        fallback_symbol,
        result,
    )?))
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let rows = response_result(value).as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "delta orders response is not an array",
            value,
        )
    })?;
    rows.iter()
        .map(|row| parse_order_state(exchange_id, fallback_symbol, row))
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let rows = response_result(value).as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "delta fills response is not an array",
            value,
        )
    })?;
    rows.iter()
        .map(|row| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                fallback_symbol,
                row,
            )
        })
        .collect()
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_from_private_payload(exchange_id, fallback_symbol, value)?;
    let order_type = parse_order_type(
        value
            .get("order_type")
            .or_else(|| value.get("orderType"))
            .and_then(Value::as_str)
            .unwrap_or("limit_order"),
    );
    let filled = decimal_as_f64(value.get("size")).unwrap_or(0.0)
        - decimal_as_f64(
            value
                .get("unfilled_size")
                .or_else(|| value.get("unfilledSize")),
        )
        .unwrap_or(0.0);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(
            value
                .get("client_order_id")
                .or_else(|| value.get("clientOrderId")),
        ),
        exchange_order_id: string_or_number(value.get("id")),
        side: parse_side(required_str(exchange_id, value, "side").unwrap_or("buy")),
        position_side: Some(PositionSide::Net),
        order_type,
        time_in_force: parse_time_in_force(value.get("time_in_force").and_then(Value::as_str))
            .or_else(|| time_in_force_from_order_type(order_type)),
        status: value
            .get("state")
            .or_else(|| value.get("status"))
            .and_then(Value::as_str)
            .map(map_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(value.get("size")).unwrap_or_else(|| "0".to_string()),
        price: string_or_number(
            value
                .get("limit_price")
                .or_else(|| value.get("price"))
                .or_else(|| value.get("stop_price")),
        )
        .filter(|value| !is_zero_decimal(value)),
        filled_quantity: (filled.max(0.0)).to_string(),
        average_fill_price: string_or_number(
            value
                .get("average_fill_price")
                .or_else(|| value.get("avg_price")),
        ),
        reduce_only: value
            .get("reduce_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: value
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(matches!(order_type, OrderType::PostOnly)),
        created_at: first_timestamp(value, &["created_at", "createdAt"]),
        updated_at: first_timestamp(value, &["updated_at", "updatedAt"]).unwrap_or_else(Utc::now),
    })
}

fn parse_position(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<ExchangePosition> {
    let symbol = symbol_from_private_payload(exchange_id, None, value)?;
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .unwrap_or(CanonicalSymbol::new("UNKNOWN", "USDT").map_err(validation_error)?);
    let size = decimal_as_f64(value.get("size").or_else(|| value.get("position"))).unwrap_or(0.0);
    let position = ExchangePosition {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        side: if size > 0.0 {
            PositionSide::Long
        } else if size < 0.0 {
            PositionSide::Short
        } else {
            PositionSide::Net
        },
        quantity: size.abs(),
        entry_price: decimal_as_f64(value.get("entry_price").or_else(|| value.get("entryPrice"))),
        mark_price: decimal_as_f64(value.get("mark_price").or_else(|| value.get("markPrice"))),
        liquidation_price: decimal_as_f64(
            value
                .get("liquidation_price")
                .or_else(|| value.get("liquidationPrice")),
        )
        .filter(|price| *price > 0.0),
        unrealized_pnl: decimal_as_f64(
            value
                .get("unrealized_pnl")
                .or_else(|| value.get("unrealizedPnl")),
        ),
        leverage: decimal_as_f64(value.get("leverage")),
        observed_at: first_timestamp(value, &["updated_at", "timestamp"]).unwrap_or_else(Utc::now),
    };
    position.validate().map_err(validation_error)?;
    Ok(position)
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = symbol_from_private_payload(exchange_id, fallback_symbol, value)?;
    let canonical_symbol = symbol
        .canonical_symbol
        .clone()
        .unwrap_or(CanonicalSymbol::new("UNKNOWN", "USDT").map_err(validation_error)?);
    let price = decimal_as_f64(value.get("price")).unwrap_or(0.0);
    let quantity =
        decimal_as_f64(value.get("size").or_else(|| value.get("quantity"))).unwrap_or(0.0);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("order_id").or_else(|| value.get("orderId"))),
        client_order_id: string_or_number(
            value
                .get("client_order_id")
                .or_else(|| value.get("clientOrderId")),
        ),
        fill_id: string_or_number(value.get("id").or_else(|| value.get("fill_id"))),
        side: parse_side(required_str(exchange_id, value, "side").unwrap_or("buy")),
        position_side: PositionSide::Net,
        status: FillStatus::Confirmed,
        liquidity_role: match value
            .get("role")
            .or_else(|| value.get("liquidity"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "maker" => LiquidityRole::Maker,
            "taker" => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        },
        price,
        quantity,
        quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
        fee_asset: value
            .get("commission_asset_symbol")
            .or_else(|| value.get("fee_asset"))
            .and_then(Value::as_str)
            .map(str::to_ascii_uppercase),
        fee_amount: decimal_as_f64(
            value
                .get("commission")
                .or_else(|| value.get("paid_commission"))
                .or_else(|| value.get("fee")),
        )
        .map(f64::abs),
        fee_rate: None,
        realized_pnl: decimal_as_f64(
            value
                .get("realized_pnl")
                .or_else(|| value.get("realizedPnl")),
        ),
        filled_at: first_timestamp(value, &["created_at", "updated_at", "timestamp"])
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn symbol_from_private_payload(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = fallback_symbol {
        return Ok(symbol.clone());
    }
    if let Some(product) = value.get("product") {
        return symbol_scope_from_product(exchange_id, product, None);
    }
    let symbol = value
        .get("product_symbol")
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "delta private payload missing product_symbol",
                value,
            )
        })?;
    let market_type = match value
        .get("contract_type")
        .or_else(|| value.get("contractType"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        contract
            if contract.contains("option")
                || contract.contains("call")
                || contract.contains("put") =>
        {
            MarketType::Option
        }
        contract if contract.contains("future") && !contract.contains("perpetual") => {
            MarketType::Futures
        }
        _ => MarketType::Perpetual,
    };
    let (base, quote) =
        split_symbol_guess(symbol).unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            market_type,
            symbol.to_ascii_uppercase(),
        )
        .map_err(validation_error)?,
    })
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

fn parse_side(side: &str) -> rustcta_types::OrderSide {
    match side.to_ascii_lowercase().as_str() {
        "sell" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(order_type: &str) -> OrderType {
    match order_type.to_ascii_lowercase().as_str() {
        "market_order" | "market" => OrderType::Market,
        "post_only" | "limit_order_post_only" => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value?.to_ascii_lowercase().as_str() {
        "gtc" => Some(TimeInForce::GTC),
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        "gtx" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn time_in_force_from_order_type(order_type: OrderType) -> Option<TimeInForce> {
    match order_type {
        OrderType::Market => None,
        OrderType::PostOnly => Some(TimeInForce::GTX),
        OrderType::IOC => Some(TimeInForce::IOC),
        OrderType::FOK => Some(TimeInForce::FOK),
        _ => Some(TimeInForce::GTC),
    }
}

fn map_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_lowercase().as_str() {
        "open" | "pending" => OrderStatus::New,
        "partially_filled" | "partial_filled" => OrderStatus::PartiallyFilled,
        "closed" | "filled" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|value| value == 0.0)
}
