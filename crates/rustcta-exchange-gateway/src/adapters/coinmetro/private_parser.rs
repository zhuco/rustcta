use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OrderState, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill, FillStatus, LiquidityRole,
    MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    decimal_value_to_f64, format_decimal, infer_assets_from_symbol, normalize_coinmetro_symbol,
    number_from_value, parse_error, string_or_number, validation_error, value_as_i64,
    value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let object = value
        .get("data")
        .unwrap_or(value)
        .as_object()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "coinmetro balances response is not an object",
                value,
            )
        })?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for (asset, row) in object {
        let asset = asset.trim().to_ascii_uppercase();
        if asset == "TOTAL" || (!requested.is_empty() && !requested.contains(&asset)) {
            continue;
        }
        let total = decimal_value_to_f64(row.get(&asset))?.unwrap_or(0.0);
        balances.push(AssetBalance::new(asset, total, total, 0.0).map_err(validation_error)?);
    }
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let (exchange_symbol, canonical_symbol, base_asset, quote_asset) =
        order_symbol(exchange_id, symbol_hint, value)?;
    let side = parse_order_side(value, &base_asset, &quote_asset)?;
    let (quantity, filled_quantity) = base_quantities(value, side);
    let price = limit_price(value, side);
    let average_fill_price = average_fill_price(value, side);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol,
        client_order_id: None,
        exchange_order_id: value_as_string(value.get("orderID").or_else(|| value.get("_id"))),
        side,
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(value),
        time_in_force: parse_time_in_force(value),
        status: parse_order_status(value, &quantity, &filled_quantity),
        quantity,
        price,
        filled_quantity,
        average_fill_price,
        reduce_only: false,
        post_only: false,
        created_at: first_timestamp(value, &["creationTime"]),
        updated_at: first_timestamp(value, &["completionTime", "lastFillTime", "creationTime"])
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "coinmetro open orders response is not an array",
                value,
            )
        })?
        .iter()
        .map(|order| parse_order_state(exchange_id, symbol_hint, order))
        .collect()
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinmetro get_recent_fills requires canonical_symbol".to_string(),
            })?;
    let fills = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "coinmetro fills response is not an array",
                value,
            )
        })?;
    fills
        .iter()
        .map(|fill| {
            let price = decimal_value_to_f64(fill.get("price"))?.unwrap_or(0.0);
            let quantity = decimal_value_to_f64(fill.get("qty"))?.unwrap_or(0.0);
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("orderID")),
                client_order_id: None,
                fill_id: value_as_string(fill.get("seqNumber")),
                side: fill
                    .get("side")
                    .and_then(Value::as_str)
                    .map(parse_fill_side)
                    .transpose()?
                    .unwrap_or(OrderSide::Buy),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price,
                quantity,
                quote_quantity: Some(price * quantity),
                fee_asset: None,
                fee_amount: None,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp(fill, &["timestamp"]).unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn order_symbol(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<(
    ExchangeSymbol,
    Option<rustcta_types::CanonicalSymbol>,
    String,
    String,
)> {
    if let Some(symbol) = symbol_hint {
        let canonical = symbol.canonical_symbol.clone();
        let (base, quote) = if let Some(canonical) = &canonical {
            (
                canonical.base_asset().to_string(),
                canonical.quote_asset().to_string(),
            )
        } else {
            infer_assets_from_symbol(&symbol.exchange_symbol.symbol)?
        };
        return Ok((symbol.exchange_symbol.clone(), canonical, base, quote));
    }
    let buying = value
        .get("buyingCurrency")
        .and_then(Value::as_str)
        .unwrap_or("BTC")
        .to_ascii_uppercase();
    let selling = value
        .get("sellingCurrency")
        .and_then(Value::as_str)
        .unwrap_or("EUR")
        .to_ascii_uppercase();
    let (base, quote) = if is_quote_asset(&buying) && !is_quote_asset(&selling) {
        (selling, buying)
    } else {
        (buying, selling)
    };
    let canonical = rustcta_types::CanonicalSymbol::new(&base, &quote).map_err(validation_error)?;
    let exchange_symbol = ExchangeSymbol::new(
        exchange_id.clone(),
        MarketType::Spot,
        normalize_coinmetro_symbol(&format!("{base}{quote}"))?,
    )
    .map_err(validation_error)?;
    Ok((exchange_symbol, Some(canonical), base, quote))
}

fn parse_order_side(
    value: &Value,
    base_asset: &str,
    quote_asset: &str,
) -> ExchangeApiResult<OrderSide> {
    if let Some(fill_style) = value.get("fillStyle").and_then(Value::as_str) {
        match fill_style.trim().to_ascii_lowercase().as_str() {
            "buy" | "base" => return Ok(OrderSide::Buy),
            "sell" => return Ok(OrderSide::Sell),
            "quote" => return Ok(OrderSide::Sell),
            _ => {}
        }
    }
    let buying = value
        .get("buyingCurrency")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    let selling = value
        .get("sellingCurrency")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    if buying == base_asset && selling == quote_asset {
        Ok(OrderSide::Buy)
    } else if buying == quote_asset && selling == base_asset {
        Ok(OrderSide::Sell)
    } else {
        Err(ExchangeApiError::InvalidRequest {
            message: format!(
                "cannot map coinmetro order side for buying={buying} selling={selling}"
            ),
        })
    }
}

fn parse_fill_side(side: &str) -> ExchangeApiResult<OrderSide> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported coinmetro fill side {other}"),
        }),
    }
}

fn parse_order_type(value: &Value) -> OrderType {
    match value
        .get("orderType")
        .and_then(Value::as_str)
        .unwrap_or("limit")
        .to_ascii_lowercase()
        .as_str()
    {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: &Value) -> Option<TimeInForce> {
    match value.get("timeInForce").and_then(value_as_i64) {
        Some(1) => Some(TimeInForce::GTC),
        Some(2) => Some(TimeInForce::IOC),
        Some(4) => Some(TimeInForce::FOK),
        _ => None,
    }
}

fn parse_order_status(value: &Value, quantity: &str, filled_quantity: &str) -> OrderStatus {
    if value.get("status").and_then(Value::as_str) == Some("fail") {
        return OrderStatus::Rejected;
    }
    let quantity_value = quantity.parse::<f64>().unwrap_or(0.0);
    let filled_value = filled_quantity.parse::<f64>().unwrap_or(0.0);
    if value.get("completionTime").is_some() {
        if filled_value > 0.0 && (quantity_value == 0.0 || filled_value >= quantity_value) {
            OrderStatus::Filled
        } else if filled_value > 0.0 {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::Cancelled
        }
    } else if filled_value > 0.0 {
        OrderStatus::PartiallyFilled
    } else {
        OrderStatus::New
    }
}

fn base_quantities(value: &Value, side: OrderSide) -> (String, String) {
    match side {
        OrderSide::Buy => (
            string_or_number(value.get("buyingQty")).unwrap_or_else(|| "0".to_string()),
            string_or_number(value.get("boughtQty")).unwrap_or_else(|| "0".to_string()),
        ),
        OrderSide::Sell => (
            string_or_number(value.get("sellingQty")).unwrap_or_else(|| "0".to_string()),
            string_or_number(value.get("soldQty")).unwrap_or_else(|| "0".to_string()),
        ),
    }
}

fn limit_price(value: &Value, side: OrderSide) -> Option<String> {
    let buying = number_from_value(value.get("buyingQty")?)?;
    let selling = number_from_value(value.get("sellingQty")?)?;
    if buying <= 0.0 || selling <= 0.0 {
        return None;
    }
    match side {
        OrderSide::Buy => Some(format_decimal(selling / buying)),
        OrderSide::Sell => Some(format_decimal(buying / selling)),
    }
}

fn average_fill_price(value: &Value, side: OrderSide) -> Option<String> {
    let bought = number_from_value(value.get("boughtQty")?)?;
    let sold = number_from_value(value.get("soldQty")?)?;
    if bought <= 0.0 || sold <= 0.0 {
        return None;
    }
    match side {
        OrderSide::Buy => Some(format_decimal(sold / bought)),
        OrderSide::Sell => Some(format_decimal(bought / sold)),
    }
}

fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
    })
}

fn is_quote_asset(asset: &str) -> bool {
    matches!(
        asset,
        "USDT" | "USDC" | "EUR" | "USD" | "GBP" | "AUD" | "BTC"
    )
}
