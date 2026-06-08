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
    normalize_gemini_symbol, parse_error, required_str, string_or_number, validation_error,
    value_as_i64, value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let items = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "gemini balances response is not an array",
                value,
            )
        })?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for item in items {
        let asset = required_str(exchange_id, item, "currency")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_value_to_f64(item.get("available"))?.unwrap_or(0.0);
        let total = decimal_value_to_f64(item.get("amount"))?.unwrap_or(available);
        let locked = (total - available).max(0.0);
        if total > 0.0 || available > 0.0 || locked > 0.0 || !requested.is_empty() {
            balances.push(
                AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
            );
        }
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
    let symbol_text = value
        .get("symbol")
        .and_then(Value::as_str)
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.as_str()))
        .unwrap_or("unknown");
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            normalize_gemini_symbol(symbol_text)?,
        )
        .map_err(validation_error)?
    };
    let is_live = value.get("is_live").and_then(Value::as_bool);
    let is_cancelled = value.get("is_cancelled").and_then(Value::as_bool);
    let executed_amount =
        string_or_number(value.get("executed_amount")).unwrap_or_else(|| "0".to_string());
    let original_amount =
        string_or_number(value.get("original_amount").or_else(|| value.get("amount")))
            .unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone()),
        exchange_symbol,
        client_order_id: value_as_string(
            value
                .get("client_order_id")
                .or_else(|| value.get("client_order_id")),
        ),
        exchange_order_id: value_as_string(value.get("order_id").or_else(|| value.get("id"))),
        side: value
            .get("side")
            .and_then(Value::as_str)
            .map(parse_side)
            .transpose()?
            .unwrap_or(OrderSide::Buy),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(value),
        time_in_force: parse_time_in_force(value),
        status: map_gemini_order_status(
            value,
            is_live,
            is_cancelled,
            &executed_amount,
            &original_amount,
        ),
        quantity: original_amount,
        price: non_zero_string(
            string_or_number(value.get("price")).unwrap_or_else(|| "0".to_string()),
        ),
        filled_quantity: executed_amount,
        average_fill_price: non_zero_string(
            string_or_number(value.get("avg_execution_price"))
                .or_else(|| string_or_number(value.get("average_fill_price")))
                .unwrap_or_else(|| "0".to_string()),
        ),
        reduce_only: false,
        post_only: value
            .get("options")
            .and_then(Value::as_array)
            .is_some_and(|items| {
                items
                    .iter()
                    .any(|item| item.as_str() == Some("maker-or-cancel"))
            }),
        created_at: first_timestamp(value, &["timestampms", "timestamp_ms"]),
        updated_at: first_timestamp(value, &["timestampms", "timestamp_ms"])
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "gemini open orders response is not an array",
                value,
            )
        })?;
    orders
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
                message: "gemini get_recent_fills requires canonical_symbol".to_string(),
            })?;
    let fills = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "gemini fills response is not an array",
                value,
            )
        })?;
    fills
        .iter()
        .map(|fill| {
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("order_id")),
                client_order_id: value_as_string(fill.get("client_order_id")),
                fill_id: value_as_string(fill.get("tid").or_else(|| fill.get("trade_id"))),
                side: fill
                    .get("type")
                    .or_else(|| fill.get("side"))
                    .and_then(Value::as_str)
                    .map(parse_side)
                    .transpose()?
                    .unwrap_or(OrderSide::Buy),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price: decimal_value_to_f64(fill.get("price"))?.unwrap_or(0.0),
                quantity: decimal_value_to_f64(
                    fill.get("amount").or_else(|| fill.get("quantity")),
                )?
                .unwrap_or(0.0),
                quote_quantity: None,
                fee_asset: value_as_string(fill.get("fee_currency")),
                fee_amount: decimal_value_to_f64(
                    fill.get("fee_amount").or_else(|| fill.get("fee")),
                )?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp(fill, &["timestampms", "timestamp_ms"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_side(side: &str) -> ExchangeApiResult<OrderSide> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" | "bid" => Ok(OrderSide::Buy),
        "sell" | "ask" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported gemini order side {other}"),
        }),
    }
}

fn parse_order_type(value: &Value) -> OrderType {
    if value
        .get("options")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.as_str() == Some("maker-or-cancel"))
        })
    {
        return OrderType::PostOnly;
    }
    match value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("exchange limit")
    {
        text if text.contains("market") => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: &Value) -> Option<TimeInForce> {
    value
        .get("options")
        .and_then(Value::as_array)
        .and_then(|items| {
            if items
                .iter()
                .any(|item| item.as_str() == Some("immediate-or-cancel"))
            {
                Some(TimeInForce::IOC)
            } else if items
                .iter()
                .any(|item| item.as_str() == Some("maker-or-cancel"))
            {
                Some(TimeInForce::GTX)
            } else {
                Some(TimeInForce::GTC)
            }
        })
}

fn map_gemini_order_status(
    value: &Value,
    is_live: Option<bool>,
    is_cancelled: Option<bool>,
    executed_amount: &str,
    original_amount: &str,
) -> OrderStatus {
    if let Some(status) = value.get("status").and_then(Value::as_str) {
        return match status.trim().to_ascii_lowercase().as_str() {
            "accepted" | "open" | "live" => OrderStatus::New,
            "booked" => OrderStatus::New,
            "fill" | "filled" | "closed" => OrderStatus::Filled,
            "partial_fill" | "partially_filled" => OrderStatus::PartiallyFilled,
            "cancelled" | "canceled" | "cancel" => OrderStatus::Cancelled,
            "rejected" => OrderStatus::Rejected,
            _ => OrderStatus::Unknown,
        };
    }
    if is_cancelled == Some(true) {
        OrderStatus::Cancelled
    } else if executed_amount != "0" && executed_amount == original_amount {
        OrderStatus::Filled
    } else if executed_amount != "0" {
        OrderStatus::PartiallyFilled
    } else if is_live == Some(true) {
        OrderStatus::New
    } else {
        OrderStatus::Unknown
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

fn non_zero_string(value: String) -> Option<String> {
    if value.trim().is_empty() || value.trim() == "0" || value.trim() == "0.0" {
        None
    } else {
        Some(value)
    }
}

fn decimal_value_to_f64(value: Option<&Value>) -> ExchangeApiResult<Option<f64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let text = string_or_number(Some(value)).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: format!("invalid gemini decimal value {value}"),
    })?;
    text.parse::<f64>()
        .map(Some)
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid gemini decimal value {text}: {error}"),
        })
}
