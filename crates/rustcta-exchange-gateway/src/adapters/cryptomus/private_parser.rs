#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, Balance, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState,
    SymbolScope, TenantId, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeId, Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus,
    OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

use super::parser::{
    decimal_value_to_f64, invalid, items_array, parse_timestamp, string_or_number, symbol_scope,
    validation_error,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<Balance>> {
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for row in items_array(value)? {
        let asset = row
            .get("ticker")
            .or_else(|| row.get("currency"))
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("cryptomus balance row missing ticker"))?
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_value_to_f64(row.get("available"))?.unwrap_or(0.0);
        let held = decimal_value_to_f64(row.get("held"))?.unwrap_or(0.0);
        balances.push(
            AssetBalance::new(asset, available + held, available, held)
                .map_err(validation_error)?,
        );
    }
    Ok(vec![Balance {
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
    let order = first_item_or_self(value);
    let symbol = symbol_scope(exchange_id, symbol_hint, order)?;
    let quantity = string_or_number(
        order
            .get("quantity")
            .or_else(|| order.get("filledQuantity"))
            .or_else(|| order.get("filled_quantity")),
    )
    .unwrap_or_else(|| "0".to_string());
    let filled_quantity = string_or_number(
        order
            .get("filledQuantity")
            .or_else(|| order.get("filled_quantity")),
    )
    .unwrap_or_else(|| "0".to_string());
    let status = parse_status(
        order.get("state").and_then(Value::as_str),
        order
            .get("internalState")
            .or_else(|| order.get("internal_state"))
            .and_then(Value::as_str),
        &quantity,
        &filled_quantity,
    );
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(
            order
                .get("clientOrderId")
                .or_else(|| order.get("client_order_id"))
                .or_else(|| order.get("clientOid")),
        ),
        exchange_order_id: string_or_number(order.get("id").or_else(|| order.get("order_id"))),
        side: parse_side(
            order
                .get("direction")
                .or_else(|| order.get("side"))
                .and_then(Value::as_str),
        ),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(
            order
                .get("type")
                .or_else(|| order.get("orderType"))
                .and_then(Value::as_str),
        ),
        time_in_force: Some(TimeInForce::GTC),
        status,
        quantity,
        price: string_or_number(order.get("price")).filter(|price| !is_zero_decimal(price)),
        filled_quantity: filled_quantity.clone(),
        average_fill_price: average_fill_price(order, &filled_quantity),
        reduce_only: false,
        post_only: false,
        created_at: order
            .get("createdAt")
            .or_else(|| order.get("created_at"))
            .and_then(parse_timestamp),
        updated_at: order
            .get("finishedAt")
            .or_else(|| order.get("updatedAt"))
            .or_else(|| order.get("createdAt"))
            .and_then(parse_timestamp)
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    items_array(value)?
        .iter()
        .map(|order| parse_order_state(exchange_id, symbol_hint, order))
        .collect()
}

pub fn parse_fee_snapshots(
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let current = value
        .get("result")
        .and_then(|result| result.get("current_tariff_step"))
        .or_else(|| value.get("current_tariff_step"))
        .or_else(|| value.get("currentTariffStep"))
        .unwrap_or(value);
    let maker_rate = percent_to_decimal_string(
        current
            .get("maker_percent")
            .or_else(|| current.get("makerPercent")),
    )?;
    let taker_rate = percent_to_decimal_string(
        current
            .get("taker_percent")
            .or_else(|| current.get("takerPercent")),
    )?;
    Ok(requested_symbols
        .iter()
        .cloned()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: maker_rate.clone(),
            taker_rate: taker_rate.clone(),
            source: Some("cryptomus.account.tariffs".to_string()),
            updated_at: Utc::now(),
        })
        .collect())
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let mut fills = Vec::new();
    for order in items_array(value)? {
        let symbol = symbol_scope(exchange_id, symbol_hint, order)?;
        let canonical_symbol =
            symbol
                .canonical_symbol
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "cryptomus fill requires canonical_symbol".to_string(),
                })?;
        let side = parse_side(
            order
                .get("direction")
                .or_else(|| order.get("side"))
                .and_then(Value::as_str),
        );
        let order_id = string_or_number(order.get("id").or_else(|| order.get("order_id")));
        let client_order_id = string_or_number(
            order
                .get("clientOrderId")
                .or_else(|| order.get("client_order_id")),
        );
        let transactions = order
            .get("deal")
            .and_then(|deal| deal.get("transactions"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for transaction in transactions {
            let price = decimal_value_to_f64(transaction.get("filledPrice"))?.unwrap_or(0.0);
            let quantity = decimal_value_to_f64(transaction.get("filledQuantity"))?.unwrap_or(0.0);
            fills.push(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: order_id.clone(),
                client_order_id: client_order_id.clone(),
                fill_id: string_or_number(transaction.get("id")),
                side,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity(
                    transaction.get("tradeRole").and_then(Value::as_str),
                ),
                price,
                quantity,
                quote_quantity: decimal_value_to_f64(transaction.get("filledValue"))?
                    .or_else(|| (price > 0.0 && quantity > 0.0).then_some(price * quantity)),
                fee_asset: string_or_number(transaction.get("feeCurrency")),
                fee_amount: decimal_value_to_f64(transaction.get("fee"))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: transaction
                    .get("committedAt")
                    .or_else(|| transaction.get("completedAt"))
                    .and_then(parse_timestamp)
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            });
        }
    }
    Ok(fills)
}

fn first_item_or_self(value: &Value) -> &Value {
    value
        .get("result")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .or_else(|| value.get("result"))
        .or_else(|| value.get("data"))
        .unwrap_or(value)
}

fn parse_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "sell" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or("limit").to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_status(
    state: Option<&str>,
    internal_state: Option<&str>,
    quantity: &str,
    filled_quantity: &str,
) -> OrderStatus {
    match (
        state.unwrap_or_default().to_ascii_lowercase(),
        internal_state.unwrap_or_default().to_ascii_lowercase(),
    ) {
        (state, internal) if state == "completed" || internal == "filled" => OrderStatus::Filled,
        (state, internal)
            if state == "canceled"
                || state == "cancelled"
                || state == "canceled_by_stop_loss"
                || internal == "empty" =>
        {
            OrderStatus::Cancelled
        }
        (_, internal) if internal == "partially_filled" => OrderStatus::PartiallyFilled,
        (state, _) if state == "rejected" => OrderStatus::Rejected,
        (state, _) if state == "expired" => OrderStatus::Expired,
        _ => {
            let quantity = quantity.parse::<f64>().unwrap_or(0.0);
            let filled = filled_quantity.parse::<f64>().unwrap_or(0.0);
            if quantity > 0.0 && filled >= quantity {
                OrderStatus::Filled
            } else if filled > 0.0 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Open
            }
        }
    }
}

fn parse_liquidity(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" => LiquidityRole::Maker,
        "taker" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn average_fill_price(value: &Value, filled_quantity: &str) -> Option<String> {
    value
        .get("deal")
        .and_then(|deal| {
            string_or_number(
                deal.get("averageFilledPrice")
                    .or_else(|| deal.get("average_filled_price")),
            )
        })
        .or_else(|| {
            let filled_quantity = filled_quantity.parse::<f64>().ok()?;
            let filled_value = string_or_number(
                value
                    .get("filledValue")
                    .or_else(|| value.get("filled_value")),
            )?
            .parse::<f64>()
            .ok()?;
            (filled_quantity > 0.0).then(|| (filled_value / filled_quantity).to_string())
        })
}

fn percent_to_decimal_string(value: Option<&Value>) -> ExchangeApiResult<String> {
    let percent = string_or_number(value)
        .ok_or_else(|| invalid("cryptomus tariff missing percent"))?
        .parse::<f64>()
        .map_err(|error| invalid(format!("invalid cryptomus tariff percent: {error}")))?;
    Ok((percent / 100.0).to_string())
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().is_ok_and(|number| number == 0.0)
}
