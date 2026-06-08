use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill, FillStatus, LiquidityRole,
    MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

use super::parser::{
    decimal_value_to_f64, first_timestamp, normalize_coinspot_symbol, parse_error, schema_version,
    string_or_number, validation_error, value_as_string,
};

pub fn parse_account_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let balance_root = value
        .get("balances")
        .or_else(|| value.get("balance"))
        .and_then(Value::as_object)
        .ok_or_else(|| parse_error(exchange_id.clone(), "balances response missing data", value))?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for (asset, data) in balance_root {
        let asset = asset.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_value_to_f64(
            data.get("available")
                .or_else(|| data.get("balance"))
                .or_else(|| data.get("audbalance")),
        )?
        .unwrap_or(0.0);
        let locked =
            decimal_value_to_f64(data.get("reserved").or_else(|| data.get("held")))?.unwrap_or(0.0);
        let total = decimal_value_to_f64(data.get("total"))?.unwrap_or_else(|| available + locked);
        if total > 0.0 || !requested.is_empty() {
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

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let mut orders = Vec::new();
    for field in ["buyorders", "sellorders", "orders"] {
        if let Some(items) = value.get(field).and_then(Value::as_array) {
            let side = match field {
                "sellorders" => Some(OrderSide::Sell),
                "buyorders" => Some(OrderSide::Buy),
                _ => None,
            };
            for item in items {
                orders.push(parse_order_state(exchange_id, symbol_hint, item, side)?);
            }
        }
    }
    Ok(orders)
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
    side_hint: Option<OrderSide>,
) -> ExchangeApiResult<OrderState> {
    let symbol = symbol_scope(exchange_id, symbol_hint, value)?;
    let quantity = string_or_number(
        value
            .get("amount")
            .or_else(|| value.get("quantity"))
            .or_else(|| value.get("volume")),
    )
    .unwrap_or_else(|| "0".to_string());
    let filled_quantity = string_or_number(value.get("filled")).unwrap_or_else(|| "0".to_string());
    let side =
        side_hint.unwrap_or_else(|| parse_side(value.get("type").or_else(|| value.get("side"))));
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value_as_string(value.get("clientOrderId")),
        exchange_order_id: value_as_string(value.get("id").or_else(|| value.get("orderid"))),
        side,
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(value.get("ordertype").or_else(|| value.get("orderType"))),
        time_in_force: None,
        status: parse_status(value),
        quantity,
        price: string_or_number(value.get("rate").or_else(|| value.get("price"))),
        filled_quantity,
        average_fill_price: string_or_number(value.get("averageprice")),
        reduce_only: false,
        post_only: false,
        created_at: first_timestamp(value, &["created", "created_at", "timestamp"]),
        updated_at: first_timestamp(value, &["updated", "updated_at", "timestamp"])
            .unwrap_or_else(Utc::now),
    })
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
                message: "coinspot recent fills request requires canonical_symbol".to_string(),
            })?;
    let items = value
        .get("buyorders")
        .or_else(|| value.get("sellorders"))
        .or_else(|| value.get("orders"))
        .or_else(|| value.get("transactions"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    items
        .iter()
        .map(|fill| {
            let price = decimal_value_to_f64(fill.get("rate").or_else(|| fill.get("price")))?
                .unwrap_or(0.0);
            let quantity = decimal_value_to_f64(
                fill.get("amount")
                    .or_else(|| fill.get("quantity"))
                    .or_else(|| fill.get("volume")),
            )?
            .unwrap_or(0.0);
            Ok(Fill {
                schema_version: schema_version(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("orderid").or_else(|| fill.get("id"))),
                client_order_id: value_as_string(fill.get("clientOrderId")),
                fill_id: value_as_string(fill.get("txid").or_else(|| fill.get("tradeid"))),
                side: parse_side(fill.get("type").or_else(|| fill.get("side"))),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price,
                quantity,
                quote_quantity: decimal_value_to_f64(fill.get("total"))?
                    .or_else(|| (price > 0.0 && quantity > 0.0).then_some(price * quantity)),
                fee_asset: Some("AUD".to_string()),
                fee_amount: decimal_value_to_f64(fill.get("fee"))?,
                fee_rate: None,
                realized_pnl: None,
                filled_at: first_timestamp(fill, &["solddate", "created", "timestamp"])
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_fee_snapshots(
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> Vec<FeeRateSnapshot> {
    let maker = string_or_number(value.get("makerfee")).unwrap_or_else(|| "0".to_string());
    let taker = string_or_number(value.get("takerfee"))
        .or_else(|| string_or_number(value.get("fee")))
        .unwrap_or_else(|| maker.clone());
    requested_symbols
        .iter()
        .cloned()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: maker.clone(),
            taker_rate: taker.clone(),
            source: Some("coinspot.fees".to_string()),
            updated_at: Utc::now(),
        })
        .collect()
}

fn symbol_scope(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let coin = value
        .get("coin")
        .or_else(|| value.get("cointype"))
        .and_then(Value::as_str)
        .ok_or_else(|| parse_error(exchange_id.clone(), "order missing coin", value))?;
    let (base, _) = normalize_coinspot_symbol(coin)?;
    let base = base.to_ascii_uppercase();
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: None,
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            format!("{base}/AUD"),
        )
        .map_err(validation_error)?,
    })
}

fn parse_side(value: Option<&Value>) -> OrderSide {
    let side = value
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if side.contains("sell") {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
}

fn parse_order_type(value: Option<&Value>) -> OrderType {
    if value
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .contains("market")
    {
        OrderType::Market
    } else {
        OrderType::Limit
    }
}

fn parse_status(value: &Value) -> OrderStatus {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    match status.as_str() {
        "filled" | "complete" | "completed" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "partially_filled" | "partial" => OrderStatus::PartiallyFilled,
        "" | "open" | "active" => OrderStatus::New,
        _ => OrderStatus::Unknown,
    }
}
