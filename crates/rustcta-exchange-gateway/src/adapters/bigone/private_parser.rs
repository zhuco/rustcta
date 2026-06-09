use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, ExchangePosition, Fill, FillStatus, LiquidityRole,
    MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    data_payload, normalize_bigone_symbol, rows, symbol_scope, timestamp_from_value,
    validation_error, value_as_f64, value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: rustcta_exchange_api::TenantId,
    account_id: rustcta_exchange_api::AccountId,
    market_type: MarketType,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let requested = requested_assets
        .iter()
        .map(|asset| asset.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let balances = rows(value)
        .iter()
        .filter_map(|row| {
            let asset = value_as_string(
                row.get("asset")
                    .or_else(|| row.get("currency"))
                    .or_else(|| row.get("symbol")),
            )?
            .to_ascii_uppercase();
            if !requested.is_empty() && !requested.contains(&asset) {
                return None;
            }
            let available = value_as_f64(
                row.get("available")
                    .or_else(|| row.get("available_balance"))
                    .or_else(|| row.get("free")),
            )
            .unwrap_or(0.0);
            let locked = value_as_f64(
                row.get("locked")
                    .or_else(|| row.get("freeze"))
                    .or_else(|| row.get("frozen")),
            )
            .unwrap_or(0.0);
            let total = value_as_f64(row.get("balance").or_else(|| row.get("total")))
                .unwrap_or(available + locked);
            Some(AssetBalance::new(asset, total, available, locked).map_err(validation_error))
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(vec![ExchangeBalance {
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
    tenant_id: rustcta_exchange_api::TenantId,
    account_id: rustcta_exchange_api::AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    rows(value)
        .iter()
        .filter_map(|row| {
            let symbol = value_as_string(
                row.get("contract")
                    .or_else(|| row.get("symbol"))
                    .or_else(|| row.get("instrument_id")),
            )?;
            let scope = symbol_scope(exchange_id, MarketType::Perpetual, &symbol).ok()?;
            let quantity = value_as_f64(
                row.get("amount")
                    .or_else(|| row.get("size"))
                    .or_else(|| row.get("quantity"))
                    .or_else(|| row.get("position")),
            )
            .unwrap_or(0.0);
            if quantity == 0.0 {
                return None;
            }
            let canonical_symbol = scope.canonical_symbol.clone()?;
            let side = if quantity < 0.0 {
                PositionSide::Short
            } else {
                PositionSide::Long
            };
            Some(Ok(ExchangePosition {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol,
                exchange_symbol: Some(scope.exchange_symbol),
                side,
                quantity: quantity.abs(),
                entry_price: value_as_f64(
                    row.get("entry_price").or_else(|| row.get("avg_open_price")),
                ),
                mark_price: value_as_f64(row.get("mark_price").or_else(|| row.get("index_price"))),
                liquidation_price: value_as_f64(row.get("liquidation_price")),
                unrealized_pnl: value_as_f64(row.get("unrealized_pnl").or_else(|| row.get("pnl"))),
                leverage: value_as_f64(row.get("leverage")),
                observed_at: Utc::now(),
            }))
        })
        .collect()
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let data = data_payload(value);
    if data.as_array().is_some_and(Vec::is_empty) {
        return Err(validation_error("BigONE order payload was empty"));
    }
    let row = rows(data)
        .into_iter()
        .next()
        .unwrap_or_else(|| data.clone());
    let symbol = value_as_string(
        row.get("market_id")
            .or_else(|| row.get("asset_pair_name"))
            .or_else(|| row.get("contract"))
            .or_else(|| row.get("symbol")),
    )
    .and_then(|symbol| symbol_scope(exchange_id, market_type, &symbol).ok())
    .or_else(|| fallback_symbol.cloned())
    .ok_or_else(|| validation_error("BigONE order payload missing symbol"))?;
    let side = parse_side(
        value_as_string(row.get("side").or_else(|| row.get("type")))
            .unwrap_or_else(|| "BUY".to_string())
            .as_str(),
    );
    let order_type = parse_order_type(
        value_as_string(row.get("order_type").or_else(|| row.get("type")))
            .unwrap_or_else(|| "LIMIT".to_string())
            .as_str(),
    );
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value_as_string(
            row.get("client_id").or_else(|| row.get("client_order_id")),
        ),
        exchange_order_id: value_as_string(row.get("id").or_else(|| row.get("order_id"))),
        side,
        position_side: Some(parse_position_side(
            row.get("position_side").and_then(Value::as_str),
        )),
        order_type,
        time_in_force: parse_tif(row.get("time_in_force").and_then(Value::as_str)),
        status: parse_order_status(
            row.get("state")
                .or_else(|| row.get("status"))
                .and_then(Value::as_str),
        ),
        quantity: value_as_string(
            row.get("amount")
                .or_else(|| row.get("quantity"))
                .or_else(|| row.get("size")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: value_as_string(row.get("price")),
        filled_quantity: value_as_string(
            row.get("filled_amount")
                .or_else(|| row.get("filled_quantity"))
                .or_else(|| row.get("deal_amount")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: value_as_string(
            row.get("avg_price").or_else(|| row.get("average_price")),
        ),
        reduce_only: row
            .get("reduce_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: row
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(order_type == OrderType::PostOnly),
        created_at: timestamp_from_value(row.get("created_at").or_else(|| row.get("created_time"))),
        updated_at: timestamp_from_value(row.get("updated_at").or_else(|| row.get("updated_time")))
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    rows(value)
        .iter()
        .map(|row| parse_order_state(exchange_id, fallback_symbol, market_type, row))
        .collect()
}

pub fn parse_fees(symbols: &[SymbolScope], value: &Value) -> Vec<FeeRateSnapshot> {
    let data = data_payload(value);
    symbols
        .iter()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: symbol.clone(),
            maker_rate: value_as_string(
                fee_row(data, symbol)
                    .get("maker_fee_rate")
                    .or_else(|| fee_row(data, symbol).get("maker")),
            )
            .unwrap_or_else(|| "0.001".to_string()),
            taker_rate: value_as_string(
                fee_row(data, symbol)
                    .get("taker_fee_rate")
                    .or_else(|| fee_row(data, symbol).get("taker")),
            )
            .unwrap_or_else(|| "0.001".to_string()),
            source: Some("bigone.viewer.trading_fees".to_string()),
            updated_at: Utc::now(),
        })
        .collect()
}

fn fee_row<'a>(data: &'a Value, symbol: &SymbolScope) -> &'a Value {
    let normalized = normalize_bigone_symbol(&symbol.exchange_symbol.symbol, symbol.market_type);
    data.as_array()
        .and_then(|rows| {
            rows.iter().find(|row| {
                value_as_string(
                    row.get("asset_pair_name")
                        .or_else(|| row.get("asset_pair_names"))
                        .or_else(|| row.get("market_id"))
                        .or_else(|| row.get("symbol")),
                )
                .is_some_and(|value| value.eq_ignore_ascii_case(&normalized))
            })
        })
        .unwrap_or(data)
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: rustcta_exchange_api::TenantId,
    account_id: rustcta_exchange_api::AccountId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    rows(value)
        .iter()
        .filter_map(|row| {
            let symbol = value_as_string(
                row.get("market_id")
                    .or_else(|| row.get("asset_pair_name"))
                    .or_else(|| row.get("contract"))
                    .or_else(|| row.get("symbol")),
            )
            .and_then(|symbol| symbol_scope(exchange_id, market_type, &symbol).ok())
            .or_else(|| fallback_symbol.cloned())?;
            let price = value_as_f64(row.get("price")).unwrap_or(0.0);
            let quantity = value_as_f64(
                row.get("amount")
                    .or_else(|| row.get("quantity"))
                    .or_else(|| row.get("size")),
            )
            .unwrap_or(0.0);
            if price <= 0.0 || quantity <= 0.0 {
                return None;
            }
            Some(Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type,
                canonical_symbol: symbol.canonical_symbol.clone()?,
                exchange_symbol: Some(symbol.exchange_symbol),
                order_id: value_as_string(row.get("order_id")),
                client_order_id: value_as_string(row.get("client_order_id")),
                fill_id: value_as_string(row.get("id").or_else(|| row.get("trade_id"))),
                side: parse_side(
                    value_as_string(row.get("side"))
                        .unwrap_or_else(|| "BUY".to_string())
                        .as_str(),
                ),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: if row
                    .get("is_maker")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    LiquidityRole::Maker
                } else {
                    LiquidityRole::Taker
                },
                price,
                quantity,
                quote_quantity: Some(price * quantity),
                fee_asset: value_as_string(
                    row.get("fee_asset").or_else(|| row.get("fee_currency")),
                ),
                fee_amount: value_as_f64(row.get("fee")),
                fee_rate: None,
                realized_pnl: value_as_f64(row.get("realized_pnl")),
                filled_at: timestamp_from_value(row.get("created_at").or_else(|| row.get("time")))
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            }))
        })
        .collect()
}

pub fn parse_side(value: &str) -> OrderSide {
    if value.to_ascii_uppercase().contains("SELL") || value.to_ascii_lowercase().contains("ask") {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
}

pub fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "NEW" | "PENDING" => OrderStatus::New,
        "OPEN" | "LIVE" => OrderStatus::Open,
        "PARTIALLY_FILLED" | "PARTIAL_FILLED" | "PARTIAL" => OrderStatus::PartiallyFilled,
        "FILLED" | "DONE" => OrderStatus::Filled,
        "CANCELING" | "PENDING_CANCEL" => OrderStatus::PendingCancel,
        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_order_type(value: &str) -> OrderType {
    match value.to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        "POST_ONLY" | "LIMIT_MAKER" => OrderType::PostOnly,
        "IOC" => OrderType::IOC,
        "FOK" => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_tif(value: Option<&str>) -> Option<TimeInForce> {
    match value?.to_ascii_uppercase().as_str() {
        "GTC" => Some(TimeInForce::GTC),
        "IOC" => Some(TimeInForce::IOC),
        "FOK" => Some(TimeInForce::FOK),
        "GTX" | "POST_ONLY" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_position_side(value: Option<&str>) -> PositionSide {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "LONG" => PositionSide::Long,
        "SHORT" => PositionSide::Short,
        "NET" | "BOTH" => PositionSide::Net,
        _ => PositionSide::None,
    }
}
