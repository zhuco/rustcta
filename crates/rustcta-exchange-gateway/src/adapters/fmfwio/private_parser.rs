use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, BatchCancelOrdersResponse, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot,
    OrderListKind, OrderListResponse, ResponseMetadata, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, Fill, FillStatus, LiquidityRole, MarketType,
    OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

use super::parser::{
    number_from_value, parse_datetime_value, string_or_number, symbol_scope_from_exchange_symbol,
    validation_error,
};

pub fn parse_account_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let rows = order_rows(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "FMFW.io balances response missing balance array".to_string(),
    })?;
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for row in rows {
        let asset = row
            .get("currency")
            .or_else(|| row.get("asset"))
            .and_then(Value::as_str)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "FMFW.io balance row missing currency".to_string(),
            })?
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = row
            .get("available")
            .and_then(number_from_value)
            .unwrap_or(0.0);
        let locked = row
            .get("reserved")
            .or_else(|| row.get("locked"))
            .and_then(number_from_value)
            .unwrap_or(0.0);
        let total = row
            .get("total")
            .or_else(|| row.get("balance"))
            .and_then(number_from_value)
            .unwrap_or(available + locked);
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

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let rows = order_rows(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "FMFW.io fee response missing fee array".to_string(),
    })?;
    let mut fees = Vec::new();
    for row in rows {
        let symbol = fee_symbol(exchange_id, requested_symbols, row)?;
        fees.push(FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: string_or_number(row.get("make_rate").or_else(|| row.get("maker_rate")))
                .unwrap_or_else(|| "0".to_string()),
            taker_rate: string_or_number(row.get("take_rate").or_else(|| row.get("taker_rate")))
                .unwrap_or_else(|| "0".to_string()),
            source: Some("fmfwio.spot_fee".to_string()),
            updated_at: Utc::now(),
        });
    }
    Ok(fees)
}

pub fn parse_spot_order_ack(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<rustcta_exchange_api::OrderState> {
    let side = match value
        .get("side")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "buy" => OrderSide::Buy,
        "sell" => OrderSide::Sell,
        other => {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("unknown FMFW.io order side {other}"),
            })
        }
    };
    Ok(rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value
            .get("client_order_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        exchange_order_id: string_or_number(value.get("id")),
        side,
        position_side: Some(PositionSide::None),
        order_type: match value.get("type").and_then(Value::as_str).unwrap_or("limit") {
            "market" => OrderType::Market,
            _ => OrderType::Limit,
        },
        time_in_force: None,
        status: parse_status(value.get("status").and_then(Value::as_str)),
        quantity: string_or_number(value.get("quantity")).unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")),
        filled_quantity: string_or_number(value.get("quantity_cumulative"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("price_average")),
        reduce_only: false,
        post_only: value
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: value.get("created_at").and_then(parse_datetime_value),
        updated_at: value
            .get("updated_at")
            .and_then(parse_datetime_value)
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_spot_order_list_ack(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderListResponse> {
    let orders = value
        .get("orders")
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "FMFW.io order-list response missing orders array".to_string(),
        })?
        .iter()
        .map(|order| parse_spot_order_ack(exchange_id, symbol.clone(), order))
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    let kind = match value
        .get("contingency_type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "oneCancelOther" => OrderListKind::Oco,
        "oneTriggerOther" | "oneTriggerOneCancelOther" => OrderListKind::Oto,
        _ => OrderListKind::Oto,
    };
    let mut metadata = ResponseMetadata::new(exchange_id.clone(), Utc::now());
    metadata.exchange_timestamp = value.get("updated_at").and_then(parse_datetime_value);
    Ok(OrderListResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata,
        symbol,
        kind,
        order_list_id: string_or_number(value.get("id")),
        list_client_order_id: value
            .get("client_order_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        list_status_type: value
            .get("contingency_type")
            .and_then(Value::as_str)
            .map(str::to_string),
        list_order_status: value
            .get("status")
            .and_then(Value::as_str)
            .map(str::to_string),
        orders,
    })
}

pub fn parse_spot_batch_cancel_ack(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<BatchCancelOrdersResponse> {
    let order_values = value
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "FMFW.io batch-cancel response missing order array".to_string(),
        })?;
    let orders = order_values
        .iter()
        .map(|order| parse_spot_order_ack(exchange_id, symbol.clone(), order))
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(BatchCancelOrdersResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        cancelled_count: orders.len() as u32,
        orders,
        report: None,
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<rustcta_exchange_api::OrderState>> {
    let orders = order_rows(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "FMFW.io open orders response missing order array".to_string(),
    })?;
    orders
        .iter()
        .map(|order| parse_spot_order_with_optional_symbol(exchange_id, symbol_hint, order))
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
                message: "FMFW.io recent fills require canonical_symbol".to_string(),
            })?;
    let trades = order_rows(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "FMFW.io trade history response missing trade array".to_string(),
    })?;
    trades
        .iter()
        .map(|trade| {
            let price = trade
                .get("price")
                .and_then(number_from_value)
                .unwrap_or(0.0);
            let quantity = trade
                .get("quantity")
                .or_else(|| trade.get("qty"))
                .or_else(|| trade.get("exec_quantity"))
                .and_then(number_from_value)
                .unwrap_or(0.0);
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: string_or_number(trade.get("order_id").or_else(|| trade.get("orderId"))),
                client_order_id: trade
                    .get("client_order_id")
                    .or_else(|| trade.get("clientOrderId"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                fill_id: string_or_number(trade.get("id").or_else(|| trade.get("trade_id"))),
                side: parse_side(trade.get("side").and_then(Value::as_str))?,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity_role(
                    trade
                        .get("liquidity")
                        .or_else(|| trade.get("liquidity_indicator"))
                        .and_then(Value::as_str),
                ),
                price,
                quantity,
                quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
                fee_asset: trade
                    .get("fee_currency")
                    .or_else(|| trade.get("feeCurrency"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| Some(canonical_symbol.quote_asset().to_string())),
                fee_amount: trade.get("fee").and_then(number_from_value),
                fee_rate: None,
                realized_pnl: None,
                filled_at: trade
                    .get("timestamp")
                    .or_else(|| trade.get("created_at"))
                    .and_then(parse_datetime_value)
                    .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_spot_order_with_optional_symbol(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<rustcta_exchange_api::OrderState> {
    let symbol = if let Some(symbol) = symbol_hint {
        symbol.clone()
    } else {
        let exchange_symbol = value.get("symbol").and_then(Value::as_str).ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "FMFW.io order response missing symbol".to_string(),
            }
        })?;
        symbol_scope_from_exchange_symbol(exchange_id, exchange_symbol)?
    };
    parse_spot_order_ack(exchange_id, symbol, value)
}

fn fee_symbol(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = value.get("symbol").and_then(Value::as_str) {
        return symbol_scope_from_exchange_symbol(exchange_id, symbol);
    }
    if requested_symbols.len() == 1 {
        return Ok(requested_symbols[0].clone());
    }
    Err(ExchangeApiError::InvalidRequest {
        message: "FMFW.io fee row missing symbol".to_string(),
    })
}

fn order_rows(value: &Value) -> Option<&Vec<Value>> {
    value
        .as_array()
        .or_else(|| value.get("orders").and_then(Value::as_array))
        .or_else(|| value.get("data").and_then(Value::as_array))
        .or_else(|| value.get("result").and_then(Value::as_array))
}

fn parse_side(side: Option<&str>) -> ExchangeApiResult<OrderSide> {
    match side.unwrap_or_default() {
        "buy" | "BUY" => Ok(OrderSide::Buy),
        "sell" | "SELL" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unknown FMFW.io fill side {other}"),
        }),
    }
}

fn parse_liquidity_role(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" | "m" | "mkr" => LiquidityRole::Maker,
        "taker" | "t" | "tkr" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn parse_status(status: Option<&str>) -> OrderStatus {
    match status.unwrap_or_default() {
        "new" | "suspended" => OrderStatus::New,
        "partiallyFilled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "canceled" => OrderStatus::Cancelled,
        "expired" => OrderStatus::Expired,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}
