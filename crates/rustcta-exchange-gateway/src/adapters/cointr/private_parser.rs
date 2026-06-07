use chrono::Utc;
use rustcta_exchange_api::{
    Balance, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, Fill, OrderState, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeId, ExchangePosition, ExchangeSymbol,
    FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion, TenantId, TimeInForce,
};
use serde_json::{json, Value};

use super::parser::{required_text, symbol_scope, text, timestamp, value_as_f64};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Balance>> {
    let rows = rows(value);
    let balances = rows
        .iter()
        .filter_map(|row| {
            let asset = text(
                row.get("coin")
                    .or_else(|| row.get("asset"))
                    .or_else(|| row.get("marginCoin")),
            )?;
            if !assets.is_empty()
                && !assets
                    .iter()
                    .any(|requested| requested.eq_ignore_ascii_case(&asset))
            {
                return None;
            }
            let free = row
                .get("free")
                .or_else(|| row.get("available"))
                .or_else(|| row.get("availableBalance"))
                .and_then(value_as_f64)
                .unwrap_or(0.0);
            let locked = row
                .get("locked")
                .or_else(|| row.get("frozen"))
                .or_else(|| row.get("lockedBalance"))
                .and_then(value_as_f64)
                .unwrap_or(0.0);
            AssetBalance::new(asset, free + locked, free, locked).ok()
        })
        .collect::<Vec<_>>();
    Ok(vec![Balance {
        schema_version: rustcta_types::SchemaVersion::current(),
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
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    rows(value)
        .into_iter()
        .filter_map(|row| {
            let symbol_text = text(
                row.get("symbol")
                    .or_else(|| row.get("instId"))
                    .or_else(|| row.get("contractCode")),
            )?;
            let (base, quote) = infer_assets(&symbol_text);
            let quantity = row
                .get("total")
                .or_else(|| row.get("available"))
                .or_else(|| row.get("holdVol"))
                .or_else(|| row.get("size"))
                .or_else(|| row.get("positionAmt"))
                .and_then(value_as_f64)
                .unwrap_or(0.0)
                .abs();
            if quantity == 0.0 {
                return None;
            }
            Some(Ok(ExchangePosition {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol: CanonicalSymbol::new(base, quote).ok()?,
                exchange_symbol: Some(
                    ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol_text)
                        .ok()?,
                ),
                side: parse_position_side(
                    text(
                        row.get("holdSide")
                            .or_else(|| row.get("posSide"))
                            .or_else(|| row.get("side")),
                    )
                    .as_deref(),
                ),
                quantity,
                entry_price: row
                    .get("averageOpenPrice")
                    .or_else(|| row.get("openPriceAvg"))
                    .or_else(|| row.get("entryPrice"))
                    .and_then(value_as_f64),
                mark_price: row.get("markPrice").and_then(value_as_f64),
                liquidation_price: row.get("liquidationPrice").and_then(value_as_f64),
                unrealized_pnl: row
                    .get("unrealizedPL")
                    .or_else(|| row.get("unrealizedPnl"))
                    .and_then(value_as_f64),
                leverage: row.get("leverage").and_then(value_as_f64),
                observed_at: Utc::now(),
            }))
        })
        .collect()
}

pub fn parse_fee_rates(
    symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let maker = text(
        value
            .get("makerCommission")
            .or_else(|| value.get("makerFee")),
    )
    .unwrap_or_else(|| "0.001".to_string());
    let taker = text(
        value
            .get("takerCommission")
            .or_else(|| value.get("takerFee")),
    )
    .unwrap_or_else(|| "0.001".to_string());
    Ok(symbols
        .iter()
        .cloned()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: normalize_rate(&maker),
            taker_rate: normalize_rate(&taker),
            source: Some("cointr.private_get_account".to_string()),
            updated_at: Utc::now(),
        })
        .collect())
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let row = value
        .get("order")
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let symbol = match symbol_hint {
        Some(symbol) => symbol.clone(),
        None => {
            let symbol_text = required_text(exchange_id, row, &["symbol"])?;
            let (base, quote) = infer_assets(&symbol_text);
            symbol_scope(exchange_id, MarketType::Spot, &symbol_text, &base, &quote)?
        }
    };
    let side = parse_side(text(row.get("side")).as_deref()).unwrap_or(OrderSide::Buy);
    let order_type =
        parse_order_type(text(row.get("type").or_else(|| row.get("orderType"))).as_deref());
    let status = parse_status(text(row.get("status")).as_deref());
    let quantity = text(
        row.get("origQty")
            .or_else(|| row.get("quantity"))
            .or_else(|| row.get("qty")),
    )
    .unwrap_or_else(|| "0".to_string());
    let filled_quantity = text(row.get("executedQty").or_else(|| row.get("filledQty")))
        .unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: text(
            row.get("clientOrderId")
                .or_else(|| row.get("clientOid"))
                .or_else(|| row.get("client_order_id")),
        ),
        exchange_order_id: text(row.get("orderId").or_else(|| row.get("id"))),
        side,
        position_side: Some(parse_position_side(
            text(row.get("posSide").or_else(|| row.get("holdSide"))).as_deref(),
        )),
        order_type,
        time_in_force: parse_tif(text(row.get("timeInForce")).as_deref()),
        status,
        quantity,
        price: text(row.get("price")),
        filled_quantity,
        average_fill_price: text(row.get("avgPrice").or_else(|| row.get("averagePrice"))),
        reduce_only: text(row.get("reduceOnly"))
            .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "true" | "yes")),
        post_only: matches!(order_type, OrderType::PostOnly),
        created_at: timestamp(row, &["time", "createdTime", "created_at"]),
        updated_at: timestamp(row, &["updateTime", "updatedTime", "updated_at"])
            .unwrap_or_else(Utc::now),
    })
}

pub fn cancelled_order(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        status: OrderStatus::Cancelled,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    }
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    rows(value)
        .iter()
        .map(|row| parse_order_state(exchange_id, symbol_hint, row))
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    rows(value)
        .iter()
        .map(|row| {
            Ok(Fill {
                schema_version: rustcta_types::SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: symbol.canonical_symbol.clone().ok_or_else(|| {
                    ExchangeApiError::InvalidRequest {
                        message: "cointr fill requires canonical_symbol".to_string(),
                    }
                })?,
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: text(row.get("orderId").or_else(|| row.get("id"))),
                client_order_id: text(row.get("clientOrderId").or_else(|| row.get("clientOid"))),
                fill_id: Some(required_text(exchange_id, row, &["tradeId", "id"])?),
                side: parse_side(text(row.get("side")).as_deref()).unwrap_or(OrderSide::Buy),
                position_side: PositionSide::None,
                price: row.get("price").and_then(value_as_f64).unwrap_or(0.0),
                quantity: row
                    .get("qty")
                    .or_else(|| row.get("quantity"))
                    .or_else(|| row.get("size"))
                    .and_then(value_as_f64)
                    .unwrap_or(0.0),
                quote_quantity: row.get("quoteQty").and_then(value_as_f64),
                fee_asset: text(row.get("commissionAsset").or_else(|| row.get("feeAsset"))),
                fee_amount: row
                    .get("commission")
                    .or_else(|| row.get("fee"))
                    .and_then(value_as_f64),
                fee_rate: None,
                realized_pnl: None,
                liquidity_role: text(row.get("isMaker"))
                    .and_then(|value| {
                        value.parse::<bool>().ok().map(|is_maker| {
                            if is_maker {
                                LiquidityRole::Maker
                            } else {
                                LiquidityRole::Taker
                            }
                        })
                    })
                    .unwrap_or(LiquidityRole::Unknown),
                status: FillStatus::Confirmed,
                filled_at: timestamp(row, &["time", "timestamp"]).unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

#[allow(dead_code)]
pub fn stream_order_payload(value: &Value) -> Value {
    value
        .get("data")
        .cloned()
        .unwrap_or_else(|| value.get("o").cloned().unwrap_or_else(|| json!({})))
}

fn rows(value: &Value) -> Vec<Value> {
    if let Some(array) = value.as_array() {
        return array.clone();
    }
    for field in ["data", "list", "orders", "balances", "fills"] {
        if let Some(array) = value.get(field).and_then(Value::as_array) {
            return array.clone();
        }
    }
    if value.is_object() {
        return vec![value.clone()];
    }
    Vec::new()
}

fn parse_side(value: Option<&str>) -> Option<OrderSide> {
    match value?.to_ascii_uppercase().as_str() {
        "BUY" | "OPEN_LONG" | "CLOSE_SHORT" => Some(OrderSide::Buy),
        "SELL" | "OPEN_SHORT" | "CLOSE_LONG" => Some(OrderSide::Sell),
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

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or("LIMIT").to_ascii_uppercase().as_str() {
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
        "GTX" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or("NEW").to_ascii_uppercase().as_str() {
        "NEW" | "OPEN" => OrderStatus::New,
        "PARTIALLY_FILLED" | "PARTIAL_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn infer_assets(symbol: &str) -> (String, String) {
    for quote in ["USDT", "USDC", "BTC", "ETH", "USD"] {
        if symbol.to_ascii_uppercase().ends_with(quote) && symbol.len() > quote.len() {
            return (
                symbol[..symbol.len() - quote.len()].to_string(),
                quote.to_string(),
            );
        }
    }
    (symbol.to_string(), "USDT".to_string())
}

fn normalize_rate(value: &str) -> String {
    value
        .parse::<f64>()
        .map(|rate| {
            if rate > 1.0 {
                format!("{}", rate / 10_000.0)
            } else {
                value.to_string()
            }
        })
        .unwrap_or_else(|_| value.to_string())
}
