use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiResult, OrderState, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    data_array, decimal_field, decimal_text_to_f64, normalize_futures_symbol_lossy,
    normalize_spot_symbol_lossy, parse_error, string_or_number, validation_error, value_as_i64,
    value_as_string,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let data = value.get("data").unwrap_or(value);
    let rows = data
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(data));
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for row in rows {
        let asset = value_as_string(
            row.get("currency")
                .or_else(|| row.get("asset"))
                .or_else(|| row.get("coin"))
                .or_else(|| row.get("token")),
        )
        .unwrap_or_default()
        .to_ascii_uppercase();
        if asset.is_empty() || (!requested.is_empty() && !requested.contains(&asset)) {
            continue;
        }
        let available = string_or_number(
            row.get("available")
                .or_else(|| row.get("availableBalance"))
                .or_else(|| row.get("avail"))
                .or_else(|| row.get("free")),
        )
        .unwrap_or_else(|| "0".to_string());
        let locked = string_or_number(
            row.get("frozen")
                .or_else(|| row.get("locked"))
                .or_else(|| row.get("hold"))
                .or_else(|| row.get("freeze")),
        )
        .unwrap_or_else(|| "0".to_string());
        let total = string_or_number(
            row.get("total")
                .or_else(|| row.get("balance"))
                .or_else(|| row.get("equity")),
        );
        let available_number = decimal_text_to_f64(&available)?;
        let locked_number = decimal_text_to_f64(&locked)?;
        let total_number = match total {
            Some(total) => decimal_text_to_f64(&total)?,
            None => available_number + locked_number,
        };
        if total_number > 0.0 || available_number > 0.0 || !requested.is_empty() {
            balances.push(
                AssetBalance::new(asset, total_number, available_number, locked_number)
                    .map_err(validation_error)?,
            );
        }
    }
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
    tenant_id: TenantId,
    account_id: AccountId,
    requested_symbols: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let rows = data_array(
        exchange_id,
        value,
        "coinstore positions response missing data",
    )?;
    let requested = requested_symbols
        .iter()
        .map(|symbol| normalize_futures_symbol_lossy(&symbol.symbol))
        .collect::<Vec<_>>();
    let mut positions = Vec::new();
    for row in rows {
        let raw_symbol = value_as_string(
            row.get("contractName")
                .or_else(|| row.get("contract"))
                .or_else(|| row.get("symbol"))
                .or_else(|| row.get("name")),
        )
        .map(|symbol| normalize_futures_symbol_lossy(&symbol))
        .unwrap_or_default();
        if raw_symbol.is_empty()
            || (!requested.is_empty() && !requested.iter().any(|symbol| symbol == &raw_symbol))
        {
            continue;
        }
        let quantity = decimal_field(
            row,
            &[
                "quantity",
                "position",
                "positionSize",
                "available",
                "holdVol",
                "volume",
            ],
        )
        .unwrap_or(0.0)
        .abs();
        if quantity == 0.0 && requested.is_empty() {
            continue;
        }
        let (base, quote) = split_futures_assets(&raw_symbol);
        let canonical_symbol = CanonicalSymbol::new(base, quote).map_err(validation_error)?;
        let exchange_symbol =
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, raw_symbol)
                .map_err(validation_error)?;
        positions.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol,
            exchange_symbol: Some(exchange_symbol),
            side: parse_position_side(
                row.get("side")
                    .or_else(|| row.get("positionSide"))
                    .or_else(|| row.get("direction"))
                    .and_then(Value::as_str),
            ),
            quantity,
            entry_price: decimal_field(row, &["entryPrice", "avgPrice", "openPrice"]),
            mark_price: decimal_field(row, &["markPrice", "mp"]),
            liquidation_price: decimal_field(row, &["liquidationPrice", "liqPrice"]),
            unrealized_pnl: decimal_field(row, &["unrealizedPnl", "unrealizedPNL", "profit"]),
            leverage: decimal_field(row, &["leverage"]),
            observed_at: Utc::now(),
        });
    }
    Ok(positions)
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let data = value
        .get("data")
        .and_then(|data| data.as_array().and_then(|rows| rows.first()).or(Some(data)))
        .unwrap_or(value);
    let raw_symbol = value_as_string(
        data.get("symbol")
            .or_else(|| data.get("name"))
            .or_else(|| data.get("contractName"))
            .or_else(|| data.get("instrument")),
    )
    .map(|symbol| normalize_order_symbol(&symbol, market_type))
    .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.clone()))
    .ok_or_else(|| parse_error(exchange_id.clone(), "coinstore order missing symbol", data))?;
    let exchange_symbol = symbol_hint
        .map(|symbol| symbol.exchange_symbol.clone())
        .unwrap_or(
            ExchangeSymbol::new(exchange_id.clone(), market_type, raw_symbol)
                .map_err(validation_error)?,
        );
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone()),
        exchange_symbol,
        client_order_id: value_as_string(
            data.get("clientOrderId")
                .or_else(|| data.get("clOrdId"))
                .or_else(|| data.get("clientId")),
        ),
        exchange_order_id: value_as_string(
            data.get("orderId")
                .or_else(|| data.get("ordId"))
                .or_else(|| data.get("id")),
        ),
        side: map_order_side(
            data.get("side")
                .or_else(|| data.get("orderSide"))
                .and_then(Value::as_str)
                .unwrap_or("buy"),
        ),
        position_side: Some(if market_type == MarketType::Perpetual {
            parse_position_side(
                data.get("positionSide")
                    .or_else(|| data.get("position_side"))
                    .and_then(Value::as_str),
            )
        } else {
            PositionSide::None
        }),
        order_type: map_order_type(
            data.get("type")
                .or_else(|| data.get("orderType"))
                .and_then(Value::as_str),
        ),
        time_in_force: Some(TimeInForce::GTC),
        status: data
            .get("status")
            .or_else(|| data.get("state"))
            .and_then(Value::as_str)
            .map(map_order_status)
            .unwrap_or(OrderStatus::Open),
        quantity: string_or_number(
            data.get("quantity")
                .or_else(|| data.get("qty"))
                .or_else(|| data.get("orderQty"))
                .or_else(|| data.get("amount")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(
            data.get("price")
                .or_else(|| data.get("orderPrice"))
                .or_else(|| data.get("limitPrice")),
        )
        .filter(|price| price != "0"),
        filled_quantity: string_or_number(
            data.get("filledQuantity")
                .or_else(|| data.get("filledQty"))
                .or_else(|| data.get("dealQty"))
                .or_else(|| data.get("executedQty")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(
            data.get("avgPrice")
                .or_else(|| data.get("averagePrice"))
                .or_else(|| data.get("dealPrice")),
        ),
        reduce_only: data
            .get("reduceOnly")
            .or_else(|| data.get("reduce_only"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: data
            .get("postOnly")
            .or_else(|| data.get("post_only"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: data
            .get("createdAt")
            .or_else(|| data.get("createTime"))
            .or_else(|| data.get("time"))
            .and_then(value_as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        updated_at: now,
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    data_array(exchange_id, value, "coinstore open orders missing data")?
        .iter()
        .map(|row| parse_order_state(exchange_id, symbol_hint, market_type, row))
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol_hint: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let rows = data_array(exchange_id, value, "coinstore fills response missing data")?;
    let mut fills = Vec::new();
    for row in rows {
        let raw_symbol = value_as_string(
            row.get("symbol")
                .or_else(|| row.get("name"))
                .or_else(|| row.get("contractName")),
        )
        .map(|symbol| normalize_order_symbol(&symbol, market_type))
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .unwrap_or_else(|| "BTCUSDT".to_string());
        let (base, quote) = symbol_hint
            .and_then(|symbol| symbol.canonical_symbol.clone())
            .map(|canonical| {
                (
                    canonical.base_asset().to_string(),
                    canonical.quote_asset().to_string(),
                )
            })
            .unwrap_or_else(|| split_assets(&raw_symbol, market_type));
        let price = decimal_field(row, &["price", "tradePrice", "fillPrice"]).unwrap_or(0.0);
        let quantity = decimal_field(row, &["quantity", "qty", "fillQty", "amount"]).unwrap_or(0.0);
        if price <= 0.0 || quantity <= 0.0 {
            continue;
        }
        fills.push(Fill {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type,
            canonical_symbol: CanonicalSymbol::new(base, quote).map_err(validation_error)?,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), market_type, raw_symbol)
                    .map_err(validation_error)?,
            ),
            order_id: value_as_string(row.get("orderId").or_else(|| row.get("ordId"))),
            client_order_id: value_as_string(
                row.get("clientOrderId")
                    .or_else(|| row.get("clOrdId"))
                    .or_else(|| row.get("clientId")),
            ),
            fill_id: value_as_string(row.get("tradeId").or_else(|| row.get("matchId"))),
            side: map_order_side(row.get("side").and_then(Value::as_str).unwrap_or("buy")),
            position_side: parse_position_side(row.get("positionSide").and_then(Value::as_str)),
            status: FillStatus::Confirmed,
            liquidity_role: parse_liquidity(
                row.get("liquidity")
                    .or_else(|| row.get("role"))
                    .and_then(Value::as_str),
            ),
            price,
            quantity,
            quote_quantity: decimal_field(row, &["quoteQty", "notional", "amountValue"]),
            fee_asset: value_as_string(row.get("feeAsset").or_else(|| row.get("feeCurrency"))),
            fee_amount: decimal_field(row, &["fee", "feeAmount", "commission"]),
            fee_rate: decimal_field(row, &["feeRate"]),
            realized_pnl: decimal_field(row, &["realizedPnl", "pnl"]),
            filled_at: row
                .get("tradeTime")
                .or_else(|| row.get("time"))
                .or_else(|| row.get("createdAt"))
                .and_then(value_as_i64)
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .unwrap_or_else(Utc::now),
            received_at: Utc::now(),
        });
    }
    Ok(fills)
}

pub fn map_order_side(side: &str) -> OrderSide {
    match side.to_ascii_lowercase().as_str() {
        "2" | "sell" | "s" | "ask" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

pub fn parse_position_side(side: Option<&str>) -> PositionSide {
    match side.unwrap_or_default().to_ascii_lowercase().as_str() {
        "short" | "sell" | "2" => PositionSide::Short,
        "long" | "buy" | "1" => PositionSide::Long,
        "net" => PositionSide::Net,
        _ => PositionSide::None,
    }
}

fn map_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_lowercase().as_str() {
        "open" | "new" | "submitted" | "1" => OrderStatus::Open,
        "partially_filled" | "partial" | "part_filled" | "partiallyfilled" => {
            OrderStatus::PartiallyFilled
        }
        "filled" | "complete" | "completed" | "done" | "2" => OrderStatus::Filled,
        "cancelled" | "canceled" | "cancel" | "4" => OrderStatus::Cancelled,
        "rejected" | "reject" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn map_order_type(order_type: Option<&str>) -> OrderType {
    match order_type.unwrap_or_default().to_ascii_lowercase().as_str() {
        "market" | "1" => OrderType::Market,
        "post_only" | "postonly" => OrderType::PostOnly,
        "ioc" => OrderType::IOC,
        "fok" => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_liquidity(role: Option<&str>) -> LiquidityRole {
    match role.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" | "m" => LiquidityRole::Maker,
        "taker" | "t" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn normalize_order_symbol(symbol: &str, market_type: MarketType) -> String {
    if market_type == MarketType::Perpetual {
        normalize_futures_symbol_lossy(symbol)
    } else {
        normalize_spot_symbol_lossy(symbol)
    }
}

fn split_assets(symbol: &str, market_type: MarketType) -> (String, String) {
    if market_type == MarketType::Perpetual {
        split_futures_assets(symbol)
    } else {
        split_spot_assets(symbol)
    }
}

fn split_spot_assets(symbol: &str) -> (String, String) {
    let normalized = normalize_spot_symbol_lossy(symbol);
    ["USDT", "USDC", "BTC", "ETH"]
        .iter()
        .find_map(|quote| {
            normalized
                .strip_suffix(quote)
                .filter(|base| !base.is_empty())
                .map(|base| (base.to_string(), (*quote).to_string()))
        })
        .unwrap_or((normalized, "USDT".to_string()))
}

fn split_futures_assets(symbol: &str) -> (String, String) {
    let normalized = normalize_futures_symbol_lossy(symbol);
    if let Some((base, quote)) = normalized.split_once('-') {
        (base.to_string(), quote.to_string())
    } else {
        split_spot_assets(&normalized)
    }
}
