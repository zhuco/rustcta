#![cfg_attr(not(test), allow(dead_code))]

use chrono::{TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, AmendOrderResponse, BatchCancelOrdersResponse, BatchPlaceOrdersResponse,
    ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, ResponseMetadata,
    SymbolRules, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeError, ExchangeErrorClass, ExchangeId,
    ExchangePosition, ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType, OrderBookLevel,
    OrderBookSnapshot, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

pub fn parse_hibachi_symbol_rules(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<SymbolRules>> {
    let contracts = value
        .get("futureContracts")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Hibachi exchange-info missing futureContracts",
                value,
            )
        })?;
    contracts
        .iter()
        .map(|contract| parse_contract(exchange_id, contract))
        .collect()
}

pub fn parse_hibachi_orderbook_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let bids = parse_levels(exchange_id, value.get("bid"))?;
    let asks = parse_levels(exchange_id, value.get("ask"))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "Hibachi order book request requires canonical_symbol".to_string(),
            })?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = value
        .get("sequence")
        .or_else(|| value.get("seqNum"))
        .and_then(value_as_u64);
    snapshot.exchange_timestamp = value
        .get("timestamp")
        .or_else(|| value.get("time"))
        .and_then(value_as_i64)
        .and_then(timestamp_auto);
    Ok(snapshot)
}

pub fn parse_hibachi_fee_snapshots(symbols: &[SymbolScope], value: &Value) -> Vec<FeeRateSnapshot> {
    let fee_config = value.get("feeConfig");
    let maker_rate =
        decimal_path(fee_config, &["tradeMakerFeeRate"]).unwrap_or_else(|| "0".to_string());
    let taker_rate =
        decimal_path(fee_config, &["tradeTakerFeeRate"]).unwrap_or_else(|| "0".to_string());
    symbols
        .iter()
        .cloned()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: maker_rate.clone(),
            taker_rate: taker_rate.clone(),
            source: Some("hibachi.market.exchange_info.feeConfig".to_string()),
            updated_at: Utc::now(),
        })
        .collect()
}

pub fn parse_hibachi_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let requested_assets = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();

    if let Some(assets) = value
        .get("assets")
        .or_else(|| value.get("data").and_then(|data| data.get("assets")))
        .and_then(Value::as_array)
    {
        for asset in assets {
            let asset_name = asset
                .get("symbol")
                .or_else(|| asset.get("asset"))
                .or_else(|| asset.get("currency"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    parse_error(
                        exchange_id.clone(),
                        "Hibachi account info asset row missing symbol",
                        asset,
                    )
                })?;
            let normalized_asset = asset_name.trim().to_ascii_uppercase();
            if !requested_assets.is_empty() && !requested_assets.contains(&normalized_asset) {
                continue;
            }
            let total = asset
                .get("quantity")
                .or_else(|| asset.get("balance"))
                .or_else(|| asset.get("total"))
                .and_then(value_as_f64)
                .unwrap_or_default();
            let available = asset
                .get("available")
                .or_else(|| asset.get("availableBalance"))
                .or_else(|| asset.get("free"))
                .and_then(value_as_f64)
                .unwrap_or(total);
            let locked = asset
                .get("locked")
                .or_else(|| asset.get("reserved"))
                .and_then(value_as_f64)
                .unwrap_or_else(|| (total - available).max(0.0));
            balances.push(
                AssetBalance::new(asset_name, total, available, locked)
                    .map_err(validation_error)?,
            );
        }
    } else {
        let asset = value
            .get("asset")
            .or_else(|| value.get("currency"))
            .and_then(Value::as_str)
            .unwrap_or("USDT");
        let normalized_asset = asset.trim().to_ascii_uppercase();
        if requested_assets.is_empty() || requested_assets.contains(&normalized_asset) {
            let total = value
                .get("balance")
                .or_else(|| value.get("equity"))
                .and_then(value_as_f64)
                .ok_or_else(|| {
                    parse_error(
                        exchange_id.clone(),
                        "Hibachi account info missing balance/equity",
                        value,
                    )
                })?;
            let available = value
                .get("availableBalance")
                .or_else(|| value.get("maximalWithdraw"))
                .and_then(value_as_f64)
                .unwrap_or(total);
            let locked = (total - available).max(0.0);
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
        market_type: MarketType::Perpetual,
        balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_hibachi_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let rows = value
        .get("positions")
        .or_else(|| value.get("data").and_then(|data| data.get("positions")))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Hibachi account info missing positions",
                value,
            )
        })?;
    let requested = requested
        .iter()
        .map(|symbol| symbol.symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect::<Vec<_>>();

    let mut positions = Vec::new();
    for row in rows {
        let symbol_text = required_str(exchange_id, row, "symbol")?;
        let normalized_symbol = symbol_text.trim().to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&normalized_symbol) {
            continue;
        }
        let quantity = row
            .get("quantity")
            .and_then(value_as_f64)
            .unwrap_or_default()
            .abs();
        if quantity == 0.0 {
            continue;
        }
        let market_type = MarketType::Perpetual;
        let unrealized_trading_pnl = row.get("unrealizedTradingPnl").and_then(value_as_f64);
        let unrealized_funding_pnl = row.get("unrealizedFundingPnl").and_then(value_as_f64);
        let unrealized_pnl = match (unrealized_trading_pnl, unrealized_funding_pnl) {
            (Some(trading), Some(funding)) => Some(trading + funding),
            (Some(trading), None) => Some(trading),
            (None, Some(funding)) => Some(funding),
            (None, None) => None,
        };

        positions.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type,
            canonical_symbol: canonical_from_perp_symbol(symbol_text)?,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), market_type, symbol_text)
                    .map_err(validation_error)?,
            ),
            side: parse_position_side(row.get("direction").and_then(Value::as_str)),
            quantity,
            entry_price: row
                .get("openPrice")
                .or_else(|| row.get("entryPrice"))
                .and_then(value_as_f64),
            mark_price: row.get("markPrice").and_then(value_as_f64),
            liquidation_price: None,
            unrealized_pnl,
            leverage: None,
            observed_at: Utc::now(),
        });
    }
    Ok(positions)
}

pub fn parse_hibachi_order(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol_text = required_str(exchange_id, value, "symbol")?;
    let exchange_symbol = ExchangeSymbol::new(
        exchange_id.clone(),
        MarketType::Perpetual,
        symbol_text.to_string(),
    )
    .map_err(validation_error)?;
    let canonical_symbol = canonical_from_perp_symbol(symbol_text).ok();
    let order_type_text = value
        .get("orderType")
        .and_then(|order_type| {
            order_type
                .get("type")
                .or_else(|| order_type.get("orderType"))
                .or(Some(order_type))
        })
        .and_then(Value::as_str)
        .unwrap_or("LIMIT");
    let side = match required_str(exchange_id, value, "side")?
        .to_ascii_uppercase()
        .as_str()
    {
        "BID" | "BUY" => OrderSide::Buy,
        "ASK" | "SELL" => OrderSide::Sell,
        _ => OrderSide::Buy,
    };
    let quantity = decimal_path(Some(value), &["totalQuantity"]).unwrap_or_else(|| "0".to_string());
    let available =
        decimal_path(Some(value), &["availableQuantity"]).unwrap_or_else(|| quantity.clone());
    let filled_quantity =
        decimal_subtract(&quantity, &available).unwrap_or_else(|| "0".to_string());
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol,
        exchange_symbol,
        client_order_id: value
            .get("clientOrderId")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        exchange_order_id: value
            .get("orderId")
            .and_then(value_as_u64)
            .map(|value| value.to_string()),
        side,
        position_side: Some(PositionSide::Net),
        order_type: parse_order_type(order_type_text),
        time_in_force: None,
        status: parse_order_status(value.get("status").and_then(Value::as_str)),
        quantity,
        price: decimal_path(Some(value), &["price"]),
        filled_quantity,
        average_fill_price: None,
        reduce_only: false,
        post_only: order_type_text.eq_ignore_ascii_case("POST_ONLY"),
        created_at: value
            .get("creationTime")
            .and_then(value_as_i64)
            .and_then(timestamp_auto),
        updated_at: value
            .get("finishTime")
            .and_then(value_as_i64)
            .and_then(timestamp_auto)
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_hibachi_orders(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value
        .get("orders")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Hibachi orders response must be an array",
                value,
            )
        })?;
    orders
        .iter()
        .map(|order| parse_hibachi_order(exchange_id, order))
        .collect()
}

pub fn parse_hibachi_recent_fills(
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
                message: "hibachi get_recent_fills requires canonical_symbol".to_string(),
            })?;
    let rows = response_trades(value).as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "Hibachi account trades response must be an array",
            value,
        )
    })?;
    rows.iter()
        .map(|row| {
            let price = number_from_fields(row, &["price", "tradePrice", "executionPrice"])
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "Hibachi fill missing price", row)
                })?;
            let quantity = number_from_fields(
                row,
                &[
                    "quantity",
                    "filledQuantity",
                    "executedQuantity",
                    "size",
                    "amount",
                ],
            )
            .ok_or_else(|| {
                parse_error(exchange_id.clone(), "Hibachi fill missing quantity", row)
            })?;
            let symbol_text = string_from_fields(row, &["symbol"])
                .unwrap_or_else(|| symbol.exchange_symbol.symbol.clone());
            let exchange_symbol =
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol_text)
                    .map_err(validation_error)?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(exchange_symbol),
                order_id: string_from_fields(row, &["orderId", "order_id", "orderid"]),
                client_order_id: string_from_fields(row, &["clientOrderId", "client_order_id"]),
                fill_id: string_from_fields(row, &["tradeId", "trade_id", "fillId", "id"]),
                side: parse_fill_side(string_from_fields(row, &["side"]).as_deref()),
                position_side: PositionSide::Net,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity_role(
                    string_from_fields(row, &["liquidity", "liquidityRole", "role"]).as_deref(),
                ),
                price,
                quantity,
                quote_quantity: number_from_fields(row, &["quoteQuantity", "notional", "value"])
                    .or_else(|| Some(price * quantity)),
                fee_asset: string_from_fields(row, &["feeAsset", "feeCurrency"]),
                fee_amount: number_from_fields(row, &["fee", "feeAmount", "commission"]),
                fee_rate: number_from_fields(row, &["feeRate"]),
                realized_pnl: number_from_fields(row, &["realizedPnl", "realizedPNL", "pnl"]),
                filled_at: timestamp_from_fields(
                    row,
                    &[
                        "tradeTime",
                        "timestamp",
                        "time",
                        "createdAt",
                        "creationTime",
                    ],
                )
                .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_hibachi_amend_order_ack(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<AmendOrderResponse> {
    let order = parse_hibachi_order(exchange_id, response_order(value))?;
    let mut metadata = ResponseMetadata::new(exchange_id.clone(), Utc::now());
    metadata.exchange_timestamp = Some(order.updated_at);
    Ok(AmendOrderResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata,
        order,
    })
}

pub fn parse_hibachi_batch_place_orders_ack(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
    let orders = parse_hibachi_orders(exchange_id, response_orders(value))?;
    let mut metadata = ResponseMetadata::new(exchange_id.clone(), Utc::now());
    metadata.exchange_timestamp = orders.iter().map(|order| order.updated_at).max();
    Ok(BatchPlaceOrdersResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata,
        orders,
        report: None,
    })
}

pub fn parse_hibachi_batch_cancel_orders_ack(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<BatchCancelOrdersResponse> {
    let orders = parse_hibachi_orders(exchange_id, response_orders(value))?;
    let cancelled_count = orders
        .iter()
        .filter(|order| order.status == OrderStatus::Cancelled)
        .count() as u32;
    let mut metadata = ResponseMetadata::new(exchange_id.clone(), Utc::now());
    metadata.exchange_timestamp = orders.iter().map(|order| order.updated_at).max();
    Ok(BatchCancelOrdersResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata,
        orders,
        cancelled_count,
        report: None,
    })
}

pub fn hibachi_order_book_channel(symbol: &str) -> String {
    format!("orderbook/{symbol}")
}

pub fn hibachi_trade_channel(symbol: &str) -> String {
    format!("trades/{symbol}")
}

fn parse_contract(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolRules> {
    let symbol_text = required_str(exchange_id, value, "symbol")?.to_string();
    let base_asset = required_str(exchange_id, value, "underlyingSymbol")?.to_ascii_uppercase();
    let quote_asset = required_str(exchange_id, value, "settlementSymbol")?.to_ascii_uppercase();
    let canonical_symbol =
        CanonicalSymbol::new(&base_asset, &quote_asset).map_err(validation_error)?;
    let exchange_symbol =
        ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol_text)
            .map_err(validation_error)?;
    let tradable = value
        .get("status")
        .and_then(Value::as_str)
        .map(|status| {
            matches!(
                status.to_ascii_lowercase().as_str(),
                "live" | "active" | "trading" | "open"
            )
        })
        .unwrap_or(true);
    Ok(SymbolRules {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: SymbolScope {
            exchange: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: Some(canonical_symbol),
            exchange_symbol,
        },
        base_asset,
        quote_asset,
        price_increment: decimal_path(Some(value), &["tickSize"]),
        quantity_increment: decimal_path(Some(value), &["stepSize"]),
        min_price: None,
        max_price: None,
        min_quantity: decimal_path(Some(value), &["minOrderSize"]),
        max_quantity: None,
        min_notional: decimal_path(Some(value), &["minNotional"]),
        max_notional: None,
        price_precision: decimal_path(Some(value), &["tickSize"]).and_then(decimal_precision),
        quantity_precision: decimal_path(Some(value), &["stepSize"]).and_then(decimal_precision),
        supports_market_orders: tradable,
        supports_limit_orders: tradable,
        supports_post_only: tradable,
        supports_reduce_only: true,
        updated_at: Utc::now(),
    })
}

fn parse_levels(
    exchange_id: &ExchangeId,
    side: Option<&Value>,
) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    let levels = side
        .and_then(|side| side.get("levels"))
        .or(side)
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "Hibachi order book missing levels",
                &Value::Null,
            )
        })?;
    levels
        .iter()
        .filter_map(|level| {
            let price = level.get("price").and_then(value_as_f64)?;
            let quantity = level.get("quantity").and_then(value_as_f64)?;
            (price > 0.0 && quantity > 0.0).then_some((price, quantity))
        })
        .map(|(price, quantity)| OrderBookLevel::new(price, quantity).map_err(validation_error))
        .collect()
}

fn response_order(value: &Value) -> &Value {
    value
        .get("order")
        .or_else(|| value.get("data").and_then(|data| data.get("order")))
        .or_else(|| value.get("data"))
        .unwrap_or(value)
}

fn response_orders(value: &Value) -> &Value {
    value
        .get("orders")
        .or_else(|| value.get("data").and_then(|data| data.get("orders")))
        .or_else(|| value.get("data").and_then(|data| data.get("rows")))
        .or_else(|| value.get("data"))
        .unwrap_or(value)
}

fn response_trades(value: &Value) -> &Value {
    value
        .get("trades")
        .or_else(|| value.get("fills"))
        .or_else(|| value.get("data").and_then(|data| data.get("trades")))
        .or_else(|| value.get("data").and_then(|data| data.get("fills")))
        .or_else(|| value.get("data").and_then(|data| data.get("rows")))
        .or_else(|| value.get("data"))
        .unwrap_or(value)
}

fn canonical_from_perp_symbol(symbol: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let (base, quote) =
        symbol
            .trim()
            .split_once('/')
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("cannot infer Hibachi canonical symbol from {symbol}"),
            })?;
    let quote = quote.trim_end_matches("-P").trim_end_matches("-PERP");
    CanonicalSymbol::new(base, quote).map_err(validation_error)
}

fn parse_order_type(value: &str) -> OrderType {
    match value.to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        "POST_ONLY" => OrderType::PostOnly,
        "IOC" => OrderType::IOC,
        "FOK" => OrderType::FOK,
        _ => OrderType::Limit,
    }
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "OPEN" | "NEW" | "PLACED" => OrderStatus::Open,
        "PARTIALLY_FILLED" | "PARTIAL" => OrderStatus::PartiallyFilled,
        "FILLED" | "FINISHED" => OrderStatus::Filled,
        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn parse_position_side(value: Option<&str>) -> PositionSide {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "LONG" | "BID" | "BUY" => PositionSide::Long,
        "SHORT" | "ASK" | "SELL" => PositionSide::Short,
        "NET" | "BOTH" => PositionSide::Net,
        _ => PositionSide::None,
    }
}

fn parse_fill_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "ASK" | "SELL" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_liquidity_role(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "MAKER" | "M" => LiquidityRole::Maker,
        "TAKER" | "T" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn required_str<'a>(
    exchange_id: &ExchangeId,
    value: &'a Value,
    field: &str,
) -> ExchangeApiResult<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            format!("Hibachi payload missing {field}"),
            value,
        )
    })
}

fn decimal_path(value: Option<&Value>, path: &[&str]) -> Option<String> {
    let mut cursor = value?;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    value_as_decimal_string(cursor)
}

fn value_as_decimal_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

fn string_from_fields(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        value.get(*field).and_then(|value| match value {
            Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
    })
}

fn number_from_fields(value: &Value, fields: &[&str]) -> Option<f64> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_f64))
}

fn timestamp_from_fields(value: &Value, fields: &[&str]) -> Option<chrono::DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(value_as_i64)
            .and_then(timestamp_auto)
    })
}

fn decimal_precision(value: String) -> Option<u32> {
    let trimmed = value.trim_end_matches('0').trim_end_matches('.');
    trimmed
        .split_once('.')
        .map(|(_, fraction)| fraction.len() as u32)
        .or(Some(0))
}

fn decimal_subtract(total: &str, available: &str) -> Option<String> {
    let total = total.parse::<f64>().ok()?;
    let available = available.parse::<f64>().ok()?;
    Some(
        format!("{:.12}", (total - available).max(0.0))
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string(),
    )
}

fn timestamp_auto(value: i64) -> Option<chrono::DateTime<Utc>> {
    if value > 10_000_000_000 {
        Utc.timestamp_millis_opt(value).single()
    } else {
        Utc.timestamp_opt(value, 0).single()
    }
}

fn parse_error(
    exchange_id: ExchangeId,
    message: impl Into<String>,
    raw: &Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange_id,
        ExchangeErrorClass::Decode,
        message.into(),
        Utc::now(),
    );
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
