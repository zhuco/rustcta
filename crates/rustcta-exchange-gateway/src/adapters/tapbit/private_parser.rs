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
    data_array, decimal_text_to_f64, normalize_spot_symbol, parse_error, string_or_number,
    validation_error, value_as_i64, value_as_string,
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
        let asset = row
            .get("asset")
            .or_else(|| row.get("coin"))
            .or_else(|| row.get("currency"))
            .or_else(|| row.get("assetName"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_uppercase();
        if asset.is_empty() || (!requested.is_empty() && !requested.contains(&asset)) {
            continue;
        }
        let available = string_or_number(
            row.get("available")
                .or_else(|| row.get("available_balance"))
                .or_else(|| row.get("availableBalance"))
                .or_else(|| row.get("margin_available"))
                .or_else(|| row.get("available_margin")),
        )
        .unwrap_or_else(|| "0".to_string());
        let locked = string_or_number(
            row.get("frozen_balance")
                .or_else(|| row.get("frozen"))
                .or_else(|| row.get("locked"))
                .or_else(|| row.get("order_margin")),
        )
        .unwrap_or_else(|| "0".to_string());
        let total = string_or_number(
            row.get("total_balance")
                .or_else(|| row.get("balance"))
                .or_else(|| row.get("equity"))
                .or_else(|| row.get("total")),
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
    let data = value.get("data").unwrap_or(value);
    let rows = data
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(data));
    let requested = requested_symbols
        .iter()
        .map(|symbol| symbol.symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let mut positions = Vec::new();
    for row in rows {
        let raw_symbol = value_as_string(
            row.get("contract_code")
                .or_else(|| row.get("instrument_id"))
                .or_else(|| row.get("symbol"))
                .or_else(|| row.get("contract")),
        )
        .unwrap_or_default()
        .to_ascii_uppercase();
        if raw_symbol.is_empty() {
            continue;
        }
        let normalized = normalize_perp_symbol_lossy(&raw_symbol);
        if !requested.is_empty()
            && !requested
                .iter()
                .any(|symbol| symbol == &normalized || symbol == &raw_symbol)
        {
            continue;
        }
        let quantity = decimal_field(
            row,
            &[
                "quantity",
                "position",
                "position_amount",
                "positionAmount",
                "available",
                "volume",
            ],
        )
        .unwrap_or(0.0)
        .abs();
        if quantity == 0.0 && requested.is_empty() {
            continue;
        }
        let base = normalized
            .strip_suffix("-SWAP")
            .unwrap_or(normalized.as_str())
            .to_string();
        let canonical_symbol = CanonicalSymbol::new(base, "USDT").map_err(validation_error)?;
        let exchange_symbol =
            ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, normalized)
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
                    .or_else(|| row.get("direction"))
                    .or_else(|| row.get("position_side"))
                    .and_then(Value::as_str),
            ),
            quantity,
            entry_price: decimal_field(row, &["entry_price", "entryPrice", "avg_price", "price"]),
            mark_price: decimal_field(row, &["mark_price", "markPrice"]),
            liquidation_price: decimal_field(row, &["liquidation_price", "liquidationPrice"]),
            unrealized_pnl: decimal_field(
                row,
                &[
                    "unrealized_pnl",
                    "unrealizedPnl",
                    "profit",
                    "floating_profit_loss",
                ],
            ),
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
    let raw_symbol = data
        .get("trade_pair_name")
        .or_else(|| data.get("instrument_id"))
        .or_else(|| data.get("contract_code"))
        .or_else(|| data.get("symbol"))
        .and_then(Value::as_str)
        .map(|symbol| normalize_order_symbol(symbol, market_type))
        .transpose()?
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| parse_error(exchange_id.clone(), "tapbit order missing symbol", data))?;
    let exchange_symbol = symbol_hint
        .map(|symbol| symbol.exchange_symbol.clone())
        .unwrap_or(
            ExchangeSymbol::new(exchange_id.clone(), market_type, raw_symbol)
                .map_err(validation_error)?,
        );
    let side = map_order_side(
        data.get("direction")
            .or_else(|| data.get("side"))
            .and_then(Value::as_str)
            .unwrap_or("buy"),
    );
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone()),
        exchange_symbol,
        client_order_id: None,
        exchange_order_id: value_as_string(data.get("order_id").or_else(|| data.get("orderId"))),
        side,
        position_side: Some(if market_type == MarketType::Perpetual {
            parse_position_side(data.get("position_side").and_then(Value::as_str))
        } else {
            PositionSide::None
        }),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: data
            .get("status")
            .and_then(Value::as_str)
            .map(map_order_status)
            .unwrap_or(OrderStatus::Open),
        quantity: string_or_number(data.get("quantity").or_else(|| data.get("amount")))
            .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(data.get("price")).filter(|price| price != "0"),
        filled_quantity: string_or_number(
            data.get("filled_quantity")
                .or_else(|| data.get("filledQuantity"))
                .or_else(|| data.get("deal_quantity")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(
            data.get("average_price").or_else(|| data.get("avg_price")),
        ),
        reduce_only: data
            .get("reduce_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: false,
        created_at: data
            .get("order_time")
            .or_else(|| data.get("create_time"))
            .or_else(|| data.get("created_at"))
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
    data_array(
        exchange_id,
        value,
        "tapbit open order response missing data",
    )?
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
    let rows = data_array(exchange_id, value, "tapbit fills response missing data")?;
    let mut fills = Vec::new();
    for row in rows {
        let raw_symbol = value_as_string(
            row.get("trade_pair_name")
                .or_else(|| row.get("instrument_id"))
                .or_else(|| row.get("contract_code"))
                .or_else(|| row.get("symbol")),
        )
        .map(|symbol| normalize_order_symbol(&symbol, market_type))
        .transpose()?
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .unwrap_or_else(|| {
            if market_type == MarketType::Perpetual {
                "BTC-SWAP".to_string()
            } else {
                "BTC/USDT".to_string()
            }
        });
        let (base, quote) = if let Some(scope) = symbol_hint {
            let canonical = scope.canonical_symbol.clone().ok_or_else(|| {
                parse_error(exchange_id.clone(), "fill symbol missing canonical", row)
            })?;
            (
                canonical.base_asset().to_string(),
                canonical.quote_asset().to_string(),
            )
        } else {
            split_symbol_assets(&raw_symbol, market_type)
        };
        let canonical_symbol = CanonicalSymbol::new(base, quote).map_err(validation_error)?;
        let exchange_symbol = ExchangeSymbol::new(exchange_id.clone(), market_type, raw_symbol)
            .map_err(validation_error)?;
        let price = decimal_field(row, &["price", "trade_price", "fill_price"])
            .or_else(|| decimal_field(row, &["average_price"]))
            .unwrap_or(0.0);
        let quantity =
            decimal_field(row, &["quantity", "filled_quantity", "amount", "volume"]).unwrap_or(0.0);
        if price <= 0.0 || quantity <= 0.0 {
            continue;
        }
        fills.push(Fill {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type,
            canonical_symbol,
            exchange_symbol: Some(exchange_symbol),
            order_id: value_as_string(row.get("order_id").or_else(|| row.get("orderId"))),
            client_order_id: None,
            fill_id: value_as_string(row.get("fill_id").or_else(|| row.get("trade_id"))),
            side: map_order_side(
                row.get("direction")
                    .or_else(|| row.get("side"))
                    .and_then(Value::as_str)
                    .unwrap_or("buy"),
            ),
            position_side: parse_position_side(row.get("position_side").and_then(Value::as_str)),
            status: FillStatus::Confirmed,
            liquidity_role: parse_liquidity(
                row.get("role")
                    .or_else(|| row.get("liquidity"))
                    .and_then(Value::as_str),
            ),
            price,
            quantity,
            quote_quantity: decimal_field(row, &["filled_amount", "quote_quantity", "notional"]),
            fee_asset: value_as_string(row.get("fee_asset").or_else(|| row.get("feeAsset"))),
            fee_amount: decimal_field(row, &["fee", "fee_amount", "commission"]),
            fee_rate: decimal_field(row, &["fee_rate", "maker_fee_rate", "taker_fee_rate"]),
            realized_pnl: decimal_field(row, &["realized_pnl", "realizedPnl"]),
            filled_at: row
                .get("trade_time")
                .or_else(|| row.get("time"))
                .or_else(|| row.get("created_at"))
                .and_then(value_as_i64)
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .unwrap_or_else(Utc::now),
            received_at: Utc::now(),
        });
    }
    Ok(fills)
}

pub fn map_order_side(direction: &str) -> OrderSide {
    match direction.to_ascii_lowercase().as_str() {
        "2" | "sell" | "short" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn map_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_lowercase().as_str() {
        "unsettled" | "open" | "new" => OrderStatus::Open,
        "complete" | "filled" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "partially cancelled" | "partial_cancelled" | "partially_filled" | "partial" => {
            OrderStatus::PartiallyFilled
        }
        _ => OrderStatus::Unknown,
    }
}

fn normalize_order_symbol(symbol: &str, market_type: MarketType) -> ExchangeApiResult<String> {
    if market_type == MarketType::Perpetual {
        Ok(normalize_perp_symbol_lossy(symbol))
    } else {
        normalize_spot_symbol(symbol)
    }
}

fn normalize_perp_symbol_lossy(symbol: &str) -> String {
    let upper = symbol.trim().replace(['/', '_'], "-").to_ascii_uppercase();
    if upper.ends_with("-SWAP") {
        upper
    } else if let Some(base) = upper.strip_suffix("-USDT") {
        format!("{base}-SWAP")
    } else if let Some(base) = upper.strip_suffix("USDT") {
        format!("{base}-SWAP")
    } else {
        format!("{upper}-SWAP")
    }
}

fn split_symbol_assets(symbol: &str, market_type: MarketType) -> (String, String) {
    if market_type == MarketType::Perpetual {
        (
            symbol
                .strip_suffix("-SWAP")
                .unwrap_or(symbol)
                .to_ascii_uppercase(),
            "USDT".to_string(),
        )
    } else if let Some((base, quote)) = symbol.split_once('/') {
        (base.to_ascii_uppercase(), quote.to_ascii_uppercase())
    } else if let Some(base) = symbol.strip_suffix("USDT") {
        (base.to_ascii_uppercase(), "USDT".to_string())
    } else {
        (symbol.to_ascii_uppercase(), "USDT".to_string())
    }
}

fn parse_position_side(value: Option<&str>) -> PositionSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "long" | "buy" | "1" => PositionSide::Long,
        "short" | "sell" | "2" => PositionSide::Short,
        "net" | "both" => PositionSide::Net,
        _ => PositionSide::Net,
    }
}

fn parse_liquidity(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" | "m" => LiquidityRole::Maker,
        "taker" | "t" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn decimal_field(value: &Value, fields: &[&str]) -> Option<f64> {
    fields
        .iter()
        .find_map(|field| {
            value
                .get(*field)
                .and_then(|value| string_or_number(Some(value)))
        })
        .and_then(|value| value.parse().ok())
}
