use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol, Fill, FillStatus,
    LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

use super::parser::{
    canonical_from_phemex_symbol, decimal_text_to_f64, decimal_value_to_f64,
    first_timestamp_millis, parse_error, required_str, string_or_number, validation_error,
    value_as_string,
};

pub fn parse_account_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let data = value.get("data").unwrap_or(value);
    let rows = data
        .get("rows")
        .or_else(|| data.get("accounts"))
        .or_else(|| data.get("accounts_p"))
        .or_else(|| data.get("balances"))
        .or_else(|| data.get("wallets"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(data));
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut asset_balances = Vec::new();
    for row in rows {
        let asset = row
            .get("currency")
            .or_else(|| row.get("asset"))
            .and_then(Value::as_str)
            .unwrap_or("USDT")
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_value_to_f64(
            row.get("availableBalanceRv")
                .or_else(|| row.get("availableRv"))
                .or_else(|| row.get("available"))
                .or_else(|| row.get("free")),
        )?
        .or_else(|| {
            let total = scaled_value_to_f64(row.get("balanceEv"), 8)
                .or_else(|| scaled_value_to_f64(row.get("balanceRq"), 8))?;
            let locked = scaled_value_to_f64(row.get("lockedTradingBalanceEv"), 8)
                .or_else(|| scaled_value_to_f64(row.get("lockedTradingBalanceRq"), 8))
                .unwrap_or(0.0)
                + scaled_value_to_f64(row.get("lockedWithdrawEv"), 8)
                    .or_else(|| scaled_value_to_f64(row.get("lockedWithdrawRq"), 8))
                    .unwrap_or(0.0);
            Some((total - locked).max(0.0))
        })
        .unwrap_or(0.0);
        let total = decimal_value_to_f64(
            row.get("accountBalanceRv")
                .or_else(|| row.get("totalBalanceRv"))
                .or_else(|| row.get("balanceRv"))
                .or_else(|| row.get("total"))
                .or_else(|| row.get("balance")),
        )?
        .or_else(|| {
            scaled_value_to_f64(row.get("balanceEv"), 8)
                .or_else(|| scaled_value_to_f64(row.get("balanceRq"), 8))
        })
        .unwrap_or(available);
        let locked = (total - available).max(0.0);
        if total > 0.0 || available > 0.0 || !requested.is_empty() {
            asset_balances.push(
                AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
            );
        }
    }
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        balances: asset_balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbols: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let data = value.get("data").unwrap_or(value);
    let rows = data
        .get("positions")
        .or_else(|| data.get("positions_p"))
        .or_else(|| data.get("rows"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(data));
    let requested = symbols
        .iter()
        .map(|symbol| symbol.symbol.as_str())
        .collect::<Vec<_>>();
    let mut positions = Vec::new();
    for row in rows {
        let symbol_text = row
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if symbol_text.is_empty() {
            continue;
        }
        if !requested.is_empty() && !requested.contains(&symbol_text.as_str()) {
            continue;
        }
        let scope = canonical_from_phemex_symbol(
            exchange_id,
            MarketType::Perpetual,
            &symbol_text,
            None,
            None,
        )?;
        let quantity = decimal_value_to_f64(
            row.get("sizeRq")
                .or_else(|| row.get("posSizeRq"))
                .or_else(|| row.get("size")),
        )?
        .unwrap_or(0.0)
        .abs();
        if quantity == 0.0 && requested.is_empty() {
            continue;
        }
        positions.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: scope.canonical_symbol.ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "phemex position missing canonical symbol".to_string(),
                }
            })?,
            exchange_symbol: Some(scope.exchange_symbol),
            side: parse_position_side(
                row.get("side")
                    .or_else(|| row.get("posSide"))
                    .and_then(Value::as_str),
            ),
            quantity,
            entry_price: decimal_value_to_f64(
                row.get("avgEntryPriceRp")
                    .or_else(|| row.get("avgEntryPrice"))
                    .or_else(|| row.get("entryPriceRp")),
            )?,
            mark_price: decimal_value_to_f64(
                row.get("markPriceRp").or_else(|| row.get("markPrice")),
            )?,
            liquidation_price: decimal_value_to_f64(
                row.get("liquidationPriceRp")
                    .or_else(|| row.get("liquidationPrice")),
            )?,
            unrealized_pnl: decimal_value_to_f64(
                row.get("unRealisedPnlRv")
                    .or_else(|| row.get("unrealisedPnlRv"))
                    .or_else(|| row.get("unrealizedPnl")),
            )?,
            leverage: decimal_value_to_f64(row.get("leverageRr").or_else(|| row.get("leverage")))?,
            observed_at: Utc::now(),
        });
    }
    Ok(positions)
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let data = value.get("data").unwrap_or(value);
    let row = data
        .get("order")
        .or_else(|| data.get("rows").and_then(|rows| rows.as_array()?.first()))
        .unwrap_or(data);
    let exchange_symbol_text = row
        .get("symbol")
        .and_then(Value::as_str)
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.as_str()))
        .ok_or_else(|| parse_error(exchange_id.clone(), "order missing symbol", row))?;
    let inferred_market_type = if exchange_symbol_text.starts_with('s') {
        MarketType::Spot
    } else {
        MarketType::Perpetual
    };
    let market_type = symbol_hint
        .map(|symbol| symbol.market_type)
        .unwrap_or(inferred_market_type);
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(
            exchange_id.clone(),
            inferred_market_type,
            exchange_symbol_text,
        )
        .map_err(validation_error)?
    };
    let canonical_symbol = symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone());
    let raw_type = row
        .get("ordType")
        .or_else(|| row.get("orderType"))
        .and_then(Value::as_str)
        .unwrap_or("Limit");
    let tif = row.get("timeInForce").and_then(Value::as_str);
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol,
        exchange_symbol,
        client_order_id: value_as_string(row.get("clOrdID").or_else(|| row.get("clOrdId"))),
        exchange_order_id: value_as_string(row.get("orderID").or_else(|| row.get("orderId"))),
        side: parse_side(
            row.get("side")
                .and_then(Value::as_str)
                .ok_or_else(|| parse_error(exchange_id.clone(), "order missing side", row))?,
        )?,
        position_side: Some(parse_position_side(
            row.get("posSide").and_then(Value::as_str),
        )),
        order_type: parse_order_type(raw_type, tif),
        time_in_force: tif.and_then(parse_time_in_force),
        status: row
            .get("ordStatus")
            .or_else(|| row.get("status"))
            .and_then(Value::as_str)
            .map(map_phemex_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(
            row.get("orderQtyRq")
                .or_else(|| row.get("baseQtyRq"))
                .or_else(|| row.get("orderQty"))
                .or_else(|| row.get("baseQty")),
        )
        .or_else(|| {
            scaled_value_to_string(
                row.get("baseQtyEv")
                    .or_else(|| row.get("orderQtyEv"))
                    .or_else(|| row.get("execBaseQtyEv")),
                8,
            )
        })
        .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(row.get("priceRp").or_else(|| row.get("price")))
            .or_else(|| scaled_value_to_string(row.get("priceEp"), 8)),
        filled_quantity: string_or_number(
            row.get("cumQtyRq")
                .or_else(|| row.get("cumBaseQtyRq"))
                .or_else(|| row.get("cumQty"))
                .or_else(|| row.get("cumBaseQty")),
        )
        .or_else(|| scaled_value_to_string(row.get("cumBaseQtyEv"), 8))
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(
            row.get("avgPriceRp")
                .or_else(|| row.get("avgPrice"))
                .or_else(|| row.get("avgTransactPriceRp")),
        )
        .or_else(|| scaled_value_to_string(row.get("avgPriceEp"), 8)),
        reduce_only: row
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: tif.is_some_and(|value| value.eq_ignore_ascii_case("PostOnly")),
        created_at: first_timestamp_millis(row, &["actionTimeNs", "transactTimeNs", "createdAt"]),
        updated_at: first_timestamp_millis(row, &["transactTimeNs", "updateTimeNs", "updatedAt"])
            .unwrap_or(now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let data = value.get("data").unwrap_or(value);
    let orders = data
        .get("rows")
        .or_else(|| data.get("orders"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "open orders response missing rows",
                value,
            )
        })?;
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, symbol_hint, order))
        .collect()
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let items = value
        .get("symbolFeeRates")
        .or_else(|| {
            value
                .get("data")
                .and_then(|data| data.get("symbolFeeRates"))
        })
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "fee response missing rates", value))?;
    items
        .iter()
        .map(|item| {
            let symbol_text = required_str(exchange_id, item, "symbol")?;
            let market_type = requested_symbols
                .iter()
                .find(|symbol| symbol.exchange_symbol.symbol == symbol_text)
                .map(|symbol| symbol.market_type)
                .unwrap_or_else(|| {
                    if symbol_text.starts_with('s') {
                        MarketType::Spot
                    } else {
                        MarketType::Perpetual
                    }
                });
            let symbol = requested_symbols
                .iter()
                .find(|symbol| symbol.exchange_symbol.symbol == symbol_text)
                .cloned()
                .unwrap_or(canonical_from_phemex_symbol(
                    exchange_id,
                    market_type,
                    symbol_text,
                    None,
                    None,
                )?);
            Ok(FeeRateSnapshot {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol,
                maker_rate: ratio_er_to_decimal(item.get("makerFeeRateEr"))?,
                taker_rate: ratio_er_to_decimal(item.get("takerFeeRateEr"))?,
                source: Some(if market_type == MarketType::Spot {
                    "phemex.spot_fee_rate".to_string()
                } else {
                    "phemex.futures_fee_rate".to_string()
                }),
                updated_at: Utc::now(),
            })
        })
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
                message: "phemex recent fills request requires canonical_symbol".to_string(),
            })?;
    let rows = value
        .get("data")
        .unwrap_or(value)
        .get("rows")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "fills response missing rows", value))?;
    rows.iter()
        .map(|fill| {
            let price = decimal_value_to_f64(
                fill.get("priceRp")
                    .or_else(|| fill.get("price"))
                    .or_else(|| fill.get("execPriceRp")),
            )?
            .or_else(|| scaled_value_to_f64(fill.get("execPriceEp"), 8))
            .unwrap_or(0.0);
            let quantity = decimal_value_to_f64(
                fill.get("qtyRq")
                    .or_else(|| fill.get("execQtyRq"))
                    .or_else(|| fill.get("baseQtyRq"))
                    .or_else(|| fill.get("quantity")),
            )?
            .or_else(|| scaled_value_to_f64(fill.get("execBaseQtyEv"), 8))
            .unwrap_or(0.0);
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("orderID").or_else(|| fill.get("orderId"))),
                client_order_id: value_as_string(
                    fill.get("clOrdID").or_else(|| fill.get("clOrdId")),
                ),
                fill_id: value_as_string(fill.get("execID").or_else(|| fill.get("execId"))),
                side: parse_side(fill.get("side").and_then(Value::as_str).unwrap_or("Buy"))?,
                position_side: parse_position_side(fill.get("posSide").and_then(Value::as_str)),
                status: FillStatus::Confirmed,
                liquidity_role: LiquidityRole::Unknown,
                price,
                quantity,
                quote_quantity: decimal_value_to_f64(
                    fill.get("execValueRv")
                        .or_else(|| fill.get("quoteQtyRv"))
                        .or_else(|| fill.get("amount_quote")),
                )?
                .or_else(|| scaled_value_to_f64(fill.get("execQuoteQtyEv"), 8)),
                fee_asset: value_as_string(
                    fill.get("feeCurrency").or_else(|| fill.get("currency")),
                ),
                fee_amount: decimal_value_to_f64(
                    fill.get("feeRv")
                        .or_else(|| fill.get("fee"))
                        .or_else(|| fill.get("execFeeRv")),
                )?
                .or_else(|| scaled_value_to_f64(fill.get("execFeeEv"), 8)),
                fee_rate: None,
                realized_pnl: decimal_value_to_f64(fill.get("realizedPnlRv"))?,
                filled_at: first_timestamp_millis(
                    fill,
                    &["transactTimeNs", "execTimeNs", "createTime"],
                )
                .unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

#[allow(dead_code)]
pub fn symbol_scope_from_phemex_row(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let symbol = required_str(exchange_id, value, "symbol")?;
    canonical_from_phemex_symbol(exchange_id, market_type, symbol, None, None)
}

pub fn parse_side(side: &str) -> ExchangeApiResult<OrderSide> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported Phemex side {side}"),
        }),
    }
}

fn parse_position_side(side: Option<&str>) -> PositionSide {
    match side
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "long" => PositionSide::Long,
        "short" => PositionSide::Short,
        "merged" | "net" => PositionSide::Net,
        _ => PositionSide::None,
    }
}

fn parse_order_type(order_type: &str, time_in_force: Option<&str>) -> OrderType {
    if time_in_force.is_some_and(|tif| tif.eq_ignore_ascii_case("PostOnly")) {
        return OrderType::PostOnly;
    }
    match order_type.trim().to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        "stop" | "marketiftouched" => OrderType::StopMarket,
        "stoplimit" | "limitiftouched" => OrderType::StopLimit,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: &str) -> Option<rustcta_types::TimeInForce> {
    match value.trim().to_ascii_lowercase().as_str() {
        "goodtillcancel" | "gtc" => Some(rustcta_types::TimeInForce::GTC),
        "immediateorcancel" | "ioc" => Some(rustcta_types::TimeInForce::IOC),
        "fillorkill" | "fok" => Some(rustcta_types::TimeInForce::FOK),
        "postonly" => Some(rustcta_types::TimeInForce::GTX),
        _ => None,
    }
}

fn map_phemex_order_status(status: &str) -> OrderStatus {
    match status.trim().to_ascii_lowercase().as_str() {
        "new" | "untriggered" => OrderStatus::New,
        "partiallyfilled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "pendingcancel" => OrderStatus::PendingCancel,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

fn ratio_er_to_decimal(value: Option<&Value>) -> ExchangeApiResult<String> {
    let raw = string_or_number(value).ok_or_else(|| ExchangeApiError::InvalidRequest {
        message: "fee rate missing scaled ratio".to_string(),
    })?;
    let number = decimal_text_to_f64(&raw)? / 100_000_000.0;
    Ok(format!("{number:.8}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string())
}

fn scaled_value_to_f64(value: Option<&Value>, scale: u32) -> Option<f64> {
    let raw = value.and_then(value_as_i128)?;
    Some(raw as f64 / 10_f64.powi(scale as i32))
}

fn scaled_value_to_string(value: Option<&Value>, scale: u32) -> Option<String> {
    let raw = value.and_then(value_as_i128)?;
    Some(decimal_from_scaled(raw, scale))
}

fn value_as_i128(value: &Value) -> Option<i128> {
    value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(i128::from))
        .or_else(|| value.as_str()?.parse().ok())
}

fn decimal_from_scaled(value: i128, scale: u32) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let sign = if value < 0 { "-" } else { "" };
    let digits = value.abs().to_string();
    if digits.len() <= scale as usize {
        let padded = format!(
            "{}{}",
            "0".repeat(scale as usize + 1 - digits.len()),
            digits
        );
        let split = padded.len() - scale as usize;
        return format!("{sign}{}.{}", &padded[..split], &padded[split..])
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string();
    }
    let split = digits.len() - scale as usize;
    format!("{sign}{}.{}", &digits[..split], &digits[split..])
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}
