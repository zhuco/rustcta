use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderListKind,
    OrderListResponse, OrderState, ResponseMetadata, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, PositionSide,
    SchemaVersion,
};
use serde_json::Value;

use super::parser::{
    average_price_text, compact_symbol_assets, decimal_text_to_f64, decimal_value_to_f64,
    first_timestamp_millis, non_zero_string, parse_error, parse_order_type, parse_side,
    parse_time_in_force, required_str, string_or_number, validation_error, value_as_i64,
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
    let balances = if market_type == MarketType::Perpetual {
        value.as_array().ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "futures balance response is not an array",
                value,
            )
        })?
    } else {
        value
            .get("balances")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "account response missing balances",
                    value,
                )
            })?
    };
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut asset_balances = Vec::new();
    for balance in balances {
        let asset = required_str(exchange_id, balance, "asset")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let (total, available_number, locked_number) = if market_type == MarketType::Perpetual {
            let total = string_or_number(
                balance
                    .get("balance")
                    .or_else(|| balance.get("walletBalance")),
            )
            .unwrap_or_else(|| "0".to_string());
            let available = string_or_number(
                balance
                    .get("availableBalance")
                    .or_else(|| balance.get("maxWithdrawAmount"))
                    .or_else(|| balance.get("crossWalletBalance")),
            )
            .unwrap_or_else(|| total.clone());
            let total_number = decimal_text_to_f64(&total)?;
            let available_number = decimal_text_to_f64(&available)?;
            (
                total_number,
                available_number,
                (total_number - available_number).max(0.0),
            )
        } else {
            let available =
                string_or_number(balance.get("free")).unwrap_or_else(|| "0".to_string());
            let locked = string_or_number(balance.get("locked")).unwrap_or_else(|| "0".to_string());
            let available_number = decimal_text_to_f64(&available)?;
            let locked_number = decimal_text_to_f64(&locked)?;
            (
                available_number + locked_number,
                available_number,
                locked_number,
            )
        };
        if total > 0.0 || available_number > 0.0 || locked_number > 0.0 || !requested.is_empty() {
            asset_balances.push(
                AssetBalance::new(asset, total, available_number, locked_number)
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
        balances: asset_balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let exchange_symbol_text = required_str(exchange_id, value, "symbol")
        .or_else(|_| required_str(exchange_id, value, "s"))?
        .to_ascii_uppercase();
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            exchange_symbol_text.clone(),
        )
        .map_err(validation_error)?
    };
    let canonical_symbol = symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone());
    let tif_text = value
        .get("timeInForce")
        .or_else(|| value.get("f"))
        .and_then(Value::as_str);
    let raw_type = value
        .get("type")
        .or_else(|| value.get("o"))
        .and_then(Value::as_str)
        .unwrap_or("LIMIT");
    let order_type = parse_order_type(raw_type, tif_text);
    let market_type = symbol_hint
        .map(|symbol| symbol.market_type)
        .unwrap_or(MarketType::Spot);
    let side = parse_side(
        required_str(exchange_id, value, "side")
            .or_else(|_| required_str(exchange_id, value, "S"))?,
    )?;
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol,
        exchange_symbol,
        client_order_id: value_as_string(value.get("clientOrderId").or_else(|| value.get("c"))),
        exchange_order_id: value_as_string(value.get("orderId").or_else(|| value.get("i"))),
        side,
        position_side: Some(parse_position_side(
            value.get("positionSide").and_then(Value::as_str),
            decimal_value_to_f64(value.get("positionAmt"))?,
            side,
            market_type,
        )),
        order_type,
        time_in_force: parse_time_in_force(tif_text),
        status: value
            .get("status")
            .or_else(|| value.get("X"))
            .and_then(Value::as_str)
            .map(map_binance_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity: string_or_number(
            value
                .get("origQty")
                .or_else(|| value.get("q"))
                .or_else(|| value.get("qty")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: non_zero_string(
            string_or_number(value.get("price").or_else(|| value.get("p")))
                .unwrap_or_else(|| "0".to_string()),
        ),
        filled_quantity: string_or_number(value.get("executedQty").or_else(|| value.get("z")))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: value_as_string(value.get("avgPrice"))
            .or_else(|| average_price_text(value)),
        reduce_only: value
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: raw_type.eq_ignore_ascii_case("LIMIT_MAKER")
            || tif_text.is_some_and(|tif| tif.eq_ignore_ascii_case("GTX")),
        created_at: first_timestamp_millis(value, &["transactTime", "time", "O", "E"]),
        updated_at: first_timestamp_millis(value, &["updateTime", "T", "E"]).unwrap_or(now),
    })
}

pub fn parse_open_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "open orders response is not an array",
            value,
        )
    })?;
    orders
        .iter()
        .map(|order| {
            if let Some(symbol_hint) = symbol_hint {
                parse_order_state(exchange_id, Some(symbol_hint), order)
            } else {
                let symbol = required_str(exchange_id, order, "symbol")?.to_ascii_uppercase();
                let symbol_hint =
                    symbol_scope_from_exchange_symbol(exchange_id, market_type, symbol)?;
                parse_order_state(exchange_id, Some(&symbol_hint), order)
            }
        })
        .collect()
}

pub fn parse_binance_cancel_all_orders(
    exchange_id: &ExchangeId,
    symbol_hint: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "cancel-all response is not an array",
            value,
        )
    })?;
    let mut orders = Vec::new();
    for row in rows {
        if let Some(reports) = row.get("orderReports").and_then(Value::as_array) {
            for report in reports {
                orders.push(parse_order_state(exchange_id, Some(symbol_hint), report)?);
            }
        } else {
            orders.push(parse_order_state(exchange_id, Some(symbol_hint), row)?);
        }
    }
    Ok(orders)
}

pub fn parse_order_list_response(
    exchange_id: &ExchangeId,
    symbol_hint: &SymbolScope,
    kind: OrderListKind,
    value: &Value,
) -> ExchangeApiResult<OrderListResponse> {
    let reports = value
        .get("orderReports")
        .or_else(|| value.get("orders"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let orders = reports
        .iter()
        .map(|order| parse_order_state(exchange_id, Some(symbol_hint), order))
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(OrderListResponse {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        metadata: ResponseMetadata::new(exchange_id.clone(), Utc::now()),
        symbol: symbol_hint.clone(),
        kind,
        order_list_id: value_as_string(value.get("orderListId")),
        list_client_order_id: value_as_string(value.get("listClientOrderId")),
        list_status_type: value_as_string(value.get("listStatusType")),
        list_order_status: value_as_string(value.get("listOrderStatus")),
        orders,
    })
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    if let Some(array) = value.as_array() {
        return array
            .iter()
            .map(|item| {
                let symbol = symbol_scope_from_fee_payload(exchange_id, requested_symbols, item)?;
                parse_fee_snapshot(exchange_id, symbol, item)
            })
            .collect();
    }
    if let Some(array) = value
        .get("data")
        .or_else(|| value.get("tradeFee"))
        .and_then(Value::as_array)
    {
        return array
            .iter()
            .map(|item| {
                let symbol = symbol_scope_from_fee_payload(exchange_id, requested_symbols, item)?;
                parse_fee_snapshot(exchange_id, symbol, item)
            })
            .collect();
    }
    let symbol = symbol_scope_from_fee_payload(exchange_id, requested_symbols, value)?;
    Ok(vec![parse_fee_snapshot(exchange_id, symbol, value)?])
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
                message: "binance recent fills request requires canonical_symbol".to_string(),
            })?;
    let fills = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "recent fills response is not an array",
            value,
        )
    })?;
    fills
        .iter()
        .map(|fill| {
            let is_buyer = fill
                .get("isBuyer")
                .or_else(|| fill.get("buyer"))
                .and_then(Value::as_bool)
                .unwrap_or_else(|| {
                    fill.get("side")
                        .and_then(Value::as_str)
                        .is_some_and(|side| side.eq_ignore_ascii_case("BUY"))
                });
            let is_maker = fill
                .get("isMaker")
                .or_else(|| fill.get("maker"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let price = decimal_value_to_f64(fill.get("price"))?.unwrap_or(0.0);
            let quantity = decimal_value_to_f64(fill.get("qty"))?.unwrap_or(0.0);
            let quote_quantity = decimal_value_to_f64(fill.get("quoteQty"))?;
            let fee_amount = decimal_value_to_f64(fill.get("commission"))?;
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("orderId")),
                client_order_id: value_as_string(fill.get("clientOrderId")),
                fill_id: value_as_string(fill.get("id")),
                side: if is_buyer {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                },
                position_side: parse_position_side(
                    fill.get("positionSide").and_then(Value::as_str),
                    None,
                    if is_buyer {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    },
                    symbol.market_type,
                ),
                status: FillStatus::Confirmed,
                liquidity_role: if is_maker {
                    LiquidityRole::Maker
                } else {
                    LiquidityRole::Taker
                },
                price,
                quantity,
                quote_quantity,
                fee_asset: value_as_string(fill.get("commissionAsset")),
                fee_amount,
                fee_rate: None,
                realized_pnl: decimal_value_to_f64(fill.get("realizedPnl"))?,
                filled_at: first_timestamp_millis(fill, &["time"]).unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "futures positionRisk response is not an array",
            value,
        )
    })?;
    let requested = requested
        .iter()
        .map(|symbol| super::parser::normalize_binance_symbol(&symbol.symbol))
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    let mut positions = Vec::new();
    for row in rows {
        let symbol_text = required_str(exchange_id, row, "symbol")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&symbol_text) {
            continue;
        }
        let signed_quantity =
            decimal_value_to_f64(row.get("positionAmt").or_else(|| row.get("positionAmount")))?
                .unwrap_or(0.0);
        if signed_quantity == 0.0 {
            continue;
        }
        let (base, quote) = compact_symbol_assets(&symbol_text).ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "futures position symbol could not be split",
                row,
            )
        })?;
        let side = parse_position_side(
            row.get("positionSide").and_then(Value::as_str),
            Some(signed_quantity),
            if signed_quantity < 0.0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            },
            MarketType::Perpetual,
        );
        positions.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: CanonicalSymbol::new(base, quote).map_err(validation_error)?,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol_text)
                    .map_err(validation_error)?,
            ),
            side,
            quantity: signed_quantity.abs(),
            entry_price: decimal_value_to_f64(row.get("entryPrice"))?,
            mark_price: decimal_value_to_f64(row.get("markPrice"))?,
            liquidation_price: decimal_value_to_f64(row.get("liquidationPrice"))?,
            unrealized_pnl: decimal_value_to_f64(
                row.get("unRealizedProfit")
                    .or_else(|| row.get("unrealizedProfit")),
            )?,
            leverage: decimal_value_to_f64(row.get("leverage"))?,
            observed_at: row
                .get("updateTime")
                .and_then(value_as_i64)
                .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
                .unwrap_or_else(Utc::now),
        });
    }
    Ok(positions)
}

fn symbol_scope_from_fee_payload(
    exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = requested_symbols.first() {
        return Ok(symbol.clone());
    }
    let exchange_symbol = required_str(exchange_id, value, "symbol")
        .or_else(|_| required_str(exchange_id, value, "s"))?
        .to_ascii_uppercase();
    let market_type = requested_symbols
        .first()
        .map(|symbol| symbol.market_type)
        .unwrap_or(MarketType::Spot);
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: None,
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, exchange_symbol)
            .map_err(validation_error)?,
    })
}

fn parse_fee_snapshot(
    exchange_id: &ExchangeId,
    symbol: SymbolScope,
    value: &Value,
) -> ExchangeApiResult<FeeRateSnapshot> {
    let maker = string_or_number(
        value
            .get("standardCommission")
            .and_then(|commission| commission.get("maker"))
            .or_else(|| value.get("makerCommission"))
            .or_else(|| value.get("makerCommissionRate"))
            .or_else(|| value.get("maker")),
    )
    .ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "fee response missing maker rate",
            value,
        )
    })?;
    let taker = string_or_number(
        value
            .get("standardCommission")
            .and_then(|commission| commission.get("taker"))
            .or_else(|| value.get("takerCommission"))
            .or_else(|| value.get("takerCommissionRate"))
            .or_else(|| value.get("taker")),
    )
    .ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "fee response missing taker rate",
            value,
        )
    })?;
    let source = if symbol.market_type == MarketType::Perpetual {
        "binance.futures_commission_rate"
    } else {
        "binance.account_commission"
    };
    Ok(FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol,
        maker_rate: maker,
        taker_rate: taker,
        source: Some(source.to_string()),
        updated_at: Utc::now(),
    })
}

fn symbol_scope_from_exchange_symbol(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    symbol: String,
) -> ExchangeApiResult<SymbolScope> {
    let canonical_symbol = compact_symbol_assets(&symbol)
        .and_then(|(base, quote)| CanonicalSymbol::new(base, quote).ok());
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol,
        exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, symbol)
            .map_err(validation_error)?,
    })
}

fn parse_position_side(
    value: Option<&str>,
    signed_quantity: Option<f64>,
    fallback_side: OrderSide,
    market_type: MarketType,
) -> PositionSide {
    if market_type == MarketType::Spot {
        return PositionSide::None;
    }
    match value.unwrap_or_default().to_ascii_uppercase().as_str() {
        "LONG" => PositionSide::Long,
        "SHORT" => PositionSide::Short,
        "BOTH" => match signed_quantity {
            Some(quantity) if quantity > 0.0 => PositionSide::Long,
            Some(quantity) if quantity < 0.0 => PositionSide::Short,
            _ => PositionSide::Net,
        },
        _ => match signed_quantity {
            Some(quantity) if quantity > 0.0 => PositionSide::Long,
            Some(quantity) if quantity < 0.0 => PositionSide::Short,
            _ => match fallback_side {
                OrderSide::Buy => PositionSide::Long,
                OrderSide::Sell => PositionSide::Short,
            },
        },
    }
}

fn map_binance_order_status(status: &str) -> OrderStatus {
    match status.trim().to_ascii_uppercase().as_str() {
        "NEW" | "PENDING_NEW" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "PENDING_CANCEL" => OrderStatus::PendingCancel,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" | "EXPIRED_IN_MATCH" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}
