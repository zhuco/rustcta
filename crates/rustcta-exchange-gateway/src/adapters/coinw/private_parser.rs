use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    normalize_coinw_perp_symbol, normalize_coinw_spot_symbol, parse_error, validation_error,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let data = data_payload(value);
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    match (market_type, data) {
        (MarketType::Perpetual, Value::Object(map))
            if map.contains_key("availableUsdt") || map.contains_key("availableMargin") =>
        {
            let asset = value_as_string(data.get("quote"))
                .unwrap_or_else(|| "USDT".to_string())
                .to_ascii_uppercase();
            if requested.is_empty() || requested.contains(&asset) {
                let available = decimal_as_f64(
                    data.get("availableUsdt")
                        .or_else(|| data.get("availableMargin"))
                        .or_else(|| data.get("value")),
                )
                .unwrap_or(0.0);
                let locked = decimal_as_f64(data.get("alFreeze").or_else(|| data.get("frozen")))
                    .unwrap_or(0.0);
                let total = decimal_as_f64(
                    data.get("equity")
                        .or_else(|| data.get("balance"))
                        .or_else(|| data.get("alMargin")),
                )
                .unwrap_or(available + locked);
                balances.push(
                    AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
                );
            }
        }
        (_, Value::Object(map)) => {
            for (asset, item) in map {
                let asset = asset.to_ascii_uppercase();
                if !requested.is_empty() && !requested.contains(&asset) {
                    continue;
                }
                let available = decimal_as_f64(
                    item.get("available")
                        .or_else(|| item.get("free"))
                        .or_else(|| item.get("normal")),
                )
                .or_else(|| decimal_as_f64(Some(item)))
                .unwrap_or(0.0);
                let locked = decimal_as_f64(
                    item.get("locked")
                        .or_else(|| item.get("frozen"))
                        .or_else(|| item.get("onOrders")),
                )
                .unwrap_or(0.0);
                let total = decimal_as_f64(
                    item.get("total")
                        .or_else(|| item.get("balance"))
                        .or_else(|| item.get("all")),
                )
                .unwrap_or(available + locked);
                balances.push(
                    AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
                );
            }
        }
        (_, Value::Array(items)) => {
            for item in items {
                let asset = value_as_string(
                    item.get("currency")
                        .or_else(|| item.get("asset"))
                        .or_else(|| item.get("coin"))
                        .or_else(|| item.get("token")),
                )
                .unwrap_or_else(|| "USDT".to_string())
                .to_ascii_uppercase();
                if !requested.is_empty() && !requested.contains(&asset) {
                    continue;
                }
                let available = decimal_as_f64(
                    item.get("available")
                        .or_else(|| item.get("free"))
                        .or_else(|| item.get("normal")),
                )
                .unwrap_or(0.0);
                let locked = decimal_as_f64(item.get("locked").or_else(|| item.get("frozen")))
                    .unwrap_or(0.0);
                let total = decimal_as_f64(
                    item.get("total")
                        .or_else(|| item.get("balance"))
                        .or_else(|| item.get("equity")),
                )
                .unwrap_or(available + locked);
                balances.push(
                    AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
                );
            }
        }
        _ => {}
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
    let requested = requested_symbols
        .iter()
        .map(|symbol| normalize_coinw_perp_symbol(&symbol.symbol))
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    data_rows(data_payload(value))
        .iter()
        .filter_map(|item| {
            let symbol = symbol_from_futures_payload(exchange_id, item).ok()?;
            if !requested.is_empty() && !requested.contains(&symbol.exchange_symbol.symbol) {
                return None;
            }
            let quantity = decimal_as_f64(
                item.get("currentPiece")
                    .or_else(|| item.get("totalPiece"))
                    .or_else(|| item.get("baseSize"))
                    .or_else(|| item.get("quantity")),
            )
            .unwrap_or(0.0)
            .abs();
            if quantity == 0.0 {
                return None;
            }
            let canonical_symbol = symbol.canonical_symbol.clone()?;
            Some(Ok(ExchangePosition {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Perpetual,
                canonical_symbol,
                exchange_symbol: Some(symbol.exchange_symbol),
                side: parse_position_side(item.get("direction").and_then(Value::as_str)),
                quantity,
                entry_price: decimal_as_f64(
                    item.get("openPrice")
                        .or_else(|| item.get("orderPrice"))
                        .or_else(|| item.get("avgPrice")),
                ),
                mark_price: decimal_as_f64(
                    item.get("markPrice").or_else(|| item.get("indexPrice")),
                ),
                liquidation_price: decimal_as_f64(item.get("liquidationPrice")),
                unrealized_pnl: decimal_as_f64(
                    item.get("profit")
                        .or_else(|| item.get("unrealizedPnl"))
                        .or_else(|| item.get("floatingProfitLoss")),
                ),
                leverage: decimal_as_f64(item.get("leverage")),
                observed_at: Utc::now(),
            }))
        })
        .collect()
}

pub fn parse_fee_snapshots(
    _exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: Option<&Value>,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let (maker, taker, source) = if let Some(value) = value {
        let data = data_payload(value);
        let item = data_rows(data)
            .into_iter()
            .find(|item| symbol_matches_payload(symbol, item).unwrap_or(false));
        (
            item.and_then(|item| {
                string_or_number(item.get("makerFee").or_else(|| item.get("makerFeeRate")))
            })
            .unwrap_or_else(|| default_maker_fee(symbol.market_type).to_string()),
            item.and_then(|item| {
                string_or_number(item.get("takerFee").or_else(|| item.get("takerFeeRate")))
            })
            .unwrap_or_else(|| default_taker_fee(symbol.market_type).to_string()),
            Some("coinw.symbol_metadata".to_string()),
        )
    } else {
        (
            default_maker_fee(symbol.market_type).to_string(),
            default_taker_fee(symbol.market_type).to_string(),
            Some("coinw.default_fee".to_string()),
        )
    };
    Ok(vec![FeeRateSnapshot {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        symbol: symbol.clone(),
        maker_rate: maker,
        taker_rate: taker,
        source,
        updated_at: Utc::now(),
    }])
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let data = data_payload(value);
    if data.is_null() {
        return Ok(None);
    }
    let item = data
        .get("rows")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .or_else(|| data.as_array().and_then(|rows| rows.first()))
        .unwrap_or(data);
    Ok(Some(parse_order_state(
        exchange_id,
        fallback_symbol,
        market_type,
        item,
    )?))
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    data_rows(data_payload(value))
        .iter()
        .map(|order| parse_order_state(exchange_id, fallback_symbol, market_type, order))
        .collect()
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| match market_type {
            MarketType::Spot => symbol_from_spot_payload(exchange_id, value),
            MarketType::Perpetual => symbol_from_futures_payload(exchange_id, value),
            _ => Err(ExchangeApiError::Unsupported {
                operation: "coinw.order_state_unsupported_market",
            }),
        })?;
    let raw_type = value_as_string(
        value
            .get("orderType")
            .or_else(|| value.get("originalType"))
            .or_else(|| value.get("posType"))
            .or_else(|| value.get("positionType")),
    )
    .unwrap_or_else(|| {
        if market_type == MarketType::Spot {
            if value
                .get("isMarket")
                .and_then(Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case("true"))
            {
                "market".to_string()
            } else {
                "limit".to_string()
            }
        } else {
            "plan".to_string()
        }
    });
    let side = if market_type == MarketType::Spot {
        parse_spot_side(value_as_string(value.get("type").or_else(|| value.get("side"))).as_deref())
    } else {
        parse_futures_side(value.get("direction").and_then(Value::as_str))
    };
    let status = if market_type == MarketType::Spot {
        parse_spot_order_status(value.get("status"))
    } else {
        parse_futures_order_status(
            value_as_string(value.get("orderStatus").or_else(|| value.get("status"))).as_deref(),
        )
    };
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value_as_string(
            value
                .get("out_trade_no")
                .or_else(|| value.get("thirdOrderId"))
                .or_else(|| value.get("clientOrderId")),
        ),
        exchange_order_id: value_as_string(
            value
                .get("orderNumber")
                .or_else(|| value.get("id"))
                .or_else(|| value.get("openId"))
                .or_else(|| value.get("sourceId"))
                .or_else(|| value.get("value")),
        ),
        side,
        position_side: (market_type == MarketType::Perpetual)
            .then(|| parse_position_side(value.get("direction").and_then(Value::as_str))),
        order_type: parse_order_type(&raw_type),
        time_in_force: parse_time_in_force(&raw_type),
        status,
        quantity: string_or_number(
            value
                .get("total")
                .or_else(|| value.get("quantity"))
                .or_else(|| value.get("totalPiece"))
                .or_else(|| value.get("baseSize")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(
            value
                .get("prize")
                .or_else(|| value.get("price"))
                .or_else(|| value.get("orderPrice"))
                .or_else(|| value.get("openPrice")),
        )
        .filter(|value| !is_zero_decimal(value)),
        filled_quantity: string_or_number(
            value
                .get("success_count")
                .or_else(|| value.get("dealQuantity"))
                .or_else(|| value.get("dealSize"))
                .or_else(|| value.get("filledQuantity")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(
            value
                .get("avgPrice")
                .or_else(|| value.get("dealPrice"))
                .or_else(|| value.get("averagePrice")),
        )
        .filter(|value| !is_zero_decimal(value)),
        reduce_only: false,
        post_only: raw_type.eq_ignore_ascii_case("PostOnly"),
        created_at: first_timestamp(value, &["date", "createdDate", "ctime", "ts"]),
        updated_at: first_timestamp(value, &["updatedDate", "mtime", "date", "ts"])
            .unwrap_or_else(Utc::now),
    })
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    data_rows(data_payload(value))
        .iter()
        .map(|fill| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                fallback_symbol,
                market_type,
                fill,
            )
        })
        .collect()
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| match market_type {
            MarketType::Spot => symbol_from_spot_payload(exchange_id, value),
            MarketType::Perpetual => symbol_from_futures_payload(exchange_id, value),
            _ => Err(ExchangeApiError::Unsupported {
                operation: "coinw.fill_unsupported_market",
            }),
        })?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinw fill requires canonical_symbol".to_string(),
            })?;
    let price = decimal_as_f64(
        value
            .get("rate")
            .or_else(|| value.get("price"))
            .or_else(|| value.get("dealPrice"))
            .or_else(|| value.get("orderPrice")),
    )
    .unwrap_or(0.0);
    let quantity = decimal_as_f64(
        value
            .get("amount")
            .or_else(|| value.get("quantity"))
            .or_else(|| value.get("dealQuantity"))
            .or_else(|| value.get("baseSize")),
    )
    .unwrap_or(0.0);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: value_as_string(
            value
                .get("orderNumber")
                .or_else(|| value.get("orderId"))
                .or_else(|| value.get("openId"))
                .or_else(|| value.get("sourceId")),
        ),
        client_order_id: value_as_string(
            value
                .get("out_trade_no")
                .or_else(|| value.get("thirdOrderId"))
                .or_else(|| value.get("clientOrderId")),
        ),
        fill_id: value_as_string(
            value
                .get("id")
                .or_else(|| value.get("tradeID"))
                .or_else(|| value.get("tradeId"))
                .or_else(|| value.get("primaryId")),
        ),
        side: if market_type == MarketType::Spot {
            parse_spot_side(
                value_as_string(value.get("type").or_else(|| value.get("side"))).as_deref(),
            )
        } else {
            parse_futures_side(value.get("direction").and_then(Value::as_str))
        },
        position_side: if market_type == MarketType::Perpetual {
            parse_position_side(value.get("direction").and_then(Value::as_str))
        } else {
            PositionSide::None
        },
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity(value),
        price,
        quantity,
        quote_quantity: decimal_as_f64(
            value
                .get("total")
                .or_else(|| value.get("startingAmount"))
                .or_else(|| value.get("success_amount"))
                .or_else(|| value.get("quoteQuantity")),
        )
        .or_else(|| (price > 0.0 && quantity > 0.0).then_some(price * quantity)),
        fee_asset: value_as_string(value.get("feeCoin").or_else(|| value.get("feeAsset"))),
        fee_amount: decimal_as_f64(value.get("fee").or_else(|| value.get("tradeFee"))),
        fee_rate: decimal_as_f64(value.get("feeRate")),
        realized_pnl: decimal_as_f64(value.get("profit").or_else(|| value.get("realizedPnl"))),
        filled_at: first_timestamp(value, &["date", "createdDate", "dealTime", "time"])
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

pub fn order_state_from_spot_place_ack(
    exchange_id: &ExchangeId,
    request: &rustcta_exchange_api::PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    let data = data_payload(value);
    order_state_from_request(
        exchange_id,
        &request.symbol,
        request.client_order_id.clone(),
        value_as_string(data.get("orderNumber")),
        request.side,
        request.position_side,
        request.order_type,
        request.time_in_force,
        request.quantity.clone(),
        request.price.clone(),
        request.reduce_only,
        request.post_only,
        OrderStatus::New,
    )
}

pub fn order_state_from_futures_place_ack(
    exchange_id: &ExchangeId,
    request: &rustcta_exchange_api::PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    let data = data_payload(value);
    order_state_from_request(
        exchange_id,
        &request.symbol,
        request.client_order_id.clone(),
        value_as_string(
            data.get("value")
                .or_else(|| data.get("openId"))
                .or_else(|| data.get("id")),
        ),
        request.side,
        request.position_side,
        request.order_type,
        request.time_in_force,
        request.quantity.clone(),
        request.price.clone(),
        request.reduce_only,
        request.post_only,
        OrderStatus::New,
    )
}

pub fn order_state_from_quote_ack(
    exchange_id: &ExchangeId,
    request: &rustcta_exchange_api::QuoteMarketOrderRequest,
    value: &Value,
) -> OrderState {
    let data = data_payload(value);
    order_state_from_request(
        exchange_id,
        &request.symbol,
        request.client_order_id.clone(),
        value_as_string(data.get("orderNumber")),
        request.side,
        None,
        OrderType::Market,
        None,
        "0".to_string(),
        None,
        false,
        false,
        OrderStatus::New,
    )
}

pub fn order_state_from_cancel_ack(
    exchange_id: &ExchangeId,
    request: &rustcta_exchange_api::CancelOrderRequest,
) -> OrderState {
    order_state_from_request(
        exchange_id,
        &request.symbol,
        request.client_order_id.clone(),
        request.exchange_order_id.clone(),
        OrderSide::Buy,
        None,
        OrderType::Limit,
        None,
        "0".to_string(),
        None,
        false,
        false,
        OrderStatus::Cancelled,
    )
}

fn order_state_from_request(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
    side: OrderSide,
    position_side: Option<PositionSide>,
    order_type: OrderType,
    time_in_force: Option<TimeInForce>,
    quantity: String,
    price: Option<String>,
    reduce_only: bool,
    post_only: bool,
    status: OrderStatus,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
        side,
        position_side,
        order_type,
        time_in_force,
        status,
        quantity,
        price,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only,
        post_only,
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

fn symbol_from_spot_payload(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let symbol = value_as_string(
        value
            .get("currencyPair")
            .or_else(|| value.get("symbol"))
            .or_else(|| value.get("pair")),
    )
    .ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "CoinW spot payload missing symbol",
            value,
        )
    })?;
    let exchange_symbol = normalize_coinw_spot_symbol(&symbol)?;
    let (base, quote) = split_coinw_symbol(&exchange_symbol);
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            exchange_symbol,
        )
        .map_err(validation_error)?,
    })
}

fn symbol_from_futures_payload(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    let symbol = value_as_string(
        value
            .get("instrument")
            .or_else(|| value.get("base"))
            .or_else(|| value.get("name"))
            .or_else(|| value.get("symbol")),
    )
    .ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "CoinW futures payload missing instrument",
            value,
        )
    })?;
    let exchange_symbol = normalize_coinw_perp_symbol(&symbol)?;
    let (base, quote) = split_coinw_symbol(&exchange_symbol);
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            exchange_symbol,
        )
        .map_err(validation_error)?,
    })
}

fn split_coinw_symbol(symbol: &str) -> (String, String) {
    if let Some((base, quote)) = symbol.split_once('_') {
        return (base.to_ascii_uppercase(), quote.to_ascii_uppercase());
    }
    for quote in ["USDT", "USDC", "USD", "BTC", "ETH"] {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return (base.to_ascii_uppercase(), quote.to_string());
            }
        }
    }
    (symbol.to_ascii_uppercase(), "USDT".to_string())
}

fn data_payload(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn data_rows(value: &Value) -> Vec<&Value> {
    if let Some(rows) = value.get("rows").and_then(Value::as_array) {
        return rows.iter().collect();
    }
    if let Some(rows) = value.get("list").and_then(Value::as_array) {
        return rows.iter().collect();
    }
    if let Some(rows) = value.as_array() {
        return rows.iter().collect();
    }
    if value.is_null() {
        Vec::new()
    } else {
        vec![value]
    }
}

fn symbol_matches_payload(symbol: &SymbolScope, value: &Value) -> ExchangeApiResult<bool> {
    let payload_symbol = if symbol.market_type == MarketType::Spot {
        symbol_from_spot_payload(&symbol.exchange, value)?
    } else {
        symbol_from_futures_payload(&symbol.exchange, value)?
    };
    Ok(payload_symbol.exchange_symbol.symbol == symbol.exchange_symbol.symbol)
}

pub(super) fn decimal_as_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn string_or_number(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    })
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    string_or_number(value)
}

fn parse_spot_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "1" | "sell" | "ask" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_futures_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "short" | "sell" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_position_side(value: Option<&str>) -> PositionSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "long" => PositionSide::Long,
        "short" => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn parse_spot_order_status(value: Option<&Value>) -> OrderStatus {
    match value_as_string(value).unwrap_or_default().as_str() {
        "1" => OrderStatus::New,
        "2" => OrderStatus::PartiallyFilled,
        "3" => OrderStatus::Filled,
        "4" => OrderStatus::Cancelled,
        _ => OrderStatus::Unknown,
    }
}

fn parse_futures_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "unfinish" | "open" => OrderStatus::New,
        "part" => OrderStatus::PartiallyFilled,
        "finish" | "close" | "filled" => OrderStatus::Filled,
        "cancel" | "cancelled" | "canceled" => OrderStatus::Cancelled,
        _ => OrderStatus::Unknown,
    }
}

fn parse_order_type(value: &str) -> OrderType {
    match value.to_ascii_lowercase().as_str() {
        "execute" | "market" => OrderType::Market,
        "ioc" => OrderType::IOC,
        "fok" => OrderType::FOK,
        "postonly" | "post_only" => OrderType::PostOnly,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: &str) -> Option<TimeInForce> {
    match value.to_ascii_lowercase().as_str() {
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        "postonly" | "post_only" => Some(TimeInForce::GTX),
        _ => Some(TimeInForce::GTC),
    }
}

fn parse_liquidity(value: &Value) -> LiquidityRole {
    match value
        .get("role")
        .or_else(|| value.get("liquidity"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "maker" => LiquidityRole::Maker,
        "taker" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn first_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(|value| timestamp_from_value(value).or_else(|| datetime_from_value(value)))
    })
}

fn timestamp_from_value(value: &Value) -> Option<DateTime<Utc>> {
    let raw = match value {
        Value::Number(number) => number.as_i64()?,
        Value::String(text) => text.parse().ok()?,
        _ => return None,
    };
    let millis = if raw > 10_000_000_000 {
        raw
    } else {
        raw * 1000
    };
    Utc.timestamp_millis_opt(millis).single()
}

fn datetime_from_value(value: &Value) -> Option<DateTime<Utc>> {
    let text = value.as_str()?;
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .or_else(|| {
            NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|timestamp| Utc.from_utc_datetime(&timestamp))
        })
}

fn default_maker_fee(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Perpetual => "0.0001",
        _ => "0.001",
    }
}

fn default_taker_fee(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Perpetual => "0.0006",
        _ => "0.001",
    }
}

fn is_zero_decimal(value: &str) -> bool {
    value.trim().trim_matches('0').trim_matches('.').is_empty()
}
