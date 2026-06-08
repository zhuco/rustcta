use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderStatus, OrderType, SchemaVersion,
    TimeInForce,
};
use serde_json::Value;

use super::parser::{
    data_payload, decimal_as_f64, first_timestamp_millis, normalize_bydfi_symbol, parse_error,
    parse_position_side, parse_side, split_bydfi_symbol, string_or_number, validation_error,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let data = data_payload(value);
    let items = data
        .get("balances")
        .or_else(|| data.get("assets"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .unwrap_or_else(|| std::slice::from_ref(data));
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut balances = Vec::new();
    for item in items {
        let asset = item
            .get("asset")
            .or_else(|| item.get("coin"))
            .and_then(Value::as_str)
            .unwrap_or("USDT")
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_as_f64(
            item.get("availableBalance")
                .or_else(|| item.get("available"))
                .or_else(|| item.get("free")),
        )
        .unwrap_or(0.0);
        let locked = decimal_as_f64(
            item.get("frozen")
                .or_else(|| item.get("positionMargin"))
                .or_else(|| item.get("locked")),
        )
        .unwrap_or(0.0);
        let total = decimal_as_f64(item.get("balance").or_else(|| item.get("total")))
            .unwrap_or(available + locked);
        balances
            .push(AssetBalance::new(asset, total, available, locked).map_err(validation_error)?);
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

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangePosition>> {
    let data = data_payload(value);
    let positions = data
        .get("positions")
        .or_else(|| data.get("list"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .unwrap_or_else(|| std::slice::from_ref(data));
    let mut output = Vec::new();
    for position in positions {
        let symbol = normalize_bydfi_symbol(
            position
                .get("symbol")
                .or_else(|| position.get("s"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    parse_error(exchange_id.clone(), "position missing symbol", position)
                })?,
        )?;
        let (base, quote) = split_bydfi_symbol(&symbol)
            .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
        let quantity = decimal_as_f64(
            position
                .get("quantity")
                .or_else(|| position.get("volume"))
                .or_else(|| position.get("v")),
        )
        .unwrap_or(0.0)
        .abs();
        if quantity == 0.0 {
            continue;
        }
        output.push(ExchangePosition {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: CanonicalSymbol::new(base, quote).map_err(validation_error)?,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, symbol)
                    .map_err(validation_error)?,
            ),
            side: parse_position_side(
                position
                    .get("positionSide")
                    .or_else(|| position.get("side"))
                    .or_else(|| position.get("S"))
                    .and_then(Value::as_str),
            ),
            quantity,
            entry_price: decimal_as_f64(position.get("avgPrice").or_else(|| position.get("ap"))),
            mark_price: decimal_as_f64(position.get("markPrice")),
            liquidation_price: decimal_as_f64(
                position.get("liqPrice").or_else(|| position.get("lq")),
            ),
            unrealized_pnl: decimal_as_f64(position.get("unPnl")),
            leverage: decimal_as_f64(position.get("leverage").or_else(|| position.get("l"))),
            observed_at: Utc::now(),
        });
    }
    Ok(output)
}

pub fn parse_fee_snapshots(
    symbols: &[SymbolScope],
    exchange_info: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let rules = super::parser::parse_symbol_rules(
        symbols
            .first()
            .map(|symbol| &symbol.exchange)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "BYDFi get_fees requires at least one symbol".to_string(),
            })?,
        exchange_info,
    )?;
    Ok(symbols
        .iter()
        .map(|symbol| {
            let matched = rules
                .iter()
                .find(|rule| rule.symbol.exchange_symbol.symbol == symbol.exchange_symbol.symbol);
            FeeRateSnapshot {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: symbol.clone(),
                maker_rate: matched
                    .and_then(|rule| rule.raw_fee("maker"))
                    .unwrap_or_else(|| "0.0002".to_string()),
                taker_rate: matched
                    .and_then(|rule| rule.raw_fee("taker"))
                    .unwrap_or_else(|| "0.0006".to_string()),
                source: Some("bydfi.fapi.exchange_info".to_string()),
                updated_at: Utc::now(),
            }
        })
        .collect())
}

trait BydfiRuleFee {
    fn raw_fee(&self, side: &str) -> Option<String>;
}

impl BydfiRuleFee for rustcta_exchange_api::SymbolRules {
    fn raw_fee(&self, side: &str) -> Option<String> {
        match side {
            "maker" => Some("0.0002".to_string()),
            "taker" => Some("0.0006".to_string()),
            _ => None,
        }
    }
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let data = data_payload(value);
    let order = data
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(data);
    if order.is_null() {
        return Ok(None);
    }
    Ok(Some(parse_order_state(
        exchange_id,
        fallback_symbol,
        order,
    )?))
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let data = data_payload(value);
    let orders = data
        .get("orders")
        .or_else(|| data.get("list"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .ok_or_else(|| parse_error(exchange_id.clone(), "orders response missing list", value))?;
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, fallback_symbol, order))
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let data = data_payload(value);
    let fills = data
        .get("fills")
        .or_else(|| data.get("trades"))
        .or_else(|| data.get("list"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data.as_array().map(Vec::as_slice))
        .ok_or_else(|| parse_error(exchange_id.clone(), "fills response missing list", value))?;
    fills
        .iter()
        .map(|fill| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                fallback_symbol,
                fill,
            )
        })
        .collect()
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_from_payload(exchange_id, value))?;
    let raw_type = string_or_number(
        value
            .get("orderType")
            .or_else(|| value.get("type"))
            .or_else(|| value.get("t")),
    )
    .unwrap_or_else(|| "LIMIT".to_string());
    let order_type = parse_order_type(&raw_type);
    let time_in_force = value
        .get("timeInForce")
        .and_then(Value::as_str)
        .and_then(parse_time_in_force);
    let status = value
        .get("status")
        .or_else(|| value.get("st"))
        .and_then(|value| string_or_number(Some(value)))
        .map(|status| parse_order_status(&status))
        .unwrap_or(OrderStatus::Unknown);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(value.get("clientOrderId").or_else(|| value.get("cid"))),
        exchange_order_id: string_or_number(value.get("orderId").or_else(|| value.get("o"))),
        side: parse_side(
            exchange_id,
            value
                .get("side")
                .or_else(|| value.get("S"))
                .and_then(Value::as_str)
                .unwrap_or("BUY"),
        )?,
        position_side: Some(parse_position_side(
            value
                .get("positionSide")
                .or_else(|| value.get("ps"))
                .and_then(Value::as_str),
        )),
        order_type,
        time_in_force,
        status,
        quantity: string_or_number(
            value
                .get("quantity")
                .or_else(|| value.get("origQty"))
                .or_else(|| value.get("qty"))
                .or_else(|| value.get("v")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price").or_else(|| value.get("p"))),
        filled_quantity: string_or_number(
            value
                .get("dealQuantity")
                .or_else(|| value.get("executedQty"))
                .or_else(|| value.get("ev")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("avgPrice").or_else(|| value.get("ap"))),
        reduce_only: value
            .get("reduceOnly")
            .or_else(|| value.get("ro"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: matches!(time_in_force, Some(TimeInForce::GTX)),
        created_at: first_timestamp_millis(value, &["createTime", "ctime"]),
        updated_at: first_timestamp_millis(value, &["updateTime", "mtime", "E", "T"])
            .unwrap_or_else(Utc::now),
    })
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_from_payload(exchange_id, value))?;
    let price = decimal_as_f64(value.get("dealPrice").or_else(|| value.get("price")))
        .ok_or_else(|| parse_error(exchange_id.clone(), "fill missing price", value))?;
    let quantity = decimal_as_f64(
        value
            .get("dealQuantity")
            .or_else(|| value.get("executedQty")),
    )
    .ok_or_else(|| parse_error(exchange_id.clone(), "fill missing quantity", value))?;
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: symbol.canonical_symbol.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "BYDFi fill requires canonical symbol".to_string(),
            }
        })?,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("orderId")),
        client_order_id: string_or_number(value.get("clientOrderId")),
        fill_id: string_or_number(value.get("tradeId").or_else(|| value.get("id"))),
        side: parse_side(
            exchange_id,
            value.get("side").and_then(Value::as_str).unwrap_or("BUY"),
        )?,
        position_side: parse_position_side(value.get("positionSide").and_then(Value::as_str)),
        status: FillStatus::Confirmed,
        liquidity_role: LiquidityRole::Unknown,
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: Some("USDT".to_string()),
        fee_amount: decimal_as_f64(value.get("fee")),
        fee_rate: None,
        realized_pnl: decimal_as_f64(value.get("tradePnl")),
        filled_at: first_timestamp_millis(value, &["time", "createTime"]).unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

fn symbol_from_payload(exchange_id: &ExchangeId, value: &Value) -> ExchangeApiResult<SymbolScope> {
    let exchange_symbol = normalize_bydfi_symbol(
        value
            .get("symbol")
            .or_else(|| value.get("s"))
            .and_then(Value::as_str)
            .ok_or_else(|| parse_error(exchange_id.clone(), "payload missing symbol", value))?,
    )?;
    let (base, quote) = split_bydfi_symbol(&exchange_symbol)
        .unwrap_or_else(|| ("UNKNOWN".to_string(), "USDT".to_string()));
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

fn parse_order_type(raw: &str) -> OrderType {
    match raw.to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        "POST_ONLY" | "LIMIT_MAKER" => OrderType::PostOnly,
        "STOP" => OrderType::StopLimit,
        "STOP_MARKET" | "TAKE_PROFIT_MARKET" | "TRAILING_STOP_MARKET" => OrderType::StopMarket,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(raw: &str) -> Option<TimeInForce> {
    match raw.to_ascii_uppercase().as_str() {
        "GTC" => Some(TimeInForce::GTC),
        "IOC" => Some(TimeInForce::IOC),
        "FOK" => Some(TimeInForce::FOK),
        "POST_ONLY" | "GTX" => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn parse_order_status(raw: &str) -> OrderStatus {
    match raw.to_ascii_uppercase().as_str() {
        "NEW" => OrderStatus::New,
        "OPEN" => OrderStatus::Open,
        "PARTIALLY_FILLED" | "PART_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "PENDING_CANCEL" => OrderStatus::PendingCancel,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}
