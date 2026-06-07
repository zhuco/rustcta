use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, Position,
    SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill, FillStatus, LiquidityRole,
    MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    decimal_text_to_f64, number_from_value, parse_error, parse_rfc3339, required_str,
    string_or_number, validation_error, value_as_string,
};

pub fn parse_account_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let accounts = value
        .get("accounts")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "accounts response missing accounts array",
                value,
            )
        })?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut asset_balances = Vec::new();
    for account in accounts {
        let asset = required_str(exchange_id, account, "currency")?.to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = balance_value(account.get("available_balance")).unwrap_or(0.0);
        let locked = balance_value(account.get("hold")).unwrap_or(0.0);
        let total = available + locked;
        if total > 0.0 || !requested.is_empty() {
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

pub fn parse_intx_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let portfolio_balances = value
        .get("portfolio_balances")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "intx balances response missing portfolio_balances array",
                value,
            )
        })?;
    let requested = assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let mut asset_balances = Vec::new();
    for portfolio in portfolio_balances {
        let balances = portfolio
            .get("balances")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "intx portfolio balance missing balances array",
                    portfolio,
                )
            })?;
        for balance in balances {
            let asset = intx_asset_code(exchange_id, balance)?.to_ascii_uppercase();
            if !requested.is_empty() && !requested.contains(&asset) {
                continue;
            }
            let total = decimal_option(balance.get("quantity")).unwrap_or(0.0);
            let hold = decimal_option(balance.get("hold")).unwrap_or(0.0);
            let transfer_hold = decimal_option(balance.get("transfer_hold")).unwrap_or(0.0);
            let pledged = decimal_option(balance.get("pledged_quantity")).unwrap_or(0.0);
            let max_withdraw = decimal_option(balance.get("max_withdraw_amount"));
            let locked = hold + transfer_hold + pledged;
            let available = max_withdraw.unwrap_or_else(|| (total - locked).max(0.0));
            if total > 0.0 || locked > 0.0 || !requested.is_empty() {
                asset_balances.push(
                    AssetBalance::new(asset, total, available, (total - available).max(locked))
                        .map_err(validation_error)?,
                );
            }
        }
    }
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        balances: asset_balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_fee_snapshots(
    _exchange_id: &ExchangeId,
    requested_symbols: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let fee_tier = value.get("fee_tier").unwrap_or(value);
    let maker_rate =
        string_or_number(fee_tier.get("maker_fee_rate")).unwrap_or_else(|| "0".to_string());
    let taker_rate =
        string_or_number(fee_tier.get("taker_fee_rate")).unwrap_or_else(|| "0".to_string());
    Ok(requested_symbols
        .iter()
        .cloned()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol,
            maker_rate: maker_rate.clone(),
            taker_rate: taker_rate.clone(),
            source: Some("coinbase.transaction_summary.fee_tier".to_string()),
            updated_at: Utc::now(),
        })
        .collect())
}

pub fn parse_positions(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    market_type: MarketType,
    requested_symbols: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<Position>> {
    let positions = value
        .get("positions")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "positions response missing array",
                value,
            )
        })?;
    let requested = requested_symbols
        .iter()
        .map(|symbol| symbol.symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let mut parsed = Vec::new();
    for position in positions {
        let product_id = position_product_id(exchange_id, position)?;
        if !requested.is_empty() && !requested.contains(&product_id) {
            continue;
        }
        let (base_asset, quote_asset) = split_position_symbol(&product_id);
        let canonical_symbol = rustcta_types::CanonicalSymbol::new(base_asset, quote_asset)
            .map_err(validation_error)?;
        let raw_quantity = string_or_number(position.get("net_size"))
            .or_else(|| string_or_number(position.get("number_of_contracts")))
            .unwrap_or_else(|| "0".to_string());
        let quantity = decimal_text_to_f64(&raw_quantity)?.abs();
        if quantity == 0.0 && requested.is_empty() {
            continue;
        }
        parsed.push(Position {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type,
            canonical_symbol,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), market_type, product_id)
                    .map_err(validation_error)?,
            ),
            side: parse_position_side(
                position
                    .get("position_side")
                    .or_else(|| position.get("side"))
                    .and_then(Value::as_str),
                &raw_quantity,
            ),
            quantity,
            entry_price: money_value(position.get("entry_vwap"))
                .or_else(|| decimal_option(position.get("avg_entry_price"))),
            mark_price: money_value(position.get("mark_price"))
                .or_else(|| decimal_option(position.get("current_price"))),
            liquidation_price: money_value(position.get("liquidation_price")),
            unrealized_pnl: money_value(position.get("unrealized_pnl"))
                .or_else(|| decimal_option(position.get("unrealized_pnl"))),
            leverage: decimal_option(position.get("leverage")),
            observed_at: Utc::now(),
        });
    }
    Ok(parsed)
}

pub fn parse_order_ack(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    request_side: OrderSide,
    request_order_type: OrderType,
    request_quantity: String,
    request_price: Option<String>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let success = value
        .get("success_response")
        .ok_or_else(|| parse_error(exchange_id.clone(), "create order missing success", value))?;
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: value_as_string(success.get("client_order_id")),
        exchange_order_id: value_as_string(success.get("order_id")),
        side: parse_side(success.get("side").and_then(Value::as_str).unwrap_or(
            match request_side {
                OrderSide::Buy => "BUY",
                OrderSide::Sell => "SELL",
            },
        ))?,
        position_side: Some(PositionSide::None),
        order_type: request_order_type,
        time_in_force: None,
        status: OrderStatus::New,
        quantity: request_quantity,
        price: request_price,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: request_order_type == OrderType::PostOnly,
        created_at: Some(now),
        updated_at: now,
    })
}

pub fn parse_cancel_order(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    exchange_order_id: &str,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let result = value
        .get("results")
        .and_then(Value::as_array)
        .and_then(|results| results.first())
        .ok_or_else(|| parse_error(exchange_id.clone(), "cancel response missing result", value))?;
    if result.get("success").and_then(Value::as_bool) == Some(false) {
        return Err(parse_error(
            exchange_id.clone(),
            "coinbase cancel was rejected",
            result,
        ));
    }
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: None,
        exchange_order_id: value_as_string(result.get("order_id"))
            .or_else(|| Some(exchange_order_id.to_string())),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: None,
        status: OrderStatus::PendingCancel,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: now,
    })
}

pub fn parse_batch_cancel_orders(
    exchange_id: &ExchangeId,
    cancels: &[(String, SymbolScope)],
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let results = value
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "cancel response missing results",
                value,
            )
        })?;
    let mut orders = Vec::with_capacity(results.len());
    for (index, result) in results.iter().enumerate() {
        if result.get("success").and_then(Value::as_bool) == Some(false) {
            return Err(parse_error(
                exchange_id.clone(),
                "coinbase batch cancel was rejected",
                result,
            ));
        }
        let exchange_order_id = value_as_string(result.get("order_id"))
            .or_else(|| cancels.get(index).map(|(order_id, _)| order_id.to_string()))
            .ok_or_else(|| {
                parse_error(
                    exchange_id.clone(),
                    "cancel result missing order_id",
                    result,
                )
            })?;
        let symbol = cancels
            .iter()
            .find(|(order_id, _)| order_id == &exchange_order_id)
            .or_else(|| cancels.get(index))
            .map(|(_, symbol)| symbol)
            .ok_or_else(|| {
                parse_error(exchange_id.clone(), "cancel result missing symbol", result)
            })?;
        orders.push(cancel_order_state(exchange_id, symbol, &exchange_order_id));
    }
    Ok(orders)
}

fn cancel_order_state(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    exchange_order_id: &str,
) -> OrderState {
    let now = Utc::now();
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: None,
        exchange_order_id: Some(exchange_order_id.to_string()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: None,
        status: OrderStatus::PendingCancel,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: now,
    }
}

pub fn parse_amend_order_ack(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    exchange_order_id: &str,
    new_quantity: String,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    if value.get("success").and_then(Value::as_bool) == Some(false) {
        return Err(parse_error(
            exchange_id.clone(),
            "coinbase amend_order was rejected",
            value,
        ));
    }
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: None,
        exchange_order_id: Some(exchange_order_id.to_string()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: OrderStatus::Open,
        quantity: new_quantity,
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: now,
    })
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let order = value.get("order").unwrap_or(value);
    let product_id = required_str(exchange_id, order, "product_id")?.to_ascii_uppercase();
    let market_type = symbol_hint
        .map(|symbol| symbol.market_type)
        .unwrap_or_else(|| infer_market_type_from_product(&product_id));
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(exchange_id.clone(), market_type, product_id)
            .map_err(validation_error)?
    };
    let config = order.get("order_configuration");
    let (order_type, price, quantity, tif, post_only) = parse_order_config(config, order);
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: symbol_hint.and_then(|symbol| symbol.canonical_symbol.clone()),
        exchange_symbol,
        client_order_id: value_as_string(order.get("client_order_id")),
        exchange_order_id: value_as_string(order.get("order_id")),
        side: parse_side(required_str(exchange_id, order, "side")?)?,
        position_side: Some(PositionSide::None),
        order_type,
        time_in_force: tif,
        status: order
            .get("status")
            .and_then(Value::as_str)
            .map(map_coinbase_order_status)
            .unwrap_or(OrderStatus::Unknown),
        quantity,
        price,
        filled_quantity: string_or_number(order.get("filled_size")).unwrap_or_else(|| "0".into()),
        average_fill_price: value_as_string(order.get("average_filled_price")),
        reduce_only: false,
        post_only,
        created_at: parse_rfc3339(order.get("created_time")),
        updated_at: parse_rfc3339(order.get("last_update_time")).unwrap_or(now),
    })
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let orders = value
        .get("orders")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "orders response missing orders", value))?;
    orders
        .iter()
        .map(|order| parse_order_state(exchange_id, symbol_hint, order))
        .collect()
}

pub fn parse_recent_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let fills = value
        .get("fills")
        .and_then(Value::as_array)
        .ok_or_else(|| parse_error(exchange_id.clone(), "fills response missing fills", value))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinbase fills request requires canonical_symbol".to_string(),
            })?;
    fills
        .iter()
        .map(|fill| {
            let price = string_or_number(fill.get("price"))
                .as_deref()
                .map(decimal_text_to_f64)
                .transpose()?
                .unwrap_or(0.0);
            let quantity = string_or_number(fill.get("size").or_else(|| fill.get("base_size")))
                .as_deref()
                .map(decimal_text_to_f64)
                .transpose()?
                .unwrap_or(0.0);
            let quote_quantity = string_or_number(fill.get("trade_value"))
                .as_deref()
                .map(decimal_text_to_f64)
                .transpose()?;
            let fee_amount = string_or_number(fill.get("commission"))
                .or_else(|| string_or_number(fill.get("fee")))
                .as_deref()
                .map(decimal_text_to_f64)
                .transpose()?;
            let received_at = Utc::now();
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: symbol.market_type,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: value_as_string(fill.get("order_id")),
                client_order_id: None,
                fill_id: value_as_string(fill.get("trade_id").or_else(|| fill.get("entry_id"))),
                side: parse_side(fill.get("side").and_then(Value::as_str).unwrap_or("BUY"))?,
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: fill
                    .get("liquidity_indicator")
                    .or_else(|| fill.get("liquidity"))
                    .and_then(Value::as_str)
                    .map(parse_liquidity_role)
                    .unwrap_or(LiquidityRole::Unknown),
                price,
                quantity,
                quote_quantity,
                fee_asset: string_or_number(fill.get("commission_asset"))
                    .or_else(|| Some(symbol.exchange_symbol.symbol.clone())),
                fee_amount,
                fee_rate: None,
                realized_pnl: None,
                filled_at: parse_rfc3339(fill.get("trade_time").or_else(|| fill.get("time")))
                    .unwrap_or(received_at),
                received_at,
            })
        })
        .collect()
}

fn parse_order_config(
    config: Option<&Value>,
    order: &Value,
) -> (OrderType, Option<String>, String, Option<TimeInForce>, bool) {
    let Some(config) = config else {
        return (
            map_order_type(order.get("order_type").and_then(Value::as_str)),
            value_as_string(order.get("price")),
            string_or_number(order.get("size")).unwrap_or_else(|| "0".to_string()),
            None,
            false,
        );
    };
    for (key, order_type, tif) in [
        (
            "market_market_ioc",
            OrderType::Market,
            Some(TimeInForce::IOC),
        ),
        (
            "market_market_fok",
            OrderType::Market,
            Some(TimeInForce::FOK),
        ),
        ("sor_limit_ioc", OrderType::IOC, Some(TimeInForce::IOC)),
        ("limit_limit_fok", OrderType::FOK, Some(TimeInForce::FOK)),
        ("limit_limit_gtc", OrderType::Limit, Some(TimeInForce::GTC)),
    ] {
        if let Some(payload) = config.get(key) {
            let price = value_as_string(payload.get("limit_price"));
            let quantity = string_or_number(payload.get("base_size"))
                .or_else(|| string_or_number(payload.get("quote_size")))
                .unwrap_or_else(|| "0".to_string());
            let post_only = payload
                .get("post_only")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let order_type = if post_only {
                OrderType::PostOnly
            } else {
                order_type
            };
            return (order_type, price, quantity, tif, post_only);
        }
    }
    (OrderType::Limit, None, "0".to_string(), None, false)
}

fn balance_value(value: Option<&Value>) -> Option<f64> {
    value
        .and_then(|value| value.get("value").or(Some(value)))
        .and_then(number_from_value)
}

fn money_value(value: Option<&Value>) -> Option<f64> {
    value
        .and_then(|value| value.get("value").or(Some(value)))
        .and_then(number_from_value)
}

fn decimal_option(value: Option<&Value>) -> Option<f64> {
    string_or_number(value).and_then(|value| value.parse().ok())
}

fn intx_asset_code<'a>(exchange_id: &ExchangeId, balance: &'a Value) -> ExchangeApiResult<&'a str> {
    balance
        .get("asset")
        .and_then(|asset| {
            asset
                .get("asset_id")
                .or_else(|| asset.get("asset_name"))
                .or_else(|| asset.get("symbol"))
        })
        .and_then(Value::as_str)
        .or_else(|| balance.get("asset_id").and_then(Value::as_str))
        .or_else(|| balance.get("asset").and_then(Value::as_str))
        .ok_or_else(|| parse_error(exchange_id.clone(), "intx balance missing asset", balance))
}

fn position_product_id(exchange_id: &ExchangeId, position: &Value) -> ExchangeApiResult<String> {
    required_str(exchange_id, position, "product_id")
        .or_else(|_| required_str(exchange_id, position, "symbol"))
        .map(|symbol| symbol.to_ascii_uppercase())
}

fn split_position_symbol(product_id: &str) -> (&str, &str) {
    let mut parts = product_id.split('-').filter(|part| !part.is_empty());
    let base = parts.next().unwrap_or(product_id);
    let quote = parts
        .find(|part| *part != "PERP" && *part != "INTX")
        .unwrap_or("USDC");
    (base, quote)
}

fn infer_market_type_from_product(product_id: &str) -> MarketType {
    let upper = product_id.to_ascii_uppercase();
    if upper.contains("-PERP") || upper.ends_with("-INTX") {
        MarketType::Perpetual
    } else {
        MarketType::Spot
    }
}

fn parse_position_side(side: Option<&str>, raw_quantity: &str) -> PositionSide {
    match side.unwrap_or_default().to_ascii_uppercase().as_str() {
        "LONG" | "POSITION_SIDE_LONG" | "BUY" => PositionSide::Long,
        "SHORT" | "POSITION_SIDE_SHORT" | "SELL" => PositionSide::Short,
        _ if raw_quantity.trim().starts_with('-') => PositionSide::Short,
        _ if raw_quantity.trim() == "0" || raw_quantity.trim().is_empty() => PositionSide::None,
        _ => PositionSide::Long,
    }
}

fn parse_side(side: &str) -> ExchangeApiResult<OrderSide> {
    match side.to_ascii_uppercase().as_str() {
        "BUY" | "BID" => Ok(OrderSide::Buy),
        "SELL" | "ASK" => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported coinbase side {other}"),
        }),
    }
}

fn map_order_type(order_type: Option<&str>) -> OrderType {
    match order_type.unwrap_or_default().to_ascii_uppercase().as_str() {
        "MARKET" => OrderType::Market,
        "LIMIT" => OrderType::Limit,
        "STOP_LIMIT" => OrderType::StopLimit,
        _ => OrderType::Limit,
    }
}

fn map_coinbase_order_status(status: &str) -> OrderStatus {
    match status.to_ascii_uppercase().as_str() {
        "PENDING" => OrderStatus::New,
        "OPEN" => OrderStatus::Open,
        "FILLED" => OrderStatus::Filled,
        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
        "EXPIRED" => OrderStatus::Expired,
        "FAILED" | "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity_role(role: &str) -> LiquidityRole {
    match role.to_ascii_uppercase().as_str() {
        "MAKER" | "M" => LiquidityRole::Maker,
        "TAKER" | "T" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}
