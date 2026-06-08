use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, Balance, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState,
    SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeId, Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus,
    OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    parse_error, parse_market_id, parse_time, required_str, string_or_number,
    symbol_from_market_id, validation_error, value_as_f64,
};

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    requested_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let value = value.get("data").unwrap_or(value);
    let symbol = match requested_symbol {
        Some(symbol) => symbol.clone(),
        None => {
            let market_id = required_str(exchange_id, value, "marketId")?;
            symbol_from_market_id(exchange_id, market_id)?
        }
    };
    let quantity = string_or_number(value.get("amount"))
        .or_else(|| string_or_number(value.get("volume")))
        .unwrap_or_else(|| "0".to_string());
    let open = string_or_number(value.get("openAmount"))
        .or_else(|| string_or_number(value.get("openVolume")))
        .unwrap_or_else(|| "0".to_string());
    let filled_quantity = subtract_strings(&quantity, &open);
    let updated_at = value
        .get("lastUpdate")
        .or_else(|| value.get("timestamp"))
        .and_then(Value::as_str)
        .and_then(parse_time)
        .unwrap_or_else(Utc::now);

    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(value.get("clientOrderId")),
        exchange_order_id: string_or_number(value.get("orderId")).or_else(|| {
            value.get("id").and_then(|id| {
                id.as_str()
                    .map(str::to_string)
                    .or_else(|| Some(id.to_string()))
            })
        }),
        side: parse_side(value.get("side").and_then(Value::as_str))?,
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(value.get("type").and_then(Value::as_str))?,
        time_in_force: parse_time_in_force(value.get("timeInForce").and_then(Value::as_str)),
        status: parse_order_status(value.get("status").and_then(Value::as_str)),
        quantity,
        price: string_or_number(value.get("price")),
        filled_quantity,
        average_fill_price: string_or_number(value.get("averagePrice"))
            .or_else(|| string_or_number(value.get("avgPrice"))),
        reduce_only: false,
        post_only: value
            .get("postOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: value
            .get("creationTime")
            .or_else(|| value.get("timestamp"))
            .and_then(Value::as_str)
            .and_then(parse_time),
        updated_at,
    })
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    requested_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let data = value.get("data").unwrap_or(value);
    if let Some(array) = data.as_array() {
        return array
            .iter()
            .map(|order| parse_order_state(exchange_id, requested_symbol, order))
            .collect();
    }
    Ok(vec![parse_order_state(
        exchange_id,
        requested_symbol,
        data,
    )?])
}

pub fn parse_cancel_ack(
    exchange_id: &ExchangeId,
    requested_symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let data = value.get("data").unwrap_or(value);
    let mut order = OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: requested_symbol.canonical_symbol.clone(),
        exchange_symbol: requested_symbol.exchange_symbol.clone(),
        client_order_id: string_or_number(data.get("clientOrderId")),
        exchange_order_id: string_or_number(data.get("orderId")),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: OrderStatus::Cancelled,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    };
    if let Ok(parsed) = parse_order_state(exchange_id, Some(requested_symbol), data) {
        order = parsed;
        order.status = OrderStatus::Cancelled;
    }
    Ok(order)
}

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<Balance>> {
    let rows = value
        .get("data")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "btcmarkets balances response is not an array",
                value,
            )
        })?;
    let wanted = assets
        .iter()
        .map(|asset| asset.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let balances = rows
        .iter()
        .filter(|row| {
            wanted.is_empty()
                || row
                    .get("assetName")
                    .and_then(Value::as_str)
                    .is_some_and(|asset| wanted.contains(&asset.to_ascii_uppercase()))
        })
        .map(|row| {
            let asset = required_str(exchange_id, row, "assetName")?;
            let total = value_as_f64(row.get("balance")).unwrap_or(0.0);
            let available = value_as_f64(row.get("available")).unwrap_or(total);
            let locked = value_as_f64(row.get("locked")).unwrap_or((total - available).max(0.0));
            let asset_balance =
                AssetBalance::new(asset, total, available, locked).map_err(validation_error)?;
            Ok(Balance {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                balances: vec![asset_balance],
                observed_at: Utc::now(),
            })
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(balances)
}

pub fn parse_fee_snapshots(
    exchange_id: &ExchangeId,
    requested: &[SymbolScope],
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let rows = value
        .get("feeByMarkets")
        .or_else(|| value.get("data").and_then(|data| data.get("feeByMarkets")))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "btcmarkets fees missing feeByMarkets",
                value,
            )
        })?;
    let wanted = requested
        .iter()
        .map(|symbol| symbol.exchange_symbol.symbol.clone())
        .collect::<Vec<_>>();
    rows.iter()
        .filter(|row| {
            wanted.is_empty()
                || row
                    .get("marketId")
                    .and_then(Value::as_str)
                    .is_some_and(|market| wanted.contains(&market.to_ascii_uppercase()))
        })
        .map(|row| {
            let market_id = required_str(exchange_id, row, "marketId")?;
            Ok(FeeRateSnapshot {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: symbol_from_market_id(exchange_id, market_id)?,
                maker_rate: required_str(exchange_id, row, "makerFeeRate")?.to_string(),
                taker_rate: required_str(exchange_id, row, "takerFeeRate")?.to_string(),
                source: Some("btcmarkets.accounts.me.trading-fees".to_string()),
                updated_at: Utc::now(),
            })
        })
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let data = value.get("data").unwrap_or(value);
    let rows = data.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "btcmarkets trades response is not an array",
            value,
        )
    })?;
    rows.iter()
        .map(|row| {
            parse_fill(
                exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                requested_symbol,
                row,
            )
        })
        .collect()
}

pub fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = match requested_symbol {
        Some(symbol) => symbol.clone(),
        None => {
            let (market_id, _, _) = parse_market_id(exchange_id, value)?;
            symbol_from_market_id(exchange_id, &market_id)?
        }
    };
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "btcmarkets fill parsing requires canonical symbol".to_string(),
            })?;
    let quote_quantity = match (
        value_as_f64(value.get("price")),
        value_as_f64(value.get("amount")),
    ) {
        (Some(price), Some(amount)) => Some(price * amount),
        _ => None,
    };
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(value.get("orderId")),
        client_order_id: string_or_number(value.get("clientOrderId")),
        fill_id: string_or_number(value.get("id"))
            .or_else(|| string_or_number(value.get("tradeId"))),
        side: parse_side(value.get("side").and_then(Value::as_str))?,
        position_side: PositionSide::None,
        status: FillStatus::Confirmed,
        liquidity_role: match value.get("liquidityType").and_then(Value::as_str) {
            Some(text) if text.eq_ignore_ascii_case("Maker") => LiquidityRole::Maker,
            Some(text) if text.eq_ignore_ascii_case("Taker") => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        },
        price: value_as_f64(value.get("price")).ok_or_else(|| {
            parse_error(exchange_id.clone(), "btcmarkets fill missing price", value)
        })?,
        quantity: value_as_f64(value.get("amount")).ok_or_else(|| {
            parse_error(exchange_id.clone(), "btcmarkets fill missing amount", value)
        })?,
        quote_quantity,
        fee_asset: Some("AUD".to_string()),
        fee_amount: value_as_f64(value.get("fee")),
        fee_rate: None,
        realized_pnl: None,
        filled_at: value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_time)
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    })
}

pub(super) fn parse_side(side: Option<&str>) -> ExchangeApiResult<OrderSide> {
    match side.map(str::to_ascii_lowercase).as_deref() {
        Some("bid") | Some("buy") => Ok(OrderSide::Buy),
        Some("ask") | Some("sell") => Ok(OrderSide::Sell),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported btcmarkets order side {other:?}"),
        }),
    }
}

pub(super) fn parse_order_type(order_type: Option<&str>) -> ExchangeApiResult<OrderType> {
    match order_type.map(str::to_ascii_lowercase).as_deref() {
        Some("market") => Ok(OrderType::Market),
        Some("limit") => Ok(OrderType::Limit),
        Some("stop") | Some("take profit") => Ok(OrderType::StopMarket),
        Some("stop limit") => Ok(OrderType::StopLimit),
        other => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported btcmarkets order type {other:?}"),
        }),
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value.map(str::to_ascii_uppercase).as_deref() {
        Some("IOC") => Some(TimeInForce::IOC),
        Some("FOK") => Some(TimeInForce::FOK),
        Some("GTC") | None => Some(TimeInForce::GTC),
        _ => None,
    }
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.map(str::to_ascii_lowercase).as_deref() {
        Some("accepted") => OrderStatus::New,
        Some("placed") => OrderStatus::Open,
        Some("partially matched") => OrderStatus::PartiallyFilled,
        Some("fully matched") => OrderStatus::Filled,
        Some("cancelled") | Some("canceled") | Some("partially cancelled") => {
            OrderStatus::Cancelled
        }
        Some("failed") => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn subtract_strings(total: &str, open: &str) -> String {
    match (total.parse::<f64>(), open.parse::<f64>()) {
        (Ok(total), Ok(open)) => {
            let filled = (total - open).max(0.0);
            if filled.fract() == 0.0 {
                format!("{filled:.0}")
            } else {
                let text = format!("{filled:.8}");
                text.trim_end_matches('0').trim_end_matches('.').to_string()
            }
        }
        _ => "0".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use rustcta_exchange_api::{AccountId, TenantId};
    use rustcta_types::{ExchangeId, OrderSide, OrderStatus};
    use serde_json::Value;

    use super::{parse_balances, parse_fills, parse_order_state};

    fn fixture(name: &str) -> Value {
        let text = match name {
            "order_ack.json" => {
                include_str!("../../../../../tests/fixtures/exchanges/btcmarkets/order_ack.json")
            }
            "fills.json" => {
                include_str!("../../../../../tests/fixtures/exchanges/btcmarkets/fills.json")
            }
            "balances.json" => {
                include_str!("../../../../../tests/fixtures/exchanges/btcmarkets/balances.json")
            }
            _ => panic!("unknown btcmarkets fixture {name}"),
        };
        serde_json::from_str(text).expect("btcmarkets fixture")
    }

    #[test]
    fn order_state_should_map_status_and_fill_quantity() {
        let exchange = ExchangeId::new("btcmarkets").unwrap();
        let order = parse_order_state(&exchange, None, &fixture("order_ack.json")).unwrap();

        assert_eq!(order.exchange_order_id.as_deref(), Some("7524"));
        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.filled_quantity, "0");
    }

    #[test]
    fn fills_should_parse_private_trades() {
        let exchange = ExchangeId::new("btcmarkets").unwrap();
        let fills = parse_fills(
            &exchange,
            TenantId::new("tenant-a").unwrap(),
            AccountId::new("acct-a").unwrap(),
            None,
            &fixture("fills.json"),
        )
        .unwrap();

        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].side, OrderSide::Sell);
        assert_eq!(fills[0].fee_asset.as_deref(), Some("AUD"));
    }

    #[test]
    fn balances_should_parse_available_and_locked_amounts() {
        let exchange = ExchangeId::new("btcmarkets").unwrap();
        let balances = parse_balances(
            &exchange,
            TenantId::new("tenant-a").unwrap(),
            AccountId::new("acct-a").unwrap(),
            &[],
            &fixture("balances.json"),
        )
        .unwrap();

        assert_eq!(balances.len(), 2);
        assert_eq!(balances[0].balances[0].asset, "AUD");
        assert_eq!(balances[0].balances[0].locked, 100.0);
    }
}
