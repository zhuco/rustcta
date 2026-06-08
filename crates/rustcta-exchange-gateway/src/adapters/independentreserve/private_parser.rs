use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, OrderState, SymbolScope,
    TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeBalance, ExchangeId, Fill, FillStatus, LiquidityRole, MarketType,
    OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::Value;

use super::parser::{
    decimal_as_f64, independentreserve_symbol, market_rows, normalize_asset, parse_error,
    parse_timestamp_value, split_symbol_assets, string_or_number, symbol_scope, validation_error,
};

pub fn parse_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let requested = requested_assets
        .iter()
        .map(|asset| normalize_asset(asset))
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let balances = market_rows(value)
        .into_iter()
        .filter_map(|item| {
            let asset = item
                .get("currencyCode")
                .or_else(|| item.get("CurrencyCode"))
                .or_else(|| item.get("currency"))
                .and_then(Value::as_str)
                .map(normalize_asset)?;
            if !requested.is_empty() && !requested.contains(&asset) {
                return None;
            }
            let available = decimal_as_f64(
                item.get("availableBalance")
                    .or_else(|| item.get("AvailableBalance"))
                    .or_else(|| item.get("available")),
            )
            .unwrap_or(0.0);
            let locked = decimal_as_f64(
                item.get("lockedBalance")
                    .or_else(|| item.get("LockedBalance"))
                    .or_else(|| item.get("reservedBalance"))
                    .or_else(|| item.get("ReservedBalance")),
            )
            .unwrap_or(0.0);
            let total = decimal_as_f64(
                item.get("totalBalance")
                    .or_else(|| item.get("TotalBalance"))
                    .or_else(|| item.get("balance")),
            )
            .unwrap_or(available + locked);
            Some(AssetBalance::new(asset, total, available, locked).map_err(validation_error))
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(vec![ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        balances,
        observed_at: Utc::now(),
    }])
}

pub fn parse_fee_snapshots(symbols: &[SymbolScope]) -> Vec<FeeRateSnapshot> {
    symbols
        .iter()
        .map(|symbol| FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: symbol.clone(),
            maker_rate: "0".to_string(),
            taker_rate: "0".to_string(),
            source: Some(
                "independentreserve.fees_are_account_tiered_no_symbol_fee_endpoint".to_string(),
            ),
            updated_at: Utc::now(),
        })
        .collect()
}

pub fn parse_order(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Option<OrderState>> {
    let item = value
        .get("Data")
        .or_else(|| value.get("data"))
        .or_else(|| value.get("Order"))
        .or_else(|| value.get("order"))
        .unwrap_or(value);
    if item.is_null() {
        return Ok(None);
    }
    Ok(Some(parse_order_state(exchange_id, fallback_symbol, item)?))
}

pub fn parse_orders(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    market_rows(value)
        .iter()
        .map(|item| parse_order_state(exchange_id, fallback_symbol, item))
        .collect()
}

pub fn parse_fills(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    market_rows(value)
        .iter()
        .map(|item| parse_fill(exchange_id, &tenant_id, &account_id, fallback_symbol, item))
        .collect()
}

pub fn order_state_from_cancel_ack(
    exchange_id: &ExchangeId,
    fallback_symbol: &SymbolScope,
    value: Option<&Value>,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> ExchangeApiResult<OrderState> {
    if let Some(value) = value {
        if let Some(order) = parse_order(exchange_id, Some(fallback_symbol), value)? {
            return Ok(OrderState {
                status: OrderStatus::Cancelled,
                ..order
            });
        }
    }
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: fallback_symbol.canonical_symbol.clone(),
        exchange_symbol: fallback_symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
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
    })
}

pub fn parse_order_state(
    exchange_id: &ExchangeId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_scope_from_value(exchange_id, value))?;
    let created_at = value
        .get("CreatedTimestampUtc")
        .or_else(|| value.get("createdTimestampUtc"))
        .or_else(|| value.get("createdAtUtc"))
        .or_else(|| value.get("created_at"))
        .and_then(parse_timestamp_value);
    let updated_at = value
        .get("UpdatedTimestampUtc")
        .or_else(|| value.get("updatedTimestampUtc"))
        .or_else(|| value.get("updatedAtUtc"))
        .or_else(|| value.get("createdTimestampUtc"))
        .and_then(parse_timestamp_value)
        .unwrap_or_else(Utc::now);
    let order_type = parse_order_type(
        value
            .get("OrderType")
            .or_else(|| value.get("orderType"))
            .or_else(|| value.get("type"))
            .and_then(Value::as_str),
    );
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: string_or_number(
            value
                .get("ClientOrderIdentifier")
                .or_else(|| value.get("clientOrderIdentifier"))
                .or_else(|| value.get("brokerOrderId"))
                .or_else(|| value.get("BrokerOrderId")),
        ),
        exchange_order_id: string_or_number(
            value
                .get("OrderGuid")
                .or_else(|| value.get("orderGuid"))
                .or_else(|| value.get("guid"))
                .or_else(|| value.get("id")),
        ),
        side: parse_side(
            value
                .get("OrderType")
                .or_else(|| value.get("orderType"))
                .or_else(|| value.get("side"))
                .and_then(Value::as_str),
        ),
        position_side: Some(PositionSide::None),
        order_type,
        time_in_force: Some(TimeInForce::GTC),
        status: parse_order_status(
            value
                .get("Status")
                .or_else(|| value.get("status"))
                .and_then(Value::as_str),
        ),
        quantity: string_or_number(
            value
                .get("Volume")
                .or_else(|| value.get("volume"))
                .or_else(|| value.get("Outstanding"))
                .or_else(|| value.get("outstanding")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("Price").or_else(|| value.get("price"))),
        filled_quantity: string_or_number(
            value
                .get("VolumeFilled")
                .or_else(|| value.get("volumeFilled"))
                .or_else(|| value.get("filledVolume")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(
            value
                .get("AvgPrice")
                .or_else(|| value.get("AveragePrice"))
                .or_else(|| value.get("averagePrice")),
        ),
        reduce_only: false,
        post_only: false,
        created_at,
        updated_at,
    })
}

fn parse_fill(
    exchange_id: &ExchangeId,
    tenant_id: &TenantId,
    account_id: &AccountId,
    fallback_symbol: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = fallback_symbol
        .cloned()
        .map(Ok)
        .unwrap_or_else(|| symbol_scope_from_value(exchange_id, value))?;
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "independentreserve fill requires canonical symbol".to_string(),
            })?;
    let price = decimal_as_f64(
        value
            .get("Price")
            .or_else(|| value.get("price"))
            .or_else(|| value.get("tradePrice")),
    )
    .ok_or_else(|| parse_error(exchange_id.clone(), "IR fill missing price", value))?;
    let quantity = decimal_as_f64(
        value
            .get("Volume")
            .or_else(|| value.get("volume"))
            .or_else(|| value.get("tradeVolume")),
    )
    .ok_or_else(|| parse_error(exchange_id.clone(), "IR fill missing volume", value))?;
    let filled_at = value
        .get("TradeTimestampUtc")
        .or_else(|| value.get("CreatedTimestampUtc"))
        .or_else(|| value.get("timestamp"))
        .and_then(parse_timestamp_value)
        .unwrap_or_else(Utc::now);
    Ok(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id: tenant_id.clone(),
        account_id: account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: string_or_number(
            value
                .get("OrderGuid")
                .or_else(|| value.get("orderGuid"))
                .or_else(|| value.get("orderId")),
        ),
        client_order_id: string_or_number(
            value
                .get("ClientOrderIdentifier")
                .or_else(|| value.get("clientOrderIdentifier"))
                .or_else(|| value.get("brokerOrderId")),
        ),
        fill_id: string_or_number(
            value
                .get("TradeGuid")
                .or_else(|| value.get("tradeGuid"))
                .or_else(|| value.get("id")),
        ),
        side: parse_side(
            value
                .get("OrderType")
                .or_else(|| value.get("orderType"))
                .or_else(|| value.get("side"))
                .and_then(Value::as_str),
        ),
        position_side: PositionSide::None,
        status: FillStatus::Confirmed,
        liquidity_role: parse_liquidity(
            value
                .get("LiquidityType")
                .or_else(|| value.get("liquidityType"))
                .or_else(|| value.get("liquidity"))
                .and_then(Value::as_str),
        ),
        price,
        quantity,
        quote_quantity: decimal_as_f64(
            value
                .get("Value")
                .or_else(|| value.get("value"))
                .or_else(|| value.get("notional")),
        ),
        fee_asset: value
            .get("FeeCurrencyCode")
            .or_else(|| value.get("feeCurrencyCode"))
            .and_then(Value::as_str)
            .map(normalize_asset),
        fee_amount: decimal_as_f64(
            value
                .get("Fee")
                .or_else(|| value.get("fee"))
                .or_else(|| value.get("feeAmount")),
        ),
        fee_rate: decimal_as_f64(value.get("FeeRate").or_else(|| value.get("feeRate"))),
        realized_pnl: None,
        filled_at,
        received_at: Utc::now(),
    })
}

fn symbol_scope_from_value(
    exchange_id: &ExchangeId,
    value: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = value
        .get("Market")
        .or_else(|| value.get("market"))
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
    {
        return symbol_scope(exchange_id, symbol);
    }
    let primary = value
        .get("PrimaryCurrencyCode")
        .or_else(|| value.get("primaryCurrencyCode"))
        .and_then(Value::as_str)
        .map(normalize_asset);
    let secondary = value
        .get("SecondaryCurrencyCode")
        .or_else(|| value.get("secondaryCurrencyCode"))
        .and_then(Value::as_str)
        .map(normalize_asset);
    match (primary, secondary) {
        (Some(primary), Some(secondary)) => symbol_scope(
            exchange_id,
            &independentreserve_symbol(&primary, &secondary),
        ),
        _ => Err(parse_error(
            exchange_id.clone(),
            "Independent Reserve order/fill missing symbol",
            value,
        )),
    }
}

fn parse_side(value: Option<&str>) -> OrderSide {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        text if text.contains("sell") || text == "ask" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        text if text.contains("market") => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "open" | "partiallyfilled" | "partially_filled" | "part filled" => {
            OrderStatus::PartiallyFilled
        }
        "filled" | "closed" | "fullymatched" | "fully_matched" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        "new" | "pending" => OrderStatus::New,
        "" => OrderStatus::Unknown,
        _ => OrderStatus::Open,
    }
}

fn parse_liquidity(value: Option<&str>) -> LiquidityRole {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "maker" | "m" => LiquidityRole::Maker,
        "taker" | "t" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

#[allow(dead_code)]
fn _split_order_symbol(symbol: &str) -> (String, String) {
    split_symbol_assets(symbol)
}

#[cfg(test)]
mod tests {
    use rustcta_exchange_api::{AccountId, TenantId};
    use rustcta_types::{ExchangeId, OrderStatus};

    use super::{parse_fills, parse_order};

    #[test]
    fn independentreserve_order_fixture_should_parse_guid_and_client_id() {
        let exchange = ExchangeId::new("independentreserve").expect("exchange id");
        let value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/independentreserve/rest/order.json"
        ))
        .expect("order fixture");

        let order = parse_order(&exchange, None, &value)
            .expect("parse order")
            .expect("order");

        assert_eq!(order.exchange_order_id.as_deref(), Some("order-guid-1"));
        assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
    }

    #[test]
    fn independentreserve_fill_fixture_should_parse_trade_and_fee() {
        let exchange = ExchangeId::new("independentreserve").expect("exchange id");
        let tenant = TenantId::new("tenant").expect("tenant");
        let account = AccountId::new("account").expect("account");
        let value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/independentreserve/rest/fills.json"
        ))
        .expect("fills fixture");

        let fills = parse_fills(&exchange, tenant, account, None, &value).expect("fills");

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_id.as_deref(), Some("trade-guid-1"));
        assert_eq!(fills[0].fee_asset.as_deref(), Some("AUD"));
    }
}
