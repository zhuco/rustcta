use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, Balance, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, Fill, OrderState,
    Position, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, ExchangeSymbol, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus,
    OrderType, PositionSide, SchemaVersion,
};
use serde_json::Value;

use super::parser::{
    normalize_orangex_symbol, number_from_value as raw_number_from_value, parse_error,
    parse_millis, split_orangex_instrument, string_or_number, validation_error,
};

pub fn parse_balances(
    tenant_id: TenantId,
    account_id: AccountId,
    exchange_id: &rustcta_exchange_api::ExchangeId,
    market_type: MarketType,
    assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<Balance>> {
    let result = value.get("result").unwrap_or(value);
    let account_key = match market_type {
        MarketType::Spot => "SPOT",
        MarketType::Perpetual => "PERPETUAL",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "orangex.unsupported_balance_market_type",
            });
        }
    };
    let account = result.get(account_key).ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "OrangeX assets response missing account section",
            result,
        )
    })?;
    let mut balances = Vec::new();
    if let Some(details) = account.get("details").and_then(Value::as_array) {
        for item in details {
            let asset = item
                .get("coin_type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_uppercase();
            if asset.is_empty()
                || (!assets.is_empty()
                    && !assets
                        .iter()
                        .any(|requested| requested.eq_ignore_ascii_case(&asset)))
            {
                continue;
            }
            let available = number_from_value(item.get("available")).unwrap_or(0.0);
            let locked = number_from_value(item.get("freeze")).unwrap_or(0.0);
            let total = number_from_value(item.get("total")).unwrap_or(available + locked);
            balances.push(
                AssetBalance::new(asset, total, available, locked).map_err(validation_error)?,
            );
        }
    } else if market_type == MarketType::Perpetual {
        let asset_balance = AssetBalance::new(
            "USDT",
            number_from_value(account.get("wallet_balance"))
                .or_else(|| number_from_value(account.get("total_margin_balance")))
                .unwrap_or(0.0),
            number_from_value(account.get("available_funds")).unwrap_or(0.0),
            number_from_value(account.get("order_frozen")).unwrap_or(0.0),
        )
        .map_err(validation_error)?;
        balances.push(asset_balance);
    }

    Ok(vec![Balance {
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
    tenant_id: TenantId,
    account_id: AccountId,
    exchange_id: &rustcta_exchange_api::ExchangeId,
    value: &Value,
) -> ExchangeApiResult<Vec<Position>> {
    let rows = value
        .get("result")
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "OrangeX positions response is not an array",
                value,
            )
        })?;
    rows.iter()
        .filter_map(|item| {
            let instrument = item.get("instrument_name")?.as_str()?;
            let (market_type, base, quote) = split_orangex_instrument(instrument).ok()?;
            if market_type == MarketType::Spot {
                return None;
            }
            let canonical = rustcta_exchange_api::CanonicalSymbol::new(base, quote).ok()?;
            let exchange_symbol =
                ExchangeSymbol::new(exchange_id.clone(), market_type, instrument).ok()?;
            let size = number_from_value(item.get("size")).unwrap_or(0.0);
            let side = if size < 0.0 {
                PositionSide::Short
            } else if size > 0.0 {
                PositionSide::Long
            } else {
                PositionSide::Net
            };
            Some(Position {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type,
                canonical_symbol: canonical,
                exchange_symbol: Some(exchange_symbol),
                side,
                quantity: size.abs(),
                entry_price: number_from_value(item.get("average_price")),
                mark_price: number_from_value(item.get("mark_price")),
                liquidation_price: number_from_value(item.get("liquid_price")),
                unrealized_pnl: number_from_value(item.get("floating_profit_loss"))
                    .or_else(|| number_from_value(item.get("total_profit_loss"))),
                leverage: number_from_value(item.get("leverage")),
                observed_at: Utc::now(),
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|position| {
            position.validate().map_err(validation_error)?;
            Ok(position)
        })
        .collect()
}

pub fn parse_fee_snapshots(
    exchange_id: &rustcta_exchange_api::ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<FeeRateSnapshot>> {
    let requested = normalize_orangex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?;
    let rows = value.as_array().ok_or_else(|| {
        parse_error(
            exchange_id.clone(),
            "OrangeX instruments fee response is not an array",
            value,
        )
    })?;
    let mut fees = Vec::new();
    for row in rows {
        let instrument = row
            .get("instrument_name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let normalized = normalize_orangex_symbol(instrument, symbol.market_type)?;
        if !normalized.eq_ignore_ascii_case(&requested) {
            continue;
        }
        let maker = string_or_number(
            row.get("maker_commission")
                .or_else(|| row.get("maker_fee"))
                .or_else(|| row.get("maker_rate")),
        )
        .unwrap_or_else(|| "0".to_string());
        let taker = string_or_number(
            row.get("taker_commission")
                .or_else(|| row.get("taker_fee"))
                .or_else(|| row.get("taker_rate")),
        )
        .unwrap_or_else(|| maker.clone());
        fees.push(FeeRateSnapshot {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            symbol: symbol.clone(),
            maker_rate: maker,
            taker_rate: taker,
            source: Some("orangex.public_get_instruments".to_string()),
            updated_at: Utc::now(),
        });
    }
    if fees.is_empty() {
        return Err(parse_error(
            exchange_id.clone(),
            "OrangeX instruments fee response missing requested symbol",
            value,
        ));
    }
    Ok(fees)
}

pub fn parse_order_state(
    exchange_id: &rustcta_exchange_api::ExchangeId,
    fallback_symbol: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let value = value
        .get("order")
        .or_else(|| value.get("result").and_then(|result| result.get("order")))
        .unwrap_or(value);
    let symbol = match (
        fallback_symbol,
        value.get("instrument_name").and_then(Value::as_str),
    ) {
        (Some(symbol), _) => symbol.clone(),
        (None, Some(instrument)) => {
            let (market_type, base, quote) = split_orangex_instrument(instrument)?;
            rustcta_exchange_api::SymbolScope {
                exchange: exchange_id.clone(),
                market_type,
                canonical_symbol: Some(
                    rustcta_exchange_api::CanonicalSymbol::new(base, quote)
                        .map_err(validation_error)?,
                ),
                exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, instrument)
                    .map_err(validation_error)?,
            }
        }
        (None, None) => {
            return Err(parse_error(
                exchange_id.clone(),
                "OrangeX order response missing instrument_name",
                value,
            ));
        }
    };
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol,
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: value
            .get("custom_order_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        exchange_order_id: value.get("order_id").and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        }),
        side: parse_side(value.get("direction").and_then(Value::as_str)).unwrap_or(OrderSide::Buy),
        position_side: Some(parse_position_side(
            value.get("position_side").and_then(Value::as_str),
        )),
        order_type: parse_order_type(value.get("order_type").and_then(Value::as_str)),
        time_in_force: parse_time_in_force(value.get("time_in_force").and_then(Value::as_str)),
        status: parse_order_status(value.get("order_state").and_then(Value::as_str)),
        quantity: string_or_number(value.get("amount")).unwrap_or_else(|| "0".to_string()),
        price: string_or_number(value.get("price")).filter(|price| price != "0"),
        filled_quantity: string_or_number(value.get("filled_amount"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: string_or_number(value.get("average_price"))
            .filter(|price| price != "0"),
        reduce_only: value
            .get("reduce_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: value
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: parse_millis(value.get("creation_timestamp")),
        updated_at: parse_millis(value.get("last_update_timestamp")).unwrap_or_else(Utc::now),
    })
}

pub fn parse_recent_fills(
    tenant_id: TenantId,
    account_id: AccountId,
    exchange_id: &rustcta_exchange_api::ExchangeId,
    fallback_symbol: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<Fill>> {
    let rows = value
        .get("result")
        .unwrap_or(value)
        .get("trades")
        .or_else(|| value.get("trades"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            parse_error(
                exchange_id.clone(),
                "OrangeX trades response missing trades",
                value,
            )
        })?;
    rows.iter()
        .map(|item| {
            parse_fill(
                tenant_id.clone(),
                account_id.clone(),
                exchange_id,
                fallback_symbol,
                item,
            )
        })
        .collect()
}

pub(super) fn parse_fill(
    tenant_id: TenantId,
    account_id: AccountId,
    exchange_id: &rustcta_exchange_api::ExchangeId,
    fallback_symbol: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Fill> {
    let symbol = match (
        fallback_symbol,
        value.get("instrument_name").and_then(Value::as_str),
    ) {
        (Some(symbol), _) => symbol.clone(),
        (None, Some(instrument)) => {
            let (market_type, base, quote) = split_orangex_instrument(instrument)?;
            rustcta_exchange_api::SymbolScope {
                exchange: exchange_id.clone(),
                market_type,
                canonical_symbol: Some(
                    rustcta_exchange_api::CanonicalSymbol::new(base, quote)
                        .map_err(validation_error)?,
                ),
                exchange_symbol: ExchangeSymbol::new(exchange_id.clone(), market_type, instrument)
                    .map_err(validation_error)?,
            }
        }
        (None, None) => {
            return Err(parse_error(
                exchange_id.clone(),
                "OrangeX trade response missing instrument_name",
                value,
            ));
        }
    };
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "orangex fill requires canonical_symbol".to_string(),
            })?;
    let price = number_from_value(value.get("price")).unwrap_or(0.0);
    let quantity = number_from_value(value.get("amount")).unwrap_or(0.0);
    let fill = Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: value
            .get("order_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        client_order_id: None,
        fill_id: value
            .get("trade_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        side: parse_side(value.get("direction").and_then(Value::as_str)).unwrap_or(OrderSide::Buy),
        position_side: PositionSide::None,
        status: FillStatus::Confirmed,
        liquidity_role: match value.get("role").and_then(Value::as_str) {
            Some(role) if role.eq_ignore_ascii_case("maker") => LiquidityRole::Maker,
            Some(role) if role.eq_ignore_ascii_case("taker") => LiquidityRole::Taker,
            _ => LiquidityRole::Unknown,
        },
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: value
            .get("fee_coin_type")
            .and_then(Value::as_str)
            .map(str::to_ascii_uppercase),
        fee_amount: number_from_value(value.get("fee")),
        fee_rate: None,
        realized_pnl: None,
        filled_at: parse_millis(value.get("timestamp")).unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    };
    fill.validate().map_err(validation_error)?;
    Ok(fill)
}

fn parse_side(value: Option<&str>) -> Option<OrderSide> {
    match value?.to_ascii_lowercase().as_str() {
        "buy" => Some(OrderSide::Buy),
        "sell" => Some(OrderSide::Sell),
        _ => None,
    }
}

fn number_from_value(value: Option<&Value>) -> Option<f64> {
    raw_number_from_value(value)
}

fn parse_position_side(value: Option<&str>) -> PositionSide {
    match value.unwrap_or("BOTH").to_ascii_uppercase().as_str() {
        "LONG" => PositionSide::Long,
        "SHORT" => PositionSide::Short,
        "BOTH" => PositionSide::Net,
        _ => PositionSide::None,
    }
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or("limit").to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn parse_time_in_force(value: Option<&str>) -> Option<rustcta_exchange_api::TimeInForce> {
    match value?.to_ascii_lowercase().as_str() {
        "gtc" | "good_til_cancelled" => Some(rustcta_exchange_api::TimeInForce::GTC),
        "ioc" | "immediate_or_cancel" => Some(rustcta_exchange_api::TimeInForce::IOC),
        "fok" | "fill_or_kill" => Some(rustcta_exchange_api::TimeInForce::FOK),
        _ => None,
    }
}

fn parse_order_status(value: Option<&str>) -> OrderStatus {
    match value.unwrap_or("unknown").to_ascii_lowercase().as_str() {
        "open" => OrderStatus::Open,
        "filled" => OrderStatus::Filled,
        "partially_filled" | "partial_filled" => OrderStatus::PartiallyFilled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}
