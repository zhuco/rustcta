#![allow(dead_code)]

use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};
use rustcta_exchange_api::{
    AccountId, BalancesRequest, BalancesResponse, BatchCancelOrdersRequest,
    BatchCancelOrdersResponse, CancelOrderRequest, ExchangeApiError, ExchangeApiResult,
    OpenOrdersRequest, OpenOrdersResponse, OrderState, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangeSymbol, Fill, FillStatus,
    LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType, PositionSide, SchemaVersion,
    TimeInForce,
};
use serde_json::{json, Value};

use super::parser::normalize_bitbank_pair;
use super::BitbankGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbankRequestSpec {
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub body: String,
}

pub fn assets_spec() -> BitbankRequestSpec {
    BitbankRequestSpec {
        method: "GET".to_string(),
        path: "/v1/user/assets".to_string(),
        query: Vec::new(),
        body: String::new(),
    }
}

pub fn query_order_spec(pair: &str, order_id: &str) -> ExchangeApiResult<BitbankRequestSpec> {
    Ok(BitbankRequestSpec {
        method: "GET".to_string(),
        path: "/v1/user/spot/order".to_string(),
        query: vec![
            ("order_id".to_string(), numeric_order_id(order_id)?),
            ("pair".to_string(), normalize_bitbank_pair(pair)?),
        ],
        body: String::new(),
    })
}

pub fn open_orders_spec(pair: &str) -> ExchangeApiResult<BitbankRequestSpec> {
    Ok(BitbankRequestSpec {
        method: "GET".to_string(),
        path: "/v1/user/spot/active_orders".to_string(),
        query: vec![("pair".to_string(), normalize_bitbank_pair(pair)?)],
        body: String::new(),
    })
}

pub fn recent_fills_spec(
    pair: &str,
    order_id: Option<&str>,
    limit: Option<u32>,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
) -> ExchangeApiResult<BitbankRequestSpec> {
    let mut query = vec![("pair".to_string(), normalize_bitbank_pair(pair)?)];
    if let Some(order_id) = order_id {
        query.push(("order_id".to_string(), numeric_order_id(order_id)?));
    }
    if let Some(limit) = limit {
        query.push(("count".to_string(), limit.min(1000).to_string()));
    }
    if let Some(start_time) = start_time {
        query.push((
            "since".to_string(),
            start_time.timestamp_millis().to_string(),
        ));
    }
    if let Some(end_time) = end_time {
        query.push(("end".to_string(), end_time.timestamp_millis().to_string()));
    }
    Ok(BitbankRequestSpec {
        method: "GET".to_string(),
        path: "/v1/user/spot/trade_history".to_string(),
        query,
        body: String::new(),
    })
}

pub fn place_limit_order_spec(
    pair: &str,
    side: &str,
    price: &str,
    amount: &str,
    post_only: bool,
) -> ExchangeApiResult<BitbankRequestSpec> {
    spec(
        "POST",
        "/v1/user/spot/order",
        json!({
            "pair": normalize_bitbank_pair(pair)?,
            "side": normalize_side(side)?,
            "type": "limit",
            "price": price,
            "amount": amount,
            "post_only": post_only,
        }),
    )
}

pub fn cancel_order_spec(pair: &str, order_id: u64) -> ExchangeApiResult<BitbankRequestSpec> {
    spec(
        "POST",
        "/v1/user/spot/cancel_order",
        json!({
            "pair": normalize_bitbank_pair(pair)?,
            "order_id": order_id,
        }),
    )
}

pub fn batch_cancel_orders_spec(
    pair: &str,
    order_ids: &[u64],
) -> ExchangeApiResult<BitbankRequestSpec> {
    if order_ids.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitbank batch cancel requires at least one order_id".to_string(),
        });
    }
    spec(
        "POST",
        "/v1/user/spot/cancel_orders",
        json!({
            "pair": normalize_bitbank_pair(pair)?,
            "order_ids": order_ids,
        }),
    )
}

fn spec(method: &str, path: &str, body: Value) -> ExchangeApiResult<BitbankRequestSpec> {
    Ok(BitbankRequestSpec {
        method: method.to_string(),
        path: path.to_string(),
        query: Vec::new(),
        body: serde_json::to_string(&body).map_err(|error| ExchangeApiError::Serialization {
            message: error.to_string(),
        })?,
    })
}

fn numeric_order_id(order_id: &str) -> ExchangeApiResult<String> {
    order_id
        .trim()
        .parse::<u64>()
        .map(|value| value.to_string())
        .map_err(|_| ExchangeApiError::InvalidRequest {
            message: format!("bitbank order_id must be numeric: {order_id}"),
        })
}

fn normalize_side(side: &str) -> ExchangeApiResult<&'static str> {
    match side.trim().to_ascii_lowercase().as_str() {
        "buy" => Ok("buy"),
        "sell" => Ok("sell"),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: format!("unsupported bitbank side {side}"),
        }),
    }
}

impl BitbankGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let spec = assets_spec();
        let value = self
            .send_signed_get("bitbank.get_balances", &spec.path, &query_map(&spec))
            .await?;
        let balances = parse_bitbank_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            &request.assets,
            &value,
        )?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitbank.query_order.client_order_id",
            });
        }
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "bitbank query_order requires exchange_order_id".to_string(),
            }
        })?;
        let spec = query_order_spec(&request.symbol.exchange_symbol.symbol, order_id)?;
        let value = self
            .send_signed_get("bitbank.query_order", &spec.path, &query_map(&spec))
            .await?;
        let order = parse_bitbank_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order: Some(order),
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitbank get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let spec = open_orders_spec(&symbol.exchange_symbol.symbol)?;
        let value = self
            .send_signed_get("bitbank.get_open_orders", &spec.path, &query_map(&spec))
            .await?;
        let orders = parse_bitbank_open_orders(&self.exchange_id, symbol, &value)?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitbank.get_recent_fills.client_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitbank.get_recent_fills.from_trade_id",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitbank get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let spec = recent_fills_spec(
            &symbol.exchange_symbol.symbol,
            request.exchange_order_id.as_deref(),
            request.limit,
            request.start_time,
            request.end_time,
        )?;
        let value = self
            .send_signed_get("bitbank.get_recent_fills", &spec.path, &query_map(&spec))
            .await?;
        let fills =
            parse_bitbank_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }

    pub(super) async fn batch_cancel_orders_impl(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.cancels.is_empty() {
            return Ok(BatchCancelOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                cancelled_count: 0,
                report: None,
            });
        }
        let (pair, order_ids) = bitbank_batch_cancel_ids(&request)?;
        let spec = batch_cancel_orders_spec(&pair, &order_ids)?;
        let value = self
            .send_signed_post("bitbank.batch_cancel_orders", &spec.path, &spec.body)
            .await?;
        let orders = parse_bitbank_batch_cancel_response(&request, &value);
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: None,
        })
    }
}

fn query_map(spec: &BitbankRequestSpec) -> HashMap<String, String> {
    spec.query.iter().cloned().collect()
}

fn bitbank_batch_cancel_ids(
    request: &BatchCancelOrdersRequest,
) -> ExchangeApiResult<(String, Vec<u64>)> {
    let first_pair = normalize_bitbank_pair(
        &request
            .cancels
            .first()
            .expect("non-empty checked")
            .symbol
            .exchange_symbol
            .symbol,
    )?;
    let mut order_ids = Vec::with_capacity(request.cancels.len());
    for cancel in &request.cancels {
        ensure_exchange_api_schema(cancel.schema_version)?;
        if cancel.symbol.market_type != rustcta_types::MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitbank.batch_cancel_orders.non_spot",
            });
        }
        let pair = normalize_bitbank_pair(&cancel.symbol.exchange_symbol.symbol)?;
        if pair != first_pair {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitbank batch_cancel_orders requires all cancels to use one pair"
                    .to_string(),
            });
        }
        if cancel.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitbank.batch_cancel_orders.client_order_id",
            });
        }
        let order_id =
            cancel
                .exchange_order_id
                .as_ref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitbank batch_cancel_orders requires exchange_order_id".to_string(),
                })?;
        order_ids.push(
            order_id
                .parse::<u64>()
                .map_err(|_| ExchangeApiError::InvalidRequest {
                    message: format!("bitbank order_id must be numeric: {order_id}"),
                })?,
        );
    }
    Ok((first_pair, order_ids))
}

fn parse_bitbank_batch_cancel_response(
    request: &BatchCancelOrdersRequest,
    value: &Value,
) -> Vec<rustcta_exchange_api::OrderState> {
    value
        .get("data")
        .and_then(|data| data.get("orders"))
        .and_then(Value::as_array)
        .filter(|rows| rows.len() == request.cancels.len())
        .map(|rows| {
            rows.iter()
                .zip(&request.cancels)
                .map(|(row, cancel)| cancelled_order_state(cancel, row))
                .collect()
        })
        .unwrap_or_else(|| {
            request
                .cancels
                .iter()
                .map(|cancel| cancelled_order_state(cancel, &Value::Null))
                .collect()
        })
}

fn cancelled_order_state(request: &CancelOrderRequest, value: &Value) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: request.symbol.exchange.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: value
            .get("order_id")
            .or_else(|| value.get("id"))
            .and_then(value_to_string)
            .or_else(|| request.exchange_order_id.clone()),
        side: value
            .get("side")
            .and_then(Value::as_str)
            .and_then(parse_side)
            .unwrap_or(OrderSide::Buy),
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: None,
        status: OrderStatus::Cancelled,
        quantity: value_as_string(value.get("start_amount").or_else(|| value.get("amount")))
            .unwrap_or_else(|| "0".to_string()),
        price: value_as_string(value.get("price")),
        filled_quantity: value_as_string(
            value
                .get("executed_amount")
                .or_else(|| value.get("executedAmount")),
        )
        .unwrap_or_else(|| "0".to_string()),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: chrono::Utc::now(),
    }
}

fn parse_bitbank_open_orders(
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    value
        .get("data")
        .and_then(|data| data.get("orders"))
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitbank open orders response missing data.orders".to_string(),
        })?
        .iter()
        .map(|row| parse_bitbank_order_state(exchange_id, Some(symbol), row))
        .collect()
}

fn parse_bitbank_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<OrderState> {
    let row = value.get("data").unwrap_or(value);
    let pair = row
        .get("pair")
        .and_then(Value::as_str)
        .map(normalize_bitbank_pair)
        .transpose()?
        .or_else(|| symbol_hint.map(|symbol| symbol.exchange_symbol.symbol.clone()))
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitbank order row missing pair".to_string(),
        })?;
    let exchange_symbol = if let Some(symbol) = symbol_hint {
        symbol.exchange_symbol.clone()
    } else {
        ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, &pair)
            .map_err(validation_error)?
    };
    let canonical_symbol = symbol_hint
        .and_then(|symbol| symbol.canonical_symbol.clone())
        .or_else(|| canonical_from_pair(&pair).ok());
    let now = Utc::now();
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol,
        exchange_symbol,
        client_order_id: None,
        exchange_order_id: row
            .get("order_id")
            .and_then(value_to_string)
            .or_else(|| row.get("id").and_then(value_to_string)),
        side: row
            .get("side")
            .and_then(Value::as_str)
            .and_then(parse_side)
            .unwrap_or(OrderSide::Buy),
        position_side: Some(PositionSide::None),
        order_type: parse_order_type(row.get("type").and_then(Value::as_str)),
        time_in_force: Some(TimeInForce::GTC),
        status: map_order_status(row.get("status").and_then(Value::as_str)),
        quantity: value_as_string(
            row.get("start_amount")
                .or_else(|| row.get("amount"))
                .or_else(|| row.get("remaining_amount")),
        )
        .unwrap_or_else(|| "0".to_string()),
        price: value_as_string(row.get("price")).filter(|value| value != "0"),
        filled_quantity: value_as_string(row.get("executed_amount"))
            .unwrap_or_else(|| "0".to_string()),
        average_fill_price: value_as_string(row.get("average_price")).filter(|value| value != "0"),
        reduce_only: false,
        post_only: row
            .get("post_only")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: timestamp_millis(row.get("ordered_at")),
        updated_at: timestamp_millis(row.get("ordered_at")).unwrap_or(now),
    })
}

fn parse_bitbank_recent_fills(
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
                message: "bitbank get_recent_fills requires canonical_symbol".to_string(),
            })?;
    value
        .get("data")
        .and_then(|data| data.get("trades"))
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitbank trade history response missing data.trades".to_string(),
        })?
        .iter()
        .map(|row| {
            let price = decimal_to_f64(row.get("price")).unwrap_or(0.0);
            let quantity = decimal_to_f64(row.get("amount")).unwrap_or(0.0);
            Ok(Fill {
                schema_version: SchemaVersion::current(),
                tenant_id: tenant_id.clone(),
                account_id: account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol.clone(),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                order_id: row.get("order_id").and_then(value_to_string),
                client_order_id: None,
                fill_id: row.get("trade_id").and_then(value_to_string),
                side: row
                    .get("side")
                    .and_then(Value::as_str)
                    .and_then(parse_side)
                    .unwrap_or(OrderSide::Buy),
                position_side: PositionSide::None,
                status: FillStatus::Confirmed,
                liquidity_role: parse_liquidity_role(
                    row.get("maker_taker").and_then(Value::as_str),
                ),
                price,
                quantity,
                quote_quantity: (price > 0.0 && quantity > 0.0).then_some(price * quantity),
                fee_asset: fee_asset(row, &canonical_symbol),
                fee_amount: decimal_to_f64(row.get("fee_amount_quote"))
                    .or_else(|| decimal_to_f64(row.get("fee_amount_base")))
                    .map(f64::abs),
                fee_rate: None,
                realized_pnl: None,
                filled_at: timestamp_millis(row.get("executed_at")).unwrap_or_else(Utc::now),
                received_at: Utc::now(),
            })
        })
        .collect()
}

fn parse_bitbank_balances(
    exchange_id: &ExchangeId,
    tenant_id: TenantId,
    account_id: AccountId,
    requested_assets: &[String],
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeBalance>> {
    let requested = requested_assets
        .iter()
        .map(|asset| asset.trim().to_ascii_uppercase())
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let rows = value
        .get("data")
        .and_then(|data| data.get("assets"))
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitbank assets response missing data.assets".to_string(),
        })?;
    let mut balances = Vec::new();
    for row in rows {
        let asset = row
            .get("asset")
            .and_then(Value::as_str)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitbank asset row missing asset".to_string(),
            })?
            .to_ascii_uppercase();
        if !requested.is_empty() && !requested.contains(&asset) {
            continue;
        }
        let available = decimal_to_f64(row.get("free_amount")).unwrap_or(0.0);
        let locked = decimal_to_f64(row.get("locked_amount")).unwrap_or(0.0);
        let total = decimal_to_f64(row.get("onhand_amount")).unwrap_or(available + locked);
        if total > 0.0 || !requested.is_empty() {
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
        market_type: MarketType::Spot,
        balances,
        observed_at: Utc::now(),
    }])
}

fn canonical_from_pair(pair: &str) -> ExchangeApiResult<CanonicalSymbol> {
    let (base, quote) = pair
        .split_once('_')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("bitbank pair {pair} is not base_quote"),
        })?;
    CanonicalSymbol::new(&base.to_ascii_uppercase(), &quote.to_ascii_uppercase())
        .map_err(validation_error)
}

fn parse_order_type(value: Option<&str>) -> OrderType {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn map_order_status(value: Option<&str>) -> OrderStatus {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase()
        .as_str()
    {
        "UNFILLED" => OrderStatus::Open,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FULLY_FILLED" => OrderStatus::Filled,
        "CANCELED_UNFILLED" | "CANCELED_PARTIALLY_FILLED" => OrderStatus::Cancelled,
        _ => OrderStatus::Unknown,
    }
}

fn parse_liquidity_role(value: Option<&str>) -> LiquidityRole {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "maker" => LiquidityRole::Maker,
        "taker" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn timestamp_millis(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let millis = value.and_then(|value| match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    })?;
    Utc.timestamp_millis_opt(millis).single()
}

fn decimal_to_f64(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| match value {
        Value::String(text) => text.parse::<f64>().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    })
}

fn fee_asset(row: &Value, canonical_symbol: &CanonicalSymbol) -> Option<String> {
    if decimal_to_f64(row.get("fee_amount_quote")).unwrap_or(0.0) != 0.0 {
        return Some(canonical_symbol.quote_asset().to_string());
    }
    if decimal_to_f64(row.get("fee_amount_base")).unwrap_or(0.0) != 0.0 {
        return Some(canonical_symbol.base_asset().to_string());
    }
    None
}

fn validation_error(error: rustcta_types::ValidationError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn parse_side(value: &str) -> Option<OrderSide> {
    match value.trim().to_ascii_lowercase().as_str() {
        "buy" => Some(OrderSide::Buy),
        "sell" => Some(OrderSide::Sell),
        _ => None,
    }
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{
        assets_spec, batch_cancel_orders_spec, cancel_order_spec, open_orders_spec,
        place_limit_order_spec, query_order_spec, recent_fills_spec,
    };

    #[test]
    fn bitbank_request_specs_should_match_fixtures() {
        let place =
            place_limit_order_spec("btc_jpy", "buy", "3000000", "0.01", true).expect("place");
        let expected: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitbank/request_specs/place_limit_order.json"
        ))
        .expect("fixture");
        assert_eq!(place.method, expected["method"].as_str().unwrap());
        assert_eq!(place.path, expected["path"].as_str().unwrap());
        assert_eq!(
            serde_json::from_str::<Value>(&place.body).unwrap(),
            expected["body"]
        );

        let cancel = cancel_order_spec("btc_jpy", 1).expect("cancel");
        let expected_cancel: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitbank/request_specs/cancel_order.json"
        ))
        .expect("cancel fixture");
        assert_eq!(cancel.method, expected_cancel["method"].as_str().unwrap());
        assert_eq!(cancel.path, expected_cancel["path"].as_str().unwrap());
        assert_eq!(
            serde_json::from_str::<Value>(&cancel.body).unwrap(),
            expected_cancel["body"]
        );

        assert_eq!(assets_spec().path, "/v1/user/assets");

        let query = query_order_spec("btc_jpy", "123456").expect("query order");
        let expected_query: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitbank/request_specs/query_order.json"
        ))
        .expect("query fixture");
        assert_eq!(query.method, expected_query["method"].as_str().unwrap());
        assert_eq!(query.path, expected_query["path"].as_str().unwrap());
        assert_eq!(
            serde_json::to_value(&query.query).unwrap(),
            expected_query["query"]
        );

        let open = open_orders_spec("BTC_JPY").expect("open orders");
        let expected_open: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitbank/request_specs/get_open_orders.json"
        ))
        .expect("open fixture");
        assert_eq!(open.method, expected_open["method"].as_str().unwrap());
        assert_eq!(open.path, expected_open["path"].as_str().unwrap());
        assert_eq!(
            serde_json::to_value(&open.query).unwrap(),
            expected_open["query"]
        );

        let fills = recent_fills_spec("btc_jpy", Some("123457"), Some(100), None, None)
            .expect("recent fills");
        let expected_fills: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitbank/request_specs/get_recent_fills.json"
        ))
        .expect("fills fixture");
        assert_eq!(fills.method, expected_fills["method"].as_str().unwrap());
        assert_eq!(fills.path, expected_fills["path"].as_str().unwrap());
        assert_eq!(
            serde_json::to_value(&fills.query).unwrap(),
            expected_fills["query"]
        );
    }

    #[test]
    fn bitbank_batch_cancel_spec_should_match_fixture() {
        let request = batch_cancel_orders_spec("BTC_JPY", &[1, 2]).expect("batch cancel");
        let expected: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitbank/request_specs/batch_cancel_orders.json"
        ))
        .expect("batch cancel fixture");
        assert_eq!(request.method, expected["method"].as_str().unwrap());
        assert_eq!(request.path, expected["path"].as_str().unwrap());
        assert_eq!(
            serde_json::from_str::<Value>(&request.body).unwrap(),
            expected["body"]
        );

        let err = batch_cancel_orders_spec("btc_jpy", &[])
            .expect_err("empty batch cancel should be rejected");
        assert!(format!("{err:?}").contains("at least one order_id"));
    }
}
