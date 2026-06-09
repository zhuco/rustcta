#![allow(dead_code)]

use std::collections::HashMap;

use chrono::Utc;
use reqwest::Method;
use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, PlaceOrderRequest,
    PlaceOrderResponse, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce};
use serde_json::{json, Value};

use super::parser::normalize_bitopro_pair;
use super::private_parser::{
    parse_balances, parse_open_orders, parse_order_state, parse_recent_fills,
};
use super::BitoproGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitoproRequestSpec {
    pub operation: String,
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub body: Option<String>,
}

pub fn balances_spec() -> BitoproRequestSpec {
    BitoproRequestSpec {
        operation: "bitopro.get_balances".to_string(),
        method: "GET".to_string(),
        path: "/accounts/balance".to_string(),
        query: Vec::new(),
        body: None,
    }
}

pub fn query_order_spec(pair: &str, order_id: &str) -> ExchangeApiResult<BitoproRequestSpec> {
    Ok(BitoproRequestSpec {
        operation: "bitopro.query_order".to_string(),
        method: "GET".to_string(),
        path: format!("/orders/{}/{}", normalize_bitopro_pair(pair)?, order_id),
        query: Vec::new(),
        body: None,
    })
}

pub fn open_orders_spec(pair: Option<&str>) -> ExchangeApiResult<BitoproRequestSpec> {
    let query = pair
        .map(|pair| normalize_bitopro_pair(pair).map(|pair| vec![("pair".to_string(), pair)]))
        .transpose()?
        .unwrap_or_default();
    Ok(BitoproRequestSpec {
        operation: "bitopro.get_open_orders".to_string(),
        method: "GET".to_string(),
        path: "/orders/open".to_string(),
        query,
        body: None,
    })
}

pub fn recent_fills_spec(pair: &str, limit: u32) -> ExchangeApiResult<BitoproRequestSpec> {
    Ok(BitoproRequestSpec {
        operation: "bitopro.get_recent_fills".to_string(),
        method: "GET".to_string(),
        path: format!("/orders/trades/{}", normalize_bitopro_pair(pair)?),
        query: vec![("limit".to_string(), limit.min(1000).to_string())],
        body: None,
    })
}

pub fn place_limit_order_spec(
    pair: &str,
    side: OrderSide,
    price: &str,
    amount: &str,
    timestamp_millis: i64,
    post_only: bool,
    client_id: Option<u64>,
) -> ExchangeApiResult<BitoproRequestSpec> {
    let mut body = json!({
        "action": side_text(side),
        "amount": amount,
        "price": price,
        "timestamp": timestamp_millis,
        "type": "LIMIT",
        "timeInForce": if post_only { "POST_ONLY" } else { "GTC" },
    });
    if let Some(client_id) = client_id {
        body["clientId"] = json!(client_id);
    }
    body_spec(
        "bitopro.place_order",
        "POST",
        &format!("/orders/{}", normalize_bitopro_pair(pair)?),
        body,
    )
}

pub fn cancel_order_spec(pair: &str, order_id: &str) -> ExchangeApiResult<BitoproRequestSpec> {
    Ok(BitoproRequestSpec {
        operation: "bitopro.cancel_order".to_string(),
        method: "DELETE".to_string(),
        path: format!("/orders/{}/{}", normalize_bitopro_pair(pair)?, order_id),
        query: Vec::new(),
        body: None,
    })
}

pub fn cancel_all_orders_spec(pair: Option<&str>) -> ExchangeApiResult<BitoproRequestSpec> {
    Ok(BitoproRequestSpec {
        operation: "bitopro.cancel_all_orders".to_string(),
        method: "DELETE".to_string(),
        path: match pair {
            Some(pair) => format!("/orders/{}", normalize_bitopro_pair(pair)?),
            None => "/orders/all".to_string(),
        },
        query: Vec::new(),
        body: None,
    })
}

pub fn batch_place_orders_spec(orders: Value) -> ExchangeApiResult<BitoproRequestSpec> {
    body_spec(
        "bitopro.batch_place_orders",
        "POST",
        "/orders/batch",
        orders,
    )
}

pub fn batch_cancel_orders_spec(body: Value) -> ExchangeApiResult<BitoproRequestSpec> {
    body_spec("bitopro.batch_cancel_orders", "PUT", "/orders", body)
}

fn body_spec(
    operation: &str,
    method: &str,
    path: &str,
    body: Value,
) -> ExchangeApiResult<BitoproRequestSpec> {
    Ok(BitoproRequestSpec {
        operation: operation.to_string(),
        method: method.to_string(),
        path: path.to_string(),
        query: Vec::new(),
        body: Some(serde_json::to_string(&body).map_err(|error| {
            ExchangeApiError::Serialization {
                message: error.to_string(),
            }
        })?),
    })
}

impl BitoproGatewayAdapter {
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
        let value = self
            .send_signed_get("bitopro.get_balances", "/accounts/balance", &HashMap::new())
            .await?;
        let balances = parse_balances(
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
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "bitopro query_order requires exchange_order_id".to_string(),
            }
        })?;
        let pair = normalize_bitopro_pair(&request.symbol.exchange_symbol.symbol)?;
        let value = self
            .send_signed_get(
                "bitopro.query_order",
                &format!("/orders/{pair}/{order_id}"),
                &HashMap::new(),
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
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
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "pair".to_string(),
                normalize_bitopro_pair(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get("bitopro.get_open_orders", "/orders/open", &params)
            .await?;
        let orders = parse_open_orders(&self.exchange_id, request.symbol.as_ref(), &value)?;
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitopro get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        if let Some(order_id) = request.exchange_order_id.as_ref() {
            params.insert("orderId".to_string(), order_id.clone());
        }
        if let Some(trade_id) = request.from_trade_id.as_ref() {
            params.insert("tradeId".to_string(), trade_id.clone());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "startTimestamp".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert(
                "endTimestamp".to_string(),
                end_time.timestamp_millis().to_string(),
            );
        }
        params.insert(
            "limit".to_string(),
            request.limit.unwrap_or(100).min(1000).to_string(),
        );
        let pair = normalize_bitopro_pair(&symbol.exchange_symbol.symbol)?;
        let value = self
            .send_signed_get(
                "bitopro.get_recent_fills",
                &format!("/orders/trades/{pair}"),
                &params,
            )
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let pair = normalize_bitopro_pair(&request.symbol.exchange_symbol.symbol)?;
        let body = bitopro_order_body(&request, false, "bitopro.place_order")?;
        let value = self
            .send_signed_json(
                "bitopro.place_order",
                Method::POST,
                &format!("/orders/{pair}"),
                &body,
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| ack_order_state(&self.exchange_id, &request, &value));
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let order_id =
            request
                .exchange_order_id
                .as_ref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitopro cancel_order requires exchange_order_id".to_string(),
                })?;
        let pair = normalize_bitopro_pair(&request.symbol.exchange_symbol.symbol)?;
        let value = self
            .send_signed_delete(
                "bitopro.cancel_order",
                &format!("/orders/{pair}/{order_id}"),
                &HashMap::new(),
            )
            .await?;
        let mut order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| cancelled_order_state(&self.exchange_id, &request, &value));
        order.status = OrderStatus::Cancelled;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.orders.is_empty() {
            return Ok(BatchPlaceOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                report: None,
            });
        }

        let body = request
            .orders
            .iter()
            .map(|request| bitopro_order_body(request, true, "bitopro.batch_place_orders"))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let value = self
            .send_signed_json(
                "bitopro.batch_place_orders",
                Method::POST,
                "/orders/batch",
                &Value::Array(body),
            )
            .await?;
        let orders = parse_bitopro_batch_place_response(&self.exchange_id, &request, &value);
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            report: None,
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

        let body = bitopro_batch_cancel_body(&request)?;
        let value = self
            .send_signed_json("bitopro.batch_cancel_orders", Method::PUT, "/orders", &body)
            .await?;
        let orders = parse_bitopro_batch_cancel_response(&self.exchange_id, &request, &value);
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: None,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let path = if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            format!(
                "/orders/{}",
                normalize_bitopro_pair(&symbol.exchange_symbol.symbol)?
            )
        } else {
            "/orders/all".to_string()
        };
        let value = self
            .send_signed_delete("bitopro.cancel_all_orders", &path, &HashMap::new())
            .await?;
        let orders =
            parse_bitopro_cancel_all_response(&self.exchange_id, request.symbol.as_ref(), &value);
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }
}

fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

pub fn time_in_force_text(time_in_force: Option<TimeInForce>, post_only: bool) -> &'static str {
    if post_only || matches!(time_in_force, Some(TimeInForce::GTX)) {
        "POST_ONLY"
    } else {
        "GTC"
    }
}

pub fn order_type_text(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "MARKET",
        OrderType::StopLimit => "STOP_LIMIT",
        _ => "LIMIT",
    }
}

pub fn now_millis() -> i64 {
    Utc::now().timestamp_millis()
}

fn bitopro_order_body(
    request: &PlaceOrderRequest,
    include_pair: bool,
    operation: &'static str,
) -> ExchangeApiResult<Value> {
    ensure_exchange_api_schema(request.schema_version)?;
    let pair = normalize_bitopro_pair(&request.symbol.exchange_symbol.symbol)?;
    if request.symbol.market_type != rustcta_types::MarketType::Spot {
        return Err(ExchangeApiError::Unsupported { operation });
    }
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported { operation });
    }
    if request.quote_quantity.is_some() {
        return Err(ExchangeApiError::Unsupported { operation });
    }
    let mut body = json!({
        "action": side_text(request.side),
        "type": order_type_text(request.order_type),
        "amount": request.quantity,
        "timestamp": now_millis(),
        "timeInForce": time_in_force_text(request.time_in_force, request.post_only),
    });
    if include_pair {
        body["pair"] = json!(pair);
    }
    if request.order_type != OrderType::Market {
        let price = request
            .price
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("{operation} requires price for non-market orders"),
            })?;
        body["price"] = json!(price);
    }
    if let Some(client_id) = &request.client_order_id {
        let client_id = client_id
            .parse::<u64>()
            .map_err(|_| ExchangeApiError::InvalidRequest {
                message: format!("{operation} client_order_id must be numeric"),
            })?;
        body["clientId"] = json!(client_id);
    }
    Ok(body)
}

fn bitopro_batch_cancel_body(request: &BatchCancelOrdersRequest) -> ExchangeApiResult<Value> {
    let mut by_pair = serde_json::Map::new();
    for cancel in &request.cancels {
        ensure_exchange_api_schema(cancel.schema_version)?;
        if cancel.symbol.market_type != rustcta_types::MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitopro.batch_cancel_orders.non_spot",
            });
        }
        let order_id =
            cancel
                .exchange_order_id
                .as_ref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitopro batch_cancel_orders requires exchange_order_id".to_string(),
                })?;
        if cancel.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitopro.batch_cancel_orders.client_order_id",
            });
        }
        let pair =
            normalize_bitopro_pair(&cancel.symbol.exchange_symbol.symbol)?.to_ascii_uppercase();
        by_pair
            .entry(pair)
            .or_insert_with(|| Value::Array(Vec::new()))
            .as_array_mut()
            .expect("inserted array")
            .push(Value::String(order_id.clone()));
    }
    Ok(Value::Object(by_pair))
}

fn parse_bitopro_batch_place_response(
    exchange_id: &rustcta_types::ExchangeId,
    request: &BatchPlaceOrdersRequest,
    value: &Value,
) -> Vec<rustcta_exchange_api::OrderState> {
    response_rows(value)
        .filter(|rows| rows.len() == request.orders.len())
        .map(|rows| {
            rows.iter()
                .zip(&request.orders)
                .map(|(row, request)| {
                    parse_order_state(exchange_id, Some(&request.symbol), row)
                        .unwrap_or_else(|_| ack_order_state(exchange_id, request, row))
                })
                .collect()
        })
        .unwrap_or_else(|| {
            request
                .orders
                .iter()
                .map(|request| ack_order_state(exchange_id, request, &Value::Null))
                .collect()
        })
}

fn parse_bitopro_batch_cancel_response(
    exchange_id: &rustcta_types::ExchangeId,
    request: &BatchCancelOrdersRequest,
    value: &Value,
) -> Vec<rustcta_exchange_api::OrderState> {
    response_rows(value)
        .filter(|rows| rows.len() == request.cancels.len())
        .map(|rows| {
            rows.iter()
                .zip(&request.cancels)
                .map(|(row, request)| {
                    parse_order_state(exchange_id, Some(&request.symbol), row)
                        .unwrap_or_else(|_| cancelled_order_state(exchange_id, request, row))
                })
                .collect()
        })
        .unwrap_or_else(|| {
            request
                .cancels
                .iter()
                .map(|request| cancelled_order_state(exchange_id, request, &Value::Null))
                .collect()
        })
}

fn response_rows(value: &Value) -> Option<&Vec<Value>> {
    value
        .as_array()
        .or_else(|| value.get("data").and_then(Value::as_array))
        .or_else(|| value.get("orders").and_then(Value::as_array))
}

fn parse_bitopro_cancel_all_response(
    exchange_id: &rustcta_types::ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> Vec<rustcta_exchange_api::OrderState> {
    if let Some(rows) = response_rows(value) {
        return rows
            .iter()
            .filter_map(|row| parse_order_state(exchange_id, symbol_hint, row).ok())
            .map(|mut order| {
                order.status = OrderStatus::Cancelled;
                order
            })
            .collect();
    }
    let data = value.get("data").unwrap_or(value);
    data.as_object()
        .map(|map| {
            map.values()
                .filter_map(Value::as_array)
                .flat_map(|rows| rows.iter())
                .filter_map(|row| parse_order_state(exchange_id, symbol_hint, row).ok())
                .map(|mut order| {
                    order.status = OrderStatus::Cancelled;
                    order
                })
                .collect()
        })
        .unwrap_or_default()
}

fn ack_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    let exchange_order_id = value
        .get("id")
        .or_else(|| value.get("orderId"))
        .and_then(json_value_as_string)
        .or_else(|| value.get("order_id").and_then(json_value_as_string));
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id,
        side: request.side,
        position_side: Some(request.position_side.unwrap_or(PositionSide::None)),
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: request.post_only,
        created_at: None,
        updated_at: Utc::now(),
    }
}

fn cancelled_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    request: &rustcta_exchange_api::CancelOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    let exchange_order_id = value
        .get("id")
        .or_else(|| value.get("orderId"))
        .and_then(json_value_as_string)
        .or_else(|| request.exchange_order_id.clone());
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id,
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: None,
        status: OrderStatus::Cancelled,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    }
}

fn json_value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}
