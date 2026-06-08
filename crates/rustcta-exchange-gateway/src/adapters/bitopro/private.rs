#![allow(dead_code)]

use std::collections::HashMap;

use chrono::Utc;
use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, ExchangeApiError, ExchangeApiResult, OpenOrdersRequest,
    OpenOrdersResponse, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderType, TimeInForce};
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
