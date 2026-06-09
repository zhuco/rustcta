use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, PlaceOrderRequest,
    QueryOrderRequest, QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, RequestContext,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, OrderSide, OrderType, TenantId};
use serde_json::{json, Value};
use uuid::Uuid;

use super::parser::{parse_open_orders, parse_order_state, parse_recent_fills};
use super::signing::{build_bearer_private_headers, ZebpayPrivateHeaders};
use super::ZebpayGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const ZEBPAY_BALANCE_PATH: &str = "/wallet/balance";
pub const ZEBPAY_PLACE_ORDER_PATH: &str = "/orders";
pub const ZEBPAY_CANCEL_ORDER_PATH: &str = "/orders/{order_id}";
pub const ZEBPAY_CANCEL_ALL_ORDERS_PATH: &str = "/orders/CancelAll";
pub const ZEBPAY_QUERY_ORDER_PATH: &str = "/orders";
pub const ZEBPAY_OPEN_ORDERS_PATH: &str = "/orders";
pub const ZEBPAY_RECENT_FILLS_PATH: &str = "/orders/{order_id}/fills";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZebpayPrivateRequestSpec {
    pub method: &'static str,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub headers: ZebpayPrivateHeaders,
    pub body: Option<Value>,
}

pub fn build_private_request_spec(
    method: &'static str,
    path: impl Into<String>,
    body: Option<Value>,
    client_id: &str,
    access_token: &str,
    timestamp: &str,
    request_id: &str,
) -> ExchangeApiResult<ZebpayPrivateRequestSpec> {
    Ok(ZebpayPrivateRequestSpec {
        method,
        path: path.into(),
        query: Vec::new(),
        headers: build_bearer_private_headers(client_id, access_token, timestamp, request_id)?,
        body,
    })
}

pub fn build_private_get_request_spec(
    path: impl Into<String>,
    query: Vec<(String, String)>,
    client_id: &str,
    access_token: &str,
    timestamp: &str,
    request_id: &str,
) -> ExchangeApiResult<ZebpayPrivateRequestSpec> {
    Ok(ZebpayPrivateRequestSpec {
        method: "GET",
        path: path.into(),
        query,
        headers: build_bearer_private_headers(client_id, access_token, timestamp, request_id)?,
        body: None,
    })
}

pub fn query_order_query(trade_pair: &str, order_id: &str) -> Vec<(String, String)> {
    vec![
        ("trade_pair".to_string(), trade_pair.to_string()),
        ("orderid".to_string(), order_id.to_string()),
        ("page".to_string(), "1".to_string()),
        ("limit".to_string(), "100".to_string()),
    ]
}

pub fn open_orders_query(trade_pair: &str, limit: Option<u32>) -> Vec<(String, String)> {
    vec![
        ("trade_pair".to_string(), trade_pair.to_string()),
        ("status".to_string(), "pending".to_string()),
        ("orderid".to_string(), "0".to_string()),
        ("page".to_string(), "1".to_string()),
        (
            "limit".to_string(),
            limit.unwrap_or(500).clamp(1, 500).to_string(),
        ),
    ]
}

pub fn recent_fills_path(order_id: &str) -> String {
    ZEBPAY_RECENT_FILLS_PATH.replace("{order_id}", order_id)
}

pub fn request_spec_body_from_order(request: &PlaceOrderRequest) -> Value {
    json!({
        "trade_pair": request.symbol.exchange_symbol.symbol,
        "side": order_side_as_str(request.side),
        "size": request.quantity,
        "price": request.price.clone().unwrap_or_default(),
        "tradeType": 1,
        "platform": "API_Trading"
    })
}

fn order_side_as_str(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "bid",
        OrderSide::Sell => "ask",
    }
}

pub fn zebpay_supports_order_type(order_type: OrderType) -> bool {
    matches!(order_type, OrderType::Limit)
}

impl ZebpayGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "zebpay.query_order.client_order_id",
            });
        }
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|order_id| !order_id.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "zebpay query_order requires exchange_order_id".to_string(),
            })?;
        let value = self
            .send_private_get(
                "zebpay.query_order",
                ZEBPAY_QUERY_ORDER_PATH,
                query_order_query(&request.symbol.exchange_symbol.symbol, order_id),
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .ok()
            .filter(|order| order.exchange_order_id.as_deref() == Some(order_id))
            .or_else(|| {
                parse_open_orders(&self.exchange_id, Some(&request.symbol), &value)
                    .ok()
                    .and_then(|orders| {
                        orders
                            .iter()
                            .find(|order| order.exchange_order_id.as_deref() == Some(order_id))
                            .cloned()
                            .or_else(|| (orders.len() == 1).then(|| orders[0].clone()))
                    })
            });
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order,
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
                message: "zebpay get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let value = self
            .send_private_get(
                "zebpay.get_open_orders",
                ZEBPAY_OPEN_ORDERS_PATH,
                open_orders_query(
                    &symbol.exchange_symbol.symbol,
                    request.page.as_ref().and_then(|page| page.limit),
                ),
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_open_orders(&self.exchange_id, Some(symbol), &value)?,
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
                operation: "zebpay.get_recent_fills.client_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "zebpay.get_recent_fills.from_trade_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "zebpay.get_recent_fills.time_window",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "zebpay get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|order_id| !order_id.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "zebpay get_recent_fills requires exchange_order_id".to_string(),
            })?;
        let (tenant_id, account_id) = context_account("zebpay.get_recent_fills", &request.context)?;
        let value = self
            .send_private_get(
                "zebpay.get_recent_fills",
                &recent_fills_path(order_id),
                Vec::new(),
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    async fn send_private_get(
        &self,
        operation: &'static str,
        path: &str,
        query: Vec<(String, String)>,
    ) -> ExchangeApiResult<Value> {
        let (client_id, access_token) = self.private_credentials(operation)?;
        let timestamp = chrono::Utc::now().timestamp_millis().to_string();
        let request_id = Uuid::new_v4().to_string();
        let spec = build_private_get_request_spec(
            path,
            query,
            client_id,
            access_token,
            &timestamp,
            &request_id,
        )?;
        self.rest.send_private_request(&spec).await
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.private_rest_configured() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok((
            self.config.client_id.trim(),
            self.config.access_token.trim(),
        ))
    }
}

fn context_account(
    operation: &'static str,
    context: &RequestContext,
) -> ExchangeApiResult<(TenantId, AccountId)> {
    let tenant_id = context
        .tenant_id
        .clone()
        .ok_or(ExchangeApiError::Unsupported { operation })?;
    let account_id = context
        .account_id
        .clone()
        .ok_or(ExchangeApiError::Unsupported { operation })?;
    Ok((tenant_id, account_id))
}
