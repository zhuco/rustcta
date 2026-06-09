use rustcta_exchange_api::{
    CancelOrderRequest, ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, RequestContext, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::{
    normalize_p2b_symbol, parse_open_orders, parse_order_state, parse_recent_fills,
};
use super::signing::{build_private_headers, P2bPrivateHeaders};
use super::P2bGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const P2B_BALANCE_PATH: &str = "/api/v2/account/balances";
pub const P2B_PLACE_ORDER_PATH: &str = "/api/v2/order/new";
pub const P2B_CANCEL_ORDER_PATH: &str = "/api/v2/order/cancel";
pub const P2B_OPEN_ORDERS_PATH: &str = "/api/v2/orders";
pub const P2B_RECENT_FILLS_PATH: &str = "/api/v2/account/market_deals";
pub const P2B_QUERY_ORDER_PATH: &str = "/api/v2/account/order_history";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct P2bPrivateRequestSpec {
    pub method: &'static str,
    pub path: &'static str,
    pub headers: P2bPrivateHeaders,
    pub body: Value,
}

pub fn build_private_request_spec(
    path: &'static str,
    mut body: Value,
    nonce: u64,
    api_key: &str,
    api_secret: &str,
) -> ExchangeApiResult<P2bPrivateRequestSpec> {
    body["request"] = Value::String(path.to_string());
    body["nonce"] = Value::String(nonce.to_string());
    let headers = build_private_headers(api_key, api_secret, &body)?;
    Ok(P2bPrivateRequestSpec {
        method: "POST",
        path,
        headers,
        body,
    })
}

pub fn request_spec_body_from_order(request: &PlaceOrderRequest) -> Value {
    json!({
        "market": request.symbol.exchange_symbol.symbol,
        "side": order_side_as_str(request.side),
        "amount": request.quantity,
        "price": request.price.clone().unwrap_or_default()
    })
}

pub fn request_spec_body_from_cancel(request: &CancelOrderRequest) -> Value {
    json!({
        "market": request.symbol.exchange_symbol.symbol,
        "orderId": request.exchange_order_id.as_deref().unwrap_or_default()
    })
}

pub fn request_spec_body_from_query_order(request: &QueryOrderRequest) -> ExchangeApiResult<Value> {
    let order_id =
        request
            .exchange_order_id
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "p2b query_order requires exchange_order_id".to_string(),
            })?;
    Ok(json!({
        "market": normalize_p2b_symbol(&request.symbol.exchange_symbol.symbol)?,
        "orderId": order_id,
        "limit": "100",
        "offset": "0"
    }))
}

pub fn request_spec_body_from_open_orders(request: &OpenOrdersRequest) -> ExchangeApiResult<Value> {
    let symbol = request
        .symbol
        .as_ref()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "p2b get_open_orders requires symbol".to_string(),
        })?;
    Ok(json!({
        "market": normalize_p2b_symbol(&symbol.exchange_symbol.symbol)?,
        "limit": p2b_limit(request.page.as_ref().and_then(|page| page.limit)),
        "offset": "0"
    }))
}

pub fn request_spec_body_from_recent_fills(
    request: &RecentFillsRequest,
) -> ExchangeApiResult<Value> {
    let symbol = request
        .symbol
        .as_ref()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "p2b get_recent_fills requires symbol".to_string(),
        })?;
    let mut body = json!({
        "market": normalize_p2b_symbol(&symbol.exchange_symbol.symbol)?,
        "limit": p2b_limit(request.limit.or_else(|| request.page.as_ref().and_then(|page| page.limit))),
        "offset": "0"
    });
    if let Some(order_id) = request
        .exchange_order_id
        .as_deref()
        .filter(|order_id| !order_id.trim().is_empty())
    {
        body["orderId"] = Value::String(order_id.trim().to_string());
    }
    Ok(body)
}

impl P2bGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "p2b.query_order.client_order_id",
            });
        }
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "p2b query_order requires exchange_order_id".to_string(),
            }
        })?;
        let value = self
            .send_private_post(
                "p2b.query_order",
                P2B_QUERY_ORDER_PATH,
                request_spec_body_from_query_order(&request)?,
            )
            .await?;
        let orders = parse_open_orders(&self.exchange_id, Some(&request.symbol), &value)?;
        let order = orders
            .iter()
            .find(|order| order.exchange_order_id.as_deref() == Some(order_id))
            .cloned()
            .or_else(|| (orders.len() == 1).then(|| orders[0].clone()))
            .or_else(|| parse_order_state(&self.exchange_id, Some(&request.symbol), &value).ok());
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
                message: "p2b get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let value = self
            .send_private_post(
                "p2b.get_open_orders",
                P2B_OPEN_ORDERS_PATH,
                request_spec_body_from_open_orders(&request)?,
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
                operation: "p2b.get_recent_fills.client_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "p2b.get_recent_fills.from_trade_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "p2b.get_recent_fills.time_window",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "p2b get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account("p2b.get_recent_fills", &request.context)?;
        let value = self
            .send_private_post(
                "p2b.get_recent_fills",
                P2B_RECENT_FILLS_PATH,
                request_spec_body_from_recent_fills(&request)?,
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    async fn send_private_post(
        &self,
        operation: &'static str,
        path: &'static str,
        body: Value,
    ) -> ExchangeApiResult<Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        let nonce = chrono::Utc::now()
            .timestamp_millis()
            .try_into()
            .unwrap_or_default();
        let spec = build_private_request_spec(path, body, nonce, api_key, api_secret)?;
        self.rest.send_private_post(&spec).await
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.private_rest_configured() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok((self.config.api_key.trim(), self.config.api_secret.trim()))
    }

    fn context_account(
        &self,
        operation: &'static str,
        context: &RequestContext,
    ) -> ExchangeApiResult<(
        rustcta_exchange_api::TenantId,
        rustcta_exchange_api::AccountId,
    )> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("{operation} requires context.tenant_id"),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("{operation} requires context.account_id"),
                })?;
        Ok((tenant_id, account_id))
    }
}

fn p2b_limit(limit: Option<u32>) -> String {
    limit.unwrap_or(100).clamp(1, 100).to_string()
}

fn order_side_as_str(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

pub fn p2b_supports_order_type(order_type: OrderType) -> bool {
    match order_type {
        OrderType::Limit => true,
        OrderType::Market
        | OrderType::PostOnly
        | OrderType::IOC
        | OrderType::FOK
        | OrderType::StopMarket
        | OrderType::StopLimit => false,
    }
}
