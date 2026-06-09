use rustcta_exchange_api::{
    CancelOrderRequest, ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, RequestContext, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::OrderSide;

use super::parser::{
    normalize_yobit_symbol, parse_open_orders, parse_order_state, parse_recent_fills,
};
use super::signing::{build_private_headers, form_encode, YobitPrivateHeaders};
use super::YobitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const YOBIT_TAPI_PATH: &str = "/tapi/";
pub const YOBIT_BALANCE_METHOD: &str = "getInfo";
pub const YOBIT_PLACE_ORDER_METHOD: &str = "Trade";
pub const YOBIT_CANCEL_ORDER_METHOD: &str = "CancelOrder";
pub const YOBIT_QUERY_ORDER_METHOD: &str = "OrderInfo";
pub const YOBIT_OPEN_ORDERS_METHOD: &str = "ActiveOrders";
pub const YOBIT_RECENT_FILLS_METHOD: &str = "TradeHistory";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YobitPrivateRequestSpec {
    pub method: &'static str,
    pub path: &'static str,
    pub headers: YobitPrivateHeaders,
    pub body: String,
}

pub fn build_private_request_spec(
    params: &[(&str, String)],
    nonce: u64,
    api_key: &str,
    api_secret: &str,
) -> ExchangeApiResult<YobitPrivateRequestSpec> {
    let mut body_params = params.to_vec();
    body_params.push(("nonce", nonce.to_string()));
    let body = form_encode(&body_params);
    let headers = build_private_headers(api_key, api_secret, &body)?;
    Ok(YobitPrivateRequestSpec {
        method: "POST",
        path: YOBIT_TAPI_PATH,
        headers,
        body,
    })
}

pub fn request_spec_params_from_order(request: &PlaceOrderRequest) -> Vec<(&'static str, String)> {
    vec![
        ("method", YOBIT_PLACE_ORDER_METHOD.to_string()),
        ("pair", request.symbol.exchange_symbol.symbol.clone()),
        ("type", order_side_as_str(request.side).to_string()),
        ("rate", request.price.clone().unwrap_or_default()),
        ("amount", request.quantity.clone()),
    ]
}

pub fn request_spec_params_from_cancel(
    request: &CancelOrderRequest,
) -> Vec<(&'static str, String)> {
    vec![
        ("method", YOBIT_CANCEL_ORDER_METHOD.to_string()),
        (
            "order_id",
            request.exchange_order_id.clone().unwrap_or_default(),
        ),
    ]
}

pub fn query_order_params(
    request: &QueryOrderRequest,
) -> ExchangeApiResult<Vec<(&'static str, String)>> {
    let order_id = request
        .exchange_order_id
        .as_deref()
        .filter(|order_id| !order_id.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "yobit query_order requires exchange_order_id".to_string(),
        })?;
    Ok(vec![
        ("method", YOBIT_QUERY_ORDER_METHOD.to_string()),
        ("order_id", order_id.trim().to_string()),
    ])
}

fn order_side_as_str(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

pub fn balance_params() -> Vec<(&'static str, String)> {
    vec![("method", YOBIT_BALANCE_METHOD.to_string())]
}

pub fn open_orders_params(pair: &str) -> Vec<(&'static str, String)> {
    vec![
        ("method", YOBIT_OPEN_ORDERS_METHOD.to_string()),
        ("pair", pair.to_string()),
    ]
}

pub fn recent_fills_params(pair: &str) -> Vec<(&'static str, String)> {
    vec![
        ("method", YOBIT_RECENT_FILLS_METHOD.to_string()),
        ("pair", pair.to_string()),
        ("count", "100".to_string()),
    ]
}

impl YobitGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "yobit.query_order.client_order_id",
            });
        }
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "yobit query_order requires exchange_order_id".to_string(),
            }
        })?;
        let value = self
            .send_private_tapi("yobit.query_order", query_order_params(&request)?)
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
                message: "yobit get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let pair = normalize_yobit_symbol(&symbol.exchange_symbol.symbol)?;
        let value = self
            .send_private_tapi("yobit.get_open_orders", open_orders_params(&pair))
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
                operation: "yobit.get_recent_fills.client_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "yobit.get_recent_fills.from_trade_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "yobit.get_recent_fills.time_window",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "yobit get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account("yobit.get_recent_fills", &request.context)?;
        let pair = normalize_yobit_symbol(&symbol.exchange_symbol.symbol)?;
        let mut params = recent_fills_params(&pair);
        if let Some(limit) = request
            .limit
            .or_else(|| request.page.as_ref().and_then(|page| page.limit))
        {
            params.retain(|(key, _)| *key != "count");
            params.push(("count", limit.clamp(1, 100).to_string()));
        }
        let value = self
            .send_private_tapi("yobit.get_recent_fills", params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    async fn send_private_tapi(
        &self,
        operation: &'static str,
        params: Vec<(&'static str, String)>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        let nonce = chrono::Utc::now()
            .timestamp_millis()
            .try_into()
            .unwrap_or_default();
        let spec = build_private_request_spec(&params, nonce, api_key, api_secret)?;
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
