use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, RequestContext,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{parse_open_orders, parse_order_state, parse_recent_fills};
use super::signing::signed_headers;
use super::BitbnsGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const BITBNS_QUERY_ORDER_METHOD: &str = "orderStatus";
pub const BITBNS_OPEN_ORDERS_METHOD: &str = "listOpenOrders";
pub const BITBNS_RECENT_FILLS_METHOD: &str = "listExecutedOrders";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbnsPrivateRequestSpec {
    pub method: &'static str,
    pub path: String,
    pub api_key: String,
    pub payload: String,
    pub signature: String,
    pub body: Value,
}

pub fn build_private_request_spec(
    method_name: &str,
    symbol: &str,
    body: Value,
    nonce: &str,
    api_key: &str,
    api_secret: &str,
) -> BitbnsPrivateRequestSpec {
    let symbol_path = format!("/{method_name}/{symbol}");
    let headers = signed_headers(api_key, api_secret, &symbol_path, &body, nonce);
    let api_key = header_value(&headers, "X-BITBNS-APIKEY");
    let payload = header_value(&headers, "X-BITBNS-PAYLOAD");
    let signature = header_value(&headers, "X-BITBNS-SIGNATURE");
    BitbnsPrivateRequestSpec {
        method: "POST",
        path: symbol_path,
        api_key,
        payload,
        signature,
        body,
    }
}

pub fn request_body_for_query_order(request: &QueryOrderRequest) -> ExchangeApiResult<Value> {
    let entry_id = request
        .exchange_order_id
        .as_deref()
        .filter(|entry_id| !entry_id.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bitbns query_order requires exchange_order_id".to_string(),
        })?;
    Ok(json!({ "entry_id": entry_id.trim() }))
}

pub fn request_body_for_open_orders(request: &OpenOrdersRequest) -> Value {
    let _ = request;
    json!({
        "page": 0
    })
}

pub fn request_body_for_recent_fills(request: &RecentFillsRequest) -> Value {
    let mut body = json!({
        "page": 0
    });
    if let Some(start_time) = request.start_time {
        body["since"] = Value::String(start_time.to_rfc3339());
    }
    body
}

impl BitbnsGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitbns.query_order.client_order_id",
            });
        }
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "bitbns query_order requires exchange_order_id".to_string(),
            }
        })?;
        let symbol = bitbns_private_symbol(&request.symbol.exchange_symbol.symbol)?;
        let value = self
            .send_private_post(
                "bitbns.query_order",
                BITBNS_QUERY_ORDER_METHOD,
                &symbol,
                request_body_for_query_order(&request)?,
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .ok()
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
                message: "bitbns get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let private_symbol = bitbns_private_symbol(&symbol.exchange_symbol.symbol)?;
        let value = self
            .send_private_post(
                "bitbns.get_open_orders",
                BITBNS_OPEN_ORDERS_METHOD,
                &private_symbol,
                request_body_for_open_orders(&request),
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
                operation: "bitbns.get_recent_fills.client_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitbns.get_recent_fills.from_trade_id",
            });
        }
        if request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitbns.get_recent_fills.end_time",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitbns get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account("bitbns.get_recent_fills", &request.context)?;
        let private_symbol = bitbns_private_symbol(&symbol.exchange_symbol.symbol)?;
        let value = self
            .send_private_post(
                "bitbns.get_recent_fills",
                BITBNS_RECENT_FILLS_METHOD,
                &private_symbol,
                request_body_for_recent_fills(&request),
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
        method_name: &str,
        symbol: &str,
        body: Value,
    ) -> ExchangeApiResult<Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        let nonce = Utc::now().timestamp_millis().to_string();
        let spec =
            build_private_request_spec(method_name, symbol, body, &nonce, api_key, api_secret);
        self.rest.send_private_post(&spec).await
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.private_rest_enabled() {
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

fn bitbns_private_symbol(exchange_symbol: &str) -> ExchangeApiResult<String> {
    let (base, quote) = super::parser::normalize_bitbns_symbol(exchange_symbol)?;
    Ok(if quote == "INR" {
        base
    } else {
        format!("{base}_{quote}")
    })
}

fn header_value(headers: &[(String, String)], key: &str) -> String {
    headers
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.clone())
        .unwrap_or_default()
}
