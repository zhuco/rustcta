use std::collections::BTreeMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, RequestContext,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::Value;

use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::request_spec::ActualHttpRequest;

use super::parser::normalize_hollaex_symbol;
use super::private_parser::{parse_open_orders, parse_order_ack, parse_recent_fills};
use super::signing::{rest_hmac_sha256_payload, sign_hmac_sha256_hex};
use super::HollaexGatewayAdapter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HollaexOfflineRequest {
    pub method: String,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub body: Option<Value>,
}

impl HollaexOfflineRequest {
    pub fn actual_http_request(&self) -> ActualHttpRequest {
        ActualHttpRequest::new(self.method.clone(), self.path.clone())
            .with_query(self.query.clone())
            .with_headers(self.headers.clone())
            .with_body(self.body.clone())
    }
}

pub fn build_get_balances_request_spec(
    api_key: &str,
    api_secret: &str,
    expires_s: u64,
) -> ExchangeApiResult<HollaexOfflineRequest> {
    signed_request(
        api_key,
        api_secret,
        "GET",
        "/v2/user/balance",
        "/v2/user/balance",
        BTreeMap::new(),
        None,
        expires_s,
    )
}

pub fn build_place_order_request_spec(
    api_key: &str,
    api_secret: &str,
    expires_s: u64,
) -> ExchangeApiResult<HollaexOfflineRequest> {
    let body = place_order_body_value()?;
    signed_request(
        api_key,
        api_secret,
        "POST",
        "/v2/order",
        "/v2/order",
        BTreeMap::new(),
        Some((place_order_json_body().to_string(), body)),
        expires_s,
    )
}

pub fn build_cancel_order_request_spec(
    api_key: &str,
    api_secret: &str,
    expires_s: u64,
) -> ExchangeApiResult<HollaexOfflineRequest> {
    let mut query = BTreeMap::new();
    query.insert("order_id".to_string(), "order-fixture-1".to_string());
    signed_request(
        api_key,
        api_secret,
        "DELETE",
        "/v2/order",
        "/v2/order?order_id=order-fixture-1",
        query,
        None,
        expires_s,
    )
}

pub fn build_query_order_request_spec(
    api_key: &str,
    api_secret: &str,
    expires_s: u64,
    order_id: &str,
) -> ExchangeApiResult<HollaexOfflineRequest> {
    let order_id = non_empty(order_id, "order_id")?;
    let mut query = BTreeMap::new();
    query.insert("order_id".to_string(), order_id.to_string());
    signed_request(
        api_key,
        api_secret,
        "GET",
        "/v2/order",
        &format!("/v2/order?order_id={}", urlencoding::encode(order_id)),
        query,
        None,
        expires_s,
    )
}

pub fn build_get_open_orders_request_spec(
    api_key: &str,
    api_secret: &str,
    expires_s: u64,
    symbol: &str,
    limit: Option<u32>,
) -> ExchangeApiResult<HollaexOfflineRequest> {
    let symbol = non_empty(symbol, "symbol")?;
    let mut query = BTreeMap::new();
    query.insert("symbol".to_string(), symbol.to_string());
    if let Some(limit) = limit {
        query.insert("limit".to_string(), hollaex_limit(Some(limit)).to_string());
    }
    let sign_path = path_with_query("/v2/orders", &query);
    signed_request(
        api_key,
        api_secret,
        "GET",
        "/v2/orders",
        &sign_path,
        query,
        None,
        expires_s,
    )
}

pub fn build_get_recent_fills_request_spec(
    api_key: &str,
    api_secret: &str,
    expires_s: u64,
    symbol: &str,
    limit: Option<u32>,
) -> ExchangeApiResult<HollaexOfflineRequest> {
    let symbol = non_empty(symbol, "symbol")?;
    let mut query = BTreeMap::new();
    query.insert("symbol".to_string(), symbol.to_string());
    query.insert("limit".to_string(), hollaex_limit(limit).to_string());
    let sign_path = path_with_query("/v2/user/trades", &query);
    signed_request(
        api_key,
        api_secret,
        "GET",
        "/v2/user/trades",
        &sign_path,
        query,
        None,
        expires_s,
    )
}

pub fn place_order_json_body() -> &'static str {
    r#"{"symbol":"xht-usdt","side":"sell","size":0.1,"type":"limit","price":1}"#
}

fn place_order_body_value() -> ExchangeApiResult<Value> {
    serde_json::from_str(place_order_json_body()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid hollaex fixture order body: {error}"),
        }
    })
}

impl HollaexGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "hollaex.query_order.client_order_id",
            });
        }
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "hollaex query_order requires exchange_order_id".to_string(),
            }
        })?;
        let value = self
            .send_private_get("hollaex.query_order", |api_key, api_secret, expires_s| {
                build_query_order_request_spec(api_key, api_secret, expires_s, order_id)
            })
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: Some(parse_order_ack(&self.exchange_id, request.symbol, &value)?),
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
                message: "hollaex get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let normalized_symbol = normalize_hollaex_symbol(symbol)?;
        let limit = request.page.as_ref().and_then(|page| page.limit);
        let value = self
            .send_private_get(
                "hollaex.get_open_orders",
                |api_key, api_secret, expires_s| {
                    build_get_open_orders_request_spec(
                        api_key,
                        api_secret,
                        expires_s,
                        &normalized_symbol,
                        limit,
                    )
                },
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_open_orders(&self.exchange_id, symbol, &value)?,
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
                operation: "hollaex.get_recent_fills.client_order_id",
            });
        }
        if request.exchange_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "hollaex.get_recent_fills.exchange_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "hollaex.get_recent_fills.from_trade_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "hollaex.get_recent_fills.time_window",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "hollaex get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let normalized_symbol = normalize_hollaex_symbol(symbol)?;
        let (tenant_id, account_id) =
            context_account("hollaex.get_recent_fills", &request.context)?;
        let value = self
            .send_private_get(
                "hollaex.get_recent_fills",
                |api_key, api_secret, expires_s| {
                    build_get_recent_fills_request_spec(
                        api_key,
                        api_secret,
                        expires_s,
                        &normalized_symbol,
                        request
                            .limit
                            .or_else(|| request.page.as_ref().and_then(|page| page.limit)),
                    )
                },
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    async fn send_private_get<F>(
        &self,
        operation: &'static str,
        build: F,
    ) -> ExchangeApiResult<Value>
    where
        F: FnOnce(&str, &str, u64) -> ExchangeApiResult<HollaexOfflineRequest>,
    {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        let expires_s = (chrono::Utc::now().timestamp() + 30)
            .try_into()
            .unwrap_or_default();
        let request = build(api_key, api_secret, expires_s)?;
        self.rest.send_private_request(&request).await
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.private_request_specs_enabled() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok((
            self.config.api_key.as_deref().unwrap_or_default().trim(),
            self.config.api_secret.as_deref().unwrap_or_default().trim(),
        ))
    }
}

fn signed_request(
    api_key: &str,
    api_secret: &str,
    method: &str,
    path: &str,
    sign_path: &str,
    query: BTreeMap<String, String>,
    body: Option<(String, Value)>,
    expires_s: u64,
) -> ExchangeApiResult<HollaexOfflineRequest> {
    if api_key.trim().is_empty() || api_secret.trim().is_empty() {
        return Err(ExchangeApiError::Unsupported {
            operation: "hollaex.private_request_spec_credentials",
        });
    }
    let body_text = body.as_ref().map(|(text, _)| text.as_str());
    let payload = rest_hmac_sha256_payload(method, sign_path, expires_s, body_text);
    let signature = sign_hmac_sha256_hex(api_secret, &payload)?;
    let mut headers = BTreeMap::new();
    headers.insert("api-key".to_string(), api_key.to_string());
    headers.insert("api-signature".to_string(), signature);
    headers.insert("api-expires".to_string(), expires_s.to_string());
    if body.is_some() {
        headers.insert("content-type".to_string(), "application/json".to_string());
    }
    Ok(HollaexOfflineRequest {
        method: method.to_string(),
        path: path.to_string(),
        query,
        headers,
        body: body.map(|(_, value)| value),
    })
}

fn context_account(
    operation: &'static str,
    context: &RequestContext,
) -> ExchangeApiResult<(
    rustcta_exchange_api::TenantId,
    rustcta_exchange_api::AccountId,
)> {
    let tenant_id = context
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

fn hollaex_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(50).clamp(1, 100)
}

fn path_with_query(path: &str, query: &BTreeMap<String, String>) -> String {
    if query.is_empty() {
        return path.to_string();
    }
    let query = query
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    format!("{path}?{query}")
}

fn non_empty<'a>(value: &'a str, field: &str) -> ExchangeApiResult<&'a str> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("hollaex private request requires {field}"),
        });
    }
    Ok(value)
}
