#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::Value;

use super::parser::{apex_trade_symbol, parse_open_orders, parse_order, parse_recent_fills};
use super::transport::{signed_get_request_spec, ApexPrivateAuth};
use super::ApexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const ACCOUNT_PATH: &str = "/api/v3/account";
pub const OPEN_ORDERS_PATH: &str = "/api/v3/open-orders";
pub const FILLS_PATH: &str = "/api/v3/fills";
pub const ORDER_PATH: &str = "/api/v3/order";
pub const CREATE_ORDER_PATH: &str = "/api/v3/order";
pub const CANCEL_ORDER_PATH: &str = "/api/v3/delete-order";

pub const PRIVATE_REST_DISABLED: &str = "apex.private_rest_disabled";

impl ApexGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "apex.query_order.client_order_id",
            });
        }
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "apex query_order requires exchange_order_id".to_string(),
            })?;
        let value = self
            .send_private_get(
                "apex.query_order",
                ORDER_PATH,
                query_order_params(&request.symbol, order_id),
            )
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: Some(parse_order(&self.exchange_id, &request.symbol, &value)?),
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market_type(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "apex get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let limit = request.page.as_ref().and_then(|page| page.limit);
        let value = self
            .send_private_get(
                "apex.get_open_orders",
                OPEN_ORDERS_PATH,
                symbol_page_params(symbol, limit),
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
            self.ensure_supported_market_type(market_type)?;
        }
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "apex.get_recent_fills.client_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "apex.get_recent_fills.from_trade_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "apex.get_recent_fills.time_window",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "apex get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "apex get_recent_fills requires context.tenant_id".to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "apex get_recent_fills requires context.account_id".to_string(),
                })?;
        let limit = request
            .limit
            .or_else(|| request.page.as_ref().and_then(|page| page.limit));
        let mut params = symbol_page_params(symbol, limit);
        if let Some(order_id) = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        let value = self
            .send_private_get("apex.get_recent_fills", FILLS_PATH, params)
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
        params: HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let auth = self.private_auth(operation)?;
        self.rest.send_signed_get(path, &params, &auth).await
    }

    fn private_auth(&self, _operation: &'static str) -> ExchangeApiResult<ApexPrivateAuth<'_>> {
        if !self.config.api_key_auth_available() {
            return Err(ExchangeApiError::Unsupported {
                operation: PRIVATE_REST_DISABLED,
            });
        }
        Ok(ApexPrivateAuth {
            api_key: self.config.api_key.as_deref().unwrap_or_default(),
            api_secret: self.config.api_secret.as_deref().unwrap_or_default(),
            passphrase: self.config.passphrase.as_deref().unwrap_or_default(),
        })
    }
}

pub fn query_order_params(symbol: &SymbolScope, order_id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("id".to_string(), order_id.to_string()),
        (
            "symbol".to_string(),
            apex_trade_symbol(&symbol.exchange_symbol.symbol),
        ),
    ])
}

pub fn symbol_page_params(symbol: &SymbolScope, limit: Option<u32>) -> HashMap<String, String> {
    HashMap::from([
        (
            "limit".to_string(),
            limit.unwrap_or(100).min(100).max(1).to_string(),
        ),
        ("page".to_string(), "1".to_string()),
        (
            "symbol".to_string(),
            apex_trade_symbol(&symbol.exchange_symbol.symbol),
        ),
    ])
}

pub fn query_order_request_spec_fixture() -> Value {
    signed_get_request_spec("/api/v3/order?id=offline-order-id&symbol=BTC-USDT")
}

pub fn open_orders_request_spec_fixture() -> Value {
    signed_get_request_spec("/api/v3/open-orders?limit=100&page=1&symbol=BTC-USDT")
}

pub fn recent_fills_request_spec_fixture() -> Value {
    signed_get_request_spec("/api/v3/fills?limit=100&page=1&symbol=BTC-USDT")
}
