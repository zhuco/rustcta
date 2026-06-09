#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, RequestContext,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{
    btcturk_symbol, parse_btcturk_open_orders, parse_btcturk_order_state,
    parse_btcturk_recent_fills,
};
use super::transport::{signed_get_request_spec, signed_json_request_spec};
use super::BtcTurkGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const BALANCES_PATH: &str = "/api/v1/users/balances";
pub const PLACE_ORDER_PATH: &str = "/api/v1/order";
pub const CANCEL_ORDER_PATH: &str = "/api/v1/order";
pub const OPEN_ORDERS_PATH: &str = "/api/v1/openOrders";
pub const QUERY_ORDER_PATH: &str = "/api/v1/order/{orderId}";
pub const RECENT_FILLS_PATH: &str = "/api/v1/users/transactions/trade";

pub fn place_order_request_spec_fixture() -> Value {
    signed_json_request_spec(
        "POST",
        PLACE_ORDER_PATH,
        json!({
            "quantity": "0.01",
            "price": "2500000",
            "stopPrice": "0",
            "newOrderClientId": "offline-fixture",
            "orderMethod": "limit",
            "orderType": "buy",
            "pairSymbol": "BTCTRY"
        }),
    )
}

pub fn cancel_order_request_spec_fixture() -> Value {
    signed_json_request_spec(
        "DELETE",
        CANCEL_ORDER_PATH,
        json!({
            "id": "123456789",
            "pairSymbol": "BTCTRY"
        }),
    )
}

pub fn query_order_request_spec_fixture() -> Value {
    signed_get_request_spec("/api/v1/order/123456789", json!({}))
}

pub fn open_orders_request_spec_fixture() -> Value {
    signed_get_request_spec(
        OPEN_ORDERS_PATH,
        json!({
            "pairSymbol": "BTCTRY"
        }),
    )
}

pub fn recent_fills_request_spec_fixture() -> Value {
    signed_get_request_spec(
        RECENT_FILLS_PATH,
        json!({
            "pairSymbol": "BTCTRY",
            "orderId": "123456789"
        }),
    )
}

impl BtcTurkGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "btcturk.query_order.client_order_id",
            });
        }
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "btcturk query_order requires exchange_order_id".to_string(),
            })?;
        let path = query_order_path(order_id)?;
        let (api_key, api_secret) = self.private_credentials("btcturk.query_order")?;
        let value = self
            .rest
            .send_signed_get(api_key, api_secret, &path, &HashMap::new())
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: Some(parse_btcturk_order_state(
                &self.exchange_id,
                Some(&request.symbol),
                &value,
            )?),
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
        if request.page.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "btcturk.get_open_orders.pagination",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "btcturk get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (api_key, api_secret) = self.private_credentials("btcturk.get_open_orders")?;
        let value = self
            .rest
            .send_signed_get(
                api_key,
                api_secret,
                OPEN_ORDERS_PATH,
                &pair_symbol_params(symbol),
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_btcturk_open_orders(&self.exchange_id, Some(symbol), &value)?,
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
                operation: "btcturk.get_recent_fills.client_order_id",
            });
        }
        if request.from_trade_id.is_some()
            || request.start_time.is_some()
            || request.end_time.is_some()
            || request.page.is_some()
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "btcturk.get_recent_fills.filters",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "btcturk get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let (api_key, api_secret) = self.private_credentials("btcturk.get_recent_fills")?;
        let value = self
            .rest
            .send_signed_get(
                api_key,
                api_secret,
                RECENT_FILLS_PATH,
                &recent_fills_params(symbol, request.exchange_order_id.as_deref(), request.limit),
            )
            .await?;
        let mut fills =
            parse_btcturk_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        if let Some(limit) = request.limit {
            fills.truncate(limit as usize);
        }
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.enabled_private_rest {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        let api_key = self
            .config
            .api_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let api_secret = self
            .config
            .api_secret
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        Ok((api_key, api_secret))
    }

    fn context_account(
        &self,
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
                    message: "btcturk recent fills require context.tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "btcturk recent fills require context.account_id".to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

fn query_order_path(exchange_order_id: &str) -> ExchangeApiResult<String> {
    let order_id = exchange_order_id.trim();
    if order_id.is_empty() || order_id.contains('/') || order_id.contains('?') {
        return Err(ExchangeApiError::InvalidRequest {
            message: "btcturk query_order requires a plain exchange_order_id".to_string(),
        });
    }
    Ok(format!("/api/v1/order/{}", urlencoding::encode(order_id)))
}

fn pair_symbol_params(symbol: &rustcta_exchange_api::SymbolScope) -> HashMap<String, String> {
    HashMap::from([("pairSymbol".to_string(), btcturk_symbol(symbol))])
}

fn recent_fills_params(
    symbol: &rustcta_exchange_api::SymbolScope,
    exchange_order_id: Option<&str>,
    limit: Option<u32>,
) -> HashMap<String, String> {
    let mut params = pair_symbol_params(symbol);
    if let Some(order_id) = exchange_order_id.filter(|value| !value.trim().is_empty()) {
        params.insert("orderId".to_string(), order_id.trim().to_string());
    }
    if let Some(limit) = limit {
        params.insert("limit".to_string(), limit.to_string());
    }
    params
}
