#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{
    luno_symbol, parse_luno_open_orders, parse_luno_order_state, parse_luno_recent_fills,
};
use super::transport::{basic_auth_form_request_spec, basic_auth_get_request_spec};
use super::LunoGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const BALANCES_PATH: &str = "/api/1/balance";
pub const PLACE_ORDER_PATH: &str = "/api/1/postorder";
pub const CANCEL_ORDER_PATH: &str = "/api/1/stoporder";
pub const OPEN_ORDERS_PATH: &str = "/api/1/listorders";
pub const QUERY_ORDER_PATH: &str = "/api/1/orders/{id}";
pub const RECENT_FILLS_PATH: &str = "/api/1/listtrades";

pub fn place_order_request_spec_fixture() -> Value {
    basic_auth_form_request_spec(
        "POST",
        PLACE_ORDER_PATH,
        json!({
            "pair": "XBTZAR",
            "type": "BID",
            "volume": "0.01",
            "price": "1200000",
            "base_account_id": "<redacted>",
            "counter_account_id": "<redacted>"
        }),
    )
}

pub fn cancel_order_request_spec_fixture() -> Value {
    basic_auth_form_request_spec(
        "POST",
        CANCEL_ORDER_PATH,
        json!({
            "order_id": "BXMC2CJ7HNB88U4"
        }),
    )
}

pub fn query_order_request_spec_fixture() -> Value {
    basic_auth_get_request_spec("/api/1/orders/BXMC2CJ7HNB88U4", json!({}))
}

pub fn open_orders_request_spec_fixture() -> Value {
    basic_auth_get_request_spec(
        OPEN_ORDERS_PATH,
        json!({
            "pair": "XBTZAR"
        }),
    )
}

pub fn recent_fills_request_spec_fixture() -> Value {
    basic_auth_get_request_spec(
        RECENT_FILLS_PATH,
        json!({
            "pair": "XBTZAR",
            "order_id": "BXMC2CJ7HNB88U4",
            "limit": "100"
        }),
    )
}

impl LunoGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "luno.query_order.client_order_id",
            });
        }
        let exchange_order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "luno query_order requires exchange_order_id".to_string(),
            }
        })?;
        let (api_key_id, api_key_secret) = self.private_credentials("luno.query_order")?;
        let path = query_order_path(exchange_order_id)?;
        let value = self
            .rest
            .send_private_get(api_key_id, api_key_secret, &path, &HashMap::new())
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: Some(parse_luno_order_state(
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "luno get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (api_key_id, api_key_secret) = self.private_credentials("luno.get_open_orders")?;
        let value = self
            .rest
            .send_private_get(
                api_key_id,
                api_key_secret,
                OPEN_ORDERS_PATH,
                &pair_params(symbol),
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_luno_open_orders(&self.exchange_id, Some(symbol), &value)?,
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
                operation: "luno.get_recent_fills.client_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "luno.get_recent_fills.from_trade_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "luno.get_recent_fills.time_window",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "luno get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account("luno.get_recent_fills", &request.context)?;
        let (api_key_id, api_key_secret) = self.private_credentials("luno.get_recent_fills")?;
        let value = self
            .rest
            .send_private_get(
                api_key_id,
                api_key_secret,
                RECENT_FILLS_PATH,
                &recent_fills_params(
                    symbol,
                    request.exchange_order_id.as_deref(),
                    request
                        .limit
                        .or_else(|| request.page.as_ref().and_then(|page| page.limit)),
                ),
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_luno_recent_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                symbol,
                &value,
            )?,
        })
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.enabled_private_rest {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        let api_key_id = self
            .config
            .api_key_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let api_key_secret = self
            .config
            .api_key_secret
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        Ok((api_key_id, api_key_secret))
    }

    fn context_account(
        &self,
        operation: &'static str,
        context: &rustcta_exchange_api::RequestContext,
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

fn query_order_path(exchange_order_id: &str) -> ExchangeApiResult<String> {
    let order_id = exchange_order_id.trim();
    if order_id.is_empty() || order_id.contains('/') || order_id.contains('?') {
        return Err(ExchangeApiError::InvalidRequest {
            message: "luno query_order requires a plain exchange_order_id".to_string(),
        });
    }
    Ok(format!("/api/1/orders/{}", urlencoding::encode(order_id)))
}

fn pair_params(symbol: &rustcta_exchange_api::SymbolScope) -> HashMap<String, String> {
    HashMap::from([("pair".to_string(), luno_symbol(symbol))])
}

fn recent_fills_params(
    symbol: &rustcta_exchange_api::SymbolScope,
    exchange_order_id: Option<&str>,
    limit: Option<u32>,
) -> HashMap<String, String> {
    let mut params = pair_params(symbol);
    if let Some(order_id) = exchange_order_id.filter(|value| !value.trim().is_empty()) {
        params.insert("order_id".to_string(), order_id.trim().to_string());
    }
    if let Some(limit) = limit {
        params.insert("limit".to_string(), limit.min(100).to_string());
    }
    params
}
