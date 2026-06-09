#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{json, Value};

use super::parser::arkham_symbol;
use super::private_parser::{
    parse_arkham_open_orders, parse_arkham_order, parse_arkham_recent_fills,
};
use super::transport::{signed_get_request_spec, signed_json_request_spec};
use super::ArkhamGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const BALANCES_PATH: &str = "/account/balances";
pub const POSITIONS_PATH: &str = "/account/positions";
pub const FEES_PATH: &str = "/account/fees";
pub const PLACE_ORDER_PATH: &str = "/orders/new";
pub const CANCEL_ORDER_PATH: &str = "/orders/cancel";
pub const CANCEL_ALL_ORDERS_PATH: &str = "/orders/cancel/all";
pub const OPEN_ORDERS_PATH: &str = "/orders";
pub const QUERY_ORDER_PATH: &str = "/orders/{id}";
pub const RECENT_FILLS_PATH: &str = "/trades";

pub fn balances_request_spec_fixture() -> Value {
    signed_json_request_spec("GET", BALANCES_PATH, Value::String(String::new()))
}

pub fn positions_request_spec_fixture() -> Value {
    signed_json_request_spec("GET", POSITIONS_PATH, Value::String(String::new()))
}

pub fn place_order_request_spec_fixture() -> Value {
    signed_json_request_spec(
        "POST",
        PLACE_ORDER_PATH,
        json!({
            "symbol": "BTC_USDT",
            "side": "buy",
            "type": "limitGtc",
            "size": "0.01",
            "price": "64000",
            "clientOrderId": "<redacted:order_id>"
        }),
    )
}

pub fn cancel_order_request_spec_fixture() -> Value {
    signed_json_request_spec(
        "POST",
        CANCEL_ORDER_PATH,
        json!({
            "orderId": 3694872060678_u64
        }),
    )
}

pub fn cancel_all_orders_request_spec_fixture() -> Value {
    signed_json_request_spec("POST", CANCEL_ALL_ORDERS_PATH, json!({}))
}

pub fn query_order_request_spec_fixture() -> Value {
    signed_get_request_spec("/orders/<redacted:order_id>", json!({}))
}

pub fn open_orders_request_spec_fixture() -> Value {
    signed_get_request_spec(
        OPEN_ORDERS_PATH,
        json!({
            "symbol": "BTC_USDT",
            "status": "open",
            "limit": "50"
        }),
    )
}

pub fn recent_fills_request_spec_fixture() -> Value {
    signed_get_request_spec(
        RECENT_FILLS_PATH,
        json!({
            "symbol": "BTC_USDT",
            "limit": "100"
        }),
    )
}

pub fn query_order_endpoint(order_id: &str) -> ExchangeApiResult<String> {
    let order_id = non_empty(order_id, "order_id")?;
    Ok(format!("/orders/{}", urlencoding::encode(order_id)))
}

pub fn open_orders_query(
    symbol: &rustcta_exchange_api::SymbolScope,
    limit: Option<u32>,
) -> std::collections::HashMap<String, String> {
    std::collections::HashMap::from([
        ("symbol".to_string(), arkham_symbol(symbol)),
        ("status".to_string(), "open".to_string()),
        ("limit".to_string(), page_size(limit, 50).to_string()),
    ])
}

pub fn recent_fills_query(
    symbol: &rustcta_exchange_api::SymbolScope,
    limit: Option<u32>,
) -> std::collections::HashMap<String, String> {
    std::collections::HashMap::from([
        ("symbol".to_string(), arkham_symbol(symbol)),
        ("limit".to_string(), page_size(limit, 100).to_string()),
    ])
}

impl ArkhamGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: rustcta_exchange_api::QueryOrderRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "arkham.query_order.client_order_id",
            });
        }
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|order_id| !order_id.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "arkham query_order requires exchange_order_id".to_string(),
            })?;
        let endpoint = query_order_endpoint(order_id)?;
        let value = self
            .send_private_get(
                "arkham.query_order",
                &endpoint,
                std::collections::HashMap::new(),
            )
            .await?;
        Ok(rustcta_exchange_api::QueryOrderResponse {
            schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: Some(parse_arkham_order(
                &self.exchange_id,
                &request.symbol,
                &value,
            )?),
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: rustcta_exchange_api::OpenOrdersRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market_type(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "arkham get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let value = self
            .send_private_get(
                "arkham.get_open_orders",
                OPEN_ORDERS_PATH,
                open_orders_query(symbol, request.page.as_ref().and_then(|page| page.limit)),
            )
            .await?;
        Ok(rustcta_exchange_api::OpenOrdersResponse {
            schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_arkham_open_orders(&self.exchange_id, symbol, &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: rustcta_exchange_api::RecentFillsRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market_type(market_type)?;
        }
        reject_unmapped_fill_filters(&request)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "arkham get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let (tenant_id, account_id) = context_account(&request.context)?;
        let limit = request
            .limit
            .or_else(|| request.page.as_ref().and_then(|page| page.limit));
        let value = self
            .send_private_get(
                "arkham.get_recent_fills",
                RECENT_FILLS_PATH,
                recent_fills_query(symbol, limit),
            )
            .await?;
        Ok(rustcta_exchange_api::RecentFillsResponse {
            schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_arkham_recent_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                symbol,
                &value,
            )?,
        })
    }

    async fn send_private_get(
        &self,
        operation: &'static str,
        path: &str,
        query: std::collections::HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        if !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "arkham.private_rest_disabled",
            });
        }
        self.rest.send_signed_get(operation, path, &query).await
    }
}

fn reject_unmapped_fill_filters(
    request: &rustcta_exchange_api::RecentFillsRequest,
) -> ExchangeApiResult<()> {
    if request.client_order_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "arkham.get_recent_fills.client_order_id",
        });
    }
    if request.exchange_order_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "arkham.get_recent_fills.exchange_order_id",
        });
    }
    if request.from_trade_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "arkham.get_recent_fills.from_trade_id",
        });
    }
    if request.start_time.is_some() || request.end_time.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "arkham.get_recent_fills.time_window",
        });
    }
    Ok(())
}

fn context_account(
    context: &rustcta_exchange_api::RequestContext,
) -> ExchangeApiResult<(
    rustcta_exchange_api::TenantId,
    rustcta_exchange_api::AccountId,
)> {
    let tenant_id = context
        .tenant_id
        .clone()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "arkham get_recent_fills requires tenant_id".to_string(),
        })?;
    let account_id =
        context
            .account_id
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "arkham get_recent_fills requires account_id".to_string(),
            })?;
    Ok((tenant_id, account_id))
}

fn page_size(limit: Option<u32>, default: u32) -> u32 {
    limit.unwrap_or(default).clamp(1, 100)
}

fn non_empty<'a>(value: &'a str, field: &str) -> ExchangeApiResult<&'a str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("arkham {field} must not be empty"),
        });
    }
    Ok(trimmed)
}
