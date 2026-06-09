#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, RequestContext,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::onetrading_symbol;
use super::private_parser::{
    parse_onetrading_open_orders, parse_onetrading_order, parse_onetrading_recent_fills,
};
use super::transport::bearer_request_spec;
use super::OneTradingGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const BALANCES_PATH: &str = "/account/balances";
pub const FEES_PATH: &str = "/account/fees";
pub const ORDERS_PATH: &str = "/account/orders";
pub const ORDER_BY_ID_PATH: &str = "/account/orders/<redacted:order_id>";
pub const ORDER_BY_CLIENT_ID_PATH: &str = "/account/orders/client/<redacted:order_id>";
pub const TRADES_PATH: &str = "/account/trades";

pub fn balances_request_spec_fixture() -> Value {
    bearer_request_spec("GET", BALANCES_PATH, json!({}), Value::Null)
}

pub fn fees_request_spec_fixture() -> Value {
    bearer_request_spec("GET", FEES_PATH, json!({}), Value::Null)
}

pub fn open_orders_request_spec_fixture() -> Value {
    bearer_request_spec(
        "GET",
        ORDERS_PATH,
        json!({
            "instrument_code": "BTC_EUR",
            "with_cancelled_and_rejected": "false",
            "with_just_filled_inactive": "false",
            "with_just_orders": "true",
            "max_page_size": "50"
        }),
        Value::Null,
    )
}

pub fn query_order_request_spec_fixture() -> Value {
    bearer_request_spec("GET", ORDER_BY_ID_PATH, json!({}), Value::Null)
}

pub fn recent_fills_request_spec_fixture() -> Value {
    bearer_request_spec(
        "GET",
        TRADES_PATH,
        json!({
            "instrument_code": "BTC_EUR",
            "max_page_size": "100"
        }),
        Value::Null,
    )
}

pub fn place_order_request_spec_fixture() -> Value {
    bearer_request_spec(
        "POST",
        ORDERS_PATH,
        json!({}),
        json!({
            "instrument_code": "BTC_EUR",
            "type": "LIMIT",
            "side": "BUY",
            "amount": "0.01",
            "price": "64000",
            "client_id": "<redacted:order_id>",
            "time_in_force": "GOOD_TILL_CANCELLED"
        }),
    )
}

pub fn cancel_order_request_spec_fixture() -> Value {
    bearer_request_spec("DELETE", ORDER_BY_ID_PATH, json!({}), Value::Null)
}

pub fn cancel_order_by_client_id_request_spec_fixture() -> Value {
    bearer_request_spec("DELETE", ORDER_BY_CLIENT_ID_PATH, json!({}), Value::Null)
}

pub fn cancel_all_orders_request_spec_fixture() -> Value {
    bearer_request_spec(
        "DELETE",
        ORDERS_PATH,
        json!({ "instrument_code": "BTC_EUR" }),
        Value::Null,
    )
}

pub fn query_order_endpoint(order_id: &str) -> ExchangeApiResult<String> {
    let order_id = non_empty(order_id, "order_id")?;
    Ok(format!(
        "/account/orders/{}",
        urlencoding::encode(order_id).into_owned()
    ))
}

pub fn open_orders_query(
    symbol: &rustcta_exchange_api::SymbolScope,
    limit: Option<u32>,
) -> HashMap<String, String> {
    HashMap::from([
        ("instrument_code".to_string(), onetrading_symbol(symbol)),
        (
            "with_cancelled_and_rejected".to_string(),
            "false".to_string(),
        ),
        ("with_just_filled_inactive".to_string(), "false".to_string()),
        ("with_just_orders".to_string(), "true".to_string()),
        (
            "max_page_size".to_string(),
            page_size(limit, 50).to_string(),
        ),
    ])
}

pub fn recent_fills_query(
    symbol: &rustcta_exchange_api::SymbolScope,
    limit: Option<u32>,
) -> HashMap<String, String> {
    HashMap::from([
        ("instrument_code".to_string(), onetrading_symbol(symbol)),
        (
            "max_page_size".to_string(),
            page_size(limit, 100).to_string(),
        ),
    ])
}

impl OneTradingGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "onetrading.query_order.client_order_id",
            });
        }
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "onetrading query_order requires exchange_order_id".to_string(),
            }
        })?;
        let endpoint = query_order_endpoint(order_id)?;
        let value = self
            .send_private_get("onetrading.query_order", &endpoint, HashMap::new())
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: Some(parse_onetrading_order(
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
            self.ensure_supported_market_type(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "onetrading get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let limit = request.page.as_ref().and_then(|page| page.limit);
        let value = self
            .send_private_get(
                "onetrading.get_open_orders",
                ORDERS_PATH,
                open_orders_query(symbol, limit),
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_onetrading_open_orders(&self.exchange_id, symbol, &value)?,
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
                operation: "onetrading.get_recent_fills.client_order_id",
            });
        }
        if request.exchange_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "onetrading.get_recent_fills.exchange_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "onetrading.get_recent_fills.from_trade_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "onetrading.get_recent_fills.time_window",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "onetrading get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let (tenant_id, account_id) =
            context_account("onetrading.get_recent_fills", &request.context)?;
        let limit = request
            .limit
            .or_else(|| request.page.as_ref().and_then(|page| page.limit));
        let value = self
            .send_private_get(
                "onetrading.get_recent_fills",
                TRADES_PATH,
                recent_fills_query(symbol, limit),
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_onetrading_recent_fills(
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
        endpoint: &str,
        query: HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        if !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        self.rest
            .send_private_get(operation, endpoint, &query)
            .await
    }
}

fn non_empty<'a>(value: &'a str, field: &str) -> ExchangeApiResult<&'a str> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("onetrading {field} cannot be empty"),
        });
    }
    Ok(value)
}

fn page_size(limit: Option<u32>, default: u32) -> u32 {
    limit.unwrap_or(default).clamp(1, 100)
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
        .ok_or(ExchangeApiError::Unsupported { operation })?;
    let account_id = context
        .account_id
        .clone()
        .ok_or(ExchangeApiError::Unsupported { operation })?;
    Ok((tenant_id, account_id))
}
