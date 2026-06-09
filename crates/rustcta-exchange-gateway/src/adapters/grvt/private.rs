#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{parse_grvt_fills, parse_grvt_orders, parse_grvt_single_order};
use super::signing::grvt_session_auth_headers;
use super::GrvtGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const AMEND_ORDER_UNSUPPORTED: &str =
    "grvt.amend_requires_session_renewal_eip712_signer_and_reconciliation";
pub const BULK_ORDERS_UNSUPPORTED: &str =
    "grvt.bulk_orders_requires_session_renewal_eip712_signer_partial_parser_and_dry_run_guard";
pub const ORDER_LIST_UNSUPPORTED: &str = "grvt.order_list_unsupported";
pub const POSITIONS_UNSUPPORTED: &str =
    "grvt.positions_requires_session_cookie_external_signer_subaccount_scope_parser_reconciliation";
pub const PRIVATE_REST_DISABLED: &str = "grvt.private_rest_disabled";
pub const QUERY_ORDER_PATH: &str = "/v1/order";
pub const OPEN_ORDERS_PATH: &str = "/v1/open_orders";
pub const RECENT_FILLS_PATH: &str = "/v1/fills";

impl GrvtGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "grvt query_order requires exchange_order_id".to_string(),
            })?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "grvt.query_order.client_order_id",
            });
        }
        let value = self
            .rest
            .send_session_post(
                QUERY_ORDER_PATH,
                &self.private_session_auth("grvt.query_order")?,
                query_order_body(order_id),
            )
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: Some(parse_grvt_single_order(
                &self.exchange_id,
                &request.symbol,
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
        self.ensure_optional_market_type(request.market_type)?;
        if request
            .page
            .as_ref()
            .is_some_and(|page| page.cursor.is_some())
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "grvt.get_open_orders.cursor",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "grvt get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let value = self
            .rest
            .send_session_post(
                OPEN_ORDERS_PATH,
                &self.private_session_auth("grvt.get_open_orders")?,
                scoped_symbol_body(symbol.exchange_symbol.symbol.as_str()),
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_grvt_orders(&self.exchange_id, symbol, &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_market_type(request.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "grvt.get_recent_fills.client_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "grvt.get_recent_fills.from_trade_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "grvt.get_recent_fills.time_window",
            });
        }
        if request
            .page
            .as_ref()
            .is_some_and(|page| page.cursor.is_some())
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "grvt.get_recent_fills.cursor",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "grvt get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let value = self
            .rest
            .send_session_post(
                RECENT_FILLS_PATH,
                &self.private_session_auth("grvt.get_recent_fills")?,
                recent_fills_body(
                    symbol.exchange_symbol.symbol.as_str(),
                    request.exchange_order_id.as_deref(),
                    request
                        .limit
                        .or_else(|| request.page.as_ref().and_then(|page| page.limit)),
                ),
            )
            .await?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "grvt get_recent_fills requires context.tenant_id".to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "grvt get_recent_fills requires context.account_id".to_string(),
                })?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_grvt_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    fn private_session_auth(
        &self,
        operation: &'static str,
    ) -> ExchangeApiResult<super::signing::GrvtSessionAuthHeaders> {
        if !self.config.private_session_available() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        grvt_session_auth_headers(
            self.config.session_cookie.as_deref(),
            self.config.account_id.as_deref(),
        )
        .map_err(|_| ExchangeApiError::Unsupported { operation })
    }
}

pub fn grvt_account_summary_request(sub_account_id: &str, request_id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "v1/account_summary",
        "params": {
            "sub_account_id": sub_account_id
        },
        "id": request_id
    })
}

pub fn query_order_body(order_id: &str) -> Value {
    json!({ "order_id": order_id })
}

pub fn scoped_symbol_body(instrument: &str) -> Value {
    json!({ "instrument": instrument })
}

pub fn recent_fills_body(
    instrument: &str,
    exchange_order_id: Option<&str>,
    limit: Option<u32>,
) -> Value {
    let mut body = serde_json::Map::new();
    body.insert("instrument".to_string(), json!(instrument));
    if let Some(order_id) = exchange_order_id.filter(|value| !value.trim().is_empty()) {
        body.insert("order_id".to_string(), json!(order_id));
    }
    if let Some(limit) = limit {
        body.insert("limit".to_string(), json!(limit));
    }
    Value::Object(body)
}
