#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{
    parse_lighter_open_orders, parse_lighter_query_order, parse_lighter_recent_fills,
};
use super::transport::bearer_get_request_spec;
use super::LighterGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const PRIVATE_REST_DISABLED: &str = "lighter.private_rest_disabled";
pub const AMEND_ORDER_UNSUPPORTED: &str =
    "lighter.amend_requires_signed_tx_builder_nonce_and_reconciliation";
pub const BATCH_PLACE_UNSUPPORTED: &str =
    "lighter.batch_place_requires_signed_tx_batch_builder_nonce_partial_parser_and_dry_run_guard";
pub const BATCH_CANCEL_UNSUPPORTED: &str =
    "lighter.batch_cancel_requires_signed_tx_batch_builder_nonce_partial_parser_and_dry_run_guard";
pub const ORDER_LIST_UNSUPPORTED: &str = "lighter.order_list_unsupported";
pub const CANCEL_ALL_UNSUPPORTED: &str = "lighter.cancel_all_orders_signing_unverified";
pub const POSITIONS_UNSUPPORTED: &str =
    "lighter.positions_requires_auth_token_session_renewal_subaccount_parser_reconciliation";
pub const ACCOUNT_ACTIVE_ORDERS_PATH: &str = "/accountActiveOrders";
pub const TRADES_PATH: &str = "/trades";

impl LighterGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "lighter.query_order.client_order_id",
            });
        }
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|order_id| !order_id.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "lighter query_order requires exchange_order_id".to_string(),
            })?;
        let params = account_market_query(
            self.private_read_account_index("lighter.query_order")?,
            Some(&request.symbol),
        );
        let value = self
            .rest
            .send_private_get("lighter.query_order", ACCOUNT_ACTIVE_ORDERS_PATH, &params)
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: parse_lighter_query_order(&self.exchange_id, &request.symbol, order_id, &value)?,
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
        if request
            .page
            .as_ref()
            .is_some_and(|page| page.cursor.is_some())
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "lighter.get_open_orders.cursor",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "lighter get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let params = account_market_query(
            self.private_read_account_index("lighter.get_open_orders")?,
            Some(symbol),
        );
        let value = self
            .rest
            .send_private_get(
                "lighter.get_open_orders",
                ACCOUNT_ACTIVE_ORDERS_PATH,
                &params,
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_lighter_open_orders(&self.exchange_id, symbol, &value)?,
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
                operation: "lighter.get_recent_fills.client_order_id",
            });
        }
        if request.exchange_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "lighter.get_recent_fills.exchange_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "lighter.get_recent_fills.from_trade_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "lighter.get_recent_fills.time_window",
            });
        }
        if request
            .page
            .as_ref()
            .is_some_and(|page| page.cursor.is_some())
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "lighter.get_recent_fills.cursor",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "lighter get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let params = account_market_query(
            self.private_read_account_index("lighter.get_recent_fills")?,
            Some(symbol),
        );
        let value = self
            .rest
            .send_private_get("lighter.get_recent_fills", TRADES_PATH, &params)
            .await?;
        let tenant_id =
            request
                .context
                .tenant_id
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "lighter get_recent_fills requires context.tenant_id".to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "lighter get_recent_fills requires context.account_id".to_string(),
                })?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_lighter_recent_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                symbol,
                &value,
            )?,
        })
    }

    fn private_read_account_index(&self, operation: &'static str) -> ExchangeApiResult<&str> {
        if !self.config.private_read_available() {
            return Err(ExchangeApiError::Unsupported {
                operation: PRIVATE_REST_DISABLED,
            });
        }
        self.config
            .account_index
            .as_deref()
            .map(str::trim)
            .filter(|account_index| !account_index.is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })
    }
}

pub fn account_active_orders_request_spec_fixture() -> Value {
    bearer_get_request_spec(
        ACCOUNT_ACTIVE_ORDERS_PATH,
        json!({
            "account_index": "redacted-account-index",
            "market_id": "0"
        }),
    )
}

pub fn trades_request_spec_fixture() -> Value {
    bearer_get_request_spec(
        TRADES_PATH,
        json!({
            "account_index": "redacted-account-index",
            "market_id": "0"
        }),
    )
}

pub fn lighter_read_only_auth_note(account_index: &str) -> Value {
    json!({
        "account_index": account_index,
        "auth": "bearer_token",
        "trade_enabled": false
    })
}

fn account_market_query(
    account_index: &str,
    symbol: Option<&SymbolScope>,
) -> HashMap<String, String> {
    let mut query = HashMap::from([("account_index".to_string(), account_index.to_string())]);
    if let Some(market_id) = symbol.and_then(lighter_market_id) {
        query.insert("market_id".to_string(), market_id);
    }
    query
}

fn lighter_market_id(symbol: &SymbolScope) -> Option<String> {
    symbol
        .exchange_symbol
        .symbol
        .strip_prefix("market:")
        .map(ToString::to_string)
        .or_else(|| {
            symbol
                .exchange_symbol
                .symbol
                .parse::<u64>()
                .ok()
                .map(|value| value.to_string())
        })
}
