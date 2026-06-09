#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, ExchangeApiError, ExchangeApiResult, OpenOrdersRequest,
    OpenOrdersResponse, PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{
    parse_hibachi_balances, parse_hibachi_order, parse_hibachi_orders, parse_hibachi_positions,
    parse_hibachi_recent_fills,
};
use super::transport::hibachi_account_query;
use super::HibachiGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const PRIVATE_REST_DISABLED: &str = "hibachi.private_rest_disabled";
pub const PLACE_ORDER_UNSUPPORTED: &str = "hibachi.trade_write_request_spec_only";
pub const CANCEL_ORDER_UNSUPPORTED: &str = "hibachi.cancel_write_request_spec_only";
pub const AMEND_ORDER_UNSUPPORTED: &str =
    "hibachi.amend_requires_price_signer_nonce_and_reconciliation";
pub const BATCH_PLACE_UNSUPPORTED: &str =
    "hibachi.batch_place_requires_contract_id_signer_nonce_and_dry_run_guard";
pub const BATCH_CANCEL_UNSUPPORTED: &str =
    "hibachi.batch_cancel_requires_batch_signature_parser_and_reconciliation";
pub const CANCEL_ALL_UNSUPPORTED: &str = "hibachi.cancel_all_request_spec_only";
pub const ORDER_LIST_UNSUPPORTED: &str = "hibachi.order_list_unsupported";
pub const QUOTE_MARKET_UNSUPPORTED: &str = "hibachi.quote_market_order_unsupported";

pub fn write_boundary<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

impl HibachiGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "hibachi.query_order.client_order_id",
            });
        }
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|order_id| !order_id.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "hibachi query_order requires exchange_order_id".to_string(),
            })?;
        let account_id = self.private_read_account_id("hibachi.query_order")?;
        let value = self
            .rest
            .send_account_get(
                "/trade/order",
                &hibachi_query_order_query(&account_id, order_id),
            )
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: Some(parse_hibachi_order(
                &self.exchange_id,
                response_order(&value),
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
                operation: "hibachi.get_open_orders.cursor",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "hibachi get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let account_id = self.private_read_account_id("hibachi.get_open_orders")?;
        let value = self
            .rest
            .send_account_get(
                "/trade/orders",
                &hibachi_open_orders_query(&account_id, &symbol.exchange_symbol.symbol),
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_hibachi_orders(&self.exchange_id, response_orders(&value))?,
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
                operation: "hibachi.get_recent_fills.client_order_id",
            });
        }
        if request.exchange_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "hibachi.get_recent_fills.exchange_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "hibachi.get_recent_fills.from_trade_id",
            });
        }
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "hibachi.get_recent_fills.time_window",
            });
        }
        if request
            .page
            .as_ref()
            .is_some_and(|page| page.cursor.is_some())
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "hibachi.get_recent_fills.cursor",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "hibachi get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let (tenant_id, context_account_id) =
            self.context_account(&request.context, "hibachi.get_recent_fills")?;
        let account_id = self.private_read_account_id("hibachi.get_recent_fills")?;
        let limit = request
            .limit
            .or_else(|| request.page.as_ref().and_then(|page| page.limit));
        let value = self
            .rest
            .send_account_get(
                "/trade/account/trades",
                &hibachi_recent_fills_query(&account_id, &symbol.exchange_symbol.symbol, limit),
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_hibachi_recent_fills(
                &self.exchange_id,
                tenant_id,
                context_account_id,
                symbol,
                &value,
            )?,
        })
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_market_type(request.market_type)?;
        if !self.config.private_read_available() {
            return Err(ExchangeApiError::Unsupported {
                operation: PRIVATE_REST_DISABLED,
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "hibachi.get_balances")?;
        let hibachi_account_id =
            self.config
                .normalized_account_id()
                .ok_or_else(|| ExchangeApiError::Unsupported {
                    operation: PRIVATE_REST_DISABLED,
                })?;
        let value = self
            .rest
            .send_account_get(
                "/trade/account/info",
                &hibachi_account_query(&hibachi_account_id),
            )
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: parse_hibachi_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.assets,
                &value,
            )?,
        })
    }

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_market_type(request.market_type)?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange_id)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        if !self.config.private_read_available() {
            return Err(ExchangeApiError::Unsupported {
                operation: PRIVATE_REST_DISABLED,
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "hibachi.get_positions")?;
        let hibachi_account_id =
            self.config
                .normalized_account_id()
                .ok_or_else(|| ExchangeApiError::Unsupported {
                    operation: PRIVATE_REST_DISABLED,
                })?;
        let value = self
            .rest
            .send_account_get(
                "/trade/account/info",
                &hibachi_account_query(&hibachi_account_id),
            )
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions: parse_hibachi_positions(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.symbols,
                &value,
            )?,
        })
    }

    fn private_read_account_id(&self, operation: &'static str) -> ExchangeApiResult<String> {
        if !self.config.private_read_available() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        self.config
            .normalized_account_id()
            .ok_or(ExchangeApiError::Unsupported { operation })
    }
}

pub fn hibachi_query_order_query(account_id: &str, order_id: &str) -> Vec<(&'static str, String)> {
    vec![
        ("accountId", account_id.to_string()),
        ("orderId", order_id.to_string()),
    ]
}

pub fn hibachi_open_orders_query(account_id: &str, symbol: &str) -> Vec<(&'static str, String)> {
    vec![
        ("accountId", account_id.to_string()),
        ("symbol", symbol.to_string()),
    ]
}

pub fn hibachi_recent_fills_query(
    account_id: &str,
    symbol: &str,
    limit: Option<u32>,
) -> Vec<(&'static str, String)> {
    vec![
        ("accountId", account_id.to_string()),
        ("symbol", symbol.to_string()),
        ("page", "1".to_string()),
        ("limit", limit.unwrap_or(100).clamp(1, 500).to_string()),
    ]
}

fn response_order(value: &Value) -> &Value {
    value
        .get("order")
        .or_else(|| value.get("data").and_then(|data| data.get("order")))
        .or_else(|| value.get("data"))
        .unwrap_or(value)
}

fn response_orders(value: &Value) -> &Value {
    value
        .get("orders")
        .or_else(|| value.get("data").and_then(|data| data.get("orders")))
        .or_else(|| value.get("data").and_then(|data| data.get("rows")))
        .or_else(|| value.get("data"))
        .unwrap_or(value)
}

pub fn hibachi_place_limit_order_body(
    account_id: u64,
    nonce: u64,
    symbol: &str,
    side: &str,
    quantity: &str,
    price: &str,
    max_fees_percent: &str,
    signature: &str,
) -> Value {
    json!({
        "accountId": account_id,
        "nonce": nonce,
        "symbol": symbol,
        "side": side,
        "orderType": "LIMIT",
        "quantity": quantity,
        "price": price,
        "signature": signature,
        "maxFeesPercent": max_fees_percent
    })
}

pub fn hibachi_cancel_order_body(account_id: u64, order_id: u64, signature: &str) -> Value {
    json!({
        "accountId": account_id,
        "orderId": order_id,
        "signature": signature
    })
}

pub fn hibachi_amend_order_body(
    account_id: u64,
    nonce: u64,
    order_id: &str,
    symbol: &str,
    quantity: &str,
    price: &str,
    signature: &str,
) -> Value {
    json!({
        "accountId": account_id,
        "nonce": nonce,
        "orderId": order_id,
        "symbol": symbol,
        "quantity": quantity,
        "price": price,
        "signature": signature
    })
}

pub fn hibachi_cancel_all_orders_body(account_id: u64, nonce: u64, signature: &str) -> Value {
    json!({
        "accountId": account_id,
        "nonce": nonce,
        "signature": signature
    })
}

pub fn hibachi_batch_place_orders_body(
    account_id: u64,
    nonce: u64,
    symbol: &str,
    side: &str,
    quantity: &str,
    price: &str,
    signature: &str,
) -> Value {
    json!({
        "accountId": account_id,
        "nonce": nonce,
        "orders": [
            {
                "symbol": symbol,
                "side": side,
                "orderType": "LIMIT",
                "quantity": quantity,
                "price": price,
                "signature": signature
            }
        ]
    })
}

pub fn hibachi_batch_cancel_orders_body(
    account_id: u64,
    nonce: u64,
    order_ids: &[&str],
    signature: &str,
) -> Value {
    json!({
        "accountId": account_id,
        "nonce": nonce,
        "orderIds": order_ids,
        "signature": signature
    })
}
