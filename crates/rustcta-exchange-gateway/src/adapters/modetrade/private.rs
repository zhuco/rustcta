use std::collections::BTreeMap;

use rustcta_exchange_api::{
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, PageCursor,
    QueryOrderRequest, QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, RequestContext,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, TenantId};

use super::parser::{parse_orders, parse_recent_fills, parse_single_order};
use super::signing::OrderlyAuth;
use super::ModetradeGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub(super) fn unsupported<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

pub(super) const ORDER_BOOK_UNSUPPORTED: &str =
    "modetrade.order_book_requires_orderly_account_signed_request";
pub(super) const BALANCES_UNSUPPORTED: &str = "modetrade.balances_require_orderly_account_audit";
pub(super) const POSITIONS_UNSUPPORTED: &str =
    "modetrade.positions_require_orderly_account_onboarding_ed25519_permission_parser_reconciliation";
pub(super) const FEES_UNSUPPORTED: &str = "modetrade.fees_not_mapped_to_gateway_account";
pub(super) const PLACE_ORDER_UNSUPPORTED: &str =
    "modetrade.place_order_requires_orderly_ed25519_account_audit";
pub(super) const CANCEL_ORDER_UNSUPPORTED: &str =
    "modetrade.cancel_order_requires_orderly_ed25519_account_audit";
pub(super) const AMEND_ORDER_UNSUPPORTED: &str =
    "modetrade.amend_order_spec_only_no_orderly_ed25519_permission_reconciliation_guard";
pub(super) const ORDER_LIST_UNSUPPORTED: &str = "modetrade.order_list_unverified_orderly_semantics";
pub(super) const BATCH_PLACE_UNSUPPORTED: &str =
    "modetrade.batch_place_orders_spec_only_no_orderly_ed25519_permission_reconciliation_guard";
pub(super) const BATCH_CANCEL_UNSUPPORTED: &str =
    "modetrade.batch_cancel_orders_unverified_orderly_semantics";
pub(super) const CANCEL_ALL_UNSUPPORTED: &str =
    "modetrade.cancel_all_orders_unverified_orderly_semantics";
pub(super) const QUERY_ORDER_UNSUPPORTED: &str =
    "modetrade.query_order_requires_orderly_account_audit";
pub(super) const OPEN_ORDERS_UNSUPPORTED: &str =
    "modetrade.open_orders_require_orderly_account_audit";
pub(super) const RECENT_FILLS_UNSUPPORTED: &str =
    "modetrade.recent_fills_require_orderly_account_audit";

pub(super) const QUERY_ORDER_PATH: &str = "/v1/order/{order_id}";
pub(super) const QUERY_ORDER_BY_CLIENT_ID_PATH: &str = "/v1/client/order/{client_order_id}";
pub(super) const OPEN_ORDERS_PATH: &str = "/v1/orders";
pub(super) const RECENT_FILLS_PATH: &str = "/v1/trades";

impl ModetradeGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let path = if let Some(order_id) = request
            .exchange_order_id
            .as_deref()
            .filter(|order_id| !order_id.trim().is_empty())
        {
            query_order_path(order_id)
        } else if let Some(client_order_id) = request
            .client_order_id
            .as_deref()
            .filter(|client_order_id| !client_order_id.trim().is_empty())
        {
            query_order_by_client_id_path(client_order_id)
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "modetrade query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        };
        let value = self
            .rest
            .send_signed_get(
                &path,
                &BTreeMap::new(),
                &self.orderly_auth("modetrade.query_order")?,
            )
            .await?;
        let exchange = request.symbol.exchange.clone();
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            order: Some(parse_single_order(
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
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market_type(market_type)?;
        }
        let mut params = BTreeMap::from([("status".to_string(), "INCOMPLETE".to_string())]);
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            params.insert("symbol".to_string(), symbol.exchange_symbol.symbol.clone());
        }
        if let Some(limit) = request.page.as_ref().and_then(|page| page.limit) {
            params.insert("size".to_string(), limit.min(500).to_string());
        }
        let value = self
            .rest
            .send_signed_get(
                OPEN_ORDERS_PATH,
                &params,
                &self.orderly_auth("modetrade.get_open_orders")?,
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?,
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
        reject_unmapped_fill_filters(&request)?;
        let (tenant_id, account_id) =
            context_account("modetrade.get_recent_fills", &request.context)?;
        let mut params = BTreeMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            params.insert("symbol".to_string(), symbol.exchange_symbol.symbol.clone());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "start_t".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert("end_t".to_string(), end_time.timestamp_millis().to_string());
        }
        if let Some(limit) = request.limit {
            params.insert("size".to_string(), limit.min(500).to_string());
        }
        if let Some(page) = &request.page {
            if let Some(cursor) = &page.cursor {
                params.insert("page".to_string(), page_cursor_value(cursor));
            }
        }
        let value = self
            .rest
            .send_signed_get(
                RECENT_FILLS_PATH,
                &params,
                &self.orderly_auth("modetrade.get_recent_fills")?,
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.symbol.as_ref(),
                &value,
            )?,
        })
    }

    fn orderly_auth(&self, operation: &'static str) -> ExchangeApiResult<OrderlyAuth> {
        if !self.config.private_rest_available() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok(OrderlyAuth {
            account_id: required_config(
                self.config.orderly_account_id.as_deref(),
                "orderly_account_id",
            )?,
            orderly_key: required_config(self.config.orderly_key.as_deref(), "orderly_key")?,
            orderly_secret: required_config(
                self.config.orderly_secret.as_deref(),
                "orderly_secret",
            )?,
        })
    }
}

pub(super) fn query_order_path(order_id: &str) -> String {
    QUERY_ORDER_PATH.replace("{order_id}", &urlencoding::encode(order_id))
}

pub(super) fn query_order_by_client_id_path(client_order_id: &str) -> String {
    QUERY_ORDER_BY_CLIENT_ID_PATH
        .replace("{client_order_id}", &urlencoding::encode(client_order_id))
}

fn reject_unmapped_fill_filters(request: &RecentFillsRequest) -> ExchangeApiResult<()> {
    if request.client_order_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "modetrade.get_recent_fills.client_order_id",
        });
    }
    if request.exchange_order_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "modetrade.get_recent_fills.exchange_order_id",
        });
    }
    if request.from_trade_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "modetrade.get_recent_fills.from_trade_id",
        });
    }
    Ok(())
}

fn context_account(
    operation: &'static str,
    context: &RequestContext,
) -> ExchangeApiResult<(TenantId, AccountId)> {
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

fn page_cursor_value(cursor: &PageCursor) -> String {
    match cursor {
        PageCursor::Offset { offset } => offset.to_string(),
        PageCursor::Id { id } => id.clone(),
        PageCursor::Timestamp { millis } => millis.to_string(),
        PageCursor::Token { token } => token.clone(),
        PageCursor::TimeRange { start_ms, .. } => start_ms.to_string(),
    }
}

fn required_config(value: Option<&str>, field: &str) -> ExchangeApiResult<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("modetrade private REST requires {field}"),
        })
}
