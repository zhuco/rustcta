use std::collections::BTreeMap;

use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse,
    PageCursor, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest, RecentFillsResponse,
    RequestContext, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{parse_orders, parse_recent_fills, parse_single_order};
use super::signing::OrderlyAuth;
use super::transport::AarkRest;
use super::AarkGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub(super) fn unsupported<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

pub(super) const ORDER_BOOK_UNSUPPORTED: &str =
    "aark.order_book_requires_orderly_account_signed_request";
pub(super) const BALANCES_UNSUPPORTED: &str = "aark.balances_require_orderly_account_audit";
pub(super) const POSITIONS_UNSUPPORTED: &str =
    "aark.positions_require_orderly_account_onboarding_ed25519_permission_parser_reconciliation";
pub(super) const FEES_UNSUPPORTED: &str = "aark.fees_not_mapped_to_gateway_account";
pub(super) const PLACE_ORDER_UNSUPPORTED: &str =
    "aark.place_order_requires_orderly_ed25519_account_audit";
pub(super) const CANCEL_ORDER_UNSUPPORTED: &str =
    "aark.cancel_order_requires_orderly_ed25519_account_audit";
pub(super) const AMEND_ORDER_UNSUPPORTED: &str =
    "aark.amend_order_spec_only_no_orderly_ed25519_permission_reconciliation_guard";
pub(super) const ORDER_LIST_UNSUPPORTED: &str = "aark.order_list_unverified_orderly_semantics";
pub(super) const BATCH_PLACE_UNSUPPORTED: &str =
    "aark.batch_place_orders_spec_only_no_orderly_ed25519_permission_reconciliation_guard";
pub(super) const BATCH_CANCEL_UNSUPPORTED: &str =
    "aark.batch_cancel_orders_unverified_orderly_semantics";
pub(super) const CANCEL_ALL_UNSUPPORTED: &str =
    "aark.cancel_all_orders_unverified_orderly_semantics";
pub(super) const QUERY_ORDER_UNSUPPORTED: &str = "aark.query_order_requires_orderly_account_audit";
pub(super) const OPEN_ORDERS_UNSUPPORTED: &str = "aark.open_orders_require_orderly_account_audit";
pub(super) const RECENT_FILLS_UNSUPPORTED: &str = "aark.recent_fills_require_orderly_account_audit";

impl AarkGatewayAdapter {
    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let path = if let Some(order_id) = &request.exchange_order_id {
            AarkRest::query_order_path(order_id)
        } else if let Some(client_order_id) = &request.client_order_id {
            AarkRest::query_client_order_path(client_order_id)
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "aark query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        };
        let value = self
            .rest
            .send_signed_get(&path, &BTreeMap::new(), &self.orderly_auth()?)
            .await?;
        let order = parse_single_order(&self.exchange_id, &request.symbol, &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order: Some(order),
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
        let mut params = BTreeMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            params.insert("symbol".to_string(), symbol.exchange_symbol.symbol.clone());
        }
        params.insert("status".to_string(), "INCOMPLETE".to_string());
        if let Some(page) = &request.page {
            if let Some(limit) = page.limit {
                params.insert("size".to_string(), limit.min(500).to_string());
            }
        }
        let value = self
            .rest
            .send_signed_get(AarkRest::orders_path(), &params, &self.orderly_auth()?)
            .await?;
        let orders = parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
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
        let (tenant_id, account_id) = context_account(&request.context)?;
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
            .send_signed_get(AarkRest::trades_path(), &params, &self.orderly_auth()?)
            .await?;
        let fills = parse_recent_fills(
            &self.exchange_id,
            tenant_id,
            account_id,
            request.symbol.as_ref(),
            &value,
        )?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }

    fn orderly_auth(&self) -> ExchangeApiResult<OrderlyAuth> {
        if !self.config.enabled_private_rest {
            return Err(ExchangeApiError::Unsupported {
                operation: "aark.private_rest_disabled",
            });
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

fn context_account(context: &RequestContext) -> ExchangeApiResult<(TenantId, AccountId)> {
    let tenant_id = context
        .tenant_id
        .clone()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "aark private response mapping requires tenant_id in request context"
                .to_string(),
        })?;
    let account_id =
        context
            .account_id
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "aark private response mapping requires account_id in request context"
                    .to_string(),
            })?;
    Ok((tenant_id, account_id))
}

fn required_config(value: Option<&str>, field: &str) -> ExchangeApiResult<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("aark private REST requires {field}"),
        })
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
