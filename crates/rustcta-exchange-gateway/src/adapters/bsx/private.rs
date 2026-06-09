use rustcta_exchange_api::{
    AccountId, ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse,
    PageCursor, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest, RecentFillsResponse,
    RequestContext, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};

use super::parser::{parse_orders, parse_recent_fills, parse_single_order};
use super::transport::{BsxRest, BsxRestAuth};
use super::BsxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub(super) fn unsupported<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

pub(super) const PRIVATE_REST_DISABLED: &str = "bsx.private_rest_disabled";
pub(super) const BALANCES_UNSUPPORTED: &str = "bsx.balances_require_account_scope_audit";
pub(super) const POSITIONS_UNSUPPORTED: &str = "bsx.positions_require_account_scope_audit";
pub(super) const FEES_UNSUPPORTED: &str = "bsx.fees_not_mapped_to_gateway_account";
pub(super) const PLACE_ORDER_UNSUPPORTED: &str =
    "bsx.place_order_requires_eip712_wallet_signature_audit";
pub(super) const CANCEL_ORDER_UNSUPPORTED: &str =
    "bsx.cancel_order_requires_cancel_v2_semantics_audit";
pub(super) const AMEND_ORDER_UNSUPPORTED: &str =
    "bsx.amend_order_spec_only_no_eip712_session_headers_reconciliation_guard";
pub(super) const ORDER_LIST_UNSUPPORTED: &str =
    "bsx.order_list_not_mapped_to_lossless_shared_semantics";
pub(super) const BATCH_PLACE_UNSUPPORTED: &str =
    "bsx.batch_place_orders_requires_eip712_wallet_signature_audit";
pub(super) const BATCH_CANCEL_UNSUPPORTED: &str =
    "bsx.batch_cancel_orders_spec_only_no_safe_cancel_shape_session_reconciliation_guard";
pub(super) const CANCEL_ALL_UNSUPPORTED: &str =
    "bsx.cancel_all_orders_requires_cancel_v2_semantics_audit";
pub(super) const QUERY_ORDER_UNSUPPORTED: &str = "bsx.query_order_requires_account_scope_audit";
pub(super) const OPEN_ORDERS_UNSUPPORTED: &str = "bsx.open_orders_require_account_scope_audit";
pub(super) const RECENT_FILLS_UNSUPPORTED: &str = "bsx.recent_fills_require_account_scope_audit";

impl BsxGatewayAdapter {
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
            .filter(|order_id| !order_id.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "BSX query_order requires exchange_order_id".to_string(),
            })?;
        if request
            .client_order_id
            .as_deref()
            .is_some_and(|client_order_id| !client_order_id.trim().is_empty())
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "bsx.query_order.client_order_id",
            });
        }
        let value = self
            .rest
            .send_signed_get(
                &BsxRest::query_order_path(order_id),
                &[],
                &self.read_auth()?,
            )
            .await?;
        let exchange = request.symbol.exchange.clone();
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request.context.request_id),
            order: Some(parse_single_order(
                &self.exchange_id,
                request.symbol,
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
        let mut query = Vec::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            query.push((
                "product_id".to_string(),
                symbol.exchange_symbol.symbol.clone(),
            ));
        }
        if let Some(limit) = request.page.as_ref().and_then(|page| page.limit) {
            query.push(("limit".to_string(), limit.min(500).to_string()));
        }
        if let Some(cursor) = request.page.as_ref().and_then(|page| page.cursor.as_ref()) {
            query.push(("cursor".to_string(), page_cursor_value(cursor)));
        }
        let value = self
            .rest
            .send_signed_get(BsxRest::open_orders_path(), &query, &self.read_auth()?)
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
        let (tenant_id, account_id) = context_account(&request.context)?;
        let mut query = Vec::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            query.push((
                "product_id".to_string(),
                symbol.exchange_symbol.symbol.clone(),
            ));
        }
        if let Some(start_time) = request.start_time {
            query.push((
                "start_time".to_string(),
                start_time.timestamp_millis().to_string(),
            ));
        }
        if let Some(end_time) = request.end_time {
            query.push((
                "end_time".to_string(),
                end_time.timestamp_millis().to_string(),
            ));
        }
        if let Some(limit) = request.limit {
            query.push(("limit".to_string(), limit.min(500).to_string()));
        }
        if let Some(cursor) = request.page.as_ref().and_then(|page| page.cursor.as_ref()) {
            query.push(("cursor".to_string(), page_cursor_value(cursor)));
        }
        let value = self
            .rest
            .send_signed_get(BsxRest::recent_fills_path(), &query, &self.read_auth()?)
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

    fn read_auth(&self) -> ExchangeApiResult<BsxRestAuth> {
        if !self.config.private_rest_available() {
            return Err(ExchangeApiError::Unsupported {
                operation: PRIVATE_REST_DISABLED,
            });
        }
        Ok(BsxRestAuth {
            api_key: required_config(self.config.api_key.as_deref(), "api_key")?,
            api_secret: required_config(self.config.api_secret.as_deref(), "api_secret")?,
            wallet_address: required_config(
                self.config.wallet_address.as_deref(),
                "wallet_address",
            )?,
        })
    }
}

fn reject_unmapped_fill_filters(request: &RecentFillsRequest) -> ExchangeApiResult<()> {
    if request.client_order_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "bsx.get_recent_fills.client_order_id",
        });
    }
    if request.exchange_order_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "bsx.get_recent_fills.exchange_order_id",
        });
    }
    if request.from_trade_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "bsx.get_recent_fills.from_trade_id",
        });
    }
    Ok(())
}

fn context_account(context: &RequestContext) -> ExchangeApiResult<(TenantId, AccountId)> {
    let tenant_id = context
        .tenant_id
        .clone()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "BSX recent fills mapping requires tenant_id in request context".to_string(),
        })?;
    let account_id =
        context
            .account_id
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "BSX recent fills mapping requires account_id in request context"
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
            message: format!("BSX private REST requires {field}"),
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
