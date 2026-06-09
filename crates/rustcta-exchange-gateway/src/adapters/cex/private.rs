use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderListRequest, OrderListResponse, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, RequestContext,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::parser::{cex_pair_string, parse_open_orders, parse_order_state, parse_recent_fills};
use super::signing::cex_rest_signature;
use super::CexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const QUERY_ORDER_PATH: &str = "/get_order/";
pub const OPEN_ORDERS_PATH_PREFIX: &str = "/open_orders";
pub const RECENT_FILLS_PATH_PREFIX: &str = "/archived_orders";

#[derive(Debug, Clone, PartialEq)]
pub struct CexPrivateRequestSpec {
    pub method: &'static str,
    pub path: String,
    pub body: Value,
}

pub fn build_private_request_spec(
    path: impl Into<String>,
    mut body: Value,
    nonce: &str,
    user_id: &str,
    api_key: &str,
    api_secret: &str,
) -> ExchangeApiResult<CexPrivateRequestSpec> {
    body["nonce"] = Value::String(nonce.to_string());
    body["key"] = Value::String(api_key.to_string());
    body["signature"] = Value::String(cex_rest_signature(nonce, user_id, api_key, api_secret)?);
    Ok(CexPrivateRequestSpec {
        method: "POST",
        path: path.into(),
        body,
    })
}

pub fn query_order_request_body(request: &QueryOrderRequest) -> ExchangeApiResult<Value> {
    let exchange_order_id =
        request
            .exchange_order_id
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "cex query_order requires exchange_order_id".to_string(),
            })?;
    Ok(json!({ "id": exchange_order_id.trim() }))
}

pub fn open_orders_path(request: &OpenOrdersRequest) -> ExchangeApiResult<String> {
    let symbol = request
        .symbol
        .as_ref()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "cex get_open_orders requires symbol".to_string(),
        })?;
    pair_path(OPEN_ORDERS_PATH_PREFIX, &symbol.exchange_symbol.symbol)
}

pub fn recent_fills_path(request: &RecentFillsRequest) -> ExchangeApiResult<String> {
    let symbol = request
        .symbol
        .as_ref()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "cex get_recent_fills requires symbol".to_string(),
        })?;
    pair_path(RECENT_FILLS_PATH_PREFIX, &symbol.exchange_symbol.symbol)
}

pub fn recent_fills_request_body(request: &RecentFillsRequest) -> Value {
    let mut body = json!({});
    if let Some(limit) = request
        .limit
        .or_else(|| request.page.as_ref().and_then(|page| page.limit))
    {
        body["limit"] = Value::from(limit.min(100));
    }
    if let Some(start_time) = request.start_time {
        body["dateFrom"] = Value::from(start_time.timestamp());
    }
    if let Some(end_time) = request.end_time {
        body["dateTo"] = Value::from(end_time.timestamp());
    }
    body
}

impl CexGatewayAdapter {
    pub(super) fn get_balances_unsupported(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        self.unsupported("cex.balances_private_rest_deferred")
    }

    pub(super) fn get_positions_unsupported(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("cex.positions_unsupported_for_spot")
    }

    pub(super) fn get_fees_unsupported(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        self.unsupported("cex.fees_private_rest_deferred")
    }

    pub(super) fn place_order_unsupported(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("cex.place_order_request_spec_only")
    }

    pub(super) fn place_quote_market_order_unsupported(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("cex.quote_market_order_unverified")
    }

    pub(super) fn cancel_order_unsupported(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("cex.cancel_order_request_spec_only")
    }

    pub(super) fn amend_order_unsupported(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("cex.cancel_replace_order_unmapped")
    }

    pub(super) fn place_order_list_unsupported(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.ensure_exchange(&request.symbol().exchange)?;
        self.ensure_spot(request.symbol().market_type)?;
        self.unsupported("cex.order_list_unsupported")
    }

    pub(super) fn batch_place_orders_unsupported(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_spot(order.symbol.market_type)?;
        }
        self.unsupported("cex.batch_place_orders_unverified")
    }

    pub(super) fn batch_cancel_orders_unsupported(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("cex.batch_cancel_orders_unverified")
    }

    pub(super) fn cancel_all_orders_unsupported(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        self.unsupported("cex.cancel_all_orders_pair_scoped_unverified")
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "cex.query_order.client_order_id",
            });
        }
        let value = self
            .send_private_post(
                "cex.query_order",
                QUERY_ORDER_PATH.to_string(),
                query_order_request_body(&request)?,
            )
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: Some(parse_order_state(
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
                message: "cex get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let value = self
            .send_private_post(
                "cex.get_open_orders",
                open_orders_path(&request)?,
                json!({}),
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_open_orders(&self.exchange_id, Some(symbol), &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(symbol) = request.symbol.as_ref() {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "cex.get_recent_fills.client_order_id",
            });
        }
        if request.exchange_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "cex.get_recent_fills.exchange_order_id",
            });
        }
        if request.from_trade_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "cex.get_recent_fills.from_trade_id",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "cex get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account("cex.get_recent_fills", &request.context)?;
        let value = self
            .send_private_post(
                "cex.get_recent_fills",
                recent_fills_path(&request)?,
                recent_fills_request_body(&request),
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    async fn send_private_post(
        &self,
        operation: &'static str,
        path: String,
        body: Value,
    ) -> ExchangeApiResult<Value> {
        let (api_key, api_secret, user_id) = self.private_credentials(operation)?;
        let nonce = chrono::Utc::now().timestamp_millis().to_string();
        let spec = build_private_request_spec(path, body, &nonce, user_id, api_key, api_secret)?;
        self.rest.send_private_post(&spec).await
    }

    fn private_credentials(
        &self,
        operation: &'static str,
    ) -> ExchangeApiResult<(&str, &str, &str)> {
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
        let user_id = self
            .config
            .user_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        Ok((api_key.trim(), api_secret.trim(), user_id.trim()))
    }

    fn context_account(
        &self,
        operation: &'static str,
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

fn pair_path(prefix: &str, raw_symbol: &str) -> ExchangeApiResult<String> {
    let pair = cex_pair_string(raw_symbol)?;
    let (base, quote) = pair
        .split_once(':')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("invalid CEX.IO pair {pair}"),
        })?;
    Ok(format!("{prefix}/{base}/{quote}"))
}
