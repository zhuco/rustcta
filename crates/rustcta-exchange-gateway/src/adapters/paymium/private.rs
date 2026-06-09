use std::collections::HashMap;

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

use super::parser::{
    parse_private_open_orders, parse_private_order_state, parse_private_recent_fills,
};
use super::PaymiumGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

const USER_ORDERS_PATH: &str = "/user/orders";

impl PaymiumGatewayAdapter {
    pub(super) fn get_balances_unsupported(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        self.unsupported("paymium.balances_private_rest_deferred")
    }

    pub(super) fn get_positions_unsupported(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("paymium.positions_unsupported_for_spot")
    }

    pub(super) fn get_fees_unsupported(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        self.unsupported("paymium.fees_not_documented_for_public_adapter")
    }

    pub(super) fn place_order_unsupported(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("paymium.place_order_request_spec_only")
    }

    pub(super) fn place_quote_market_order_unsupported(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("paymium.quote_market_order_request_spec_only")
    }

    pub(super) fn cancel_order_unsupported(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("paymium.cancel_order_request_spec_only")
    }

    pub(super) fn amend_order_unsupported(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("paymium.amend_order_unsupported")
    }

    pub(super) fn place_order_list_unsupported(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.ensure_exchange(&request.symbol().exchange)?;
        self.ensure_spot(request.symbol().market_type)?;
        self.unsupported("paymium.order_list_unsupported")
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
        self.unsupported("paymium.batch_place_orders_unsupported")
    }

    pub(super) fn batch_cancel_orders_unsupported(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("paymium.batch_cancel_orders_unsupported")
    }

    pub(super) fn cancel_all_orders_unsupported(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        self.unsupported("paymium.cancel_all_orders_unsupported")
    }

    pub(super) fn query_order_unsupported(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("paymium.query_order_private_rest_deferred")
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
                operation: "paymium.query_order.client_order_id",
            });
        }
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "paymium query_order requires exchange_order_id".to_string(),
            }
        })?;
        let path = order_path(order_id)?;
        let (api_key, api_secret) = self.private_credentials("paymium.query_order")?;
        let value = self
            .rest
            .send_private_get(api_key, api_secret, &path, &HashMap::new())
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: Some(parse_private_order_state(
                &self.exchange_id,
                Some(&request.symbol),
                &value,
            )?),
        })
    }

    pub(super) fn get_open_orders_unsupported(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        self.unsupported("paymium.open_orders_private_rest_deferred")
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
        if request.page.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "paymium.get_open_orders.pagination",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "paymium get_open_orders requires BTC/EUR symbol scope".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (api_key, api_secret) = self.private_credentials("paymium.get_open_orders")?;
        let value = self
            .rest
            .send_private_get(
                api_key,
                api_secret,
                USER_ORDERS_PATH,
                &HashMap::from([("active".to_string(), "true".to_string())]),
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_private_open_orders(&self.exchange_id, Some(symbol), &value)?,
        })
    }

    pub(super) fn get_recent_fills_unsupported(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        self.ensure_exchange(&request.exchange)?;
        if let Some(symbol) = request.symbol.as_ref() {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        self.unsupported("paymium.recent_fills_private_rest_deferred")
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
        if request.client_order_id.is_some()
            || request.from_trade_id.is_some()
            || request.start_time.is_some()
            || request.end_time.is_some()
            || request.page.is_some()
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "paymium.get_recent_fills.filters",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "paymium get_recent_fills requires BTC/EUR symbol scope".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let mut params = HashMap::from([("active".to_string(), "false".to_string())]);
        if let Some(order_id) = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            params.insert("order_uuid".to_string(), order_id.trim().to_string());
        }
        if let Some(limit) = request.limit {
            if limit == 0 {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "paymium get_recent_fills limit must be positive".to_string(),
                });
            }
            params.insert("limit".to_string(), limit.min(100).to_string());
        }
        let (tenant_id, account_id) =
            self.context_account("paymium.get_recent_fills", &request.context)?;
        let (api_key, api_secret) = self.private_credentials("paymium.get_recent_fills")?;
        let value = self
            .rest
            .send_private_get(api_key, api_secret, USER_ORDERS_PATH, &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_private_recent_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                symbol,
                &value,
            )?,
        })
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.private_rest_configured() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok((
            self.config.api_key.as_deref().unwrap_or_default(),
            self.config.api_secret.as_deref().unwrap_or_default(),
        ))
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

fn order_path(exchange_order_id: &str) -> ExchangeApiResult<String> {
    let order_id = exchange_order_id.trim();
    if order_id.is_empty() || order_id.contains('/') || order_id.contains('?') {
        return Err(ExchangeApiError::InvalidRequest {
            message: "paymium query_order requires a plain exchange_order_id".to_string(),
        });
    }
    Ok(format!(
        "{USER_ORDERS_PATH}/{}",
        urlencoding::encode(order_id)
    ))
}
