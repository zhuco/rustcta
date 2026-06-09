use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::Value;

use super::parser::{pair_from_symbol, parse_bit2c_open_orders, parse_bit2c_order_state};
use super::Bit2cGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl Bit2cGatewayAdapter {
    pub(super) fn unsupported_balances(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bit2c.balances_private_rest_disabled")
    }

    pub(super) fn unsupported_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bit2c.positions_not_supported")
    }

    pub(super) fn unsupported_fees(&self, request: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        self.unsupported("bit2c.fees_private_rest_disabled")
    }

    pub(super) fn unsupported_place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("bit2c.place_order_request_spec_only")
    }

    pub(super) fn unsupported_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("bit2c.quote_market_order_request_spec_only")
    }

    pub(super) fn unsupported_cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("bit2c.cancel_order_request_spec_only")
    }

    pub(super) fn unsupported_amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("bit2c.amend_order_not_supported")
    }

    pub(super) fn unsupported_batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bit2c.batch_place_orders_not_supported")
    }

    pub(super) fn unsupported_batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bit2c.batch_cancel_orders_not_supported")
    }

    pub(super) fn unsupported_cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bit2c.cancel_all_orders_not_supported")
    }

    pub(super) fn unsupported_query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("bit2c.query_order_private_rest_disabled")
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
                operation: "bit2c.query_order.client_order_id",
            });
        }
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bit2c query_order requires exchange_order_id".to_string(),
            })?;
        let value = self
            .send_signed_get(
                "bit2c.query_order",
                "/Order/GetById",
                &[("id", order_id.trim())],
            )
            .await?;
        let order = parse_bit2c_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order: Some(order),
        })
    }

    pub(super) fn unsupported_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bit2c.open_orders_private_rest_disabled")
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
                operation: "bit2c.get_open_orders.page",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bit2c get_open_orders requires symbol for pair-scoped readback"
                    .to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let pair = pair_from_symbol(&symbol.exchange_symbol.symbol)?;
        let value = self
            .send_signed_get(
                "bit2c.get_open_orders",
                "/Order/MyOrders",
                &[("pair", pair.venue)],
            )
            .await?;
        let orders = parse_bit2c_open_orders(&self.exchange_id, symbol, &value)?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
        })
    }

    pub(super) fn unsupported_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bit2c.recent_fills_private_rest_disabled")
    }

    pub(super) fn unsupported_public_stream(
        &self,
        _subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.unsupported("bit2c.public_streams_not_documented")
    }

    pub(super) fn unsupported_private_stream(
        &self,
        _subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.unsupported("bit2c.private_streams_not_documented")
    }

    async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &[(&str, &str)],
    ) -> ExchangeApiResult<Value> {
        if !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        self.rest
            .send_signed_get(
                endpoint,
                params,
                self.config.api_key.as_deref().unwrap_or_default(),
                self.config.api_secret.as_deref().unwrap_or_default(),
            )
            .await
    }
}
