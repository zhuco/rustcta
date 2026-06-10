use async_trait::async_trait;

use crate::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, ExchangeClientCapabilities,
    ExchangeId, FeesRequest, FeesResponse, FundingRatesRequest, FundingRatesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest, OrderBookResponse, OrderListRequest,
    OrderListResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, SymbolRulesRequest,
    SymbolRulesResponse,
};

#[async_trait]
pub trait ExchangeClient: Send + Sync {
    fn exchange(&self) -> ExchangeId;

    fn capabilities(&self) -> ExchangeClientCapabilities;

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse>;

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse>;

    async fn get_symbol_rules(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse>;

    async fn get_order_book(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse>;

    async fn get_fees(&self, request: FeesRequest) -> ExchangeApiResult<FeesResponse>;

    async fn get_funding_rates(
        &self,
        _request: FundingRatesRequest,
    ) -> ExchangeApiResult<FundingRatesResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "get_funding_rates",
        })
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse>;

    async fn place_quote_market_order(
        &self,
        _request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "place_quote_market_order",
        })
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse>;

    async fn amend_order(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "amend_order",
        })
    }

    async fn place_order_list(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "place_order_list",
        })
    }

    async fn batch_place_orders(
        &self,
        _request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "batch_place_orders",
        })
    }

    async fn batch_cancel_orders(
        &self,
        _request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "batch_cancel_orders",
        })
    }

    async fn cancel_all_orders(
        &self,
        _request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "cancel_all_orders",
        })
    }

    async fn query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse>;

    async fn get_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse>;

    async fn get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse>;

    async fn subscribe_public_stream(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String>;

    async fn subscribe_private_stream(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String>;
}
