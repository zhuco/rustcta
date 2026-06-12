use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Utc;
use rustcta_types::ExchangeErrorClass;

use crate::{
    AccountControlCapabilities, AmendOrderRequest, AmendOrderResponse, BalancesRequest,
    BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ClosePositionRequest, ClosePositionResponse, CountdownCancelAllRequest,
    CountdownCancelAllResponse, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, ExchangeError, ExchangeId, FeesRequest, FeesResponse,
    FundingRatesRequest, FundingRatesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse,
    PerpAccountControlProvider, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    SetLeverageRequest, SetLeverageResponse, SetMarginModeRequest, SetMarginModeResponse,
    SetPositionModeRequest, SetPositionModeResponse, SymbolAccountConfigRequest,
    SymbolAccountConfigResponse, SymbolRulesRequest, SymbolRulesResponse,
};

#[derive(Debug, Default)]
pub struct ReadOnlyMutationRecorder {
    calls: Mutex<HashMap<&'static str, u64>>,
}

impl ReadOnlyMutationRecorder {
    pub fn record(&self, operation: &'static str) {
        *self
            .calls
            .lock()
            .expect("readonly mutation recorder lock")
            .entry(operation)
            .or_default() += 1;
    }

    pub fn count(&self, operation: &'static str) -> u64 {
        self.calls
            .lock()
            .expect("readonly mutation recorder lock")
            .get(operation)
            .copied()
            .unwrap_or_default()
    }

    pub fn total_mutations(&self) -> u64 {
        [
            "place_order",
            "place_quote_market_order",
            "amend_order",
            "place_order_list",
            "batch_place_orders",
            "cancel_order",
            "batch_cancel_orders",
            "cancel_all_orders",
            "set_leverage",
            "set_margin_mode",
            "set_position_mode",
            "close_position",
            "set_countdown_cancel_all",
        ]
        .into_iter()
        .map(|operation| self.count(operation))
        .sum()
    }
}

#[async_trait]
impl<C> PerpAccountControlProvider for ReadOnlyExchangeClient<C>
where
    C: PerpAccountControlProvider + Send + Sync,
{
    fn exchange(&self) -> ExchangeId {
        PerpAccountControlProvider::exchange(&self.inner)
    }

    fn account_control_capabilities(&self) -> AccountControlCapabilities {
        self.inner.account_control_capabilities()
    }

    async fn get_symbol_account_config(
        &self,
        request: SymbolAccountConfigRequest,
    ) -> ExchangeApiResult<SymbolAccountConfigResponse> {
        self.inner.get_symbol_account_config(request).await
    }

    async fn set_leverage(
        &self,
        _request: SetLeverageRequest,
    ) -> ExchangeApiResult<SetLeverageResponse> {
        self.recorder.record("set_leverage");
        Err(readonly_error(
            PerpAccountControlProvider::exchange(self),
            "set_leverage",
        ))
    }

    async fn set_position_mode(
        &self,
        _request: SetPositionModeRequest,
    ) -> ExchangeApiResult<SetPositionModeResponse> {
        self.recorder.record("set_position_mode");
        Err(readonly_error(
            PerpAccountControlProvider::exchange(self),
            "set_position_mode",
        ))
    }

    async fn set_margin_mode(
        &self,
        _request: SetMarginModeRequest,
    ) -> ExchangeApiResult<SetMarginModeResponse> {
        self.recorder.record("set_margin_mode");
        Err(readonly_error(
            PerpAccountControlProvider::exchange(self),
            "set_margin_mode",
        ))
    }

    async fn close_position(
        &self,
        _request: ClosePositionRequest,
    ) -> ExchangeApiResult<ClosePositionResponse> {
        self.recorder.record("close_position");
        Err(readonly_error(
            PerpAccountControlProvider::exchange(self),
            "close_position",
        ))
    }

    async fn set_countdown_cancel_all(
        &self,
        _request: CountdownCancelAllRequest,
    ) -> ExchangeApiResult<CountdownCancelAllResponse> {
        self.recorder.record("set_countdown_cancel_all");
        Err(readonly_error(
            PerpAccountControlProvider::exchange(self),
            "set_countdown_cancel_all",
        ))
    }
}

#[derive(Clone)]
pub struct ReadOnlyExchangeClient<C> {
    inner: C,
    recorder: Arc<ReadOnlyMutationRecorder>,
}

impl<C> ReadOnlyExchangeClient<C> {
    pub fn new(inner: C, recorder: Arc<ReadOnlyMutationRecorder>) -> Self {
        Self { inner, recorder }
    }

    pub fn recorder(&self) -> Arc<ReadOnlyMutationRecorder> {
        self.recorder.clone()
    }
}

fn readonly_error(exchange: ExchangeId, operation: &'static str) -> ExchangeApiError {
    let mut error = ExchangeError::new(
        exchange,
        ExchangeErrorClass::Permission,
        format!("mutation endpoint {operation} is forbidden for read-only exchange client"),
        Utc::now(),
    );
    error.code = Some("readonly_guard".to_string());
    ExchangeApiError::Exchange(error)
}

#[async_trait]
impl<C> ExchangeClient for ReadOnlyExchangeClient<C>
where
    C: ExchangeClient + Send + Sync,
{
    fn exchange(&self) -> ExchangeId {
        self.inner.exchange()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        self.inner.capabilities()
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.inner.get_balances(request).await
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.inner.get_positions(request).await
    }

    async fn get_symbol_rules(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        self.inner.get_symbol_rules(request).await
    }

    async fn get_order_book(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        self.inner.get_order_book(request).await
    }

    async fn get_fees(&self, request: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        self.inner.get_fees(request).await
    }

    async fn get_funding_rates(
        &self,
        request: FundingRatesRequest,
    ) -> ExchangeApiResult<FundingRatesResponse> {
        self.inner.get_funding_rates(request).await
    }

    async fn place_order(
        &self,
        _request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.recorder.record("place_order");
        Err(readonly_error(self.exchange(), "place_order"))
    }

    async fn place_quote_market_order(
        &self,
        _request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.recorder.record("place_quote_market_order");
        Err(readonly_error(self.exchange(), "place_quote_market_order"))
    }

    async fn cancel_order(
        &self,
        _request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.recorder.record("cancel_order");
        Err(readonly_error(self.exchange(), "cancel_order"))
    }

    async fn amend_order(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.recorder.record("amend_order");
        Err(readonly_error(self.exchange(), "amend_order"))
    }

    async fn place_order_list(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.recorder.record("place_order_list");
        Err(readonly_error(self.exchange(), "place_order_list"))
    }

    async fn batch_place_orders(
        &self,
        _request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.recorder.record("batch_place_orders");
        Err(readonly_error(self.exchange(), "batch_place_orders"))
    }

    async fn batch_cancel_orders(
        &self,
        _request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.recorder.record("batch_cancel_orders");
        Err(readonly_error(self.exchange(), "batch_cancel_orders"))
    }

    async fn cancel_all_orders(
        &self,
        _request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.recorder.record("cancel_all_orders");
        Err(readonly_error(self.exchange(), "cancel_all_orders"))
    }

    async fn query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.inner.query_order(request).await
    }

    async fn get_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        self.inner.get_open_orders(request).await
    }

    async fn get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        self.inner.get_recent_fills(request).await
    }

    async fn subscribe_public_stream(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.inner.subscribe_public_stream(subscription).await
    }

    async fn subscribe_private_stream(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.inner.subscribe_private_stream(subscription).await
    }
}
