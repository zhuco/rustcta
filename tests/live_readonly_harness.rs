#![allow(clippy::all)]
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, ExchangeClient, ExchangeClientCapabilities, ExchangeId, FeesRequest,
    FeesResponse, MarketType, OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest,
    OrderBookResponse, OrderSide, OrderType, PlaceOrderRequest, PlaceOrderResponse,
    PositionsRequest, PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription,
    QueryOrderRequest, QueryOrderResponse, ReadOnlyExchangeClient, ReadOnlyMutationRecorder,
    RecentFillsRequest, RecentFillsResponse, RequestContext, ResponseMetadata, SymbolRulesRequest,
    SymbolRulesResponse, SymbolScope, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};

#[derive(Debug)]
struct MockReadonlyExchange {
    exchange: ExchangeId,
}

fn exchange_id() -> ExchangeId {
    serde_json::from_str("\"binance\"").expect("exchange id")
}

fn market_type() -> MarketType {
    serde_json::from_str("\"spot\"").expect("market type")
}

fn symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: market_type(),
        canonical_symbol: None,
        exchange_symbol: rustcta_exchange_api::ExchangeSymbol::new(
            exchange_id(),
            market_type(),
            "BTCUSDT",
        )
        .expect("exchange symbol"),
    }
}

fn context() -> RequestContext {
    RequestContext::new(Utc::now())
}

fn metadata() -> ResponseMetadata {
    ResponseMetadata::new(exchange_id(), Utc::now())
}

#[async_trait]
impl ExchangeClient for MockReadonlyExchange {
    fn exchange(&self) -> ExchangeId {
        self.exchange.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        ExchangeClientCapabilities::new(self.exchange.clone())
    }

    async fn get_balances(&self, _: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: metadata(),
            balances: Vec::new(),
        })
    }

    async fn get_positions(&self, _: PositionsRequest) -> ExchangeApiResult<PositionsResponse> {
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: metadata(),
            positions: Vec::new(),
        })
    }

    async fn get_symbol_rules(
        &self,
        _: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: metadata(),
            rules: Vec::new(),
        })
    }

    async fn get_order_book(&self, _: OrderBookRequest) -> ExchangeApiResult<OrderBookResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "get_order_book",
        })
    }

    async fn get_fees(&self, _: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: metadata(),
            fees: Vec::new(),
        })
    }

    async fn place_order(&self, _: PlaceOrderRequest) -> ExchangeApiResult<PlaceOrderResponse> {
        panic!("readonly wrapper must intercept place_order")
    }

    async fn cancel_order(&self, _: CancelOrderRequest) -> ExchangeApiResult<CancelOrderResponse> {
        panic!("readonly wrapper must intercept cancel_order")
    }

    async fn query_order(&self, _: QueryOrderRequest) -> ExchangeApiResult<QueryOrderResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "query_order",
        })
    }

    async fn get_open_orders(&self, _: OpenOrdersRequest) -> ExchangeApiResult<OpenOrdersResponse> {
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: metadata(),
            orders: Vec::new(),
        })
    }

    async fn get_recent_fills(
        &self,
        _: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: metadata(),
            fills: Vec::new(),
        })
    }

    async fn subscribe_public_stream(
        &self,
        _: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        Ok("public-subscription".to_string())
    }

    async fn subscribe_private_stream(
        &self,
        _: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        Ok("private-subscription".to_string())
    }
}

#[tokio::test]
async fn live_readonly_harness_should_block_mutations_and_forward_reads() {
    let recorder = Arc::new(ReadOnlyMutationRecorder::default());
    let client = ReadOnlyExchangeClient::new(
        MockReadonlyExchange {
            exchange: exchange_id(),
        },
        recorder.clone(),
    );

    let balances = client
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context(),
            exchange: exchange_id(),
            market_type: Some(market_type()),
            assets: Vec::new(),
        })
        .await
        .expect("read request should pass through");
    assert_eq!(balances.schema_version, EXCHANGE_API_SCHEMA_VERSION);

    let result = client
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context(),
            symbol: symbol_scope(),
            client_order_id: Some("readonly-test".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.001".to_string(),
            price: Some("50000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: true,
        })
        .await;

    match result {
        Err(ExchangeApiError::Exchange(error)) => {
            assert_eq!(error.class, rustcta_types::ExchangeErrorClass::Permission);
            assert_eq!(error.code.as_deref(), Some("readonly_guard"));
            assert!(error.message.contains("place_order"));
        }
        other => panic!("expected readonly guard error, got {other:?}"),
    }

    assert_eq!(recorder.count("place_order"), 1);
    assert_eq!(recorder.total_mutations(), 1);
}
