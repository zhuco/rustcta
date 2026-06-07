use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::OrderState;
use rustcta_types::{
    CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangeSymbol, MarketType, OrderBookSnapshot,
};

use crate::protocol::legacy_request_to_typed;
use crate::security::{ensure_secret_free_serializable, ensure_secret_free_value};
use crate::{
    BookSubscriptionAck, CredentialBoundary, ExchangeGateway, GatewayError, GatewayExchangeStatus,
    GatewayIdentity, GatewayMode, GatewayProtocolRequest, GatewayProtocolResponse, GatewayRequest,
    GatewayRequestPayload, GatewayResponse, GatewayResponsePayload, GatewayStatus,
    GetStatusResponse, LocalGateway, PrivateSubscriptionAck, GATEWAY_API_VERSION,
    GATEWAY_PROTOCOL_SCHEMA_VERSION,
};

#[derive(Debug, Clone)]
pub struct MockExchangeGateway {
    state: Arc<RwLock<MockGatewayState>>,
}

#[derive(Debug, Clone)]
pub(crate) struct MockGatewayState {
    pub(crate) status: GatewayStatus,
    pub(crate) balances: Vec<ExchangeBalance>,
    pub(crate) order_books: HashMap<String, OrderBookSnapshot>,
    pub(crate) orders: Vec<OrderState>,
    pub(crate) subscriptions: Vec<BookSubscriptionAck>,
    pub(crate) private_subscriptions: Vec<PrivateSubscriptionAck>,
    pub(crate) next_order_sequence: u64,
}

impl MockExchangeGateway {
    pub fn new(gateway_id: impl Into<String>) -> Self {
        Self::with_exchanges(gateway_id, Vec::<ExchangeId>::new())
    }

    pub fn with_exchanges(
        gateway_id: impl Into<String>,
        exchanges: impl IntoIterator<Item = ExchangeId>,
    ) -> Self {
        let now = Utc::now();
        let exchange_statuses = exchanges
            .into_iter()
            .map(|exchange| GatewayExchangeStatus {
                exchange: exchange.to_string(),
                enabled: true,
                public_stream_connected: false,
                private_stream_connected: false,
                last_heartbeat_at: Some(now),
                rate_limit_used: None,
                message: Some("mock gateway exchange".to_string()),
            })
            .collect();

        Self {
            state: Arc::new(RwLock::new(MockGatewayState {
                status: GatewayStatus {
                    api_version: GATEWAY_API_VERSION.to_string(),
                    identity: GatewayIdentity {
                        gateway_id: gateway_id.into(),
                        mode: GatewayMode::Local,
                        credential_boundary: CredentialBoundary::GatewayOnly,
                        started_at: now,
                    },
                    exchanges: exchange_statuses,
                },
                balances: Vec::new(),
                order_books: HashMap::new(),
                orders: Vec::new(),
                subscriptions: Vec::new(),
                private_subscriptions: Vec::new(),
                next_order_sequence: 1,
            })),
        }
    }

    pub fn insert_balance_snapshot(&self, balance: ExchangeBalance) -> Result<(), GatewayError> {
        self.write_state()?.balances.push(balance);
        Ok(())
    }

    pub fn insert_order_book(&self, order_book: OrderBookSnapshot) -> Result<(), GatewayError> {
        let key = order_book_key(
            &order_book.exchange_id,
            order_book.market_type,
            order_book.exchange_symbol.as_ref(),
            Some(&order_book.canonical_symbol),
        );
        self.write_state()?.order_books.insert(key, order_book);
        Ok(())
    }

    pub fn recorded_orders(&self) -> Result<Vec<OrderState>, GatewayError> {
        Ok(self.read_state()?.orders.clone())
    }

    pub fn recorded_book_subscriptions(&self) -> Result<Vec<BookSubscriptionAck>, GatewayError> {
        Ok(self.read_state()?.subscriptions.clone())
    }

    pub fn recorded_private_subscriptions(
        &self,
    ) -> Result<Vec<PrivateSubscriptionAck>, GatewayError> {
        Ok(self.read_state()?.private_subscriptions.clone())
    }

    pub(crate) fn read_state(
        &self,
    ) -> Result<std::sync::RwLockReadGuard<'_, MockGatewayState>, GatewayError> {
        self.state
            .read()
            .map_err(|_| GatewayError::Rejected("mock gateway state lock poisoned".to_string()))
    }

    pub(crate) fn write_state(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, MockGatewayState>, GatewayError> {
        self.state
            .write()
            .map_err(|_| GatewayError::Rejected("mock gateway state lock poisoned".to_string()))
    }
}

#[async_trait]
impl LocalGateway for MockExchangeGateway {
    async fn status(&self) -> Result<GatewayStatus, GatewayError> {
        let mut status = self.read_state()?.status.clone();
        for exchange in &mut status.exchanges {
            exchange.last_heartbeat_at = Some(Utc::now());
        }
        Ok(status)
    }

    async fn handle_typed(
        &self,
        request: GatewayProtocolRequest,
    ) -> Result<GatewayProtocolResponse, GatewayError> {
        request.validate()?;
        ensure_secret_free_serializable(&request, "request")?;

        let request_id = request.request_id.clone();
        let operation = request.operation;
        let payload = match request.payload {
            GatewayRequestPayload::GetStatus(request) => {
                let mut status = <Self as LocalGateway>::status(self).await?;
                if !request.include_exchanges {
                    status.exchanges.clear();
                }
                GatewayResponsePayload::Status(GetStatusResponse {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    status,
                })
            }
            GatewayRequestPayload::GetCapabilities(request) => {
                GatewayResponsePayload::Capabilities(self.handle_get_capabilities(request))
            }
            GatewayRequestPayload::GetBalances(request) => {
                GatewayResponsePayload::Balances(self.handle_get_balances(request)?)
            }
            GatewayRequestPayload::GetPositions(request) => {
                GatewayResponsePayload::Positions(self.handle_get_positions(request))
            }
            GatewayRequestPayload::GetSymbolRules(request) => {
                GatewayResponsePayload::SymbolRules(self.handle_get_symbol_rules(request)?)
            }
            GatewayRequestPayload::GetOrderBook(request) => {
                GatewayResponsePayload::OrderBook(self.handle_get_order_book(request)?)
            }
            GatewayRequestPayload::GetFees(request) => {
                GatewayResponsePayload::Fees(self.handle_get_fees(request)?)
            }
            GatewayRequestPayload::PlaceOrder(request) => {
                GatewayResponsePayload::PlaceOrder(self.handle_place_order(request)?)
            }
            GatewayRequestPayload::PlaceQuoteMarketOrder(_) => {
                return Err(GatewayError::UnsupportedOperation {
                    operation: "mock.place_quote_market_order".to_string(),
                });
            }
            GatewayRequestPayload::CancelOrder(request) => {
                GatewayResponsePayload::CancelOrder(self.handle_cancel_order(request)?)
            }
            GatewayRequestPayload::AmendOrder(_) => {
                return Err(GatewayError::UnsupportedOperation {
                    operation: "mock.amend_order".to_string(),
                });
            }
            GatewayRequestPayload::PlaceOrderList(_) => {
                return Err(GatewayError::UnsupportedOperation {
                    operation: "mock.place_order_list".to_string(),
                });
            }
            GatewayRequestPayload::BatchPlaceOrders(request) => {
                GatewayResponsePayload::BatchPlaceOrders(self.handle_batch_place_orders(request)?)
            }
            GatewayRequestPayload::BatchCancelOrders(request) => {
                GatewayResponsePayload::BatchCancelOrders(self.handle_batch_cancel_orders(request)?)
            }
            GatewayRequestPayload::CancelAllOrders(request) => {
                GatewayResponsePayload::CancelAllOrders(self.handle_cancel_all_orders(request)?)
            }
            GatewayRequestPayload::QueryOrder(request) => {
                GatewayResponsePayload::QueryOrder(self.handle_query_order(request)?)
            }
            GatewayRequestPayload::GetOpenOrders(request) => {
                GatewayResponsePayload::OpenOrders(self.handle_get_open_orders(request))
            }
            GatewayRequestPayload::GetRecentFills(request) => {
                GatewayResponsePayload::RecentFills(self.handle_get_recent_fills(request))
            }
            GatewayRequestPayload::SubscribeBooks(request) => {
                GatewayResponsePayload::BooksSubscribed(self.handle_subscribe_books(request)?)
            }
            GatewayRequestPayload::SubscribePrivate(request) => {
                GatewayResponsePayload::PrivateSubscribed(self.handle_subscribe_private(request)?)
            }
        };

        let response = GatewayProtocolResponse::accepted(request_id, operation, payload);
        ensure_secret_free_serializable(&response, "response")?;
        Ok(response)
    }
}

#[async_trait]
impl ExchangeGateway for MockExchangeGateway {
    async fn status(&self) -> Result<GatewayStatus, GatewayError> {
        <Self as LocalGateway>::status(self).await
    }

    async fn handle(&self, request: GatewayRequest) -> Result<GatewayResponse, GatewayError> {
        ensure_secret_free_value(&request.payload, "request")?;
        let typed = legacy_request_to_typed(request.clone())?;
        let typed_response = <Self as LocalGateway>::handle_typed(self, typed).await?;
        let payload = serde_json::to_value(typed_response.payload).map_err(|error| {
            GatewayError::InvalidPayload {
                message: error.to_string(),
            }
        })?;
        ensure_secret_free_value(&payload, "response")?;
        Ok(GatewayResponse {
            request_id: typed_response.request_id,
            accepted: typed_response.accepted,
            payload,
            error: typed_response.error.map(|error| error.message),
            responded_at: typed_response.responded_at,
        })
    }
}

pub(crate) fn order_book_key(
    exchange: &ExchangeId,
    market_type: MarketType,
    exchange_symbol: Option<&ExchangeSymbol>,
    canonical_symbol: Option<&CanonicalSymbol>,
) -> String {
    let symbol = exchange_symbol
        .map(|symbol| symbol.symbol.as_str())
        .or_else(|| canonical_symbol.map(CanonicalSymbol::as_str))
        .unwrap_or("*");
    format!("{exchange}:{market_type:?}:{symbol}")
}
