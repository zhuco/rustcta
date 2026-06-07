use chrono::Utc;
use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeClientCapabilities,
    FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest,
    OrderBookResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, ResponseMetadata, SymbolRulesRequest, SymbolRulesResponse,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, OrderBookSnapshot, OrderStatus};

use crate::mock::{order_book_key, MockExchangeGateway};
use crate::{
    BookSubscriptionAck, CredentialBoundary, GatewayError, GatewayIdentity, GatewayMode,
    GatewayStatus, GetCapabilitiesRequest, GetCapabilitiesResponse, PrivateSubscriptionAck,
    SubscribeBooksRequest, SubscribeBooksResponse, SubscribePrivateRequest,
    SubscribePrivateResponse, GATEWAY_API_VERSION, GATEWAY_PROTOCOL_SCHEMA_VERSION,
};

impl MockExchangeGateway {
    pub(crate) fn handle_get_balances(
        &self,
        request: BalancesRequest,
    ) -> Result<BalancesResponse, GatewayError> {
        let now = Utc::now();
        let requested_assets = request
            .assets
            .iter()
            .map(|asset| asset.trim().to_ascii_uppercase())
            .collect::<Vec<_>>();
        let balances = self
            .read_state()?
            .balances
            .iter()
            .filter(|balance| balance.exchange_id == request.exchange)
            .filter(|balance| {
                request
                    .market_type
                    .map_or(true, |market_type| balance.market_type == market_type)
            })
            .filter(|balance| {
                request
                    .context
                    .account_id
                    .as_ref()
                    .map_or(true, |account_id| &balance.account_id == account_id)
            })
            .cloned()
            .map(|mut balance| {
                if !requested_assets.is_empty() {
                    balance
                        .balances
                        .retain(|asset| requested_assets.contains(&asset.asset));
                }
                balance
            })
            .collect();

        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id: request.context.request_id,
                ..ResponseMetadata::new(request.exchange, now)
            },
            balances,
        })
    }

    pub(crate) fn handle_get_positions(&self, request: PositionsRequest) -> PositionsResponse {
        PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id: request.context.request_id,
                ..ResponseMetadata::new(request.exchange, Utc::now())
            },
            positions: Vec::new(),
        }
    }

    pub(crate) fn handle_get_capabilities(
        &self,
        request: GetCapabilitiesRequest,
    ) -> GetCapabilitiesResponse {
        let status = self
            .read_state()
            .map(|state| state.status.clone())
            .unwrap_or_else(|_| GatewayStatus {
                api_version: GATEWAY_API_VERSION.to_string(),
                identity: GatewayIdentity {
                    gateway_id: "mock".to_string(),
                    mode: GatewayMode::Local,
                    credential_boundary: CredentialBoundary::GatewayOnly,
                    started_at: Utc::now(),
                },
                exchanges: Vec::new(),
            });
        let exchanges = if request.exchanges.is_empty() {
            status
                .exchanges
                .iter()
                .filter_map(|exchange| ExchangeId::new(&exchange.exchange).ok())
                .collect::<Vec<_>>()
        } else {
            request.exchanges
        };
        let capabilities = exchanges
            .into_iter()
            .map(|exchange| {
                let mut capabilities = ExchangeClientCapabilities::new(exchange);
                capabilities.supports_public_rest = true;
                capabilities.supports_private_rest = true;
                capabilities.supports_public_streams = true;
                capabilities.supports_private_streams = true;
                capabilities.supports_order_book_snapshot = true;
                capabilities.supports_balances = true;
                capabilities.supports_positions = true;
                capabilities.supports_place_order = true;
                capabilities.supports_cancel_order = true;
                capabilities.supports_batch_place_order = true;
                capabilities.supports_batch_cancel_order = true;
                capabilities.supports_cancel_all_orders = true;
                capabilities.supports_query_order = true;
                capabilities.supports_open_orders = true;
                capabilities.supports_recent_fills = true;
                capabilities.supports_client_order_id = true;
                capabilities
            })
            .collect();

        GetCapabilitiesResponse {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            capabilities,
        }
    }

    pub(crate) fn handle_get_symbol_rules(
        &self,
        request: SymbolRulesRequest,
    ) -> Result<SymbolRulesResponse, GatewayError> {
        let exchange = request
            .symbols
            .first()
            .map(|symbol| symbol.exchange.clone())
            .ok_or_else(|| {
                GatewayError::Rejected(
                    "mock gateway get_symbol_rules requires at least one symbol".to_string(),
                )
            })?;

        Ok(SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id: request.context.request_id,
                ..ResponseMetadata::new(exchange, Utc::now())
            },
            rules: Vec::new(),
        })
    }

    pub(crate) fn handle_get_fees(
        &self,
        request: FeesRequest,
    ) -> Result<FeesResponse, GatewayError> {
        let exchange = request
            .symbols
            .first()
            .map(|symbol| symbol.exchange.clone())
            .ok_or_else(|| {
                GatewayError::Rejected(
                    "mock gateway get_fees requires at least one symbol".to_string(),
                )
            })?;

        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id: request.context.request_id,
                ..ResponseMetadata::new(exchange, Utc::now())
            },
            fees: Vec::new(),
        })
    }

    pub(crate) fn handle_query_order(
        &self,
        request: QueryOrderRequest,
    ) -> Result<QueryOrderResponse, GatewayError> {
        if request.client_order_id.is_none() && request.exchange_order_id.is_none() {
            return Err(GatewayError::Rejected(
                "query_order requires client_order_id or exchange_order_id".to_string(),
            ));
        }

        let order = self
            .read_state()?
            .orders
            .iter()
            .find(|order| {
                let same_symbol = order.exchange == request.symbol.exchange
                    && order.exchange_symbol == request.symbol.exchange_symbol;
                let same_client_order_id = request
                    .client_order_id
                    .as_ref()
                    .map_or(false, |id| order.client_order_id.as_ref() == Some(id));
                let same_exchange_order_id = request
                    .exchange_order_id
                    .as_ref()
                    .map_or(false, |id| order.exchange_order_id.as_ref() == Some(id));

                same_symbol && (same_client_order_id || same_exchange_order_id)
            })
            .cloned();

        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id: request.context.request_id,
                ..ResponseMetadata::new(request.symbol.exchange, Utc::now())
            },
            order,
        })
    }

    pub(crate) fn handle_get_open_orders(&self, request: OpenOrdersRequest) -> OpenOrdersResponse {
        let exchange = request.exchange.clone();
        let orders = self
            .read_state()
            .map(|state| {
                state
                    .orders
                    .iter()
                    .filter(|order| order.exchange == request.exchange)
                    .filter(|order| {
                        request
                            .market_type
                            .map_or(true, |market_type| order.market_type == market_type)
                    })
                    .filter(|order| {
                        request.symbol.as_ref().map_or(true, |symbol| {
                            order.exchange_symbol == symbol.exchange_symbol
                        })
                    })
                    .filter(|order| matches!(order.status, OrderStatus::Open))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id: request.context.request_id,
                ..ResponseMetadata::new(exchange, Utc::now())
            },
            orders,
        }
    }

    pub(crate) fn handle_get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> RecentFillsResponse {
        RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id: request.context.request_id,
                ..ResponseMetadata::new(request.exchange, Utc::now())
            },
            fills: Vec::new(),
        }
    }

    pub(crate) fn handle_get_order_book(
        &self,
        request: OrderBookRequest,
    ) -> Result<OrderBookResponse, GatewayError> {
        let now = Utc::now();
        let key = order_book_key(
            &request.symbol.exchange,
            request.symbol.market_type,
            Some(&request.symbol.exchange_symbol),
            request.symbol.canonical_symbol.as_ref(),
        );
        let mut order_book = self.read_state()?.order_books.get(&key).cloned();

        if order_book.is_none() {
            if let Some(canonical_symbol) = request.symbol.canonical_symbol.clone() {
                let mut empty_book = OrderBookSnapshot::new(
                    request.symbol.exchange.clone(),
                    request.symbol.market_type,
                    canonical_symbol,
                    Vec::new(),
                    Vec::new(),
                    now,
                )
                .map_err(|error| GatewayError::Rejected(error.to_string()))?;
                empty_book.exchange_symbol = Some(request.symbol.exchange_symbol.clone());
                order_book = Some(empty_book);
            }
        }

        let mut order_book = order_book.ok_or_else(|| {
            GatewayError::Rejected(
                "mock gateway has no order book and request has no canonical symbol".to_string(),
            )
        })?;

        if let Some(depth) = request.depth {
            let depth = depth as usize;
            order_book.bids.truncate(depth);
            order_book.asks.truncate(depth);
        }

        Ok(OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id: request.context.request_id,
                ..ResponseMetadata::new(request.symbol.exchange, now)
            },
            order_book,
        })
    }

    pub(crate) fn handle_place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> Result<PlaceOrderResponse, GatewayError> {
        let now = Utc::now();
        let mut state = self.write_state()?;
        let exchange_order_id = format!("mock-order-{}", state.next_order_sequence);
        state.next_order_sequence += 1;

        let order = OrderState {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: request.symbol.exchange.clone(),
            market_type: request.symbol.market_type,
            canonical_symbol: request.symbol.canonical_symbol.clone(),
            exchange_symbol: request.symbol.exchange_symbol.clone(),
            client_order_id: request.client_order_id.clone(),
            exchange_order_id: Some(exchange_order_id),
            side: request.side,
            position_side: request.position_side,
            order_type: request.order_type,
            time_in_force: request.time_in_force,
            status: OrderStatus::Open,
            quantity: request.quantity.clone(),
            price: request.price.clone(),
            filled_quantity: "0".to_string(),
            average_fill_price: None,
            reduce_only: request.reduce_only,
            post_only: request.post_only,
            created_at: Some(now),
            updated_at: now,
        };
        state.orders.push(order.clone());

        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id: request.context.request_id,
                ..ResponseMetadata::new(request.symbol.exchange, now)
            },
            order,
        })
    }

    pub(crate) fn handle_cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> Result<CancelOrderResponse, GatewayError> {
        if request.client_order_id.is_none() && request.exchange_order_id.is_none() {
            return Err(GatewayError::Rejected(
                "cancel_order requires client_order_id or exchange_order_id".to_string(),
            ));
        }

        let now = Utc::now();
        let mut state = self.write_state()?;
        let order = state
            .orders
            .iter_mut()
            .find(|order| {
                let same_symbol = order.exchange == request.symbol.exchange
                    && order.exchange_symbol == request.symbol.exchange_symbol;
                let same_client_order_id = request
                    .client_order_id
                    .as_ref()
                    .map_or(false, |id| order.client_order_id.as_ref() == Some(id));
                let same_exchange_order_id = request
                    .exchange_order_id
                    .as_ref()
                    .map_or(false, |id| order.exchange_order_id.as_ref() == Some(id));

                same_symbol && (same_client_order_id || same_exchange_order_id)
            })
            .ok_or_else(|| GatewayError::Rejected("mock order not found".to_string()))?;

        order.status = OrderStatus::Cancelled;
        order.updated_at = now;
        let order = order.clone();

        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id: request.context.request_id,
                ..ResponseMetadata::new(request.symbol.exchange, now)
            },
            order,
            cancelled: true,
        })
    }

    pub(crate) fn handle_batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> Result<BatchPlaceOrdersResponse, GatewayError> {
        let request_id = request.context.request_id.clone();
        let exchange = request.exchange;
        let mut orders = Vec::with_capacity(request.orders.len());
        for order_request in request.orders {
            orders.push(self.handle_place_order(order_request)?.order);
        }

        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id,
                ..ResponseMetadata::new(exchange, Utc::now())
            },
            orders,
        })
    }

    pub(crate) fn handle_batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> Result<BatchCancelOrdersResponse, GatewayError> {
        let request_id = request.context.request_id.clone();
        let exchange = request.exchange;
        let mut orders = Vec::with_capacity(request.cancels.len());
        for cancel_request in request.cancels {
            orders.push(self.handle_cancel_order(cancel_request)?.order);
        }
        let cancelled_count = orders.len() as u32;

        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id,
                ..ResponseMetadata::new(exchange, Utc::now())
            },
            orders,
            cancelled_count,
        })
    }

    pub(crate) fn handle_cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> Result<CancelAllOrdersResponse, GatewayError> {
        let now = Utc::now();
        let mut state = self.write_state()?;
        let mut cancelled = Vec::new();
        for order in &mut state.orders {
            if order.exchange != request.exchange {
                continue;
            }
            if !matches!(order.status, OrderStatus::Open) {
                continue;
            }
            if request
                .market_type
                .is_some_and(|market_type| order.market_type != market_type)
            {
                continue;
            }
            if request
                .symbol
                .as_ref()
                .is_some_and(|symbol| order.exchange_symbol != symbol.exchange_symbol)
            {
                continue;
            }
            order.status = OrderStatus::Cancelled;
            order.updated_at = now;
            cancelled.push(order.clone());
        }
        let cancelled_count = cancelled.len() as u32;

        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: ResponseMetadata {
                request_id: request.context.request_id,
                ..ResponseMetadata::new(request.exchange, now)
            },
            orders: cancelled,
            cancelled_count,
        })
    }

    pub(crate) fn handle_subscribe_books(
        &self,
        request: SubscribeBooksRequest,
    ) -> Result<SubscribeBooksResponse, GatewayError> {
        let now = Utc::now();
        let mut state = self.write_state()?;
        let mut acks = Vec::with_capacity(request.subscriptions.len());
        for subscription in request.subscriptions {
            let ack = BookSubscriptionAck {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                subscription_id: format!("book-sub-{}", state.subscriptions.len() + 1),
                exchange: subscription.symbol.exchange,
                market_type: subscription.symbol.market_type,
                canonical_symbol: subscription.symbol.canonical_symbol,
                exchange_symbol: subscription.symbol.exchange_symbol,
                kind: subscription.kind,
                subscribed_at: now,
            };
            state.subscriptions.push(ack.clone());
            acks.push(ack);
        }

        Ok(SubscribeBooksResponse {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            subscriptions: acks,
        })
    }

    pub(crate) fn handle_subscribe_private(
        &self,
        request: SubscribePrivateRequest,
    ) -> Result<SubscribePrivateResponse, GatewayError> {
        let now = Utc::now();
        let mut state = self.write_state()?;
        let mut acks = Vec::with_capacity(request.subscriptions.len());
        for subscription in request.subscriptions {
            let ack = PrivateSubscriptionAck {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                subscription_id: format!("private-sub-{}", state.private_subscriptions.len() + 1),
                exchange: subscription.exchange,
                market_type: subscription.market_type,
                account_id: subscription.account_id,
                kind: subscription.kind,
                subscribed_at: now,
            };
            state.private_subscriptions.push(ack.clone());
            acks.push(ack);
        }

        Ok(SubscribePrivateResponse {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            subscriptions: acks,
        })
    }
}
