use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderState, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce};
use serde_json::{json, Value};

use super::parser::{normalize_spot_symbol, parse_fee_snapshots};
use super::private_parser::{
    parse_balances, parse_fills, parse_open_orders, parse_order_state, parse_positions,
};
use super::CoinstoreGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, missing_order_identity, response_metadata};

impl CoinstoreGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "coinstore.get_balances")?;
        let value = match market_type {
            MarketType::Spot => {
                self.rest
                    .send_spot_signed_post("coinstore.get_balances", "/spot/accountList", json!({}))
                    .await?
            }
            MarketType::Perpetual => {
                self.rest
                    .send_futures_signed_post(
                        "coinstore.get_balances.perp",
                        "/api/future/queryAvail",
                        json!({}),
                    )
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                market_type,
                &request.assets,
                &value,
            )?,
        })
    }

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request
            .market_type
            .is_some_and(|market| market != MarketType::Perpetual)
        {
            return self.unsupported("coinstore.get_positions.non_perpetual");
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "coinstore.get_positions")?;
        let value = self
            .rest
            .send_futures_signed_get("coinstore.get_positions", "/api/future/queryPosi", &[])
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions: parse_positions(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.symbols,
                &value,
            )?,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinstore get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        let wants_spot = request
            .symbols
            .iter()
            .any(|symbol| symbol.market_type == MarketType::Spot);
        let wants_futures = request
            .symbols
            .iter()
            .any(|symbol| symbol.market_type == MarketType::Perpetual);
        let spot_value = if wants_spot {
            Some(
                self.rest
                    .send_spot_public_post("/v2/public/config/spot/symbols", json!({}))
                    .await?,
            )
        } else {
            None
        };
        let futures_value = if wants_futures {
            Some(
                self.rest
                    .send_futures_public_get("/api/configs/public", &[])
                    .await?,
            )
        } else {
            None
        };
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_snapshots(
                &self.exchange_id,
                &request.symbols,
                spot_value.as_ref(),
                futures_value.as_ref(),
            )?,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let (endpoint, body) = coinstore_order_endpoint_and_body(&request)?;
        let value = match request.symbol.market_type {
            MarketType::Spot => {
                self.rest
                    .send_spot_signed_post("coinstore.place_order", endpoint, body)
                    .await?
            }
            MarketType::Perpetual => {
                self.rest
                    .send_futures_signed_post("coinstore.place_order.perp", endpoint, body)
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )
        .unwrap_or_else(|_| self.order_from_place_ack(&request, &value));
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_impl(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        if request.symbol.market_type != MarketType::Spot || request.side != OrderSide::Buy {
            return self.unsupported("coinstore.place_quote_market_order.non_spot_buy");
        }
        let value = self
            .rest
            .send_spot_signed_post(
                "coinstore.place_quote_market_order",
                "/trade/order/place",
                json!({
                    "symbol": normalize_spot_symbol(&request.symbol.exchange_symbol.symbol)?,
                    "side": "BUY",
                    "ordType": "MARKET",
                    "amount": request.quote_quantity,
                    "clOrdId": request.client_order_id,
                }),
            )
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )
        .unwrap_or_else(|_| {
            let now = chrono::Utc::now();
            OrderState {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                exchange: self.exchange_id.clone(),
                market_type: request.symbol.market_type,
                canonical_symbol: request.symbol.canonical_symbol.clone(),
                exchange_symbol: request.symbol.exchange_symbol.clone(),
                client_order_id: request.client_order_id.clone(),
                exchange_order_id: value
                    .get("data")
                    .and_then(|data| {
                        data.get("ordId")
                            .or_else(|| data.get("orderId"))
                            .or_else(|| data.get("id"))
                    })
                    .and_then(Value::as_str)
                    .map(str::to_string),
                side: OrderSide::Buy,
                position_side: Some(PositionSide::None),
                order_type: OrderType::Market,
                time_in_force: None,
                status: OrderStatus::Open,
                quantity: "0".to_string(),
                price: None,
                filled_quantity: "0".to_string(),
                average_fill_price: None,
                reduce_only: false,
                post_only: false,
                created_at: None,
                updated_at: now,
            }
        });
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if missing_order_identity(&request) {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinstore cancel_order requires order id or client order id".to_string(),
            });
        }
        let body = coinstore_cancel_body(&request)?;
        let value = match request.symbol.market_type {
            MarketType::Spot => {
                self.rest
                    .send_spot_signed_post("coinstore.cancel_order", "/trade/order/cancel", body)
                    .await?
            }
            MarketType::Perpetual => {
                self.rest
                    .send_futures_signed_post(
                        "coinstore.cancel_order.perp",
                        "/api/trade/order/cancel",
                        body,
                    )
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )
        .unwrap_or_else(|_| self.cancelled_order_from_request(&request));
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.orders.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinstore batch_place_orders requires at least one order".to_string(),
            });
        }
        let market_type = batch_place_market_type(&request.orders)?;
        let bodies = request
            .orders
            .iter()
            .map(|order| {
                self.ensure_exchange(&order.symbol.exchange)?;
                let (_, body) = coinstore_order_endpoint_and_body(order)?;
                Ok(body)
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let endpoint = match market_type {
            MarketType::Spot => "/trade/order/placeBatch",
            MarketType::Perpetual => "/api/trade/order/placeBatch",
            _ => return self.unsupported("coinstore.batch_place_orders.market_type"),
        };
        let value = if market_type == MarketType::Spot {
            self.rest
                .send_spot_signed_post("coinstore.batch_place_orders", endpoint, json!(bodies))
                .await?
        } else {
            self.rest
                .send_futures_signed_post(
                    "coinstore.batch_place_orders.perp",
                    endpoint,
                    json!(bodies),
                )
                .await?
        };
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: batch_place_order_states(self, &request.orders, &value),
            report: None,
        })
    }

    pub(super) async fn batch_cancel_orders_impl(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.cancels.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinstore batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        let market_type = batch_cancel_market_type(&request.cancels)?;
        let body = coinstore_batch_cancel_body(&request.cancels, market_type)?;
        let endpoint = match market_type {
            MarketType::Spot => {
                if request
                    .cancels
                    .iter()
                    .all(|cancel| cancel.exchange_order_id.is_none())
                {
                    "/trade/order/cancelBatchByClOrdId"
                } else {
                    "/trade/order/cancelBatch"
                }
            }
            MarketType::Perpetual => "/api/trade/orders/del",
            _ => return self.unsupported("coinstore.batch_cancel_orders.market_type"),
        };
        let value = if market_type == MarketType::Spot {
            self.rest
                .send_spot_signed_post("coinstore.batch_cancel_orders", endpoint, body)
                .await?
        } else {
            self.rest
                .send_futures_signed_post("coinstore.batch_cancel_orders.perp", endpoint, body)
                .await?
        };
        let orders = batch_cancel_order_states(self, &request.cancels, &value);
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: None,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market_type(market_type)?;
        if market_type != MarketType::Perpetual {
            return self.unsupported("coinstore.spot_cancel_all_orders");
        }
        let mut body = json!({});
        if let Some(symbol) = request.symbol.as_ref() {
            self.ensure_exchange(&symbol.exchange)?;
            body = json!({ "contractId": futures_contract_id(&symbol.exchange_symbol.symbol)? });
        }
        let value = self
            .rest
            .send_futures_signed_post(
                "coinstore.cancel_all_orders.perp",
                "/api/trade/order/cancelAll",
                body,
            )
            .await?;
        let orders = parse_open_orders(
            &self.exchange_id,
            request.symbol.as_ref(),
            market_type,
            &value,
        )
        .unwrap_or_default();
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if request.exchange_order_id.is_none() && request.client_order_id.is_none() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinstore query_order requires order id or client order id".to_string(),
            });
        }
        let params = coinstore_order_query_params(
            &request.symbol,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
        )?;
        let value = match request.symbol.market_type {
            MarketType::Spot => {
                self.rest
                    .send_spot_signed_get(
                        "coinstore.query_order",
                        "/api/v2/trade/order/orderInfo",
                        &params,
                    )
                    .await?
            }
            MarketType::Perpetual => {
                self.rest
                    .send_futures_signed_get(
                        "coinstore.query_order.perp",
                        "/api/v2/trade/order/orderInfo",
                        &params,
                    )
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: Some(parse_order_state(
                &self.exchange_id,
                Some(&request.symbol),
                request.symbol.market_type,
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
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let symbol = request.symbol.as_ref();
        let mut params = Vec::new();
        if let Some(symbol) = symbol {
            self.ensure_exchange(&symbol.exchange)?;
            if symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "coinstore get_open_orders symbol market_type mismatch".to_string(),
                });
            }
            params.push(("symbol".to_string(), coinstore_symbol_param(symbol)?));
        }
        let value = match market_type {
            MarketType::Spot => {
                self.rest
                    .send_spot_signed_get(
                        "coinstore.get_open_orders",
                        "/api/v2/trade/order/active",
                        &params,
                    )
                    .await?
            }
            MarketType::Perpetual => {
                self.rest
                    .send_futures_signed_get(
                        "coinstore.get_open_orders.perp",
                        "/api/v2/trade/order/active",
                        &params,
                    )
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_open_orders(&self.exchange_id, symbol, market_type, &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "coinstore.get_recent_fills")?;
        let symbol = request.symbol.as_ref();
        let mut params = Vec::new();
        if let Some(symbol) = symbol {
            self.ensure_exchange(&symbol.exchange)?;
            if symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "coinstore get_recent_fills symbol market_type mismatch".to_string(),
                });
            }
            params.push(("symbol".to_string(), coinstore_symbol_param(symbol)?));
        }
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.push(("orderId".to_string(), order_id.to_string()));
        }
        if let Some(client_id) = request.client_order_id.as_deref() {
            params.push(("clientOrderId".to_string(), client_id.to_string()));
        }
        if let Some(limit) = request.limit {
            params.push(("limit".to_string(), limit.min(100).to_string()));
        }
        let value = match market_type {
            MarketType::Spot => {
                self.rest
                    .send_spot_signed_get(
                        "coinstore.get_recent_fills",
                        "/trade/match/accountMatches",
                        &params,
                    )
                    .await?
            }
            MarketType::Perpetual => {
                self.rest
                    .send_futures_signed_get(
                        "coinstore.get_recent_fills.perp",
                        "/api/v2/trade/order/queryHisMatch",
                        &params,
                    )
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                symbol,
                market_type,
                &value,
            )?,
        })
    }

    fn order_from_place_ack(&self, request: &PlaceOrderRequest, value: &Value) -> OrderState {
        OrderState {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: self.exchange_id.clone(),
            market_type: request.symbol.market_type,
            canonical_symbol: request.symbol.canonical_symbol.clone(),
            exchange_symbol: request.symbol.exchange_symbol.clone(),
            client_order_id: request.client_order_id.clone(),
            exchange_order_id: value
                .get("data")
                .and_then(|data| {
                    data.get("orderId")
                        .or_else(|| data.get("order_id"))
                        .or_else(|| data.get("id"))
                })
                .and_then(Value::as_str)
                .map(str::to_string),
            side: request.side,
            position_side: Some(request.position_side.unwrap_or(
                if request.symbol.market_type == MarketType::Perpetual {
                    PositionSide::Net
                } else {
                    PositionSide::None
                },
            )),
            order_type: request.order_type,
            time_in_force: request.time_in_force.or(Some(TimeInForce::GTC)),
            status: OrderStatus::Open,
            quantity: request.quantity.clone(),
            price: request.price.clone(),
            filled_quantity: "0".to_string(),
            average_fill_price: None,
            reduce_only: request.reduce_only,
            post_only: request.post_only,
            created_at: None,
            updated_at: chrono::Utc::now(),
        }
    }

    fn cancelled_order_from_request(&self, request: &CancelOrderRequest) -> OrderState {
        OrderState {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: self.exchange_id.clone(),
            market_type: request.symbol.market_type,
            canonical_symbol: request.symbol.canonical_symbol.clone(),
            exchange_symbol: request.symbol.exchange_symbol.clone(),
            client_order_id: request.client_order_id.clone(),
            exchange_order_id: request.exchange_order_id.clone(),
            side: OrderSide::Buy,
            position_side: Some(if request.symbol.market_type == MarketType::Perpetual {
                PositionSide::Net
            } else {
                PositionSide::None
            }),
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            status: OrderStatus::Cancelled,
            quantity: "0".to_string(),
            price: None,
            filled_quantity: "0".to_string(),
            average_fill_price: None,
            reduce_only: false,
            post_only: false,
            created_at: None,
            updated_at: chrono::Utc::now(),
        }
    }
}

fn coinstore_order_endpoint_and_body(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<(&'static str, Value)> {
    match request.symbol.market_type {
        MarketType::Spot => {
            coinstore_spot_order_body(request).map(|body| ("/trade/order/place", body))
        }
        MarketType::Perpetual => Ok((
            "/api/trade/order/place",
            json!({
                "contractId": futures_contract_id(&request.symbol.exchange_symbol.symbol)?,
                "clientOrderId": request.client_order_id,
                "orderSide": if request.side == OrderSide::Buy { "BUY" } else { "SELL" },
                "orderType": coinstore_futures_order_type(request.order_type)?,
                "orderPrice": request.price,
                "orderQty": request.quantity,
                "positionEffect": if request.reduce_only { 2 } else { 1 },
            }),
        )),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinstore.place_order.market_type",
        }),
    }
}

fn coinstore_spot_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "symbol": normalize_spot_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": if request.side == OrderSide::Buy { "BUY" } else { "SELL" },
        "ordType": coinstore_spot_order_type(request.order_type, request.post_only)?,
        "quantity": request.quantity,
        "price": request.price,
        "clOrdId": request.client_order_id,
    });
    if let Some(time_in_force) = request.time_in_force {
        body["timeInForce"] = json!(coinstore_spot_time_in_force(time_in_force)?);
    }
    Ok(body)
}

fn coinstore_spot_order_type(
    order_type: OrderType,
    post_only: bool,
) -> ExchangeApiResult<&'static str> {
    if post_only || order_type == OrderType::PostOnly {
        return Ok("POST_ONLY");
    }
    match order_type {
        OrderType::Market => Ok("MARKET"),
        OrderType::Limit => Ok("LIMIT"),
        OrderType::IOC => Ok("IOC"),
        OrderType::FOK => Ok("FOK"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinstore.order_type",
        }),
    }
}

fn coinstore_spot_time_in_force(time_in_force: TimeInForce) -> ExchangeApiResult<&'static str> {
    match time_in_force {
        TimeInForce::GTC => Ok("GTC"),
        TimeInForce::IOC => Ok("IOC"),
        TimeInForce::FOK => Ok("FOK"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinstore.time_in_force",
        }),
    }
}

fn coinstore_futures_order_type(order_type: OrderType) -> ExchangeApiResult<i64> {
    match order_type {
        OrderType::Limit => Ok(1),
        OrderType::Market => Ok(2),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinstore.futures_order_type",
        }),
    }
}

fn coinstore_cancel_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let symbol = coinstore_symbol_param(&request.symbol)?;
    Ok(json!({
        "symbol": symbol,
        "orderId": request.exchange_order_id,
        "clientOrderId": request.client_order_id,
    }))
}

fn coinstore_batch_cancel_body(
    cancels: &[CancelOrderRequest],
    market_type: MarketType,
) -> ExchangeApiResult<Value> {
    let first_symbol = cancels
        .first()
        .map(|cancel| coinstore_symbol_param(&cancel.symbol))
        .transpose()?
        .unwrap_or_default();
    if cancels
        .iter()
        .any(|cancel| cancel.symbol.market_type != market_type)
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinstore batch_cancel_orders requires one market_type".to_string(),
        });
    }
    let order_ids = cancels
        .iter()
        .filter_map(|cancel| cancel.exchange_order_id.clone())
        .collect::<Vec<_>>();
    let client_ids = cancels
        .iter()
        .filter_map(|cancel| cancel.client_order_id.clone())
        .collect::<Vec<_>>();
    if order_ids.is_empty() && client_ids.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinstore batch_cancel_orders requires order ids".to_string(),
        });
    }
    if market_type == MarketType::Spot && order_ids.is_empty() {
        Ok(json!({
            "symbol": first_symbol,
            "clientOrderIds": client_ids,
        }))
    } else {
        Ok(json!({
            "symbol": first_symbol,
            "orderIds": order_ids,
            "clientOrderIds": client_ids,
        }))
    }
}

fn coinstore_order_query_params(
    symbol: &SymbolScope,
    order_id: Option<&str>,
    client_order_id: Option<&str>,
) -> ExchangeApiResult<Vec<(String, String)>> {
    let mut params = vec![("symbol".to_string(), coinstore_symbol_param(symbol)?)];
    if let Some(order_id) = order_id {
        params.push(("orderId".to_string(), order_id.to_string()));
    }
    if let Some(client_order_id) = client_order_id {
        params.push(("clientOrderId".to_string(), client_order_id.to_string()));
    }
    Ok(params)
}

fn coinstore_symbol_param(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    match symbol.market_type {
        MarketType::Spot => normalize_spot_symbol(&symbol.exchange_symbol.symbol),
        MarketType::Perpetual => futures_contract_id(&symbol.exchange_symbol.symbol)
            .map(|contract_id| contract_id.to_string()),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinstore.symbol_param.market_type",
        }),
    }
}

fn futures_contract_id(symbol: &str) -> ExchangeApiResult<i64> {
    symbol
        .trim()
        .parse::<i64>()
        .map_err(|_| ExchangeApiError::InvalidRequest {
            message: format!(
                "coinstore futures requests require numeric contractId in exchange_symbol, got {symbol}"
            ),
        })
}

fn batch_place_market_type(orders: &[PlaceOrderRequest]) -> ExchangeApiResult<MarketType> {
    let market_type = orders
        .first()
        .map(|order| order.symbol.market_type)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinstore batch_place_orders requires at least one order".to_string(),
        })?;
    if orders
        .iter()
        .any(|order| order.symbol.market_type != market_type)
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinstore batch_place_orders requires one market_type".to_string(),
        });
    }
    Ok(market_type)
}

fn batch_cancel_market_type(cancels: &[CancelOrderRequest]) -> ExchangeApiResult<MarketType> {
    let market_type = cancels
        .first()
        .map(|cancel| cancel.symbol.market_type)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinstore batch_cancel_orders requires at least one cancel".to_string(),
        })?;
    if cancels
        .iter()
        .any(|cancel| cancel.symbol.market_type != market_type)
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinstore batch_cancel_orders requires one market_type".to_string(),
        });
    }
    Ok(market_type)
}

fn batch_place_order_states(
    adapter: &CoinstoreGatewayAdapter,
    requests: &[PlaceOrderRequest],
    value: &Value,
) -> Vec<OrderState> {
    value
        .get("data")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .enumerate()
                .filter_map(|(index, row)| {
                    requests.get(index).map(|request| {
                        parse_order_state(
                            &adapter.exchange_id,
                            Some(&request.symbol),
                            request.symbol.market_type,
                            row,
                        )
                        .unwrap_or_else(|_| adapter.order_from_place_ack(request, row))
                    })
                })
                .collect()
        })
        .unwrap_or_else(|| {
            requests
                .iter()
                .map(|request| adapter.order_from_place_ack(request, value))
                .collect()
        })
}

fn batch_cancel_order_states(
    adapter: &CoinstoreGatewayAdapter,
    requests: &[CancelOrderRequest],
    value: &Value,
) -> Vec<OrderState> {
    value
        .get("data")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .enumerate()
                .filter_map(|(index, row)| {
                    requests.get(index).map(|request| {
                        parse_order_state(
                            &adapter.exchange_id,
                            Some(&request.symbol),
                            request.symbol.market_type,
                            row,
                        )
                        .unwrap_or_else(|_| adapter.cancelled_order_from_request(request))
                    })
                })
                .collect()
        })
        .unwrap_or_else(|| {
            requests
                .iter()
                .map(|request| adapter.cancelled_order_from_request(request))
                .collect()
        })
}
