use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderListRequest, OrderListResponse, OrderState,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::{json, Value};

use super::parser::{coindcx_futures_symbol, coindcx_market_symbol};
use super::private_parser::{
    order_state_from_cancel_ack, parse_balances, parse_fee_snapshots, parse_fills, parse_order,
    parse_orders, parse_positions,
};
use super::CoinDcxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinDcxGatewayAdapter {
    pub(super) async fn get_balances_private_rest(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("coindcx.get_balances")?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "coindcx.get_balances")?;
        let value = if market_type == MarketType::Perpetual {
            self.rest
                .send_signed_post("/exchange/v1/derivatives/futures/wallets", json!({}))
                .await?
        } else {
            self.rest
                .send_signed_post("/exchange/v1/users/balances", json!({}))
                .await?
        };
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
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

    pub(super) async fn get_positions_private_rest(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request
            .market_type
            .is_some_and(|market_type| market_type != MarketType::Perpetual)
        {
            return self.unsupported_private("coindcx.spot_positions");
        }
        self.ensure_private_rest("coindcx.get_positions")?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "coindcx.get_positions")?;
        let value = self
            .rest
            .send_signed_post("/exchange/v1/derivatives/futures/positions", json!({}))
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

    pub(super) async fn get_fees_private_rest(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coindcx.get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_snapshots(&request.symbols),
        })
    }

    pub(super) async fn place_order_private_rest(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("coindcx.place_order")?;
        let endpoint = if request.symbol.market_type == MarketType::Perpetual {
            "/exchange/v1/derivatives/futures/orders/create"
        } else {
            "/exchange/v1/orders/create"
        };
        let body = coindcx_place_order_body(&request)?;
        let value = self.rest.send_signed_post(endpoint, body).await?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: parse_order(
                &self.exchange_id,
                Some(&request.symbol),
                request.symbol.market_type,
                &value,
            )?
            .ok_or_else(|| {
                ExchangeApiError::Exchange(rustcta_types::ExchangeError::new(
                    self.exchange_id.clone(),
                    rustcta_types::ExchangeErrorClass::UnknownOrderState,
                    "CoinDCX place order response did not include order state",
                    chrono::Utc::now(),
                ))
            })?,
        })
    }

    pub(super) async fn place_quote_market_order_private_rest(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        if request.symbol.market_type != MarketType::Spot {
            return self.unsupported_private("coindcx.perp_quote_market_order");
        }
        self.ensure_private_rest("coindcx.place_quote_market_order")?;
        let body = json!({
            "market": coindcx_market_symbol(&request.symbol.exchange_symbol.symbol),
            "side": coindcx_side(request.side),
            "order_type": "market_order",
            "total_quantity": request.quote_quantity,
            "client_order_id": request.client_order_id,
        });
        let value = self
            .rest
            .send_signed_post("/exchange/v1/orders/create", body)
            .await?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: parse_order(
                &self.exchange_id,
                Some(&request.symbol),
                request.symbol.market_type,
                &value,
            )?
            .ok_or_else(|| {
                ExchangeApiError::Exchange(rustcta_types::ExchangeError::new(
                    self.exchange_id.clone(),
                    rustcta_types::ExchangeErrorClass::UnknownOrderState,
                    "CoinDCX quote market response did not include order state",
                    chrono::Utc::now(),
                ))
            })?,
        })
    }

    pub(super) async fn cancel_order_private_rest(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("coindcx.cancel_order")?;
        let endpoint = if request.symbol.market_type == MarketType::Perpetual {
            "/exchange/v1/derivatives/futures/orders/cancel"
        } else {
            "/exchange/v1/orders/cancel"
        };
        let body = coindcx_cancel_order_body(&request)?;
        let value = self.rest.send_signed_post(endpoint, body).await?;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: order_state_from_cancel_ack(&self.exchange_id, &request, Some(&value))?,
            cancelled: true,
        })
    }

    pub(super) async fn amend_order_private_rest(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if request.new_client_order_id.is_some() {
            return self.unsupported_private("coindcx.amend_new_client_order_id");
        }
        self.ensure_private_rest("coindcx.amend_order")?;
        let endpoint = if request.symbol.market_type == MarketType::Perpetual {
            "/exchange/v1/derivatives/futures/orders/edit"
        } else {
            "/exchange/v1/orders/edit"
        };
        let mut body = serde_json::Map::new();
        if let Some(id) = request.exchange_order_id.clone() {
            body.insert("id".to_string(), json!(id));
        } else if request.symbol.market_type == MarketType::Spot {
            if let Some(client_id) = request.client_order_id.clone() {
                body.insert("client_order_id".to_string(), json!(client_id));
            } else {
                return Err(ExchangeApiError::InvalidRequest {
                    message:
                        "coindcx.amend_order requires exchange_order_id or spot client_order_id"
                            .to_string(),
                });
            }
        } else {
            return self.unsupported_private("coindcx.futures_amend_by_client_order_id");
        }
        body.insert("total_quantity".to_string(), json!(request.new_quantity));
        let value = self
            .rest
            .send_signed_post(endpoint, Value::Object(body))
            .await?;
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: parse_order(
                &self.exchange_id,
                Some(&request.symbol),
                request.symbol.market_type,
                &value,
            )?
            .unwrap_or_else(|| empty_order_from_amend(&request)),
        })
    }

    pub(super) async fn place_order_list_private_rest(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.unsupported_private("coindcx.place_order_list")
    }

    pub(super) async fn batch_place_orders_private_rest(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("coindcx.batch_place_orders")?;
        if request.orders.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coindcx.batch_place_orders requires at least one order".to_string(),
            });
        }
        if request
            .orders
            .iter()
            .any(|order| order.symbol.market_type != MarketType::Spot)
        {
            return self.unsupported_private("coindcx.futures_batch_place_orders");
        }
        let symbols = request
            .orders
            .iter()
            .map(|order| {
                self.ensure_exchange(&order.symbol.exchange)?;
                coindcx_place_order_body(order)?;
                Ok(order.symbol.clone())
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let orders = request
            .orders
            .iter()
            .map(coindcx_place_order_body)
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let value = self
            .rest
            .send_signed_post(
                "/exchange/v1/orders/create_multiple",
                json!({ "orders": orders }),
            )
            .await?;
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, symbols.first(), MarketType::Spot, &value)?,
            report: None,
        })
    }

    pub(super) async fn batch_cancel_orders_private_rest(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("coindcx.batch_cancel_orders")?;
        if request.cancels.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coindcx.batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        if request
            .cancels
            .iter()
            .any(|cancel| cancel.symbol.market_type != MarketType::Spot)
        {
            return self.unsupported_private("coindcx.futures_batch_cancel_by_ids");
        }
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
        }
        let ids = request
            .cancels
            .iter()
            .filter_map(|cancel| cancel.exchange_order_id.clone())
            .collect::<Vec<_>>();
        let client_order_ids = request
            .cancels
            .iter()
            .filter_map(|cancel| cancel.client_order_id.clone())
            .collect::<Vec<_>>();
        if ids.is_empty() && client_order_ids.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coindcx.batch_cancel_orders requires ids or client_order_ids".to_string(),
            });
        }
        let value = self
            .rest
            .send_signed_post(
                "/exchange/v1/orders/cancel_by_ids",
                json!({
                    "ids": ids,
                    "client_order_ids": client_order_ids,
                }),
            )
            .await?;
        let first_symbol = request.cancels.first().map(|cancel| &cancel.symbol);
        let orders = parse_orders(&self.exchange_id, first_symbol, MarketType::Spot, &value)
            .unwrap_or_else(|_| {
                request
                    .cancels
                    .iter()
                    .filter_map(|cancel| {
                        order_state_from_cancel_ack(&self.exchange_id, cancel, None).ok()
                    })
                    .collect()
            });
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: None,
        })
    }

    pub(super) async fn cancel_all_orders_private_rest(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("coindcx.cancel_all_orders")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coindcx.cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let endpoint = if symbol.market_type == MarketType::Perpetual {
            "/exchange/v1/derivatives/futures/positions/cancel_all_open_orders"
        } else {
            "/exchange/v1/orders/cancel_all"
        };
        let market = if symbol.market_type == MarketType::Perpetual {
            coindcx_futures_symbol(&symbol.exchange_symbol.symbol)
        } else {
            coindcx_market_symbol(&symbol.exchange_symbol.symbol)
        };
        let value = self
            .rest
            .send_signed_post(endpoint, json!({ "market": market }))
            .await?;
        let mut orders = parse_orders(&self.exchange_id, Some(symbol), symbol.market_type, &value)
            .unwrap_or_default();
        for order in &mut orders {
            order.status = OrderStatus::Cancelled;
            order.updated_at = chrono::Utc::now();
        }
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn query_order_private_rest(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("coindcx.query_order")?;
        let mut body = serde_json::Map::new();
        if let Some(id) = request.exchange_order_id.clone() {
            body.insert("id".to_string(), json!(id));
        } else if request.symbol.market_type == MarketType::Spot {
            if let Some(client_id) = request.client_order_id.clone() {
                body.insert("client_order_id".to_string(), json!(client_id));
            } else {
                return Err(ExchangeApiError::InvalidRequest {
                    message:
                        "coindcx.query_order requires exchange_order_id or spot client_order_id"
                            .to_string(),
                });
            }
        } else {
            return self.unsupported_private("coindcx.futures_query_by_client_order_id");
        }
        let value = self
            .rest
            .send_signed_post("/exchange/v1/orders/status", Value::Object(body))
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order: parse_order(
                &self.exchange_id,
                Some(&request.symbol),
                request.symbol.market_type,
                &value,
            )?,
        })
    }

    pub(super) async fn get_open_orders_private_rest(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("coindcx.get_open_orders")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coindcx.get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let market = if symbol.market_type == MarketType::Perpetual {
            coindcx_futures_symbol(&symbol.exchange_symbol.symbol)
        } else {
            coindcx_market_symbol(&symbol.exchange_symbol.symbol)
        };
        let endpoint = if symbol.market_type == MarketType::Perpetual {
            "/exchange/v1/derivatives/futures/orders/active_orders"
        } else {
            "/exchange/v1/orders/active_orders"
        };
        let value = self
            .rest
            .send_signed_post(endpoint, json!({ "market": market }))
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, Some(symbol), symbol.market_type, &value)?,
        })
    }

    pub(super) async fn get_recent_fills_private_rest(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("coindcx.get_recent_fills")?;
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "coindcx.get_recent_fills")?;
        let endpoint = if market_type == MarketType::Perpetual {
            "/exchange/v1/derivatives/futures/trades"
        } else {
            "/exchange/v1/orders/trade_history"
        };
        let mut body = serde_json::Map::new();
        if let Some(symbol) = &request.symbol {
            let market = if market_type == MarketType::Perpetual {
                coindcx_futures_symbol(&symbol.exchange_symbol.symbol)
            } else {
                coindcx_market_symbol(&symbol.exchange_symbol.symbol)
            };
            body.insert("market".to_string(), json!(market));
        }
        if let Some(order_id) = &request.exchange_order_id {
            body.insert("order_id".to_string(), json!(order_id));
        }
        if let Some(limit) = request.limit {
            body.insert("limit".to_string(), json!(limit.min(1000)));
        }
        let value = self
            .rest
            .send_signed_post(endpoint, Value::Object(body))
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.symbol.as_ref(),
                market_type,
                &value,
            )?,
        })
    }
}

pub(super) fn coindcx_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "coindcx.reduce_only_flag",
        });
    }
    let mut body = serde_json::Map::new();
    let market = if request.symbol.market_type == MarketType::Perpetual {
        coindcx_futures_symbol(&request.symbol.exchange_symbol.symbol)
    } else {
        coindcx_market_symbol(&request.symbol.exchange_symbol.symbol)
    };
    body.insert("market".to_string(), json!(market));
    body.insert("side".to_string(), json!(coindcx_side(request.side)));
    body.insert(
        "order_type".to_string(),
        json!(coindcx_order_type(request.order_type)),
    );
    body.insert("total_quantity".to_string(), json!(request.quantity));
    if let Some(price) = &request.price {
        body.insert("price_per_unit".to_string(), json!(price));
        body.insert("price".to_string(), json!(price));
    } else if !matches!(request.order_type, OrderType::Market) {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coindcx limit order requires price".to_string(),
        });
    }
    if let Some(client_order_id) = &request.client_order_id {
        if request.symbol.market_type == MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "coindcx.futures_client_order_id",
            });
        }
        body.insert("client_order_id".to_string(), json!(client_order_id));
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        if request.symbol.market_type == MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "coindcx.futures_post_only_unreliable",
            });
        }
        body.insert("post_only".to_string(), json!(true));
    }
    if let Some(tif) = request.time_in_force {
        match tif {
            TimeInForce::GTC => {}
            TimeInForce::IOC => {
                body.insert("time_in_force".to_string(), json!("IOC"));
            }
            TimeInForce::FOK => {
                body.insert("time_in_force".to_string(), json!("FOK"));
            }
            TimeInForce::GTX => {
                if request.symbol.market_type == MarketType::Perpetual {
                    return Err(ExchangeApiError::Unsupported {
                        operation: "coindcx.futures_post_only_unreliable",
                    });
                }
                body.insert("post_only".to_string(), json!(true));
            }
        }
    }
    Ok(Value::Object(body))
}

pub(super) fn coindcx_cancel_order_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    match (&request.exchange_order_id, &request.client_order_id) {
        (Some(order_id), None) => {
            body.insert("id".to_string(), json!(order_id));
        }
        (None, Some(client_id)) if request.symbol.market_type == MarketType::Spot => {
            body.insert("client_order_id".to_string(), json!(client_id));
        }
        (None, Some(_)) => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coindcx.futures_cancel_by_client_order_id",
            });
        }
        (Some(_), Some(_)) => {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coindcx.cancel_order accepts only one of id or client_order_id"
                    .to_string(),
            });
        }
        (None, None) => {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coindcx.cancel_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
    }
    Ok(Value::Object(body))
}

fn empty_order_from_amend(request: &AmendOrderRequest) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: request.symbol.exchange.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: request.exchange_order_id.clone(),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        status: OrderStatus::Open,
        quantity: request.new_quantity.clone(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: chrono::Utc::now(),
    }
}

fn coindcx_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn coindcx_order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "market_order",
        _ => "limit_order",
    }
}
