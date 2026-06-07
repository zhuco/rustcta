use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce};
use serde_json::{json, Value};

use super::parser::{
    normalize_spot_symbol, normalize_swap_symbol, parse_fee_snapshots, swap_depth_instrument,
};
use super::private_parser::{
    parse_balances, parse_fills, parse_open_orders, parse_order_state, parse_positions,
};
use super::TapbitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, missing_order_identity, response_metadata};

impl TapbitGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_market_type(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "tapbit.get_balances")?;
        let value = match market_type {
            MarketType::Spot => {
                self.send_spot_signed_get(
                    "tapbit.get_balances",
                    "/api/v1/spot/account/list",
                    &HashMap::new(),
                )
                .await?
            }
            MarketType::Perpetual => {
                self.send_swap_signed_get(
                    "tapbit.get_balances.perp",
                    "/api/v1/usdt/account",
                    &HashMap::new(),
                )
                .await?
            }
            _ => return self.unsupported("tapbit.get_balances.market_type"),
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
        if let Some(market_type) = request.market_type {
            if market_type != MarketType::Perpetual {
                return self.unsupported("tapbit.get_positions.non_perpetual");
            }
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "tapbit.get_positions")?;
        let value = self
            .send_swap_signed_get(
                "tapbit.get_positions",
                "/api/v1/usdt/position_list",
                &HashMap::new(),
            )
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
                message: "tapbit get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            if symbol.market_type != MarketType::Spot {
                return self.unsupported("tapbit.get_fees.perpetual");
            }
        }
        let value = self
            .rest
            .send_spot_public_get("/api/spot/instruments/trade_pair_list", &HashMap::new())
            .await?;
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_snapshots(&self.exchange_id, &request.symbols, &value)?,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        let (endpoint, body) = tapbit_order_endpoint_and_body(&request)?;
        let value = match request.symbol.market_type {
            MarketType::Spot => {
                self.send_spot_signed_post("tapbit.place_order", endpoint, body)
                    .await?
            }
            MarketType::Perpetual => {
                self.send_swap_signed_post("tapbit.place_order.perp", endpoint, body)
                    .await?
            }
            _ => return self.unsupported("tapbit.place_order.market_type"),
        };
        let mut order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )
        .unwrap_or_else(|_| self.order_from_place_ack(&request, &value));
        order.status = OrderStatus::Open;
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
        self.ensure_market_type(request.symbol.market_type)?;
        if missing_order_identity(&request) {
            return Err(ExchangeApiError::InvalidRequest {
                message: "tapbit cancel_order requires exchange_order_id".to_string(),
            });
        }
        let order_id =
            request
                .exchange_order_id
                .as_deref()
                .ok_or(ExchangeApiError::Unsupported {
                    operation: "tapbit.cancel_order.client_order_id",
                })?;
        let (operation, endpoint, body) = match request.symbol.market_type {
            MarketType::Spot => (
                "tapbit.cancel_order",
                "/api/v1/spot/cancel_order",
                json!({ "order_id": order_id }),
            ),
            MarketType::Perpetual => (
                "tapbit.cancel_order.perp",
                "/api/v1/usdt/cancel_order",
                json!({ "order_id": order_id }),
            ),
            _ => return self.unsupported("tapbit.cancel_order.market_type"),
        };
        let value = if request.symbol.market_type == MarketType::Spot {
            self.send_spot_signed_post(operation, endpoint, body)
                .await?
        } else {
            self.send_swap_signed_post(operation, endpoint, body)
                .await?
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
                message: "tapbit batch_place_orders requires at least one order".to_string(),
            });
        }
        let market_type = batch_place_market_type(&request.orders)?;
        if market_type == MarketType::Spot {
            let mut body = Vec::with_capacity(request.orders.len());
            for order in &request.orders {
                self.ensure_exchange(&order.symbol.exchange)?;
                let (endpoint, order_body) = tapbit_order_endpoint_and_body(order)?;
                if endpoint != "/api/v1/spot/order" {
                    return self.unsupported("tapbit.batch_place_orders.market_type");
                }
                body.push(order_body);
            }
            let value = self
                .send_spot_signed_post(
                    "tapbit.batch_place_orders",
                    "/api/v1/spot/batch_order",
                    Value::Array(body),
                )
                .await?;
            let orders = tapbit_batch_place_order_states(self, &request.orders, &value);
            return Ok(BatchPlaceOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders,
                report: None,
            });
        }
        let mut orders = Vec::with_capacity(request.orders.len());
        for order in request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            orders.push(self.place_order_impl(order).await?.order);
        }
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
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
                message: "tapbit batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        let market_type = batch_cancel_market_type(&request.cancels)?;
        if market_type == MarketType::Spot {
            let ids = request
                .cancels
                .iter()
                .map(|cancel| {
                    self.ensure_exchange(&cancel.symbol.exchange)?;
                    cancel.exchange_order_id.clone().ok_or_else(|| {
                        ExchangeApiError::InvalidRequest {
                            message: "tapbit spot batch_cancel_orders requires exchange_order_id"
                                .to_string(),
                        }
                    })
                })
                .collect::<ExchangeApiResult<Vec<_>>>()?;
            let value = self
                .send_spot_signed_post(
                    "tapbit.batch_cancel_orders",
                    "/api/v1/spot/batch_cancel_order",
                    json!({ "orderIds": ids }),
                )
                .await?;
            let orders = tapbit_batch_cancel_order_states(self, &request.cancels, &value);
            return Ok(BatchCancelOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                cancelled_count: orders.len() as u32,
                orders,
                report: None,
            });
        }
        let mut orders = Vec::with_capacity(request.cancels.len());
        for cancel in request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            orders.push(self.cancel_order_impl(cancel).await?.order);
        }
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
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_market_type(market_type)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "tapbit cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        if symbol.market_type != market_type {
            return Err(ExchangeApiError::InvalidRequest {
                message: "tapbit cancel_all_orders symbol market_type mismatch".to_string(),
            });
        }
        let open = self.open_orders_for_symbol(symbol).await?;
        let ids = open
            .iter()
            .filter_map(|order| order.exchange_order_id.clone())
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(CancelAllOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                cancelled_count: 0,
                orders: Vec::new(),
            });
        }
        if market_type == MarketType::Spot {
            let _value = self
                .send_spot_signed_post(
                    "tapbit.cancel_all_orders",
                    "/api/v1/spot/batch_cancel_order",
                    json!({ "orderIds": ids }),
                )
                .await?;
        } else {
            for order_id in &ids {
                let _value = self
                    .send_swap_signed_post(
                        "tapbit.cancel_all_orders.perp",
                        "/api/v1/usdt/cancel_order",
                        json!({ "order_id": order_id }),
                    )
                    .await?;
            }
        }
        let mut orders = open;
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

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        let order_id =
            request
                .exchange_order_id
                .as_deref()
                .ok_or(ExchangeApiError::Unsupported {
                    operation: "tapbit.query_order.client_order_id",
                })?;
        let mut params = HashMap::new();
        params.insert("order_id".to_string(), order_id.to_string());
        let value = match request.symbol.market_type {
            MarketType::Spot => {
                self.send_spot_signed_get("tapbit.query_order", "/api/v1/spot/order_info", &params)
                    .await?
            }
            MarketType::Perpetual => {
                self.send_swap_signed_get(
                    "tapbit.query_order.perp",
                    "/api/v1/usdt/order_info",
                    &params,
                )
                .await?
            }
            _ => return self.unsupported("tapbit.query_order.market_type"),
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
        self.ensure_market_type(market_type)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "tapbit get_open_orders requires symbol".to_string(),
            })?;
        if symbol.market_type != market_type {
            return Err(ExchangeApiError::InvalidRequest {
                message: "tapbit get_open_orders symbol market_type mismatch".to_string(),
            });
        }
        let orders = self.open_orders_for_symbol(symbol).await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_market_type(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "tapbit.get_recent_fills")?;
        let symbol = request.symbol.as_ref();
        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            self.ensure_exchange(&symbol.exchange)?;
            if symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "tapbit get_recent_fills symbol market_type mismatch".to_string(),
                });
            }
            params.insert("instrument_id".to_string(), instrument_param(symbol)?);
        }
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("order_id".to_string(), order_id.to_string());
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(100).to_string());
        }
        let value = match market_type {
            MarketType::Spot => {
                let Some(symbol) = symbol else {
                    return self.unsupported("tapbit.spot_fills_without_symbol");
                };
                let mut query = HashMap::new();
                if let Some(order_id) = request.exchange_order_id.as_deref() {
                    query.insert("order_id".to_string(), order_id.to_string());
                    self.send_spot_signed_get(
                        "tapbit.get_recent_fills.spot_order_reconciliation",
                        "/api/v1/spot/order_info",
                        &query,
                    )
                    .await?
                } else {
                    let orders = self.open_orders_for_symbol(symbol).await?;
                    return Ok(RecentFillsResponse {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        metadata: response_metadata(request.exchange, request.context.request_id),
                        fills: orders
                            .into_iter()
                            .filter(|order| order.filled_quantity != "0")
                            .filter_map(|order| {
                                serde_json::to_value(order)
                                    .ok()
                                    .and_then(|value| {
                                        parse_fills(
                                            &self.exchange_id,
                                            tenant_id.clone(),
                                            account_id.clone(),
                                            Some(symbol),
                                            MarketType::Spot,
                                            &json!({ "data": [value] }),
                                        )
                                        .ok()
                                    })
                                    .and_then(|mut fills| fills.pop())
                            })
                            .collect(),
                    });
                }
            }
            MarketType::Perpetual => {
                self.send_swap_signed_get(
                    "tapbit.get_recent_fills.perp",
                    "/api/v1/usdt/fills",
                    &params,
                )
                .await?
            }
            _ => return self.unsupported("tapbit.get_recent_fills.market_type"),
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

    async fn open_orders_for_symbol(
        &self,
        symbol: &SymbolScope,
    ) -> ExchangeApiResult<Vec<rustcta_exchange_api::OrderState>> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_market_type(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert("instrument_id".to_string(), instrument_param(symbol)?);
        params.insert("latestOrderId".to_string(), String::new());
        let value = match symbol.market_type {
            MarketType::Spot => {
                self.send_spot_signed_get(
                    "tapbit.get_open_orders",
                    "/api/v1/spot/open_order_list",
                    &params,
                )
                .await?
            }
            MarketType::Perpetual => {
                self.send_swap_signed_get(
                    "tapbit.get_open_orders.perp",
                    "/api/v1/usdt/open_order_list",
                    &params,
                )
                .await?
            }
            _ => return self.unsupported("tapbit.get_open_orders.market_type"),
        };
        parse_open_orders(&self.exchange_id, Some(symbol), symbol.market_type, &value)
    }

    fn order_from_place_ack(
        &self,
        request: &PlaceOrderRequest,
        value: &Value,
    ) -> rustcta_exchange_api::OrderState {
        rustcta_exchange_api::OrderState {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: self.exchange_id.clone(),
            market_type: request.symbol.market_type,
            canonical_symbol: request.symbol.canonical_symbol.clone(),
            exchange_symbol: request.symbol.exchange_symbol.clone(),
            client_order_id: None,
            exchange_order_id: value
                .get("data")
                .and_then(|data| data.get("order_id").or_else(|| data.get("orderId")))
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
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            status: OrderStatus::Open,
            quantity: request.quantity.clone(),
            price: request.price.clone(),
            filled_quantity: "0".to_string(),
            average_fill_price: None,
            reduce_only: request.reduce_only,
            post_only: false,
            created_at: None,
            updated_at: chrono::Utc::now(),
        }
    }

    fn cancelled_order_from_request(
        &self,
        request: &CancelOrderRequest,
    ) -> rustcta_exchange_api::OrderState {
        rustcta_exchange_api::OrderState {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: self.exchange_id.clone(),
            market_type: request.symbol.market_type,
            canonical_symbol: request.symbol.canonical_symbol.clone(),
            exchange_symbol: request.symbol.exchange_symbol.clone(),
            client_order_id: None,
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

fn tapbit_order_endpoint_and_body(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<(&'static str, Value)> {
    if request.client_order_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "tapbit.client_order_id",
        });
    }
    if request.post_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "tapbit.post_only",
        });
    }
    if request.order_type != OrderType::Limit || request.time_in_force != Some(TimeInForce::GTC) {
        return Err(ExchangeApiError::Unsupported {
            operation: "tapbit.only_limit_gtc_orders",
        });
    }
    let price = request
        .price
        .as_ref()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "tapbit limit order requires price".to_string(),
        })?;
    match request.symbol.market_type {
        MarketType::Spot => {
            if request.reduce_only {
                return Err(ExchangeApiError::Unsupported {
                    operation: "tapbit.spot_reduce_only",
                });
            }
            Ok((
                "/api/v1/spot/order",
                json!({
                    "instrument_id": normalize_spot_symbol(&request.symbol.exchange_symbol.symbol)?,
                    "direction": if request.side == OrderSide::Buy { "1" } else { "2" },
                    "price": price,
                    "quantity": request.quantity,
                }),
            ))
        }
        MarketType::Perpetual => Ok((
            "/api/v1/usdt/order",
            json!({
                "instrument_id": normalize_swap_symbol(&request.symbol.exchange_symbol.symbol)?,
                "direction": if request.side == OrderSide::Buy { "1" } else { "2" },
                "price": price,
                "quantity": request.quantity,
                "reduce_only": request.reduce_only,
                "position_side": match request.position_side.unwrap_or(PositionSide::Net) {
                    PositionSide::Long => "long",
                    PositionSide::Short => "short",
                    _ => "net",
                },
            }),
        )),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "tapbit.place_order.market_type",
        }),
    }
}

fn instrument_param(symbol: &SymbolScope) -> ExchangeApiResult<String> {
    match symbol.market_type {
        MarketType::Spot => normalize_spot_symbol(&symbol.exchange_symbol.symbol),
        MarketType::Perpetual => swap_depth_instrument(&symbol.exchange_symbol.symbol),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "tapbit.instrument_param.market_type",
        }),
    }
}

fn batch_place_market_type(orders: &[PlaceOrderRequest]) -> ExchangeApiResult<MarketType> {
    let market_type = orders
        .first()
        .map(|order| order.symbol.market_type)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "tapbit batch_place_orders requires at least one order".to_string(),
        })?;
    if orders
        .iter()
        .any(|order| order.symbol.market_type != market_type)
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "tapbit batch_place_orders requires one market_type".to_string(),
        });
    }
    Ok(market_type)
}

fn batch_cancel_market_type(cancels: &[CancelOrderRequest]) -> ExchangeApiResult<MarketType> {
    let market_type = cancels
        .first()
        .map(|cancel| cancel.symbol.market_type)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "tapbit batch_cancel_orders requires at least one cancel".to_string(),
        })?;
    if cancels
        .iter()
        .any(|cancel| cancel.symbol.market_type != market_type)
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "tapbit batch_cancel_orders requires one market_type".to_string(),
        });
    }
    Ok(market_type)
}

fn tapbit_batch_place_order_states(
    adapter: &TapbitGatewayAdapter,
    requests: &[PlaceOrderRequest],
    value: &Value,
) -> Vec<rustcta_exchange_api::OrderState> {
    let rows = value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    requests
        .iter()
        .enumerate()
        .map(|(index, request)| {
            let row_value = rows.get(index).cloned().unwrap_or(Value::Null);
            let ack = json!({ "data": row_value });
            let mut order = adapter.order_from_place_ack(request, &ack);
            order.exchange_order_id = row_value
                .get("order_id")
                .or_else(|| row_value.get("orderId"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .or(order.exchange_order_id);
            order
        })
        .collect()
}

fn tapbit_batch_cancel_order_states(
    adapter: &TapbitGatewayAdapter,
    requests: &[CancelOrderRequest],
    value: &Value,
) -> Vec<rustcta_exchange_api::OrderState> {
    let rows = value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    requests
        .iter()
        .enumerate()
        .map(|(index, request)| {
            let row_value = rows.get(index);
            let mut order = adapter.cancelled_order_from_request(request);
            if let Some(order_id) = row_value
                .and_then(|row| row.get("order_id").or_else(|| row.get("orderId")))
                .and_then(Value::as_str)
            {
                order.exchange_order_id = Some(order_id.to_string());
            }
            order
        })
        .collect()
}
