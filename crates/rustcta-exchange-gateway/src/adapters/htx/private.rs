use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};
use std::collections::HashMap;

use super::parser::normalize_symbol;
use super::private_parser::{
    cancelled_order, parse_balances, parse_fee_rates, parse_fills, parse_order_state, parse_orders,
    parse_positions,
};
use super::transport::HtxRestProduct;
use super::HtxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl HtxGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context, "htx.get_balances")?;
        let value = match market_type {
            MarketType::Spot => {
                let spot_account_id = self.config.spot_account_id.as_deref().ok_or(
                    ExchangeApiError::Unsupported {
                        operation: "htx.spot_balances_require_spot_account_id",
                    },
                )?;
                self.send_signed_get(
                    "htx.get_balances",
                    HtxRestProduct::Spot,
                    &format!("/v1/account/accounts/{spot_account_id}/balance"),
                    &HashMap::new(),
                )
                .await?
            }
            MarketType::Perpetual => {
                self.send_signed_post(
                    "htx.get_balances",
                    HtxRestProduct::LinearSwap,
                    "/linear-swap-api/v1/swap_cross_account_info",
                    &json!({}),
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
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
        if matches!(request.market_type, Some(MarketType::Spot)) {
            return Ok(PositionsResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                positions: Vec::new(),
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "htx.get_positions")?;
        let mut body = json!({});
        if let Some(symbol) = request.symbols.first() {
            self.ensure_exchange(&symbol.exchange_id)?;
            if symbol.market_type != MarketType::Perpetual {
                return Err(ExchangeApiError::Unsupported {
                    operation: "htx.positions_non_perpetual",
                });
            }
            body["contract_code"] = json!(symbol.symbol);
        }
        let value = self
            .send_signed_post(
                "htx.get_positions",
                HtxRestProduct::LinearSwap,
                "/linear-swap-api/v1/swap_cross_position_info",
                &body,
            )
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions: parse_positions(&self.exchange_id, tenant_id, account_id, &value)?,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "htx get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
        }
        let first = &request.symbols[0];
        let value = if first.market_type == MarketType::Perpetual {
            self.send_signed_post(
                "htx.get_fees",
                HtxRestProduct::LinearSwap,
                "/linear-swap-api/v1/swap_fee",
                &json!({ "contract_code": normalize_symbol(first)? }),
            )
            .await?
        } else {
            json!({ "maker_fee_rate": "0.002", "taker_fee_rate": "0.002" })
        };
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_rates(&request.symbols, &value),
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let body = order_body(&request)?;
        let (product, endpoint) = match request.symbol.market_type {
            MarketType::Spot => (HtxRestProduct::Spot, "/v1/order/orders/place"),
            MarketType::Perpetual => (
                HtxRestProduct::LinearSwap,
                "/linear-swap-api/v1/swap_cross_order",
            ),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post("htx.place_order", product, endpoint, &body)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| ack_order(&self.exchange_id, &request, &value));
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let body = cancel_body(&request)?;
        let (product, endpoint) = match request.symbol.market_type {
            MarketType::Spot => {
                let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
                    ExchangeApiError::InvalidRequest {
                        message: "htx spot cancel requires exchange_order_id".to_string(),
                    }
                })?;
                (
                    HtxRestProduct::Spot,
                    format!("/v1/order/orders/{order_id}/submitcancel"),
                )
            }
            MarketType::Perpetual => (
                HtxRestProduct::LinearSwap,
                "/linear-swap-api/v1/swap_cross_cancel".to_string(),
            ),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post("htx.cancel_order", product, &endpoint, &body)
            .await?;
        let order = cancelled_order(
            &self.exchange_id,
            &request.symbol,
            super::parser::text(value.get("order_id_str").or_else(|| value.get("order_id")))
                .or_else(|| request.exchange_order_id.clone()),
            super::parser::text(value.get("client_order_id"))
                .or_else(|| request.client_order_id.clone()),
        );
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
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
                message: "htx batch_place_orders requires at least one order".to_string(),
            });
        }
        let market_type = request.orders[0].symbol.market_type;
        if market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "htx.spot_batch_place_orders",
            });
        }
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            if order.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "htx batch_place_orders requires one market type".to_string(),
                });
            }
        }
        let orders_data = request
            .orders
            .iter()
            .map(order_body)
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let value = self
            .send_signed_post(
                "htx.batch_place_orders",
                HtxRestProduct::LinearSwap,
                "/linear-swap-api/v1/swap_cross_batchorder",
                &json!({ "orders_data": orders_data }),
            )
            .await?;
        let rows = value
            .get("success")
            .or_else(|| value.get("data"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let orders = request
            .orders
            .iter()
            .enumerate()
            .map(|(index, order)| {
                rows.get(index)
                    .map(|row| parse_order_state(&self.exchange_id, Some(&order.symbol), row))
                    .transpose()
                    .map(|order_state| {
                        order_state
                            .unwrap_or_else(|| ack_order(&self.exchange_id, order, &Value::Null))
                    })
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;
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
                message: "htx batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        let first_symbol = &request.cancels[0].symbol;
        if first_symbol.market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "htx.spot_batch_cancel_orders",
            });
        }
        let mut order_ids = Vec::new();
        let mut client_order_ids = Vec::new();
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            if cancel.symbol.exchange_symbol.symbol != first_symbol.exchange_symbol.symbol {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "htx batch cancel requires one contract_code".to_string(),
                });
            }
            if let Some(order_id) = &cancel.exchange_order_id {
                order_ids.push(order_id.clone());
            }
            if let Some(client_order_id) = &cancel.client_order_id {
                client_order_ids.push(client_order_id.clone());
            }
        }
        let mut body = json!({ "contract_code": normalize_symbol(first_symbol)? });
        if !order_ids.is_empty() {
            body["order_id"] = json!(order_ids.join(","));
        }
        if !client_order_ids.is_empty() {
            body["client_order_id"] = json!(client_order_ids.join(","));
        }
        self.send_signed_post(
            "htx.batch_cancel_orders",
            HtxRestProduct::LinearSwap,
            "/linear-swap-api/v1/swap_cross_cancel",
            &body,
        )
        .await?;
        let orders = request
            .cancels
            .iter()
            .map(|cancel| {
                cancelled_order(
                    &self.exchange_id,
                    &cancel.symbol,
                    cancel.exchange_order_id.clone(),
                    cancel.client_order_id.clone(),
                )
            })
            .collect::<Vec<_>>();
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "htx cancel_all_orders requires symbol scope".to_string(),
            })?;
        if symbol.market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "htx.spot_cancel_all_orders",
            });
        }
        self.send_signed_post(
            "htx.cancel_all_orders",
            HtxRestProduct::LinearSwap,
            "/linear-swap-api/v1/swap_cross_cancelall",
            &json!({ "contract_code": normalize_symbol(symbol)? }),
        )
        .await?;
        let order = cancelled_order(&self.exchange_id, symbol, None, None);
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: vec![order],
            cancelled_count: 1,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let value = match request.symbol.market_type {
            MarketType::Spot => {
                let order_id = request.exchange_order_id.as_ref().ok_or_else(|| {
                    ExchangeApiError::InvalidRequest {
                        message: "htx spot query order requires exchange_order_id".to_string(),
                    }
                })?;
                self.send_signed_get(
                    "htx.query_order",
                    HtxRestProduct::Spot,
                    &format!("/v1/order/orders/{order_id}"),
                    &HashMap::new(),
                )
                .await?
            }
            MarketType::Perpetual => {
                let body = query_perp_order_body(&request)?;
                self.send_signed_post(
                    "htx.query_order",
                    HtxRestProduct::LinearSwap,
                    "/linear-swap-api/v1/swap_cross_order_info",
                    &body,
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "htx open orders requires symbol scope".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        let value = match symbol.market_type {
            MarketType::Spot => {
                let mut params = HashMap::new();
                params.insert("symbol".to_string(), normalize_symbol(symbol)?);
                self.send_signed_get(
                    "htx.get_open_orders",
                    HtxRestProduct::Spot,
                    "/v1/order/openOrders",
                    &params,
                )
                .await?
            }
            MarketType::Perpetual => {
                self.send_signed_post(
                    "htx.get_open_orders",
                    HtxRestProduct::LinearSwap,
                    "/linear-swap-api/v1/swap_cross_openorders",
                    &json!({ "contract_code": normalize_symbol(symbol)?, "page_index": 1, "page_size": request.page.as_ref().and_then(|page| page.limit).unwrap_or(50).min(50) }),
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, Some(symbol), &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "htx.get_recent_fills")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "htx recent fills requires symbol scope".to_string(),
            })?;
        let value = match symbol.market_type {
            MarketType::Spot => {
                let mut params = HashMap::new();
                params.insert("symbol".to_string(), normalize_symbol(symbol)?);
                if let Some(limit) = request.limit {
                    params.insert("size".to_string(), limit.min(100).to_string());
                }
                self.send_signed_get(
                    "htx.get_recent_fills",
                    HtxRestProduct::Spot,
                    "/v1/order/matchresults",
                    &params,
                )
                .await?
            }
            MarketType::Perpetual => {
                self.send_signed_post(
                    "htx.get_recent_fills",
                    HtxRestProduct::LinearSwap,
                    "/linear-swap-api/v3/swap_cross_matchresults",
                    &json!({ "contract_code": normalize_symbol(symbol)?, "page_index": 1, "page_size": request.limit.unwrap_or(50).min(50) }),
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                Some(symbol),
                &value,
            ),
        })
    }
}

fn order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    match request.symbol.market_type {
        MarketType::Spot => {
            if request.reduce_only {
                return Err(ExchangeApiError::Unsupported {
                    operation: "htx.spot_reduce_only_order",
                });
            }
            let mut body = json!({
                "symbol": normalize_symbol(&request.symbol)?,
                "type": spot_order_type(request.side, request.order_type, request.post_only),
                "amount": request.quote_quantity.as_ref().unwrap_or(&request.quantity),
            });
            if let Some(client_order_id) = &request.client_order_id {
                body["client-order-id"] = json!(client_order_id);
            }
            if request.order_type != OrderType::Market {
                body["price"] = json!(request.price.as_deref().ok_or_else(|| {
                    ExchangeApiError::InvalidRequest {
                        message: "htx spot limit order requires price".to_string(),
                    }
                })?);
            }
            Ok(body)
        }
        MarketType::Perpetual => {
            let mut body = json!({
                "contract_code": normalize_symbol(&request.symbol)?,
                "volume": request.quantity,
                "direction": match request.side { OrderSide::Buy => "buy", OrderSide::Sell => "sell" },
                "offset": if request.reduce_only { "close" } else { "open" },
                "lever_rate": 1,
                "order_price_type": linear_order_price_type(request.order_type, request.time_in_force, request.post_only),
            });
            if let Some(price) = &request.price {
                body["price"] = json!(price);
            }
            if let Some(client_order_id) = &request.client_order_id {
                body["client_order_id"] = json!(client_order_id);
            }
            if let Some(position_side) = request.position_side {
                body["direction"] = json!(match position_side {
                    PositionSide::Long =>
                        if request.reduce_only {
                            "sell"
                        } else {
                            "buy"
                        },
                    PositionSide::Short =>
                        if request.reduce_only {
                            "buy"
                        } else {
                            "sell"
                        },
                    _ => match request.side {
                        OrderSide::Buy => "buy",
                        OrderSide::Sell => "sell",
                    },
                });
            }
            Ok(body)
        }
        _ => Err(ExchangeApiError::Unsupported {
            operation: "htx.unsupported_order_market",
        }),
    }
}

fn cancel_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    match request.symbol.market_type {
        MarketType::Spot => Ok(json!({})),
        MarketType::Perpetual => {
            let mut body = json!({ "contract_code": normalize_symbol(&request.symbol)? });
            if let Some(order_id) = &request.exchange_order_id {
                body["order_id"] = json!(order_id);
            }
            if let Some(client_order_id) = &request.client_order_id {
                body["client_order_id"] = json!(client_order_id);
            }
            if request.exchange_order_id.is_none() && request.client_order_id.is_none() {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "htx cancel requires order id or client order id".to_string(),
                });
            }
            Ok(body)
        }
        _ => Err(ExchangeApiError::Unsupported {
            operation: "htx.unsupported_cancel_market",
        }),
    }
}

fn query_perp_order_body(request: &QueryOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({ "contract_code": normalize_symbol(&request.symbol)? });
    if let Some(order_id) = &request.exchange_order_id {
        body["order_id"] = json!(order_id);
    }
    if let Some(client_order_id) = &request.client_order_id {
        body["client_order_id"] = json!(client_order_id);
    }
    if request.exchange_order_id.is_none() && request.client_order_id.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "htx perp query order requires order id or client order id".to_string(),
        });
    }
    Ok(body)
}

fn ack_order(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: super::parser::text(
            value
                .get("order_id_str")
                .or_else(|| value.get("order_id"))
                .or_else(|| value.get("id")),
        ),
        side: request.side,
        position_side: request.position_side,
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
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

fn spot_order_type(side: OrderSide, order_type: OrderType, post_only: bool) -> &'static str {
    match (side, order_type, post_only) {
        (OrderSide::Buy, OrderType::Market, _) => "buy-market",
        (OrderSide::Sell, OrderType::Market, _) => "sell-market",
        (OrderSide::Buy, _, true) => "buy-limit-maker",
        (OrderSide::Sell, _, true) => "sell-limit-maker",
        (OrderSide::Buy, _, _) => "buy-limit",
        (OrderSide::Sell, _, _) => "sell-limit",
    }
}

fn linear_order_price_type(
    order_type: OrderType,
    time_in_force: Option<TimeInForce>,
    post_only: bool,
) -> &'static str {
    if post_only || order_type == OrderType::PostOnly {
        "post_only"
    } else if matches!(time_in_force, Some(TimeInForce::IOC)) || order_type == OrderType::IOC {
        "ioc"
    } else if matches!(time_in_force, Some(TimeInForce::FOK)) || order_type == OrderType::FOK {
        "fok"
    } else if order_type == OrderType::Market {
        "optimal_5_ioc"
    } else {
        "limit"
    }
}
