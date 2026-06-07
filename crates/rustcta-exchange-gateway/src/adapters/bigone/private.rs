use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, TimeInForce};
use serde_json::{json, Value};

use super::parser::normalize_bigone_symbol;
use super::private_parser::{
    parse_balances, parse_fees, parse_fills, parse_order_state, parse_orders, parse_positions,
};
use super::BigOneGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, missing_order_identity, response_metadata};

impl BigOneGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bigone.get_balances")?;
        let value = self
            .signed_get(
                market_type == MarketType::Perpetual,
                if market_type == MarketType::Perpetual {
                    "/api/contract/v2/accounts"
                } else {
                    "/api/v3/viewer/accounts"
                },
                &HashMap::new(),
            )
            .await?;
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
            return Err(ExchangeApiError::Unsupported {
                operation: "bigone.positions_non_contract",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bigone.get_positions")?;
        let value = self
            .signed_get(true, "/api/contract/v2/positions", &HashMap::new())
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
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fees(&request.symbols, &Value::Null),
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let body = place_order_body(&request)?;
        let value = self
            .signed_post(
                request.symbol.market_type == MarketType::Perpetual,
                if request.symbol.market_type == MarketType::Perpetual {
                    "/api/contract/v2/orders"
                } else {
                    "/api/v3/viewer/orders"
                },
                &body,
            )
            .await?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_order_state(
                &self.exchange_id,
                Some(&request.symbol),
                request.symbol.market_type,
                &value,
            )?,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        if missing_order_identity(&request) {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bigone cancel_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let mut params = HashMap::new();
        let value = if request.symbol.market_type == MarketType::Perpetual {
            if let Some(order_id) = &request.exchange_order_id {
                params.insert("order_id".to_string(), order_id.clone());
            }
            if let Some(client_id) = &request.client_order_id {
                params.insert("client_id".to_string(), client_id.clone());
            }
            self.signed_delete(true, "/api/contract/v2/order", &params)
                .await?
        } else {
            let Some(order_id) = request.exchange_order_id.as_ref() else {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bigone spot cancel_order requires exchange_order_id".to_string(),
                });
            };
            self.signed_post(
                false,
                &format!("/api/v3/viewer/orders/{order_id}/cancel"),
                &Value::Null,
            )
            .await?
        };
        let mut order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )
        .unwrap_or_else(|_| cancelled_order_from_request(&self.exchange_id, &request));
        order.status = OrderStatus::Cancelled;
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
                message: "bigone batch_place_orders requires at least one order".to_string(),
            });
        }
        let market_type = request.orders[0].symbol.market_type;
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            if order.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bigone batch_place_orders cannot mix market types".to_string(),
                });
            }
        }
        let value = if market_type == MarketType::Spot {
            let body = json!({
                "orders": request
                    .orders
                    .iter()
                    .map(place_order_body)
                    .collect::<ExchangeApiResult<Vec<_>>>()?
            });
            self.signed_post(false, "/api/v3/viewer/orders/multi", &body)
                .await?
        } else {
            let mut orders = Vec::with_capacity(request.orders.len());
            for order in &request.orders {
                orders.push(self.place_order_impl(order.clone()).await?.order);
            }
            return Ok(BatchPlaceOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders,
                report: None,
            });
        };
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, None, market_type, &value)?,
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
                message: "bigone batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        let market_type = request.cancels[0].symbol.market_type;
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            if cancel.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bigone batch_cancel_orders cannot mix market types".to_string(),
                });
            }
        }
        if market_type == MarketType::Perpetual {
            let ids = request
                .cancels
                .iter()
                .filter_map(|cancel| cancel.exchange_order_id.clone())
                .collect::<Vec<_>>();
            let value = self
                .signed_post(
                    true,
                    "/api/contract/v2/orders/cancel",
                    &json!({ "order_ids": ids }),
                )
                .await?;
            let orders = parse_orders(&self.exchange_id, None, market_type, &value)?;
            return Ok(BatchCancelOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                cancelled_count: orders.len() as u32,
                orders,
                report: None,
            });
        }
        let mut orders = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            orders.push(self.cancel_order_impl(cancel.clone()).await?.order);
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
        let open_orders = self
            .get_open_orders_impl(OpenOrdersRequest {
                schema_version: request.schema_version,
                context: request.context.clone(),
                exchange: request.exchange.clone(),
                market_type: request.market_type,
                symbol: request.symbol.clone(),
                page: None,
            })
            .await?;
        let mut cancelled = Vec::new();
        for order in open_orders.orders {
            let symbol =
                request
                    .symbol
                    .clone()
                    .unwrap_or_else(|| rustcta_exchange_api::SymbolScope {
                        exchange: request.exchange.clone(),
                        market_type: order.market_type,
                        canonical_symbol: order.canonical_symbol.clone(),
                        exchange_symbol: order.exchange_symbol.clone(),
                    });
            cancelled.push(
                self.cancel_order_impl(CancelOrderRequest {
                    schema_version: request.schema_version,
                    context: request.context.clone(),
                    symbol,
                    client_order_id: order.client_order_id.clone(),
                    exchange_order_id: order.exchange_order_id.clone(),
                })
                .await?
                .order,
            );
        }
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: cancelled.len() as u32,
            orders: cancelled,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        let mut params = HashMap::new();
        if let Some(id) = &request.exchange_order_id {
            params.insert("order_id".to_string(), id.clone());
        }
        if let Some(client_id) = &request.client_order_id {
            params.insert("client_id".to_string(), client_id.clone());
        }
        let endpoint = if request.symbol.market_type == MarketType::Perpetual {
            "/api/contract/v2/order"
        } else {
            "/api/v3/viewer/order"
        };
        let value = self
            .signed_get(
                request.symbol.market_type == MarketType::Perpetual,
                endpoint,
                &params,
            )
            .await?;
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
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            params.insert(
                if market_type == MarketType::Perpetual {
                    "instrument_id"
                } else {
                    "asset_pair_name"
                }
                .to_string(),
                normalize_bigone_symbol(&symbol.exchange_symbol.symbol, market_type),
            );
        }
        let endpoint = if market_type == MarketType::Perpetual {
            "/api/contract/v2/orders"
        } else {
            "/api/v3/viewer/orders"
        };
        let value = self
            .signed_get(market_type == MarketType::Perpetual, endpoint, &params)
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(
                &self.exchange_id,
                request.symbol.as_ref(),
                market_type,
                &value,
            )?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bigone.get_recent_fills")?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            params.insert(
                if market_type == MarketType::Perpetual {
                    "instrument_id"
                } else {
                    "asset_pair_name"
                }
                .to_string(),
                normalize_bigone_symbol(&symbol.exchange_symbol.symbol, market_type),
            );
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(200).to_string());
        }
        let endpoint = if market_type == MarketType::Perpetual {
            "/api/contract/v2/fills"
        } else {
            "/api/v3/viewer/trades"
        };
        let value = self
            .signed_get(market_type == MarketType::Perpetual, endpoint, &params)
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

fn place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let symbol = normalize_bigone_symbol(
        &request.symbol.exchange_symbol.symbol,
        request.symbol.market_type,
    );
    let mut body = json!({
        if request.symbol.market_type == MarketType::Perpetual { "instrument_id" } else { "asset_pair_name" }: symbol,
        "side": side_text(request.side),
        "amount": request.quantity,
        "type": order_type_text(request.order_type, request.post_only),
    });
    if let Some(price) = &request.price {
        body["price"] = json!(price);
    } else if request.order_type.requires_limit_price() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bigone limit order requires price".to_string(),
        });
    }
    if let Some(client_id) = &request.client_order_id {
        body["client_id"] = json!(client_id);
    }
    if let Some(tif) = request.time_in_force {
        body["time_in_force"] = json!(time_in_force_text(tif));
    }
    if request.reduce_only {
        body["reduce_only"] = json!(true);
    }
    Ok(body)
}

fn cancelled_order_from_request(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: request.exchange_order_id.clone(),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
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

fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn order_type_text(order_type: OrderType, post_only: bool) -> &'static str {
    if post_only || order_type == OrderType::PostOnly {
        "POST_ONLY"
    } else {
        match order_type {
            OrderType::Market => "MARKET",
            OrderType::IOC => "IOC",
            OrderType::FOK => "FOK",
            _ => "LIMIT",
        }
    }
}

fn time_in_force_text(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTX",
    }
}
