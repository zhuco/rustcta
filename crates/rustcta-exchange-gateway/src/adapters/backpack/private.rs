use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderListRequest, OrderListResponse, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::{json, Value};

use super::parser::{backpack_market_type, normalize_backpack_symbol};
use super::private_parser::{
    order_state_from_cancel_ack, parse_balances, parse_batch_orders, parse_fee_snapshots,
    parse_fills, parse_order, parse_orders, parse_positions,
};
use super::BackpackGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BackpackGatewayAdapter {
    pub(super) async fn get_balances_private_rest(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("backpack.get_balances")?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "backpack.get_balances")?;
        let value = self
            .rest
            .send_signed_get("/api/v1/capital", "balanceQuery", &HashMap::new())
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.market_type.unwrap_or(MarketType::Spot),
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
            return Err(ExchangeApiError::Unsupported {
                operation: "backpack.positions_non_perpetual",
            });
        }
        self.ensure_private_rest("backpack.get_positions")?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "backpack.get_positions")?;
        let mut params = HashMap::new();
        params.insert("marketType".to_string(), "PERP".to_string());
        for symbol in &request.symbols {
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "backpack adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
        }
        if request.symbols.len() == 1 {
            let symbol = &request.symbols[0];
            params.insert(
                "symbol".to_string(),
                normalize_backpack_symbol(&symbol.symbol),
            );
        }
        let value = self
            .rest
            .send_signed_get("/api/v1/position", "positionQuery", &params)
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
                message: "backpack.get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        self.ensure_private_rest("backpack.get_fees")?;
        let account = self
            .rest
            .send_signed_get("/api/v1/account", "accountQuery", &HashMap::new())
            .await?;
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_snapshots(&request.symbols, &account)?,
        })
    }

    pub(super) async fn place_order_private_rest(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("backpack.place_order")?;
        let body = backpack_place_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("/api/v1/order", "orderExecute", &body)
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
                    "Backpack place order response did not include order state",
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
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("backpack.place_quote_market_order")?;
        let mut body = serde_json::Map::new();
        body.insert(
            "symbol".to_string(),
            json!(normalize_backpack_symbol(
                &request.symbol.exchange_symbol.symbol
            )),
        );
        body.insert("side".to_string(), json!(backpack_side(request.side)));
        body.insert("orderType".to_string(), json!("Market"));
        body.insert("quoteQuantity".to_string(), json!(request.quote_quantity));
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            body.insert(
                "clientId".to_string(),
                json!(numeric_client_id(
                    Some(client_order_id),
                    "backpack.place_quote_market_order"
                )?),
            );
        }
        let value = self
            .rest
            .send_signed_post("/api/v1/order", "orderExecute", &Value::Object(body))
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
                    "Backpack quote order response did not include order state",
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
        self.ensure_private_rest("backpack.cancel_order")?;
        let body = backpack_cancel_order_body(&request)?;
        let value = self
            .rest
            .send_signed_delete("/api/v1/order", "orderCancel", &body)
            .await?;
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
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported_private("backpack.amend_order")
    }

    pub(super) async fn place_order_list_private_rest(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.unsupported_private("backpack.place_order_list")
    }

    pub(super) async fn batch_place_orders_private_rest(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("backpack.batch_place_orders")?;
        if request.orders.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "backpack.batch_place_orders requires at least one order".to_string(),
            });
        }
        let symbols = request
            .orders
            .iter()
            .map(|order| {
                self.ensure_exchange(&order.symbol.exchange)?;
                self.ensure_supported_market_type(order.symbol.market_type)?;
                Ok(order.symbol.clone())
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let bodies = request
            .orders
            .iter()
            .map(backpack_place_order_body)
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let value = self
            .rest
            .send_signed_batch_post("/api/v1/orders", "orderExecute", &bodies)
            .await?;
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_batch_orders(&self.exchange_id, &symbols, &value)?,
            report: None,
        })
    }

    pub(super) async fn batch_cancel_orders_private_rest(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("backpack.batch_cancel_orders")?;
        let mut orders = Vec::with_capacity(request.cancels.len());
        for cancel in request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            let response = self.cancel_order_private_rest(cancel).await?;
            orders.push(response.order);
        }
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
        self.ensure_private_rest("backpack.cancel_all_orders")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "backpack.cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let body = json!({ "symbol": normalize_backpack_symbol(&symbol.exchange_symbol.symbol) });
        let value = self
            .rest
            .send_signed_delete("/api/v1/orders", "orderCancelAll", &body)
            .await?;
        let orders = parse_orders(&self.exchange_id, Some(symbol), symbol.market_type, &value)
            .unwrap_or_default()
            .into_iter()
            .map(|mut order| {
                order.status = OrderStatus::Cancelled;
                order.updated_at = chrono::Utc::now();
                order
            })
            .collect::<Vec<_>>();
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
        self.ensure_private_rest("backpack.query_order")?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_backpack_symbol(&request.symbol.exchange_symbol.symbol),
        );
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("orderId".to_string(), order_id.clone());
        } else if let Some(client_id) = request.client_order_id.as_deref() {
            params.insert(
                "clientId".to_string(),
                numeric_client_id(Some(client_id), "backpack.query_order")?,
            );
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message:
                    "backpack.query_order requires exchange_order_id or numeric client_order_id"
                        .to_string(),
            });
        }
        let value = self
            .rest
            .send_signed_get("/api/v1/order", "orderQuery", &params)
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
        self.ensure_private_rest("backpack.get_open_orders")?;
        let mut params = HashMap::new();
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        params.insert(
            "marketType".to_string(),
            backpack_market_type(market_type)?.to_string(),
        );
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            params.insert(
                "symbol".to_string(),
                normalize_backpack_symbol(&symbol.exchange_symbol.symbol),
            );
        }
        let value = self
            .rest
            .send_signed_get("/api/v1/orders", "orderQueryAll", &params)
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

    pub(super) async fn get_recent_fills_private_rest(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("backpack.get_recent_fills")?;
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "backpack.get_recent_fills")?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            params.insert(
                "symbol".to_string(),
                normalize_backpack_symbol(&symbol.exchange_symbol.symbol),
            );
        }
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("orderId".to_string(), order_id.clone());
        }
        if let Some(start) = request.start_time {
            params.insert("from".to_string(), start.timestamp_millis().to_string());
        }
        if let Some(end) = request.end_time {
            params.insert("to".to_string(), end.timestamp_millis().to_string());
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(1000).to_string());
        }
        let value = self
            .rest
            .send_signed_get("/wapi/v1/history/fills", "fillHistoryQueryAll", &params)
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

fn backpack_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.symbol.market_type == MarketType::Spot && request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "backpack.spot_reduce_only",
        });
    }
    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        json!(normalize_backpack_symbol(
            &request.symbol.exchange_symbol.symbol
        )),
    );
    body.insert("side".to_string(), json!(backpack_side(request.side)));
    body.insert(
        "orderType".to_string(),
        json!(backpack_order_type(request.order_type)),
    );
    if let Some(client_order_id) = &request.client_order_id {
        body.insert(
            "clientId".to_string(),
            json!(numeric_client_id(
                Some(client_order_id),
                "backpack.place_order"
            )?),
        );
    }
    match request.order_type {
        OrderType::Market => {
            if let Some(quote_quantity) = &request.quote_quantity {
                body.insert("quoteQuantity".to_string(), json!(quote_quantity));
            } else {
                body.insert("quantity".to_string(), json!(request.quantity));
            }
        }
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => {
            body.insert("quantity".to_string(), json!(request.quantity));
            body.insert(
                "price".to_string(),
                json!(request
                    .price
                    .clone()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "backpack limit order requires price".to_string(),
                    })?),
            );
            body.insert(
                "timeInForce".to_string(),
                json!(backpack_time_in_force(
                    request.time_in_force,
                    request.order_type
                )),
            );
            if request.post_only || request.order_type == OrderType::PostOnly {
                body.insert("postOnly".to_string(), json!(true));
            }
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "backpack.advanced_order_type",
            });
        }
    }
    if request.reduce_only {
        body.insert("reduceOnly".to_string(), json!(true));
    }
    Ok(Value::Object(body))
}

fn backpack_cancel_order_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        json!(normalize_backpack_symbol(
            &request.symbol.exchange_symbol.symbol
        )),
    );
    match (&request.exchange_order_id, &request.client_order_id) {
        (Some(order_id), None) => {
            body.insert("orderId".to_string(), json!(order_id));
        }
        (None, Some(client_id)) => {
            body.insert(
                "clientId".to_string(),
                json!(numeric_client_id(Some(client_id), "backpack.cancel_order")?),
            );
        }
        (Some(_), Some(_)) => {
            return Err(ExchangeApiError::InvalidRequest {
                message: "backpack.cancel_order accepts only one of order_id or client_order_id"
                    .to_string(),
            });
        }
        (None, None) => {
            return Err(ExchangeApiError::InvalidRequest {
                message:
                    "backpack.cancel_order requires exchange_order_id or numeric client_order_id"
                        .to_string(),
            });
        }
    }
    Ok(Value::Object(body))
}

fn backpack_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "Bid",
        OrderSide::Sell => "Ask",
    }
}

fn backpack_order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "Market",
        _ => "Limit",
    }
}

fn backpack_time_in_force(
    time_in_force: Option<TimeInForce>,
    order_type: OrderType,
) -> &'static str {
    match (time_in_force, order_type) {
        (Some(TimeInForce::IOC), _) | (_, OrderType::IOC) => "IOC",
        (Some(TimeInForce::FOK), _) | (_, OrderType::FOK) => "FOK",
        _ => "GTC",
    }
}

fn numeric_client_id(client_order_id: Option<&str>, operation: &str) -> ExchangeApiResult<String> {
    let client_order_id = client_order_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("{operation} requires numeric client_order_id"),
        })?;
    client_order_id
        .parse::<u32>()
        .map_err(|_| ExchangeApiError::InvalidRequest {
            message: format!("{operation} requires Backpack numeric uint32 client_order_id"),
        })?;
    Ok(client_order_id.to_string())
}
