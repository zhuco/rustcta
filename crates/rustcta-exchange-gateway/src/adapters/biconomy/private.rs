use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::{json, Value};

use super::parser::{normalize_symbol, text};
use super::private_parser::{
    cancelled_order, parse_balances, parse_fee_rates, parse_fills, parse_order_state, parse_orders,
};
use super::BiconomyGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BiconomyGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "biconomy.get_balances")?;
        let value = self
            .send_signed_post(
                "biconomy.get_balances",
                "/api/v2/private/account",
                &json!({}),
            )
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
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
        Err(ExchangeApiError::Unsupported {
            operation: "biconomy.futures_positions_unverified",
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "biconomy get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let value = self
            .send_signed_post("biconomy.get_fees", "/api/v2/private/account", &json!({}))
            .await?;
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_rates(&request.symbols, &value)?,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = order_body(&request)?;
        let value = self
            .send_signed_post("biconomy.place_order", "/api/v2/private/order", &body)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .ok()
            .filter(|order| order.status != OrderStatus::Unknown)
            .unwrap_or_else(|| ack_order(&self.exchange_id, &request, &value));
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
        self.ensure_spot(request.symbol.market_type)?;
        let body = cancel_body(&request)?;
        let value = self
            .send_signed_post("biconomy.cancel_order", "/api/v2/private/cancel", &body)
            .await?;
        let order = normalize_cancelled_order(
            parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
                .ok()
                .filter(|order| order.status != OrderStatus::Unknown)
                .unwrap_or_else(|| {
                    cancelled_order(
                        &self.exchange_id,
                        &request.symbol,
                        text(value.get("orderId")).or_else(|| request.exchange_order_id.clone()),
                        text(value.get("clientOrderId"))
                            .or_else(|| request.client_order_id.clone()),
                    )
                }),
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
                message: "biconomy batch_place_orders requires at least one order".to_string(),
            });
        }
        let mut orders = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_spot(order.symbol.market_type)?;
            let value = self
                .send_signed_post(
                    "biconomy.batch_place_orders",
                    "/api/v2/private/order",
                    &order_body(order)?,
                )
                .await?;
            orders.push(
                parse_order_state(&self.exchange_id, Some(&order.symbol), &value)
                    .unwrap_or_else(|_| ack_order(&self.exchange_id, order, &value)),
            );
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
                message: "biconomy batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        let mut orders = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_spot(cancel.symbol.market_type)?;
            let value = self
                .send_signed_post(
                    "biconomy.batch_cancel_orders",
                    "/api/v2/private/cancel",
                    &cancel_body(cancel)?,
                )
                .await?;
            orders.push(normalize_cancelled_order(
                parse_order_state(&self.exchange_id, Some(&cancel.symbol), &value).unwrap_or_else(
                    |_| {
                        cancelled_order(
                            &self.exchange_id,
                            &cancel.symbol,
                            text(value.get("orderId")).or_else(|| cancel.exchange_order_id.clone()),
                            text(value.get("clientOrderId"))
                                .or_else(|| cancel.client_order_id.clone()),
                        )
                    },
                ),
            ));
        }
        let cancelled_count = orders
            .iter()
            .filter(|order| order.status == OrderStatus::Cancelled)
            .count() as u32;
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
            report: None,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "biconomy cancel_all_orders requires symbol-scoped request".to_string(),
            })?;
        let open = self
            .get_open_orders_impl(OpenOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request.context.clone(),
                exchange: request.exchange.clone(),
                market_type: Some(MarketType::Spot),
                symbol: Some(symbol.clone()),
                page: None,
            })
            .await?;
        let mut orders = Vec::new();
        for order in open.orders {
            let cancel = CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request.context.clone(),
                symbol: symbol.clone(),
                client_order_id: order.client_order_id.clone(),
                exchange_order_id: order.exchange_order_id.clone(),
            };
            orders.push(self.cancel_order_impl(cancel).await?.order);
        }
        let cancelled_count = orders.len() as u32;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let value = self
            .send_signed_post(
                "biconomy.query_order",
                "/api/v2/private/orderInfo",
                &query_body(
                    &request.symbol,
                    request.exchange_order_id.as_deref(),
                    request.client_order_id.as_deref(),
                )?,
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
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "biconomy get_open_orders requires symbol".to_string(),
            })?;
        let value = self
            .send_signed_post(
                "biconomy.get_open_orders",
                "/api/v2/private/openOrders",
                &json!({ "symbol": normalize_symbol(symbol)? }),
            )
            .await?;
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
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "biconomy get_recent_fills requires symbol".to_string(),
            })?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "biconomy.get_recent_fills")?;
        let mut body = json!({ "symbol": normalize_symbol(symbol)? });
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            body["orderId"] = Value::String(order_id.to_string());
        }
        if let Some(limit) = request.limit {
            body["limit"] = Value::from(limit.min(100));
        }
        let value = self
            .send_signed_post(
                "biconomy.get_recent_fills",
                "/api/v2/private/myTrades",
                &body,
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }
}

fn order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "biconomy.reduce_only_spot_order",
        });
    }
    let mut body = json!({
        "symbol": normalize_symbol(&request.symbol)?,
        "side": side_text(request.side),
        "type": order_type_text(request.order_type, request.post_only)?,
        "quantity": non_empty("quantity", &request.quantity)?,
    });
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["newClientOrderId"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if request.order_type != OrderType::Market {
        body["price"] = Value::String(non_empty(
            "price",
            request
                .price
                .as_deref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "biconomy limit-style order requires price".to_string(),
                })?,
        )?);
        body["timeInForce"] =
            Value::String(tif_text(request.time_in_force, request.post_only).to_string());
    }
    Ok(body)
}

fn cancel_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({ "symbol": normalize_symbol(&request.symbol)? });
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body["orderId"] = Value::String(non_empty("exchange_order_id", order_id)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["origClientOrderId"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if body.get("orderId").is_none() && body.get("origClientOrderId").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "biconomy cancel_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(body)
}

fn query_body(
    symbol: &rustcta_exchange_api::SymbolScope,
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
) -> ExchangeApiResult<Value> {
    let mut body = json!({ "symbol": normalize_symbol(symbol)? });
    if let Some(order_id) = exchange_order_id {
        body["orderId"] = Value::String(non_empty("exchange_order_id", order_id)?);
    }
    if let Some(client_order_id) = client_order_id {
        body["origClientOrderId"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if body.get("orderId").is_none() && body.get("origClientOrderId").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "biconomy query_order requires exchange_order_id or client_order_id"
                .to_string(),
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
        client_order_id: text(value.get("clientOrderId"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: text(value.get("orderId").or_else(|| value.get("id"))),
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
        post_only: request.post_only || request.order_type == OrderType::PostOnly,
        created_at: None,
        updated_at: chrono::Utc::now(),
    }
}

fn normalize_cancelled_order(
    mut order: rustcta_exchange_api::OrderState,
) -> rustcta_exchange_api::OrderState {
    if matches!(order.status, OrderStatus::New | OrderStatus::Unknown) {
        order.status = OrderStatus::Cancelled;
    }
    order
}

fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn order_type_text(order_type: OrderType, post_only: bool) -> ExchangeApiResult<&'static str> {
    if post_only || order_type == OrderType::PostOnly {
        return Ok("LIMIT_MAKER");
    }
    match order_type {
        OrderType::Market => Ok("MARKET"),
        OrderType::Limit => Ok("LIMIT"),
        OrderType::IOC => Ok("LIMIT"),
        OrderType::FOK => Ok("LIMIT"),
        OrderType::PostOnly => Ok("LIMIT_MAKER"),
        OrderType::StopMarket | OrderType::StopLimit => Err(ExchangeApiError::Unsupported {
            operation: "biconomy.stop_order",
        }),
    }
}

fn tif_text(time_in_force: Option<TimeInForce>, post_only: bool) -> &'static str {
    if post_only {
        return "GTX";
    }
    match time_in_force.unwrap_or(TimeInForce::GTC) {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTX",
    }
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("biconomy {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}
