use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeApiError, ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, OrderState,
    PlaceOrderRequest, PlaceOrderResponse, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::Value;

use super::parser::{normalize_indodax_symbol, value_as_string};
use super::private_parser::{parse_balances, parse_fills, parse_order_state, parse_orders};
use super::IndodaxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl IndodaxGatewayAdapter {
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
            self.context_account(&request.context, "indodax.get_balances")?;
        let value = self
            .send_tapi("indodax.get_balances", "getInfo", HashMap::new())
            .await?;
        let balances = parse_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            &request.assets,
            &value,
        )?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let mut params = indodax_place_params(&request)?;
        let method = params
            .remove("method")
            .expect("method set by indodax_place_params");
        let value = self
            .send_tapi("indodax.place_order", &method, params)
            .await?;
        let order = order_from_place_ack(&self.exchange_id, &request, &value);
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
        self.ensure_spot(request.symbol.market_type)?;
        let mut params = HashMap::new();
        let symbol = normalize_indodax_symbol(&request.symbol.exchange_symbol.symbol)?;
        params.insert("method".to_string(), "trade".to_string());
        params.insert("pair".to_string(), symbol);
        params.insert(
            "type".to_string(),
            side_to_indodax(request.side).to_string(),
        );
        params.insert("price".to_string(), "0".to_string());
        match request.side {
            OrderSide::Buy => {
                params.insert("idr".to_string(), request.quote_quantity.clone());
            }
            OrderSide::Sell => return Err(ExchangeApiError::InvalidRequest {
                message:
                    "indodax quote market sell requires base quantity; use place_order market sell"
                        .to_string(),
            }),
        }
        let method = params.remove("method").expect("method set above");
        let value = self
            .send_tapi("indodax.place_quote_market_order", &method, params)
            .await?;
        let order = order_from_quote_ack(&self.exchange_id, &request, &value);
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
        self.ensure_spot(request.symbol.market_type)?;
        let order_id = request
            .exchange_order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "indodax cancel_order requires exchange_order_id".to_string(),
            })?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "indodax.cancel_by_client_order_id",
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "pair".to_string(),
            normalize_indodax_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert("order_id".to_string(), order_id.to_string());
        params.insert("type".to_string(), "buy".to_string());
        let value = self
            .send_tapi("indodax.cancel_order", "cancelOrder", params)
            .await?;
        let mut order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| cancelled_order_state(&self.exchange_id, &request));
        order.status = OrderStatus::Cancelled;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "indodax.query_by_client_order_id",
            });
        }
        let order_id = request
            .exchange_order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "indodax query_order requires exchange_order_id".to_string(),
            })?;
        let mut params = HashMap::new();
        params.insert(
            "pair".to_string(),
            normalize_indodax_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert("order_id".to_string(), order_id.to_string());
        let value = self
            .send_tapi("indodax.query_order", "getOrder", params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value).ok();
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
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
                message: "indodax open_orders requires symbol because openOrders is pair-scoped"
                    .to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "pair".to_string(),
            normalize_indodax_symbol(&symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_tapi("indodax.get_open_orders", "openOrders", params)
            .await?;
        let orders = parse_orders(&self.exchange_id, Some(symbol), &value)?;
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
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "indodax.get_recent_fills")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "indodax recent_fills requires symbol because tradeHistory is pair-scoped"
                    .to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "pair".to_string(),
            normalize_indodax_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(from_trade_id) = &request.from_trade_id {
            params.insert("from_id".to_string(), from_trade_id.clone());
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(1000).to_string());
        }
        if let Some(start_time) = request.start_time {
            params.insert("start".to_string(), start_time.timestamp().to_string());
        }
        if let Some(end_time) = request.end_time {
            params.insert("end".to_string(), end_time.timestamp().to_string());
        }
        let value = self
            .send_tapi("indodax.get_recent_fills", "tradeHistory", params)
            .await?;
        let fills = parse_fills(
            &self.exchange_id,
            tenant_id,
            account_id,
            Some(symbol),
            &value,
        )?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
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
                message: "indodax batch_place_orders requires at least one order".to_string(),
            });
        }
        if request.orders.len() > 20 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "indodax composed batch_place_orders supports at most 20 orders"
                    .to_string(),
            });
        }
        let mut orders = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            orders.push(self.place_order_impl(order.clone()).await?.order);
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
                message: "indodax batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        if request.cancels.len() > 20 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "indodax composed batch_cancel_orders supports at most 20 cancels"
                    .to_string(),
            });
        }
        let mut orders = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            orders.push(self.cancel_order_impl(cancel.clone()).await?.order);
        }
        let cancelled_count = orders.len() as u32;
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
            report: None,
        })
    }
}

fn indodax_place_params(request: &PlaceOrderRequest) -> ExchangeApiResult<HashMap<String, String>> {
    if request.client_order_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "indodax.client_order_id",
        });
    }
    if request.reduce_only || request.post_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "indodax.reduce_only_or_post_only",
        });
    }
    if !matches!(request.position_side, None | Some(PositionSide::None)) {
        return Err(ExchangeApiError::Unsupported {
            operation: "indodax.position_side",
        });
    }
    if !matches!(request.time_in_force, None | Some(TimeInForce::GTC)) {
        return Err(ExchangeApiError::Unsupported {
            operation: "indodax.time_in_force",
        });
    }
    let mut params = HashMap::new();
    let symbol = normalize_indodax_symbol(&request.symbol.exchange_symbol.symbol)?;
    let (base, quote) = symbol
        .split_once('_')
        .map(|(base, quote)| (base.to_string(), quote.to_string()))
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("invalid Indodax symbol {symbol}"),
        })?;
    params.insert("method".to_string(), "trade".to_string());
    params.insert("pair".to_string(), symbol);
    params.insert(
        "type".to_string(),
        side_to_indodax(request.side).to_string(),
    );
    match request.order_type {
        OrderType::Limit => {
            let price = request
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "indodax limit order requires price".to_string(),
                })?;
            params.insert("price".to_string(), price);
            params.insert(base.clone(), request.quantity.clone());
        }
        OrderType::Market => {
            params.insert("price".to_string(), "0".to_string());
            match request.side {
                OrderSide::Buy => {
                    let quote_quantity = request
                        .quote_quantity
                        .clone()
                        .or_else(|| request.price.as_ref().map(|_| request.quantity.clone()))
                        .ok_or_else(|| ExchangeApiError::InvalidRequest {
                            message:
                                "indodax market buy requires quote_quantity in IDR/quote asset"
                                    .to_string(),
                        })?;
                    params.insert(quote, quote_quantity);
                }
                OrderSide::Sell => {
                    params.insert(base, request.quantity.clone());
                }
            }
        }
        OrderType::PostOnly | OrderType::IOC | OrderType::FOK => {
            return Err(ExchangeApiError::Unsupported {
                operation: "indodax.advanced_order_type",
            })
        }
        OrderType::StopMarket | OrderType::StopLimit => {
            return Err(ExchangeApiError::Unsupported {
                operation: "indodax.stop_order",
            })
        }
    }
    Ok(params)
}

fn order_from_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    let payload = value.get("return").unwrap_or(value);
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: None,
        exchange_order_id: value_as_string(payload.get("order_id")),
        side: request.side,
        position_side: Some(PositionSide::None),
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    }
}

fn order_from_quote_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &QuoteMarketOrderRequest,
    value: &Value,
) -> OrderState {
    let payload = value.get("return").unwrap_or(value);
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: None,
        exchange_order_id: value_as_string(payload.get("order_id")),
        side: request.side,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Market,
        time_in_force: None,
        status: OrderStatus::New,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    }
}

fn cancelled_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: None,
        exchange_order_id: request.exchange_order_id.clone(),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
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

fn side_to_indodax(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}
