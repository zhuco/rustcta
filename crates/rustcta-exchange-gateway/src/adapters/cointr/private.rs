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

use super::parser::{normalize_symbol, text};
use super::private_parser::{
    cancelled_order, parse_balances, parse_fee_rates, parse_fills, parse_order_state, parse_orders,
};
use super::CointrGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CointrGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market(market_type)?;
        }
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        let (tenant_id, account_id) =
            self.context_account(&request.context, "cointr.get_balances")?;
        let value = match market_type {
            MarketType::Spot => {
                self.send_signed_get(
                    "cointr.get_balances",
                    "/api/v2/spot/account/assets",
                    &std::collections::HashMap::new(),
                )
                .await?
            }
            MarketType::Perpetual => {
                let mut params = std::collections::HashMap::new();
                params.insert("productType".to_string(), "USDT-FUTURES".to_string());
                self.send_signed_get(
                    "cointr.get_balances",
                    "/api/v2/mix/account/accounts",
                    &params,
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
                &request.assets,
                market_type,
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
            self.ensure_supported_market(market_type)?;
            if market_type == MarketType::Spot {
                return Ok(PositionsResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(request.exchange, request.context.request_id),
                    positions: Vec::new(),
                });
            }
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "cointr.get_positions")?;
        let mut params = std::collections::HashMap::new();
        params.insert("productType".to_string(), "USDT-FUTURES".to_string());
        let value = self
            .send_signed_get(
                "cointr.get_positions",
                "/api/v2/mix/position/all-position",
                &params,
            )
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions: super::private_parser::parse_positions(
                &self.exchange_id,
                tenant_id,
                account_id,
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
                message: "cointr get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
        }
        let value = self
            .send_signed_get(
                "cointr.get_fees",
                "/api/v2/spot/account/bills",
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap_or_else(|_| json!({ "makerFeeRate": "0.001", "takerFeeRate": "0.001" }));
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
        self.ensure_supported_market(request.symbol.market_type)?;
        let body = order_body(&request)?;
        let endpoint = order_endpoint(request.symbol.market_type);
        let value = self
            .send_signed_post("cointr.place_order", endpoint, &body)
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
        self.ensure_supported_market(request.symbol.market_type)?;
        let body = cancel_body(&request)?;
        let endpoint = cancel_endpoint(request.symbol.market_type);
        let value = self
            .send_signed_post("cointr.cancel_order", endpoint, &body)
            .await?;
        let order = cancelled_order(
            &self.exchange_id,
            &request.symbol,
            text(value.get("orderId")).or_else(|| request.exchange_order_id.clone()),
            text(value.get("clientOrderId")).or_else(|| request.client_order_id.clone()),
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
                message: "cointr batch_place_orders requires at least one order".to_string(),
            });
        }
        let mut orders = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market(order.symbol.market_type)?;
        }
        let market_type = common_order_market_type(&request.orders)?;
        let body = batch_order_body(&request.orders, market_type)?;
        let value = self
            .send_signed_post(
                "cointr.batch_place_orders",
                batch_order_endpoint(market_type),
                &body,
            )
            .await?;
        let data = value.get("data").unwrap_or(&value);
        let rows = data
            .as_array()
            .cloned()
            .or_else(|| data.get("orderInfo").and_then(Value::as_array).cloned())
            .unwrap_or_else(|| {
                request
                    .orders
                    .iter()
                    .map(|order| {
                        json!({
                            "orderId": order.client_order_id.clone().unwrap_or_default(),
                            "clientOrderId": order.client_order_id.clone().unwrap_or_default()
                        })
                    })
                    .collect()
            });
        for (index, order) in request.orders.iter().enumerate() {
            let row = rows.get(index).unwrap_or(&Value::Null);
            orders.push(
                parse_order_state(&self.exchange_id, Some(&order.symbol), row)
                    .unwrap_or_else(|_| ack_order(&self.exchange_id, order, row)),
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
                message: "cointr batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        let mut orders = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market(cancel.symbol.market_type)?;
        }
        let market_type = common_cancel_market_type(&request.cancels)?;
        let body = batch_cancel_body(&request.cancels, market_type)?;
        let value = self
            .send_signed_post(
                "cointr.batch_cancel_orders",
                batch_cancel_endpoint(market_type),
                &body,
            )
            .await?;
        let rows = value
            .as_array()
            .cloned()
            .or_else(|| value.get("successList").and_then(Value::as_array).cloned())
            .unwrap_or_default();
        for (index, cancel) in request.cancels.iter().enumerate() {
            let row = rows.get(index).unwrap_or(&Value::Null);
            orders.push(cancelled_order(
                &self.exchange_id,
                &cancel.symbol,
                text(row.get("orderId")).or_else(|| cancel.exchange_order_id.clone()),
                text(row.get("clientOrderId")).or_else(|| cancel.client_order_id.clone()),
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
            self.ensure_supported_market(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "cointr cancel_all_orders requires symbol-scoped request".to_string(),
            })?;
        let open = self
            .get_open_orders_impl(OpenOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request.context.clone(),
                exchange: request.exchange.clone(),
                market_type: Some(symbol.market_type),
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
        self.ensure_supported_market(request.symbol.market_type)?;
        let endpoint = query_endpoint(request.symbol.market_type);
        let value = self
            .send_signed_post(
                "cointr.query_order",
                endpoint,
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
            self.ensure_supported_market(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "cointr get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_supported_market(symbol.market_type)?;
        let body = match symbol.market_type {
            MarketType::Spot => json!({ "symbol": normalize_symbol(symbol)? }),
            MarketType::Perpetual => json!({
                "symbol": normalize_symbol(symbol)?,
                "productType": "USDT-FUTURES",
            }),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post(
                "cointr.get_open_orders",
                open_orders_endpoint(symbol.market_type),
                &body,
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
            self.ensure_supported_market(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "cointr get_recent_fills requires symbol".to_string(),
            })?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "cointr.get_recent_fills")?;
        self.ensure_supported_market(symbol.market_type)?;
        let mut body = json!({ "symbol": normalize_symbol(symbol)? });
        if symbol.market_type == MarketType::Perpetual {
            body["productType"] = Value::String("USDT-FUTURES".to_string());
        }
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            body["orderId"] = Value::String(order_id.to_string());
        }
        if let Some(limit) = request.limit {
            body["limit"] = Value::from(limit.min(100));
        }
        let value = self
            .send_signed_post(
                "cointr.get_recent_fills",
                fills_endpoint(symbol.market_type),
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
    if request.reduce_only && request.symbol.market_type == MarketType::Spot {
        return Err(ExchangeApiError::Unsupported {
            operation: "cointr.reduce_only_spot_order",
        });
    }
    if request.quote_quantity.is_some()
        && (request.symbol.market_type != MarketType::Spot
            || request.side != OrderSide::Buy
            || request.order_type != OrderType::Market)
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "cointr.quote_quantity_order",
        });
    }
    let mut body = json!({
        "symbol": normalize_symbol(&request.symbol)?,
        "side": side_text(request.side, request.symbol.market_type),
        "orderType": order_type_text(request.order_type, request.post_only, request.symbol.market_type)?,
    });
    match request.symbol.market_type {
        MarketType::Spot => {
            let size = request
                .quote_quantity
                .as_deref()
                .unwrap_or(request.quantity.as_str());
            body["size"] = Value::String(non_empty("quantity", size)?);
        }
        MarketType::Perpetual => {
            body["productType"] = Value::String("USDT-FUTURES".to_string());
            body["marginCoin"] = Value::String("USDT".to_string());
            body["size"] = Value::String(non_empty("quantity", &request.quantity)?);
            body["reduceOnly"] =
                Value::String(if request.reduce_only { "YES" } else { "NO" }.to_string());
            if let Some(position_side) = request.position_side {
                body["tradeSide"] = Value::String(position_side_text(position_side).to_string());
            }
        }
        _ => unreachable!("checked by caller"),
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if request.order_type != OrderType::Market {
        body["price"] = Value::String(non_empty(
            "price",
            request
                .price
                .as_deref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "cointr limit-style order requires price".to_string(),
                })?,
        )?);
        body["force"] = Value::String(tif_text(request.time_in_force, request.post_only));
    }
    Ok(body)
}

fn cancel_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({ "symbol": normalize_symbol(&request.symbol)? });
    if request.symbol.market_type == MarketType::Perpetual {
        body["productType"] = Value::String("USDT-FUTURES".to_string());
        body["marginCoin"] = Value::String("USDT".to_string());
    }
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body["orderId"] = Value::String(non_empty("exchange_order_id", order_id)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if body.get("orderId").is_none() && body.get("clientOid").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "cointr cancel_order requires exchange_order_id or client_order_id"
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
    if symbol.market_type == MarketType::Perpetual {
        body["productType"] = Value::String("USDT-FUTURES".to_string());
    }
    if let Some(order_id) = exchange_order_id {
        body["orderId"] = Value::String(non_empty("exchange_order_id", order_id)?);
    }
    if let Some(client_order_id) = client_order_id {
        body["clientOid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if body.get("orderId").is_none() && body.get("clientOid").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "cointr query_order requires exchange_order_id or client_order_id".to_string(),
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

fn side_text(side: OrderSide, _market_type: MarketType) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn order_type_text(
    order_type: OrderType,
    post_only: bool,
    market_type: MarketType,
) -> ExchangeApiResult<&'static str> {
    if post_only || order_type == OrderType::PostOnly {
        return Ok(match market_type {
            MarketType::Spot => "limit",
            MarketType::Perpetual => "limit",
            _ => unreachable!("checked by caller"),
        });
    }
    match order_type {
        OrderType::Market => Ok("market"),
        OrderType::Limit => Ok("limit"),
        OrderType::IOC => Ok("limit"),
        OrderType::FOK => Ok("limit"),
        OrderType::PostOnly => Ok("limit"),
        OrderType::StopMarket | OrderType::StopLimit => Err(ExchangeApiError::Unsupported {
            operation: "cointr.stop_order",
        }),
    }
}

fn position_side_text(side: PositionSide) -> &'static str {
    match side {
        PositionSide::Long => "open",
        PositionSide::Short => "open",
        PositionSide::Net | PositionSide::None => "open",
    }
}

fn order_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v2/spot/trade/place-order",
        MarketType::Perpetual => "/api/v2/mix/order/place-order",
        _ => unreachable!("checked by caller"),
    }
}

fn cancel_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v2/spot/trade/cancel-order",
        MarketType::Perpetual => "/api/v2/mix/order/cancel-order",
        _ => unreachable!("checked by caller"),
    }
}

fn batch_order_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v2/spot/trade/batch-orders",
        MarketType::Perpetual => "/api/v2/mix/order/batch-place-order",
        _ => unreachable!("checked by caller"),
    }
}

fn batch_cancel_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v2/spot/trade/batch-cancel-order",
        MarketType::Perpetual => "/api/v2/mix/order/batch-cancel-orders",
        _ => unreachable!("checked by caller"),
    }
}

fn query_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v2/spot/trade/orderInfo",
        MarketType::Perpetual => "/api/v2/mix/order/detail",
        _ => unreachable!("checked by caller"),
    }
}

fn open_orders_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v2/spot/trade/unfilled-orders",
        MarketType::Perpetual => "/api/v2/mix/order/orders-pending",
        _ => unreachable!("checked by caller"),
    }
}

fn fills_endpoint(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "/api/v2/spot/trade/fills",
        MarketType::Perpetual => "/api/v2/mix/order/fills",
        _ => unreachable!("checked by caller"),
    }
}

fn common_order_market_type(orders: &[PlaceOrderRequest]) -> ExchangeApiResult<MarketType> {
    let market_type = orders[0].symbol.market_type;
    if orders
        .iter()
        .any(|order| order.symbol.market_type != market_type)
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "cointr batch_place_orders requires one market_type".to_string(),
        });
    }
    Ok(market_type)
}

fn common_cancel_market_type(cancels: &[CancelOrderRequest]) -> ExchangeApiResult<MarketType> {
    let market_type = cancels[0].symbol.market_type;
    if cancels
        .iter()
        .any(|cancel| cancel.symbol.market_type != market_type)
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "cointr batch_cancel_orders requires one market_type".to_string(),
        });
    }
    Ok(market_type)
}

fn batch_order_body(
    orders: &[PlaceOrderRequest],
    market_type: MarketType,
) -> ExchangeApiResult<Value> {
    let orders = orders
        .iter()
        .map(order_body)
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(match market_type {
        MarketType::Spot => json!({ "orderList": orders }),
        MarketType::Perpetual => json!({ "orderList": orders, "productType": "USDT-FUTURES" }),
        _ => unreachable!("checked by caller"),
    })
}

fn batch_cancel_body(
    cancels: &[CancelOrderRequest],
    market_type: MarketType,
) -> ExchangeApiResult<Value> {
    let orders = cancels
        .iter()
        .map(cancel_body)
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(match market_type {
        MarketType::Spot => json!({ "orderList": orders }),
        MarketType::Perpetual => json!({ "orderList": orders, "productType": "USDT-FUTURES" }),
        _ => unreachable!("checked by caller"),
    })
}

fn tif_text(time_in_force: Option<TimeInForce>, post_only: bool) -> String {
    if post_only {
        return "post_only".to_string();
    }
    match time_in_force.unwrap_or(TimeInForce::GTC) {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTX",
    }
    .to_ascii_lowercase()
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("cointr {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}
