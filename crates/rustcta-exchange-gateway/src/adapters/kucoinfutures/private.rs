use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderState, PageCursor, PageRequest, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_kucoinfutures_symbol;
use super::private_parser::{
    parse_account_balances, parse_fee_snapshots, parse_open_orders, parse_order_state,
    parse_positions, parse_recent_fills,
};
use super::KuCoinFuturesGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl KuCoinFuturesGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(request.symbol.market_type)?;
        let body = kucoinfutures_order_body(&request)?;
        let value = self
            .send_signed_post(
                "kucoinfutures.place_order",
                "/api/v1/orders",
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = order_state_from_place_ack(&self.exchange_id, &request, &value);
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
        self.ensure_perpetual(request.symbol.market_type)?;
        Err(ExchangeApiError::Unsupported {
            operation: "kucoinfutures.quote_sized_futures_market_order",
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_kucoinfutures_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        let endpoint = if let Some(order_id) = request
            .exchange_order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            format!("/api/v1/orders/{order_id}")
        } else if let Some(client_order_id) = request
            .client_order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            format!("/api/v1/orders/client-order/{client_order_id}")
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "kucoinfutures cancel_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        };
        let value = self
            .send_signed_delete("kucoinfutures.cancel_order", &endpoint, &params)
            .await?;
        let order = order_state_from_cancel_ack(&self.exchange_id, &request, &value);
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
                message: "kucoinfutures batch_place_orders requires at least one order".to_string(),
            });
        }
        if request.orders.len() > 20 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "kucoinfutures composed batch_place_orders supports at most 20 orders"
                    .to_string(),
            });
        }

        let mut prepared = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            ensure_exchange_api_schema(order.schema_version)?;
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_perpetual(order.symbol.market_type)?;
            prepared.push((order, kucoinfutures_order_body(order)?));
        }

        let mut orders = Vec::with_capacity(prepared.len());
        for (order, body) in prepared {
            let value = self
                .send_signed_post(
                    "kucoinfutures.batch_place_orders.composed",
                    "/api/v1/orders",
                    &HashMap::new(),
                    &body,
                )
                .await?;
            orders.push(order_state_from_place_ack(&self.exchange_id, order, &value));
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
                message: "kucoinfutures batch_cancel_orders requires at least one cancel"
                    .to_string(),
            });
        }
        if request.cancels.len() > 20 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "kucoinfutures composed batch_cancel_orders supports at most 20 cancels"
                    .to_string(),
            });
        }

        let mut orders = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            let response = self.cancel_order_impl(cancel.clone()).await?;
            orders.push(response.order);
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

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_perpetual(market_type)?;
        }
        let mut params = HashMap::new();
        let endpoint = if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_perpetual(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_kucoinfutures_symbol(&symbol.exchange_symbol.symbol)?,
            );
            "/api/v1/orders"
        } else {
            "/api/v1/orders"
        };
        let value = self
            .send_signed_delete("kucoinfutures.cancel_all_orders", endpoint, &params)
            .await?;
        let orders =
            kucoinfutures_cancel_all_orders(&self.exchange_id, request.symbol.as_ref(), &value);
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(request.symbol.market_type)?;
        Err(ExchangeApiError::Unsupported {
            operation: "kucoinfutures.amend_order",
        })
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_perpetual(market_type)?;
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let value = self
            .send_signed_get(
                "kucoinfutures.get_balances",
                "/api/v1/account-overview",
                &HashMap::new(),
            )
            .await?;
        let balances = parse_account_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            MarketType::Perpetual,
            &request.assets,
            &value,
        )?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances,
        })
    }

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_perpetual(market_type)?;
        }
        for symbol in &request.symbols {
            if symbol.market_type != MarketType::Perpetual {
                return Err(ExchangeApiError::Unsupported {
                    operation: "kucoinfutures.positions.non_perpetual_symbol",
                });
            }
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let value = self
            .send_signed_get(
                "kucoinfutures.get_positions",
                "/api/v1/positions",
                &HashMap::new(),
            )
            .await?;
        let positions = parse_positions(
            &self.exchange_id,
            tenant_id,
            account_id,
            &request.symbols,
            &value,
        )?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "kucoinfutures get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_perpetual(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "symbols".to_string(),
                normalize_kucoinfutures_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .send_signed_get("kucoinfutures.get_fees", "/api/v1/trade-fees", &params)
                .await?;
            fees.extend(parse_fee_snapshots(
                &self.exchange_id,
                std::slice::from_ref(symbol),
                &value,
            )?);
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(request.symbol.market_type)?;
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoinfutures query_order requires exchange_order_id".to_string(),
            })?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_kucoinfutures_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_signed_get(
                "kucoinfutures.query_order",
                &format!("/api/v1/orders/{order_id}"),
                &params,
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order: Some(order),
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_perpetual(market_type)?;
        }
        let mut params = HashMap::new();
        params.insert("status".to_string(), "active".to_string());
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_perpetual(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_kucoinfutures_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        apply_kucoinfutures_page_params(&mut params, request.page.as_ref(), None)?;
        let value = self
            .send_signed_get("kucoinfutures.get_open_orders", "/api/v1/orders", &params)
            .await?;
        let orders = parse_open_orders(&self.exchange_id, request.symbol.as_ref(), &value)?;
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
            self.ensure_perpetual(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoinfutures get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_kucoinfutures_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "startAt".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert("endAt".to_string(), end_time.timestamp_millis().to_string());
        }
        apply_kucoinfutures_page_params(&mut params, request.page.as_ref(), request.limit)?;
        let value = self
            .send_signed_get("kucoinfutures.get_recent_fills", "/api/v1/fills", &params)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn apply_kucoinfutures_page_params(
    params: &mut HashMap<String, String>,
    page: Option<&PageRequest>,
    legacy_limit: Option<u32>,
) -> ExchangeApiResult<()> {
    if let Some(page) = page {
        page.validate(Some(500))
            .map_err(|message| ExchangeApiError::InvalidRequest { message })?;
    }
    if legacy_limit == Some(0) {
        return Err(ExchangeApiError::InvalidRequest {
            message: "kucoinfutures pagination limit must be positive".to_string(),
        });
    }

    let limit = page.and_then(|page| page.limit).or(legacy_limit);
    let encoded_limit = limit.map(|limit| limit.min(500));
    if let Some(limit) = encoded_limit {
        params.insert("pageSize".to_string(), limit.to_string());
    }

    let Some(cursor) = page.and_then(|page| page.cursor.as_ref()) else {
        return Ok(());
    };
    let page_size = u64::from(encoded_limit.unwrap_or(100));
    match cursor {
        PageCursor::Offset { offset } => {
            if offset % page_size != 0 {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "kucoinfutures pagination offset must align with pageSize".to_string(),
                });
            }
            params.insert(
                "currentPage".to_string(),
                (offset / page_size + 1).to_string(),
            );
            if encoded_limit.is_none() {
                params.insert("pageSize".to_string(), page_size.to_string());
            }
        }
        PageCursor::Token { token } => {
            let current_page =
                token
                    .parse::<u64>()
                    .map_err(|_| ExchangeApiError::InvalidRequest {
                        message: "kucoinfutures pagination token cursor must be a page number"
                            .to_string(),
                    })?;
            if current_page == 0 {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "kucoinfutures pagination token cursor must be positive".to_string(),
                });
            }
            params.insert("currentPage".to_string(), current_page.to_string());
            if encoded_limit.is_none() {
                params.insert("pageSize".to_string(), page_size.to_string());
            }
        }
        PageCursor::Id { .. } | PageCursor::Timestamp { .. } | PageCursor::TimeRange { .. } => {
            return Err(ExchangeApiError::InvalidRequest {
                message: "kucoinfutures pagination supports offset or page-number token cursors"
                    .to_string(),
            });
        }
    }
    Ok(())
}

fn kucoinfutures_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let client_order_id =
        request
            .client_order_id
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoinfutures place_order requires client_order_id".to_string(),
            })?;
    let mut body = json!({
        "clientOid": non_empty("client_order_id", client_order_id)?,
        "symbol": normalize_kucoinfutures_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": kucoinfutures_side(request.side),
        "size": non_empty("quantity", &request.quantity)?,
        "tradeType": "TRADE",
    });
    if request.reduce_only {
        body["reduceOnly"] = Value::Bool(true);
    }
    match request.order_type {
        OrderType::Market => {
            body["type"] = Value::String("market".to_string());
        }
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => {
            body["type"] = Value::String("limit".to_string());
            let price =
                request
                    .price
                    .as_deref()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "kucoinfutures limit-style order requires price".to_string(),
                    })?;
            body["price"] = Value::String(non_empty("price", price)?);
            if request.order_type == OrderType::PostOnly
                || matches!(request.time_in_force, Some(TimeInForce::GTX))
                || request.post_only
            {
                body["postOnly"] = Value::Bool(true);
            }
            if let Some(tif) =
                kucoinfutures_time_in_force(request.order_type, request.time_in_force)
            {
                body["timeInForce"] = Value::String(tif.to_string());
            }
        }
        OrderType::StopMarket | OrderType::StopLimit => {
            return Err(ExchangeApiError::Unsupported {
                operation: "kucoinfutures.stop_order",
            });
        }
    }
    Ok(body)
}

fn order_state_from_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(value.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(value.get("orderId").or_else(|| value.get("id"))),
        side: request.side,
        position_side: request.position_side.or(Some(PositionSide::None)),
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: request.post_only || request.order_type == OrderType::PostOnly,
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    }
}

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> OrderState {
    cancel_order_state_from_fields(
        exchange_id,
        Some(&request.symbol),
        value_text(
            value
                .get("orderId")
                .or_else(|| value.get("cancelledOrderId")),
        )
        .or_else(|| {
            value
                .get("cancelledOrderIds")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(|item| value_text(Some(item)))
        })
        .or_else(|| request.exchange_order_id.clone()),
        request.client_order_id.clone(),
    )
}

fn kucoinfutures_cancel_all_orders(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> Vec<OrderState> {
    if let Some(ids) = value.get("cancelledOrderIds").and_then(Value::as_array) {
        return ids
            .iter()
            .filter_map(|id| value_text(Some(id)))
            .map(|id| cancel_order_state_from_fields(exchange_id, symbol, Some(id), None))
            .collect();
    }
    value
        .get("items")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    cancel_order_state_from_fields(
                        exchange_id,
                        symbol,
                        value_text(item.get("orderId").or_else(|| item.get("id"))),
                        value_text(item.get("clientOid")),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn cancel_order_state_from_fields(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: Option<&rustcta_exchange_api::SymbolScope>,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> OrderState {
    let exchange_symbol = symbol
        .map(|symbol| symbol.exchange_symbol.clone())
        .unwrap_or_else(|| {
            rustcta_types::ExchangeSymbol::new(
                exchange_id.clone(),
                MarketType::Perpetual,
                "UNKNOWN",
            )
            .expect("fallback symbol")
        });
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: symbol.and_then(|symbol| symbol.canonical_symbol.clone()),
        exchange_symbol,
        client_order_id,
        exchange_order_id,
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
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

fn kucoinfutures_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn kucoinfutures_time_in_force(
    order_type: OrderType,
    tif: Option<TimeInForce>,
) -> Option<&'static str> {
    match (order_type, tif) {
        (OrderType::IOC, _) | (_, Some(TimeInForce::IOC)) => Some("IOC"),
        (OrderType::FOK, _) | (_, Some(TimeInForce::FOK)) => Some("FOK"),
        _ => None,
    }
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("kucoinfutures {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn value_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}
