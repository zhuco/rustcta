use std::collections::BTreeMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchItemResult, BatchOperationReport,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, OpenOrdersRequest, OpenOrdersResponse, OrderState, PageCursor,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    RequestContext, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, ExchangeError, ExchangeErrorClass, OrderSide, OrderStatus, OrderType, TenantId,
    TimeInForce,
};
use serde_json::{json, Value};

use super::parser::{
    parse_balances, parse_order_ack, parse_orders, parse_position, parse_recent_fills,
    parse_single_order,
};
use super::signing::OrderlyAuth;
use super::WoofiproGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub(super) fn unsupported<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

fn page_cursor_value(cursor: &PageCursor) -> String {
    match cursor {
        PageCursor::Offset { offset } => offset.to_string(),
        PageCursor::Id { id } => id.clone(),
        PageCursor::Timestamp { millis } => millis.to_string(),
        PageCursor::Token { token } => token.clone(),
        PageCursor::TimeRange { start_ms, .. } => start_ms.to_string(),
    }
}

pub(super) const FEES_UNSUPPORTED: &str = "woofipro.fees_require_orderly_account_audit";
pub(super) const ORDER_LIST_UNSUPPORTED: &str = "woofipro.order_list_unsupported";

impl WoofiproGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market_type(market_type)?;
        }
        let (tenant_id, account_id) = context_account(&request.context)?;
        let value = self
            .rest
            .send_signed_get(
                "/v1/client/holding",
                &BTreeMap::new(),
                &self.orderly_auth()?,
            )
            .await?;
        let balances = parse_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            request
                .market_type
                .unwrap_or(rustcta_types::MarketType::Perpetual),
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
            self.ensure_supported_market_type(market_type)?;
        }
        let (tenant_id, account_id) = context_account(&request.context)?;
        let mut positions = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange_id)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            let path = format!("/v1/position/{}", symbol.symbol.trim());
            let value = self
                .rest
                .send_signed_get(&path, &BTreeMap::new(), &self.orderly_auth()?)
                .await?;
            if let Some(position) = parse_position(
                &self.exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                &value,
            )? {
                positions.push(position);
            }
        }
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions,
        })
    }

    pub(super) async fn get_order_book_signed_impl(
        &self,
        request: rustcta_exchange_api::OrderBookRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::OrderBookResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let mut params = BTreeMap::new();
        if let Some(depth) = request.depth {
            params.insert("max_level".to_string(), depth.min(100).to_string());
        }
        let path = super::transport::WoofiproRest::signed_orderbook_path(
            &request.symbol.exchange_symbol.symbol,
        );
        let value = self
            .rest
            .send_signed_get(&path, &params, &self.orderly_auth()?)
            .await?;
        let order_book = super::parser::parse_orderbook_snapshot(
            &self.exchange_id,
            request.symbol.clone(),
            &value,
        )?;
        Ok(rustcta_exchange_api::OrderBookResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order_book,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let body = orderly_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("/v1/order", &body, &self.orderly_auth()?)
            .await?;
        let order = parse_order_ack(
            &self.exchange_id,
            &request.symbol,
            request.side,
            request.order_type,
            request.quantity,
            request.price,
            &value,
        )?;
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
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let mut body = json!({
            "symbol": request.symbol.exchange_symbol.symbol.clone(),
            "order_type": "MARKET",
            "side": orderly_side(request.side),
            "order_amount": decimal_json(&request.quote_quantity)?,
        });
        if let Some(client_order_id) = &request.client_order_id {
            body["client_order_id"] = json!(client_order_id);
        }
        let value = self
            .rest
            .send_signed_post("/v1/order", &body, &self.orderly_auth()?)
            .await?;
        let order = parse_order_ack(
            &self.exchange_id,
            &request.symbol,
            request.side,
            OrderType::Market,
            "0".to_string(),
            None,
            &value,
        )?;
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
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "woofipro cancel_order requires exchange_order_id".to_string(),
            }
        })?;
        let mut params = BTreeMap::new();
        params.insert("order_id".to_string(), order_id.to_string());
        params.insert(
            "symbol".to_string(),
            request.symbol.exchange_symbol.symbol.clone(),
        );
        let value = self
            .rest
            .send_signed_delete("/v1/order", &params, &self.orderly_auth()?)
            .await?;
        ensure_orderly_success(&self.exchange_id, &value)?;
        let order = ack_order_state(
            &self.exchange_id,
            &request.symbol,
            request.client_order_id.clone(),
            request.exchange_order_id.clone(),
            OrderStatus::PendingCancel,
        );
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "woofipro amend_order requires exchange_order_id".to_string(),
            }
        })?;
        let mut body = json!({
            "order_id": order_id,
            "symbol": request.symbol.exchange_symbol.symbol.clone(),
            "order_quantity": decimal_json(&request.new_quantity)?,
        });
        if let Some(client_order_id) = &request.new_client_order_id {
            body["client_order_id"] = json!(client_order_id);
        } else if let Some(client_order_id) = &request.client_order_id {
            body["client_order_id"] = json!(client_order_id);
        }
        let value = self
            .rest
            .send_signed_put("/v1/order", &body, &self.orderly_auth()?)
            .await?;
        ensure_orderly_success(&self.exchange_id, &value)?;
        let order = ack_order_state(
            &self.exchange_id,
            &request.symbol,
            request
                .new_client_order_id
                .clone()
                .or(request.client_order_id.clone()),
            request.exchange_order_id.clone(),
            OrderStatus::Open,
        );
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
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
                message: "woofipro batch_place_orders requires at least one order".to_string(),
            });
        }
        if request.orders.len() > 10 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "woofipro batch_place_orders supports at most 10 orders".to_string(),
            });
        }
        let mut bodies = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market_type(order.symbol.market_type)?;
            bodies.push(orderly_order_body(order)?);
        }
        let body = json!({ "orders": bodies });
        let value = self
            .rest
            .send_signed_post("/v1/batch-order", &body, &self.orderly_auth()?)
            .await?;
        let orders = parse_orders(&self.exchange_id, None, &value).unwrap_or_default();
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            report: Some(batch_report_from_orders(&request.exchange, &orders)),
            orders,
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
                message: "woofipro batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        if request.cancels.len() > 10 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "woofipro batch_cancel_orders supports at most 10 orders".to_string(),
            });
        }
        let order_ids = request
            .cancels
            .iter()
            .map(|cancel| {
                self.ensure_exchange(&cancel.symbol.exchange)?;
                self.ensure_supported_market_type(cancel.symbol.market_type)?;
                cancel
                    .exchange_order_id
                    .clone()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "woofipro batch_cancel_orders requires exchange_order_id"
                            .to_string(),
                    })
            })
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let mut params = BTreeMap::new();
        params.insert("order_ids".to_string(), order_ids.join(","));
        let value = self
            .rest
            .send_signed_delete("/v1/batch-order", &params, &self.orderly_auth()?)
            .await?;
        ensure_orderly_success(&self.exchange_id, &value)?;
        let orders = request
            .cancels
            .iter()
            .map(|cancel| {
                ack_order_state(
                    &self.exchange_id,
                    &cancel.symbol,
                    cancel.client_order_id.clone(),
                    cancel.exchange_order_id.clone(),
                    OrderStatus::PendingCancel,
                )
            })
            .collect::<Vec<_>>();
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            cancelled_count: orders.len() as u32,
            report: Some(batch_report_from_orders(&request.exchange, &orders)),
            orders,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market_type(market_type)?;
        }
        let mut params = BTreeMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            params.insert("symbol".to_string(), symbol.exchange_symbol.symbol.clone());
        }
        let value = self
            .rest
            .send_signed_delete("/v1/orders", &params, &self.orderly_auth()?)
            .await?;
        ensure_orderly_success(&self.exchange_id, &value)?;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: Vec::new(),
            cancelled_count: 0,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let path = if let Some(order_id) = &request.exchange_order_id {
            format!("/v1/order/{order_id}")
        } else if let Some(client_order_id) = &request.client_order_id {
            format!("/v1/client/order/{client_order_id}")
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "woofipro query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        };
        let value = self
            .rest
            .send_signed_get(&path, &BTreeMap::new(), &self.orderly_auth()?)
            .await?;
        let order = parse_single_order(&self.exchange_id, &request.symbol, &value)?;
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
            self.ensure_supported_market_type(market_type)?;
        }
        let mut params = BTreeMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            params.insert("symbol".to_string(), symbol.exchange_symbol.symbol.clone());
        }
        params.insert("status".to_string(), "INCOMPLETE".to_string());
        if let Some(page) = &request.page {
            if let Some(limit) = page.limit {
                params.insert("size".to_string(), limit.min(500).to_string());
            }
        }
        let value = self
            .rest
            .send_signed_get("/v1/orders", &params, &self.orderly_auth()?)
            .await?;
        let orders = parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?;
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
            self.ensure_supported_market_type(market_type)?;
        }
        let (tenant_id, account_id) = context_account(&request.context)?;
        let mut params = BTreeMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            params.insert("symbol".to_string(), symbol.exchange_symbol.symbol.clone());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "start_t".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert("end_t".to_string(), end_time.timestamp_millis().to_string());
        }
        if let Some(limit) = request.limit {
            params.insert("size".to_string(), limit.min(500).to_string());
        }
        if let Some(page) = &request.page {
            if let Some(cursor) = &page.cursor {
                params.insert("page".to_string(), page_cursor_value(cursor));
            }
        }
        let value = self
            .rest
            .send_signed_get("/v1/trades", &params, &self.orderly_auth()?)
            .await?;
        let fills = parse_recent_fills(
            &self.exchange_id,
            tenant_id,
            account_id,
            request.symbol.as_ref(),
            &value,
        )?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }

    fn orderly_auth(&self) -> ExchangeApiResult<OrderlyAuth> {
        if !self.config.enabled_private_rest {
            return Err(ExchangeApiError::Unsupported {
                operation: "woofipro.private_rest_disabled",
            });
        }
        Ok(OrderlyAuth {
            account_id: required_config(
                self.config.orderly_account_id.as_deref(),
                "orderly_account_id",
            )?,
            orderly_key: required_config(self.config.orderly_key.as_deref(), "orderly_key")?,
            orderly_secret: required_config(
                self.config.orderly_secret.as_deref(),
                "orderly_secret",
            )?,
        })
    }
}

fn orderly_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "symbol": request.symbol.exchange_symbol.symbol.clone(),
        "order_type": orderly_order_type(request.order_type, request.post_only),
        "side": orderly_side(request.side),
        "order_quantity": decimal_json(&request.quantity)?,
        "reduce_only": request.reduce_only,
    });
    if let Some(client_order_id) = &request.client_order_id {
        body["client_order_id"] = json!(client_order_id);
    }
    if let Some(price) = &request.price {
        body["order_price"] = decimal_json(price)?;
    }
    if let Some(quote_quantity) = &request.quote_quantity {
        body["order_amount"] = decimal_json(quote_quantity)?;
    }
    if let Some(time_in_force) = request.time_in_force {
        if matches!(time_in_force, TimeInForce::IOC | TimeInForce::FOK) {
            body["order_type"] = json!(match time_in_force {
                TimeInForce::IOC => "IOC",
                TimeInForce::FOK => "FOK",
                _ => unreachable!(),
            });
        }
    }
    Ok(body)
}

fn orderly_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn orderly_order_type(order_type: OrderType, post_only: bool) -> &'static str {
    if post_only {
        return "POST_ONLY";
    }
    match order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit => "LIMIT",
        OrderType::PostOnly => "POST_ONLY",
        OrderType::IOC => "IOC",
        OrderType::FOK => "FOK",
        OrderType::StopMarket | OrderType::StopLimit => "MARKET",
    }
}

fn decimal_json(value: &str) -> ExchangeApiResult<Value> {
    let number = value
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid decimal value {value}: {error}"),
        })?;
    Ok(json!(number))
}

fn context_account(context: &RequestContext) -> ExchangeApiResult<(TenantId, AccountId)> {
    let tenant_id = context
        .tenant_id
        .clone()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "woofipro private response mapping requires tenant_id in request context"
                .to_string(),
        })?;
    let account_id =
        context
            .account_id
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "woofipro private response mapping requires account_id in request context"
                    .to_string(),
            })?;
    Ok((tenant_id, account_id))
}

fn required_config(value: Option<&str>, field: &str) -> ExchangeApiResult<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("woofipro private REST requires {field}"),
        })
}

fn ensure_orderly_success(
    exchange_id: &rustcta_types::ExchangeId,
    value: &Value,
) -> ExchangeApiResult<()> {
    if value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        return Ok(());
    }
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::OrderRejected,
        value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("WOOFi Pro Orderly request failed"),
        chrono::Utc::now(),
    );
    error.code = value.get("code").map(ToString::to_string);
    error.raw = Some(value.clone());
    Err(ExchangeApiError::Exchange(error))
}

fn ack_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
    status: OrderStatus,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
        side: OrderSide::Buy,
        position_side: Some(rustcta_types::PositionSide::Net),
        order_type: OrderType::Limit,
        time_in_force: None,
        status,
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

fn batch_report_from_orders(
    exchange: &rustcta_types::ExchangeId,
    orders: &[OrderState],
) -> BatchOperationReport {
    BatchOperationReport {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange.clone(),
        total_items: orders.len(),
        results: orders
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, order)| BatchItemResult::success(index, order))
            .collect(),
    }
}
