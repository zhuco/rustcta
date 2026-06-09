use std::collections::HashMap;

use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchItemResult, BatchOperationReport,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderState, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    QueryOrderRequest, QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, ReconcilePlan,
    ReconcileTrigger, RetryReconcilePolicy, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    ExchangeError, ExchangeErrorClass, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
};
use serde_json::{json, Value};

use super::parser::normalize_bybit_symbol;
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_fills, parse_order_state, parse_orders,
    parse_positions,
};
use super::public::bybit_category;
use super::BybitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BybitGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let operation = self.profile_operation("bybit.get_balances", "bybiteu.get_balances");
        let (tenant_id, account_id) = self.context_account(&request.context, operation)?;
        let mut params = HashMap::new();
        params.insert("accountType".to_string(), "UNIFIED".to_string());
        let value = self
            .send_signed_get(operation, "/v5/account/wallet-balance", &params)
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.market_type.unwrap_or(MarketType::Perpetual),
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
        let market_type = request.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market_type(market_type)?;
        let operation = self.profile_operation("bybit.get_positions", "bybiteu.get_positions");
        let (tenant_id, account_id) = self.context_account(&request.context, operation)?;
        let mut params = HashMap::new();
        params.insert(
            "category".to_string(),
            bybit_category(market_type).to_string(),
        );
        if request.symbols.len() == 1 {
            params.insert(
                "symbol".to_string(),
                normalize_bybit_symbol(&request.symbols[0].symbol)?,
            );
        } else if request.symbols.is_empty() {
            params.insert("settleCoin".to_string(), "USDT".to_string());
        }
        params.insert("limit".to_string(), "50".to_string());
        let mut positions = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..20 {
            if let Some(cursor) = cursor.as_deref().filter(|cursor| !cursor.is_empty()) {
                params.insert("cursor".to_string(), cursor.to_string());
            } else {
                params.remove("cursor");
            }
            let value = self
                .send_signed_get(operation, "/v5/position/list", &params)
                .await?;
            positions.extend(parse_positions(
                &self.exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                &request.symbols,
                &value,
            )?);
            cursor = value
                .get("result")
                .and_then(|result| result.get("nextPageCursor"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .filter(|cursor| !cursor.is_empty());
            if cursor.is_none() {
                break;
            }
        }
        if cursor.is_some() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bybit get_positions exceeded cursor pagination safety limit".to_string(),
            });
        }
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let operation = self.profile_operation("bybit.place_order", "bybiteu.place_order");
        let body = bybit_place_order_body(&request)?;
        let exchange = request.symbol.exchange.clone();
        let request_id = request.context.request_id.clone();
        let value = self
            .send_signed_post_json(operation, "/v5/order/create", &body)
            .await?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request_id),
            order: parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        let operation = self.profile_operation("bybit.cancel_order", "bybiteu.cancel_order");
        let mut body = json!({
            "category": bybit_category(request.symbol.market_type),
            "symbol": normalize_bybit_symbol(&request.symbol.exchange_symbol.symbol)?,
        });
        insert_order_id(
            &mut body,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
        )?;
        let value = self
            .send_signed_post_json(operation, "/v5/order/cancel", &body)
            .await?;
        let exchange = request.symbol.exchange.clone();
        let request_id = request.context.request_id.clone();
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request_id),
            order: parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?,
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
        let operation = self.profile_operation("bybit.amend_order", "bybiteu.amend_order");
        let body = bybit_amend_order_body(&request)?;
        let value = self
            .send_signed_post_json(operation, "/v5/order/amend", &body)
            .await?;
        let exchange = request.symbol.exchange.clone();
        let request_id = request.context.request_id.clone();
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request_id),
            order: order_state_from_amend_ack(&self.exchange_id, &request, &value),
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
                message: "bybit batch_place_orders requires at least one order".to_string(),
            });
        }
        if request.orders.len() > 20 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bybit batch_place_orders supports at most 20 orders".to_string(),
            });
        }
        let market_type = request.orders[0].symbol.market_type;
        self.ensure_supported_market_type(market_type)?;
        let mut order_bodies = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            ensure_exchange_api_schema(order.schema_version)?;
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market_type(order.symbol.market_type)?;
            if order.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bybit batch_place_orders requires one market type".to_string(),
                });
            }
            order_bodies.push(bybit_place_order_batch_item(order)?);
        }
        let operation =
            self.profile_operation("bybit.batch_place_orders", "bybiteu.batch_place_orders");
        let body = json!({
            "category": bybit_category(market_type),
            "request": order_bodies,
        });
        let value = self
            .send_signed_post_json(operation, "/v5/order/create-batch", &body)
            .await?;
        let (orders, report) =
            parse_bybit_batch_place_response(&self.exchange_id, &request, &value)?;
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            report: Some(report),
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
                message: "bybit batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        if request.cancels.len() > 20 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bybit batch_cancel_orders supports at most 20 orders".to_string(),
            });
        }
        let market_type = request.cancels[0].symbol.market_type;
        self.ensure_supported_market_type(market_type)?;
        let mut cancel_bodies = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            ensure_exchange_api_schema(cancel.schema_version)?;
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market_type(cancel.symbol.market_type)?;
            if cancel.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bybit batch_cancel_orders requires one market type".to_string(),
                });
            }
            cancel_bodies.push(bybit_cancel_order_batch_item(cancel)?);
        }
        let operation =
            self.profile_operation("bybit.batch_cancel_orders", "bybiteu.batch_cancel_orders");
        let body = json!({
            "category": bybit_category(market_type),
            "request": cancel_bodies,
        });
        let value = self
            .send_signed_post_json(operation, "/v5/order/cancel-batch", &body)
            .await?;
        let (orders, report) =
            parse_bybit_batch_cancel_response(&self.exchange_id, &request, &value)?;
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: Some(report),
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        let operation = self.profile_operation("bybit.query_order", "bybiteu.query_order");
        let mut params = HashMap::new();
        params.insert(
            "category".to_string(),
            bybit_category(request.symbol.market_type).to_string(),
        );
        params.insert(
            "symbol".to_string(),
            normalize_bybit_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(order_link_id) = request.client_order_id.as_deref() {
            params.insert("orderLinkId".to_string(), order_link_id.to_string());
        }
        let value = self
            .send_signed_get(operation, "/v5/order/realtime", &params)
            .await?;
        let mut orders = parse_orders(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order: orders.pop(),
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let operation = self.profile_operation("bybit.get_open_orders", "bybiteu.get_open_orders");
        let market_type = request.market_type.unwrap_or(MarketType::Perpetual);
        let mut params = HashMap::new();
        params.insert(
            "category".to_string(),
            bybit_category(market_type).to_string(),
        );
        if let Some(symbol) = &request.symbol {
            params.insert(
                "symbol".to_string(),
                normalize_bybit_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get(operation, "/v5/order/realtime", &params)
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bybit get_recent_fills requires symbol".to_string(),
            })?;
        let operation =
            self.profile_operation("bybit.get_recent_fills", "bybiteu.get_recent_fills");
        let (tenant_id, account_id) = self.context_account(&request.context, operation)?;
        let mut params = HashMap::new();
        params.insert(
            "category".to_string(),
            bybit_category(symbol.market_type).to_string(),
        );
        params.insert(
            "symbol".to_string(),
            normalize_bybit_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(100).to_string());
        }
        let value = self
            .send_signed_get(operation, "/v5/execution/list", &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let operation =
            self.profile_operation("bybit.cancel_all_orders", "bybiteu.cancel_all_orders");
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bybit cancel_all_orders requires symbol".to_string(),
            })?;
        let body = json!({
            "category": bybit_category(symbol.market_type),
            "symbol": normalize_bybit_symbol(&symbol.exchange_symbol.symbol)?,
        });
        let value = self
            .send_signed_post_json(operation, "/v5/order/cancel-all", &body)
            .await?;
        let orders = parse_orders(&self.exchange_id, Some(symbol), &value).unwrap_or_default();
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bybit get_fees requires at least one symbol".to_string(),
            });
        }
        let operation = self.profile_operation("bybit.get_fees", "bybiteu.get_fees");
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "category".to_string(),
                bybit_category(symbol.market_type).to_string(),
            );
            params.insert(
                "symbol".to_string(),
                normalize_bybit_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .send_signed_get(operation, "/v5/account/fee-rate", &params)
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
}

fn bybit_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = bybit_place_order_batch_item(request)?;
    body["category"] = Value::String(bybit_category(request.symbol.market_type).to_string());
    Ok(body)
}

fn bybit_place_order_batch_item(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "symbol": normalize_bybit_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": bybit_side(request.side),
        "orderType": bybit_order_type(request.order_type),
        "qty": request.quantity,
    });
    if let Some(price) = request.price.as_deref() {
        body["price"] = Value::String(price.to_string());
    }
    if let Some(client_id) = request.client_order_id.as_deref() {
        body["orderLinkId"] = Value::String(client_id.to_string());
    }
    if request.reduce_only {
        body["reduceOnly"] = Value::Bool(true);
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        body["timeInForce"] = Value::String("PostOnly".to_string());
    }
    if let Some(position_side) = request.position_side {
        body["positionIdx"] = Value::Number(bybit_position_idx(position_side).into());
    }
    Ok(body)
}

fn bybit_cancel_order_batch_item(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "symbol": normalize_bybit_symbol(&request.symbol.exchange_symbol.symbol)?,
    });
    insert_order_id(
        &mut body,
        request.exchange_order_id.as_deref(),
        request.client_order_id.as_deref(),
    )?;
    Ok(body)
}

fn bybit_amend_order_body(request: &AmendOrderRequest) -> ExchangeApiResult<Value> {
    if request.new_quantity.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bybit amend_order requires non-empty new_quantity".to_string(),
        });
    }
    if request.new_client_order_id.is_some() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bybit amend_order does not support changing client_order_id".to_string(),
        });
    }
    let mut body = json!({
        "category": bybit_category(request.symbol.market_type),
        "symbol": normalize_bybit_symbol(&request.symbol.exchange_symbol.symbol)?,
        "qty": request.new_quantity.clone(),
    });
    insert_order_id(
        &mut body,
        request.exchange_order_id.as_deref(),
        request.client_order_id.as_deref(),
    )?;
    Ok(body)
}

fn insert_order_id(
    body: &mut Value,
    order_id: Option<&str>,
    order_link_id: Option<&str>,
) -> ExchangeApiResult<()> {
    if let Some(order_id) = order_id {
        body["orderId"] = Value::String(order_id.to_string());
    }
    if let Some(order_link_id) = order_link_id {
        body["orderLinkId"] = Value::String(order_link_id.to_string());
    }
    if body.get("orderId").is_none() && body.get("orderLinkId").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bybit order request requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(())
}

fn bybit_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "Buy",
        OrderSide::Sell => "Sell",
    }
}

fn bybit_order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "Market",
        _ => "Limit",
    }
}

fn bybit_position_idx(position_side: PositionSide) -> u64 {
    match position_side {
        PositionSide::Long => 1,
        PositionSide::Short => 2,
        PositionSide::Net | PositionSide::None => 0,
    }
}

fn parse_bybit_batch_place_response(
    exchange_id: &rustcta_types::ExchangeId,
    request: &BatchPlaceOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<(Vec<OrderState>, BatchOperationReport)> {
    let rows = bybit_batch_result_rows(value);
    let statuses = bybit_batch_status_rows(value);
    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(request.orders.len());
    for (index, order_request) in request.orders.iter().enumerate() {
        let Some(row) = rows.get(index) else {
            let error =
                bybit_missing_batch_item_error(exchange_id, "missing batch place response item");
            results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                None,
                error,
                Some(ReconcilePlan::for_place_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchResponseMissingItem,
                    order_request,
                    RetryReconcilePolicy::default(),
                    "Bybit did not return a batch place result for this request item",
                )),
            ));
            continue;
        };
        if let Some(error) = bybit_batch_item_error(
            exchange_id,
            statuses.get(index),
            row,
            order_request.client_order_id.clone(),
            None,
        ) {
            results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                None,
                error,
                Some(ReconcilePlan::for_place_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchPlacePartialFailure,
                    order_request,
                    RetryReconcilePolicy::default(),
                    "Bybit batch place item failed and requires order readback",
                )),
            ));
            continue;
        }
        let order = order_state_from_place_ack(exchange_id, order_request, row);
        results.push(BatchItemResult::success(index, order.clone()));
        orders.push(order);
    }
    Ok((
        orders,
        BatchOperationReport {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            total_items: request.orders.len(),
            results,
        },
    ))
}

fn parse_bybit_batch_cancel_response(
    exchange_id: &rustcta_types::ExchangeId,
    request: &BatchCancelOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<(Vec<OrderState>, BatchOperationReport)> {
    let rows = bybit_batch_result_rows(value);
    let statuses = bybit_batch_status_rows(value);
    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(request.cancels.len());
    for (index, cancel_request) in request.cancels.iter().enumerate() {
        let Some(row) = rows.get(index) else {
            let error =
                bybit_missing_batch_item_error(exchange_id, "missing batch cancel response item");
            results.push(BatchItemResult::failed(
                index,
                cancel_request.client_order_id.clone(),
                cancel_request.exchange_order_id.clone(),
                error,
                Some(ReconcilePlan::for_cancel_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchResponseMissingItem,
                    cancel_request,
                    RetryReconcilePolicy::default(),
                    "Bybit did not return a batch cancel result for this request item",
                )),
            ));
            continue;
        };
        if let Some(error) = bybit_batch_item_error(
            exchange_id,
            statuses.get(index),
            row,
            cancel_request.client_order_id.clone(),
            cancel_request.exchange_order_id.clone(),
        ) {
            results.push(BatchItemResult::failed(
                index,
                cancel_request.client_order_id.clone(),
                cancel_request.exchange_order_id.clone(),
                error,
                Some(ReconcilePlan::for_cancel_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchCancelPartialFailure,
                    cancel_request,
                    RetryReconcilePolicy::default(),
                    "Bybit batch cancel item failed and requires order readback",
                )),
            ));
            continue;
        }
        let order = order_state_from_cancel_ack(exchange_id, cancel_request, row);
        results.push(BatchItemResult::success(index, order.clone()));
        orders.push(order);
    }
    Ok((
        orders,
        BatchOperationReport {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            total_items: request.cancels.len(),
            results,
        },
    ))
}

fn bybit_batch_result_rows(value: &Value) -> &[Value] {
    value
        .get("result")
        .and_then(|result| result.get("list"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn bybit_batch_status_rows(value: &Value) -> &[Value] {
    value
        .get("retExtInfo")
        .and_then(|info| info.get("list"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn bybit_batch_item_error(
    exchange_id: &rustcta_types::ExchangeId,
    status: Option<&Value>,
    row: &Value,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
) -> Option<ExchangeError> {
    let code = status
        .and_then(|status| status.get("code"))
        .or_else(|| row.get("code"))
        .and_then(|value| super::parser::string_or_number(Some(value)))
        .unwrap_or_else(|| "0".to_string());
    if code == "0" || code.eq_ignore_ascii_case("ok") {
        return None;
    }
    let message = status
        .and_then(|status| status.get("msg"))
        .or_else(|| row.get("msg"))
        .and_then(Value::as_str)
        .unwrap_or("Bybit batch item failed");
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::OrderRejected,
        message,
        Utc::now(),
    );
    error.code = Some(code);
    error.client_order_id = client_order_id;
    error.order_id = exchange_order_id;
    error.raw = Some(status.cloned().unwrap_or_else(|| row.clone()));
    Some(error)
}

fn bybit_missing_batch_item_error(
    exchange_id: &rustcta_types::ExchangeId,
    message: &str,
) -> ExchangeError {
    ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::UnknownOrderState,
        message,
        Utc::now(),
    )
}

fn order_state_from_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: super::parser::string_or_number(ack.get("orderLinkId"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: super::parser::string_or_number(ack.get("orderId")),
        side: request.side,
        position_side: request.position_side.or(Some(PositionSide::None)),
        order_type: request.order_type,
        time_in_force: None,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only || request.order_type == OrderType::PostOnly,
        created_at: None,
        updated_at: Utc::now(),
    }
}

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: super::parser::string_or_number(ack.get("orderLinkId"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: super::parser::string_or_number(ack.get("orderId"))
            .or_else(|| request.exchange_order_id.clone()),
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
        updated_at: Utc::now(),
    }
}

fn order_state_from_amend_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &AmendOrderRequest,
    value: &Value,
) -> OrderState {
    let row = value.get("result").unwrap_or(value);
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: super::parser::string_or_number(row.get("orderLinkId"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: super::parser::string_or_number(row.get("orderId"))
            .or_else(|| request.exchange_order_id.clone()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: None,
        status: OrderStatus::Unknown,
        quantity: request.new_quantity.clone(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    }
}
