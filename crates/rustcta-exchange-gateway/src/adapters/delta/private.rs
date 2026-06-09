#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, OpenOrdersRequest,
    OpenOrdersResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::{json, Value};

use super::parser::normalize_delta_symbol;
use super::private_parser::{
    parse_balances, parse_fills, parse_order, parse_order_state, parse_orders, parse_positions,
};
use super::DeltaGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl DeltaGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market(market_type)?;
        self.ensure_private_rest("delta.get_balances")?;
        let value = self
            .rest
            .send_signed_get("delta.get_balances", "/v2/wallet/balances", &HashMap::new())
            .await?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "delta.get_balances")?;
        let balances = parse_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            &request.assets,
            market_type,
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
            self.ensure_supported_market(market_type)?;
        }
        self.ensure_private_rest("delta.get_positions")?;
        let mut params = HashMap::new();
        if let Some(market_type) = request.market_type {
            params.insert(
                "contract_types".to_string(),
                contract_types_for_market(market_type)?,
            );
        }
        let value = self
            .rest
            .send_signed_get("delta.get_positions", "/v2/positions/margined", &params)
            .await?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "delta.get_positions")?;
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

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        self.ensure_private_rest("delta.place_order")?;
        let body = delta_place_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("delta.place_order", "/v2/orders", &body)
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            response_result(&value),
        )
        .unwrap_or_else(|_| {
            order_state_from_place_ack(&self.exchange_id, &request, response_result(&value))
        });
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
        self.ensure_private_rest("delta.cancel_order")?;
        let body = delta_cancel_order_body(&request)?;
        let value = self
            .rest
            .send_signed_delete("delta.cancel_order", "/v2/orders", &body)
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            response_result(&value),
        )
        .unwrap_or_else(|_| {
            cancelled_order_state(
                &self.exchange_id,
                &request.symbol,
                request.exchange_order_id.clone(),
                request.client_order_id.clone(),
            )
        });
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

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        self.ensure_private_rest("delta.amend_order")?;
        let body = delta_amend_order_body(&request)?;
        let value = self
            .rest
            .send_signed_put("delta.amend_order", "/v2/orders", &body)
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            response_result(&value),
        )
        .unwrap_or_else(|_| {
            order_state_from_amend_ack(&self.exchange_id, &request, response_result(&value))
        });
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order,
        })
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("delta.batch_place_orders")?;
        let mut orders = Vec::new();
        for order_request in &request.orders {
            self.ensure_exchange(&order_request.symbol.exchange)?;
            self.ensure_supported_market(order_request.symbol.market_type)?;
            let value = self
                .rest
                .send_signed_post(
                    "delta.batch_place_orders",
                    "/v2/orders",
                    &delta_place_order_body(order_request)?,
                )
                .await?;
            orders.push(
                parse_order_state(
                    &self.exchange_id,
                    Some(&order_request.symbol),
                    response_result(&value),
                )
                .unwrap_or_else(|_| {
                    order_state_from_place_ack(
                        &self.exchange_id,
                        order_request,
                        response_result(&value),
                    )
                }),
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
        self.ensure_private_rest("delta.batch_cancel_orders")?;
        let mut orders = Vec::new();
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market(cancel.symbol.market_type)?;
            let value = self
                .rest
                .send_signed_delete(
                    "delta.batch_cancel_orders",
                    "/v2/orders",
                    &delta_cancel_order_body(cancel)?,
                )
                .await?;
            orders.push(
                parse_order_state(
                    &self.exchange_id,
                    Some(&cancel.symbol),
                    response_result(&value),
                )
                .unwrap_or_else(|_| {
                    cancelled_order_state(
                        &self.exchange_id,
                        &cancel.symbol,
                        cancel.exchange_order_id.clone(),
                        cancel.client_order_id.clone(),
                    )
                }),
            );
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
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market(market_type)?;
        }
        self.ensure_private_rest("delta.cancel_all_orders")?;
        let mut body = json!({});
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            body["product_symbol"] = Value::String(normalize_delta_symbol(symbol)?);
        } else if let Some(market_type) = request.market_type {
            body["contract_types"] = Value::String(contract_types_for_market(market_type)?);
        }
        let value = self
            .rest
            .send_signed_delete("delta.cancel_all_orders", "/v2/orders/all", &body)
            .await?;
        let orders = response_result(&value)
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| {
                        parse_order_state(&self.exchange_id, request.symbol.as_ref(), row).ok()
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
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
        self.ensure_supported_market(request.symbol.market_type)?;
        self.ensure_private_rest("delta.query_order")?;
        let endpoint = if let Some(order_id) = &request.exchange_order_id {
            format!("/v2/orders/{order_id}")
        } else if let Some(client_order_id) = &request.client_order_id {
            format!("/v2/orders/client_order_id/{client_order_id}")
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "delta.query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        };
        let value = self
            .rest
            .send_signed_get("delta.query_order", &endpoint, &HashMap::new())
            .await?;
        let order = parse_order(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
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
            self.ensure_supported_market(market_type)?;
        }
        self.ensure_private_rest("delta.get_open_orders")?;
        let mut params = HashMap::new();
        params.insert("states".to_string(), "open,pending".to_string());
        params.insert(
            "page_size".to_string(),
            request
                .page
                .as_ref()
                .and_then(|page| page.limit)
                .unwrap_or(50)
                .min(100)
                .to_string(),
        );
        if let Some(market_type) = request.market_type {
            params.insert(
                "contract_types".to_string(),
                contract_types_for_market(market_type)?,
            );
        }
        let value = self
            .rest
            .send_signed_get("delta.get_open_orders", "/v2/orders", &params)
            .await?;
        let mut orders = parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?;
        if let Some(symbol) = &request.symbol {
            let expected = normalize_delta_symbol(symbol)?;
            orders.retain(|order| order.exchange_symbol.symbol == expected);
        }
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
            self.ensure_supported_market(market_type)?;
        }
        self.ensure_private_rest("delta.get_recent_fills")?;
        let mut params = HashMap::new();
        params.insert(
            "page_size".to_string(),
            request.limit.unwrap_or(50).min(100).to_string(),
        );
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            params.insert(
                "product_symbol".to_string(),
                normalize_delta_symbol(symbol)?,
            );
        }
        if let Some(market_type) = request.market_type {
            params.insert(
                "contract_types".to_string(),
                contract_types_for_market(market_type)?,
            );
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "start_time".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert(
                "end_time".to_string(),
                end_time.timestamp_millis().to_string(),
            );
        }
        let value = self
            .rest
            .send_signed_get("delta.get_recent_fills", "/v2/fills", &params)
            .await?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "delta.get_recent_fills")?;
        let fills = parse_fills(
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
}

fn delta_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "product_symbol": normalize_delta_symbol(&request.symbol)?,
        "side": match request.side { OrderSide::Buy => "buy", OrderSide::Sell => "sell" },
        "order_type": delta_order_type(request.order_type, request.post_only),
        "size": request.quantity,
    });
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["client_order_id"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if request.order_type != OrderType::Market {
        body["limit_price"] = Value::String(non_empty(
            "price",
            request
                .price
                .as_deref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "delta limit-style order requires price".to_string(),
                })?,
        )?);
    }
    if let Some(time_in_force) = request.time_in_force {
        body["time_in_force"] = Value::String(delta_time_in_force(time_in_force).to_string());
    }
    if request.reduce_only {
        body["reduce_only"] = Value::Bool(true);
    }
    if request.post_only {
        body["post_only"] = Value::Bool(true);
    }
    Ok(body)
}

fn delta_cancel_order_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    if request.exchange_order_id.is_none() && request.client_order_id.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "delta.cancel_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    let mut body = json!({
        "product_symbol": normalize_delta_symbol(&request.symbol)?,
    });
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body["id"] = Value::String(non_empty("exchange_order_id", order_id)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["client_order_id"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn delta_amend_order_body(request: &AmendOrderRequest) -> ExchangeApiResult<Value> {
    if request.exchange_order_id.is_none() && request.client_order_id.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "delta.amend_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    if request.new_client_order_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "delta.amend_order_new_client_order_id",
        });
    }
    let mut body = json!({
        "product_symbol": normalize_delta_symbol(&request.symbol)?,
        "size": non_empty("new_quantity", &request.new_quantity)?,
    });
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body["id"] = Value::String(non_empty("exchange_order_id", order_id)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["client_order_id"] = Value::String(non_empty("client_order_id", client_order_id)?);
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
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: value.get("id").and_then(|id| {
            id.as_str()
                .map(str::to_string)
                .or_else(|| id.as_u64().map(|id| id.to_string()))
        }),
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
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

fn order_state_from_amend_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &AmendOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request
            .new_client_order_id
            .clone()
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value
            .get("id")
            .and_then(|id| {
                id.as_str()
                    .map(str::to_string)
                    .or_else(|| id.as_u64().map(|id| id.to_string()))
            })
            .or_else(|| request.exchange_order_id.clone()),
        side: OrderSide::Buy,
        position_side: Some(rustcta_types::PositionSide::Net),
        order_type: OrderType::Limit,
        time_in_force: None,
        status: OrderStatus::New,
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

fn cancelled_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
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
        time_in_force: Some(TimeInForce::GTC),
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

fn response_result(value: &Value) -> &Value {
    value.get("result").unwrap_or(value)
}

fn contract_types_for_market(market_type: MarketType) -> ExchangeApiResult<String> {
    match market_type {
        MarketType::Perpetual => Ok("perpetual_futures".to_string()),
        MarketType::Futures => Ok("futures".to_string()),
        MarketType::Option => Ok("call_options,put_options".to_string()),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "delta.unsupported_market_type",
        }),
    }
}

fn delta_order_type(order_type: OrderType, post_only: bool) -> &'static str {
    if post_only || matches!(order_type, OrderType::PostOnly) {
        "limit_order"
    } else {
        match order_type {
            OrderType::Market => "market_order",
            _ => "limit_order",
        }
    }
}

fn delta_time_in_force(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::GTC => "gtc",
        TimeInForce::IOC => "ioc",
        TimeInForce::FOK => "fok",
        TimeInForce::GTX => "gtc",
    }
}

fn non_empty(field: &'static str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("delta {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}
