#![allow(dead_code)]

use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, OpenOrdersRequest,
    OpenOrdersResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_bydfi_symbol;
use super::private_parser::{
    parse_balances, parse_fills, parse_order, parse_order_state, parse_orders, parse_positions,
};
use super::BydfiGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Clone, PartialEq)]
pub struct BydfiPrivateAck {
    pub operation: &'static str,
    pub data: Value,
}

impl BydfiGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_perpetual(market_type, "bydfi.spot_balances_unsupported")?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bydfi.get_balances")?;
        let mut params = HashMap::new();
        params.insert("wallet".to_string(), self.config.wallet.clone());
        if let Some(asset) = request.assets.first() {
            params.insert("asset".to_string(), asset.to_ascii_uppercase());
        }
        let value = self
            .send_signed_get("bydfi.get_balances", "/v1/fapi/account/balance", &params)
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
        if request
            .market_type
            .is_some_and(|market| market != MarketType::Perpetual)
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "bydfi.positions_non_perpetual",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bydfi.get_positions")?;
        let mut params = futures_base_params(&self.config.wallet);
        if let Some(symbol) = request.symbols.first() {
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "BYDFi adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
            params.insert(
                "symbol".to_string(),
                normalize_bydfi_symbol(&symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get("bydfi.get_positions", "/v2/fapi/trade/positions", &params)
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions: parse_positions(&self.exchange_id, tenant_id, account_id, &value)?,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(
            request.symbol.market_type,
            "bydfi.spot_place_order_unsupported",
        )?;
        let body = bydfi_place_order_body(&request, &self.config.wallet)?;
        let value = self
            .send_signed_post("bydfi.place_order", "/v2/fapi/trade/place_order", body)
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            data_or_root(&value),
        )
        .unwrap_or_else(|_| {
            order_state_from_place_ack(&self.exchange_id, &request, data_or_root(&value))
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
        self.ensure_perpetual(
            request.symbol.market_type,
            "bydfi.spot_cancel_order_unsupported",
        )?;
        let body = bydfi_cancel_body(&request, &self.config.wallet)?;
        let value = self
            .send_signed_post("bydfi.cancel_order", "/v2/fapi/trade/cancel_order", body)
            .await?;
        let order = parse_order(&self.exchange_id, Some(&request.symbol), &value)
            .ok()
            .flatten()
            .unwrap_or_else(|| {
                order_state_from_cancel_ack(&self.exchange_id, &request, data_or_root(&value))
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
        self.ensure_perpetual(
            request.symbol.market_type,
            "bydfi.spot_amend_order_unsupported",
        )?;
        let body = bydfi_amend_body(&request, &self.config.wallet)?;
        let value = self
            .send_signed_post("bydfi.amend_order", "/v2/fapi/trade/edit_order", body)
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            data_or_root(&value),
        )
        .unwrap_or_else(|_| {
            order_state_from_amend_ack(&self.exchange_id, &request, data_or_root(&value))
        });
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
            return Ok(BatchPlaceOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                report: None,
            });
        }
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_perpetual(order.symbol.market_type, "bydfi.batch_place_non_perpetual")?;
        }
        let orders = request
            .orders
            .iter()
            .map(|order| bydfi_place_order_body(order, ""))
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let value = self
            .send_signed_post(
                "bydfi.batch_place_orders",
                "/v2/fapi/trade/batch_place_order",
                json!({ "wallet": self.config.wallet, "orders": orders }),
            )
            .await?;
        let orders = parse_orders(&self.exchange_id, None, &value).unwrap_or_else(|_| {
            request
                .orders
                .iter()
                .map(|order| {
                    order_state_from_place_ack(&self.exchange_id, order, data_or_root(&value))
                })
                .collect()
        });
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
            return Ok(BatchCancelOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                cancelled_count: 0,
                report: None,
            });
        }
        let first_symbol = request.cancels[0].symbol.exchange_symbol.symbol.clone();
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_perpetual(
                cancel.symbol.market_type,
                "bydfi.batch_cancel_non_perpetual",
            )?;
            if cancel.symbol.exchange_symbol.symbol != first_symbol {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "BYDFi batch_cancel_orders requires one symbol".to_string(),
                });
            }
        }
        let body = bydfi_batch_cancel_body(&request.cancels, &self.config.wallet)?;
        let value = self
            .send_signed_post(
                "bydfi.batch_cancel_orders",
                "/v2/fapi/trade/batch_cancel_order",
                body,
            )
            .await?;
        let orders = parse_orders(&self.exchange_id, None, &value).unwrap_or_else(|_| {
            request
                .cancels
                .iter()
                .map(|cancel| {
                    order_state_from_cancel_ack(&self.exchange_id, cancel, data_or_root(&value))
                })
                .collect()
        });
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
                message: "BYDFi cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type, "bydfi.cancel_all_non_perpetual")?;
        let value = self
            .send_signed_post(
                "bydfi.cancel_all_orders",
                "/v2/fapi/trade/cancel_all_order",
                json!({
                    "wallet": self.config.wallet,
                    "symbol": normalize_bydfi_symbol(&symbol.exchange_symbol.symbol)?,
                }),
            )
            .await?;
        let orders = parse_orders(&self.exchange_id, Some(symbol), &value).unwrap_or_default();
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
        self.ensure_perpetual(
            request.symbol.market_type,
            "bydfi.query_order_non_perpetual",
        )?;
        let mut params = open_order_params(&self.config.wallet, &request.symbol)?;
        optional_order_ids(
            &mut params,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
        )?;
        let value = self
            .send_signed_get("bydfi.query_order", "/v2/fapi/trade/open_order", &params)
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_order(&self.exchange_id, Some(&request.symbol), &value)?,
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
                message: "BYDFi get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type, "bydfi.get_open_orders_non_perpetual")?;
        let params = open_order_params(&self.config.wallet, symbol)?;
        let value = self
            .send_signed_get(
                "bydfi.get_open_orders",
                "/v2/fapi/trade/open_order",
                &params,
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "BYDFi get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type, "bydfi.get_recent_fills_non_perpetual")?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bydfi.get_recent_fills")?;
        let mut params = futures_base_params(&self.config.wallet);
        params.insert(
            "symbol".to_string(),
            normalize_bydfi_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(start_time) = request.start_time {
            params.insert(
                "startTime".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert(
                "endTime".to_string(),
                end_time.timestamp_millis().to_string(),
            );
        }
        params.insert(
            "limit".to_string(),
            request.limit.unwrap_or(100).min(1000).to_string(),
        );
        let value = self
            .send_signed_get(
                "bydfi.get_recent_fills",
                "/v2/fapi/trade/history_trade",
                &params,
            )
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                Some(symbol),
                &value,
            )?,
        })
    }
}

pub fn bydfi_place_order_body(
    request: &PlaceOrderRequest,
    wallet: &str,
) -> ExchangeApiResult<Value> {
    if request.quote_quantity.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "bydfi.quote_quantity_order",
        });
    }
    if matches!(
        request.order_type,
        OrderType::StopLimit | OrderType::StopMarket
    ) {
        return Err(ExchangeApiError::Unsupported {
            operation: "bydfi.stop_order_unified_mapping",
        });
    }
    let mut body = serde_json::Map::new();
    if !wallet.is_empty() {
        body.insert("wallet".to_string(), json!(wallet));
    }
    body.insert(
        "symbol".to_string(),
        json!(normalize_bydfi_symbol(
            &request.symbol.exchange_symbol.symbol
        )?),
    );
    body.insert("side".to_string(), json!(side_text(request.side)));
    body.insert(
        "positionSide".to_string(),
        json!(position_side_text(request.position_side)),
    );
    body.insert("type".to_string(), json!(order_type_text(request)));
    body.insert(
        "quantity".to_string(),
        json!(non_empty("quantity", &request.quantity)?),
    );
    if let Some(price) = request.price.as_deref() {
        body.insert("price".to_string(), json!(non_empty("price", price)?));
    } else if request.order_type.requires_limit_price() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "BYDFi limit order requires price".to_string(),
        });
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body.insert("clientOrderId".to_string(), json!(client_order_id));
    }
    if request.reduce_only {
        body.insert("reduceOnly".to_string(), json!(true));
    }
    if let Some(time_in_force) = request.time_in_force {
        body.insert(
            "timeInForce".to_string(),
            json!(time_in_force_text(time_in_force)),
        );
    } else if request.post_only || matches!(request.order_type, OrderType::PostOnly) {
        body.insert("timeInForce".to_string(), json!("POST_ONLY"));
    }
    Ok(Value::Object(body))
}

pub fn bydfi_cancel_body(request: &CancelOrderRequest, wallet: &str) -> ExchangeApiResult<Value> {
    if request.exchange_order_id.is_none() && request.client_order_id.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "BYDFi cancel_order requires orderId or clientOrderId".to_string(),
        });
    }
    let mut body = serde_json::Map::new();
    body.insert("wallet".to_string(), json!(wallet));
    body.insert(
        "symbol".to_string(),
        json!(normalize_bydfi_symbol(
            &request.symbol.exchange_symbol.symbol
        )?),
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body.insert("orderId".to_string(), json!(order_id));
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body.insert("clientOrderId".to_string(), json!(client_order_id));
    }
    Ok(Value::Object(body))
}

pub fn bydfi_amend_body(request: &AmendOrderRequest, wallet: &str) -> ExchangeApiResult<Value> {
    if request.exchange_order_id.is_none() && request.client_order_id.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "BYDFi amend_order requires orderId or clientOrderId".to_string(),
        });
    }
    let mut body = serde_json::Map::new();
    body.insert("wallet".to_string(), json!(wallet));
    body.insert(
        "symbol".to_string(),
        json!(normalize_bydfi_symbol(
            &request.symbol.exchange_symbol.symbol
        )?),
    );
    body.insert(
        "quantity".to_string(),
        json!(non_empty("quantity", &request.new_quantity)?),
    );
    body.insert("side".to_string(), json!("BUY"));
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body.insert("orderId".to_string(), json!(order_id));
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body.insert("clientOrderId".to_string(), json!(client_order_id));
    }
    Ok(Value::Object(body))
}

pub fn bydfi_batch_cancel_body(
    cancels: &[CancelOrderRequest],
    wallet: &str,
) -> ExchangeApiResult<Value> {
    let first = cancels
        .first()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "BYDFi batch cancel requires at least one cancel".to_string(),
        })?;
    let mut order_ids = Vec::new();
    let mut client_order_ids = Vec::new();
    for cancel in cancels {
        if let Some(order_id) = cancel.exchange_order_id.as_deref() {
            order_ids.push(json!(order_id));
        }
        if let Some(client_order_id) = cancel.client_order_id.as_deref() {
            client_order_ids.push(json!(client_order_id));
        }
    }
    if order_ids.is_empty() && client_order_ids.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "BYDFi batch cancel requires order ids".to_string(),
        });
    }
    Ok(json!({
        "wallet": wallet,
        "symbol": normalize_bydfi_symbol(&first.symbol.exchange_symbol.symbol)?,
        "orderIds": order_ids,
        "clientOrderIds": client_order_ids,
    }))
}

fn futures_base_params(wallet: &str) -> HashMap<String, String> {
    HashMap::from([
        ("contractType".to_string(), "FUTURE".to_string()),
        ("wallet".to_string(), wallet.to_string()),
        ("settleCoin".to_string(), "USDT".to_string()),
    ])
}

fn open_order_params(
    wallet: &str,
    symbol: &rustcta_exchange_api::SymbolScope,
) -> ExchangeApiResult<HashMap<String, String>> {
    Ok(HashMap::from([
        ("wallet".to_string(), wallet.to_string()),
        (
            "symbol".to_string(),
            normalize_bydfi_symbol(&symbol.exchange_symbol.symbol)?,
        ),
    ]))
}

fn optional_order_ids(
    params: &mut HashMap<String, String>,
    order_id: Option<&str>,
    client_order_id: Option<&str>,
) -> ExchangeApiResult<()> {
    if order_id.is_none() && client_order_id.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "BYDFi query_order requires orderId or clientOrderId".to_string(),
        });
    }
    if let Some(order_id) = order_id {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_order_id) = client_order_id {
        params.insert("clientOrderId".to_string(), client_order_id.to_string());
    }
    Ok(())
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
        client_order_id: request
            .client_order_id
            .clone()
            .or_else(|| string_field(value, "clientOrderId")),
        exchange_order_id: string_field(value, "orderId"),
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
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    }
}

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request
            .client_order_id
            .clone()
            .or_else(|| string_field(value, "clientOrderId")),
        exchange_order_id: request
            .exchange_order_id
            .clone()
            .or_else(|| string_field(value, "orderId")),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::Net),
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

fn order_state_from_amend_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &AmendOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request
            .client_order_id
            .clone()
            .or_else(|| string_field(value, "clientOrderId")),
        exchange_order_id: request
            .exchange_order_id
            .clone()
            .or_else(|| string_field(value, "orderId")),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::Net),
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
        updated_at: chrono::Utc::now(),
    }
}

fn data_or_root(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn non_empty<'a>(field: &str, value: &'a str) -> ExchangeApiResult<&'a str> {
    if value.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("BYDFi {field} must not be empty"),
        });
    }
    Ok(value)
}

fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn position_side_text(side: Option<PositionSide>) -> &'static str {
    match side {
        Some(PositionSide::Long) => "LONG",
        Some(PositionSide::Short) => "SHORT",
        _ => "BOTH",
    }
}

fn order_type_text(request: &PlaceOrderRequest) -> &'static str {
    if request.post_only || matches!(request.order_type, OrderType::PostOnly) {
        "LIMIT"
    } else {
        match request.order_type {
            OrderType::Market => "MARKET",
            _ => "LIMIT",
        }
    }
}

fn time_in_force_text(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "POST_ONLY",
    }
}
