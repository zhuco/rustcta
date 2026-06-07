use std::collections::HashMap;

use chrono::Utc;
use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderState, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    QueryOrderRequest, QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_weex_symbol;
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_fills, parse_order, parse_order_state, parse_orders,
    parse_positions,
};
use super::WeexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl WeexGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "weex.get_balances")?;
        let endpoint = match market_type {
            MarketType::Spot => "/api/v2/account/assets",
            MarketType::Perpetual => "/capi/v3/account/balance",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("weex.get_balances", endpoint, &HashMap::new())
            .await?;
        let balances = parse_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            market_type,
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
        if request
            .market_type
            .is_some_and(|market| market != MarketType::Perpetual)
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "weex.positions_non_perpetual",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "weex.get_positions")?;
        let mut params = HashMap::new();
        if let Some(symbol) = request.symbols.first() {
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "weex adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
            params.insert(
                "symbol".to_string(),
                normalize_weex_symbol(&symbol.symbol, MarketType::Perpetual)?,
            );
        }
        let value = self
            .send_signed_get(
                "weex.get_positions",
                "/capi/v3/account/position/allPosition",
                &params,
            )
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
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "weex get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            let endpoint = match symbol.market_type {
                MarketType::Spot => "/api/v3/exchangeInfo",
                MarketType::Perpetual => "/capi/v3/account/commissionRate",
                _ => unreachable!("checked by ensure_supported_market"),
            };
            let mut params = HashMap::new();
            if symbol.market_type == MarketType::Spot {
                params.insert(
                    "symbol".to_string(),
                    normalize_weex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
                );
            }
            let value = if symbol.market_type == MarketType::Spot {
                self.rest.send_public_request(endpoint, &params).await?
            } else {
                self.send_signed_get("weex.get_fees", endpoint, &params)
                    .await?
            };
            fees.extend(parse_fee_snapshots(
                &self.exchange_id,
                std::slice::from_ref(symbol),
                symbol.market_type,
                &value,
            )?);
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let params = weex_place_order_params(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/api/v3/order",
            MarketType::Perpetual => "/capi/v3/order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post("weex.place_order", endpoint, &params)
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            data_or_root(&value),
        )
        .unwrap_or_else(|_| {
            order_state_from_place_ack(&self.exchange_id, &request, data_or_root(&value))
        });
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
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
        let params = weex_cancel_order_params(&request)?;
        let (method, endpoint) = match request.symbol.market_type {
            MarketType::Spot => ("DELETE", "/api/v3/order"),
            MarketType::Perpetual => ("DELETE", "/capi/v3/order"),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = if method == "POST" {
            self.send_signed_post("weex.cancel_order", endpoint, &params)
                .await?
        } else {
            self.send_signed_delete("weex.cancel_order", endpoint, &params)
                .await?
        };
        let order = parse_order(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )
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

    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.orders.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "weex batch_place_orders requires at least one order".to_string(),
            });
        }
        let market_type = uniform_place_market(&request.orders)?;
        let endpoint = match market_type {
            MarketType::Spot => "/api/v2/trade/batch-orders",
            MarketType::Perpetual => "/capi/v3/batchOrders",
            _ => unreachable!("checked by uniform_place_market"),
        };
        let body_orders = request
            .orders
            .iter()
            .map(weex_place_order_params)
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let body = match market_type {
            MarketType::Spot => {
                let symbol = normalize_weex_symbol(
                    &request.orders[0].symbol.exchange_symbol.symbol,
                    market_type,
                )?;
                json!({ "symbol": symbol, "orderList": body_orders })
            }
            MarketType::Perpetual => json!({ "batchOrders": body_orders }),
            _ => unreachable!("checked by uniform_place_market"),
        };
        let value = self
            .send_signed_post_json("weex.batch_place_orders", endpoint, &body)
            .await?;
        let orders = parse_orders(
            &self.exchange_id,
            Some(&request.orders[0].symbol),
            market_type,
            &value,
        )
        .unwrap_or_else(|_| {
            request
                .orders
                .iter()
                .zip(batch_items(data_or_root(&value)))
                .map(|(order_request, ack)| {
                    order_state_from_place_ack(&self.exchange_id, order_request, ack)
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
            return Err(ExchangeApiError::InvalidRequest {
                message: "weex batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        let market_type = uniform_cancel_market(&request.cancels)?;
        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/order/batch",
            MarketType::Perpetual => "/capi/v3/batchOrders",
            _ => unreachable!("checked by uniform_cancel_market"),
        };
        let body = weex_batch_cancel_body(&request.cancels, market_type)?;
        let value = self
            .send_signed_delete_json("weex.batch_cancel_orders", endpoint, &body)
            .await?;
        let orders = parse_orders(
            &self.exchange_id,
            Some(&request.cancels[0].symbol),
            market_type,
            &value,
        )
        .unwrap_or_else(|_| {
            request
                .cancels
                .iter()
                .zip(batch_items(data_or_root(&value)))
                .map(|(cancel_request, ack)| {
                    order_state_from_cancel_ack(&self.exchange_id, cancel_request, ack)
                })
                .collect()
        });
        let cancelled_count = if orders.is_empty() {
            response_item_count(data_or_root(&value)) as u32
        } else {
            orders.len() as u32
        };
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "weex cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        if request
            .market_type
            .is_some_and(|market_type| market_type != symbol.market_type)
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "weex cancel_all_orders market_type does not match symbol".to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_weex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        let value = match symbol.market_type {
            MarketType::Spot => {
                self.send_signed_delete("weex.cancel_all_orders", "/api/v3/openOrders", &params)
                    .await?
            }
            MarketType::Perpetual => {
                self.send_signed_delete("weex.cancel_all_orders", "/capi/v3/allOpenOrders", &params)
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let orders = parse_orders(&self.exchange_id, Some(symbol), symbol.market_type, &value)
            .unwrap_or_default();
        let cancelled_count = if orders.is_empty() {
            response_item_count(data_or_root(&value))
        } else {
            orders.len()
        };
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: cancelled_count as u32,
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
        let params = weex_query_order_params(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/api/v3/order",
            MarketType::Perpetual => "/capi/v3/order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("weex.query_order", endpoint, &params)
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_order(
                &self.exchange_id,
                Some(&request.symbol),
                request.symbol.market_type,
                &value,
            )?,
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_weex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
            );
        }
        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/openOrders",
            MarketType::Perpetual => "/capi/v3/openOrders",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("weex.get_open_orders", endpoint, &params)
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "weex get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "weex.get_recent_fills")?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_weex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
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
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(100).to_string());
        }
        let endpoint = match symbol.market_type {
            MarketType::Spot => "/api/v3/myTrades",
            MarketType::Perpetual => "/capi/v3/userTrades",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("weex.get_recent_fills", endpoint, &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                Some(symbol),
                symbol.market_type,
                &value,
            )?,
        })
    }
}

fn weex_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "weex gateway first version does not support reduce_only order placement"
                .to_string(),
        });
    }
    if request.post_only || matches!(request.time_in_force, Some(TimeInForce::GTX)) {
        return Err(ExchangeApiError::Unsupported {
            operation: "weex.post_only_order",
        });
    }
    if matches!(
        request.order_type,
        OrderType::PostOnly | OrderType::StopMarket | OrderType::StopLimit
    ) {
        return Err(ExchangeApiError::Unsupported {
            operation: "weex.advanced_order_type",
        });
    }
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_weex_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    params.insert("side".to_string(), side_text(request.side).to_string());
    params.insert("quantity".to_string(), request.quantity.clone());
    if request.order_type.requires_limit_price() {
        params.insert(
            "price".to_string(),
            request
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "weex limit-like order requires price".to_string(),
                })?,
        );
    } else if let Some(price) = &request.price {
        params.insert("price".to_string(), price.clone());
    }
    match request.symbol.market_type {
        MarketType::Spot => {
            params.insert(
                "side".to_string(),
                side_text(request.side).to_ascii_lowercase(),
            );
            params.insert(
                "orderType".to_string(),
                order_type_text(request.order_type).to_ascii_lowercase(),
            );
            params.insert(
                "force".to_string(),
                spot_force_text(request.time_in_force.unwrap_or(TimeInForce::GTC)).to_string(),
            );
            if let Some(client_order_id) = request.client_order_id.as_deref() {
                params.insert("clientOrderId".to_string(), client_order_id.to_string());
            }
        }
        MarketType::Perpetual => {
            params.insert(
                "type".to_string(),
                order_type_text(request.order_type).to_string(),
            );
            if request.order_type.requires_limit_price() {
                params.insert(
                    "timeInForce".to_string(),
                    tif_text(request.time_in_force.unwrap_or(TimeInForce::GTC)).to_string(),
                );
            }
            params.insert(
                "positionSide".to_string(),
                position_side_text(request.position_side, request.side)?.to_string(),
            );
            if let Some(client_order_id) = request.client_order_id.as_deref() {
                params.insert("newClientOrderId".to_string(), client_order_id.to_string());
            }
        }
        _ => unreachable!("checked by caller"),
    }
    Ok(params)
}

fn weex_cancel_order_params(
    request: &CancelOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_weex_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("origClientOrderId".to_string(), client_order_id.to_string());
    }
    if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: "weex cancel_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    Ok(params)
}

fn weex_query_order_params(
    request: &QueryOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_weex_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("origClientOrderId".to_string(), client_order_id.to_string());
    }
    if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: "weex query_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    Ok(params)
}

fn data_or_root(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn batch_items(value: &Value) -> Vec<&Value> {
    value
        .get("orders")
        .or_else(|| value.get("orderList"))
        .or_else(|| value.get("success"))
        .and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .or_else(|| value.as_array().map(|items| items.iter().collect()))
        .unwrap_or_else(|| vec![value])
}

fn response_item_count(value: &Value) -> usize {
    value
        .as_array()
        .map(Vec::len)
        .or_else(|| value.get("success").and_then(Value::as_array).map(Vec::len))
        .unwrap_or(0)
}

fn uniform_place_market(requests: &[PlaceOrderRequest]) -> ExchangeApiResult<MarketType> {
    let first = requests[0].symbol.market_type;
    if !matches!(first, MarketType::Spot | MarketType::Perpetual) {
        return Err(ExchangeApiError::Unsupported {
            operation: "weex.batch_place_orders.market_type",
        });
    }
    for request in requests {
        if request.symbol.market_type != first {
            return Err(ExchangeApiError::InvalidRequest {
                message: "weex batch_place_orders requires one market_type".to_string(),
            });
        }
    }
    Ok(first)
}

fn uniform_cancel_market(requests: &[CancelOrderRequest]) -> ExchangeApiResult<MarketType> {
    let first = requests[0].symbol.market_type;
    if !matches!(first, MarketType::Spot | MarketType::Perpetual) {
        return Err(ExchangeApiError::Unsupported {
            operation: "weex.batch_cancel_orders.market_type",
        });
    }
    for request in requests {
        if request.symbol.market_type != first {
            return Err(ExchangeApiError::InvalidRequest {
                message: "weex batch_cancel_orders requires one market_type".to_string(),
            });
        }
    }
    Ok(first)
}

fn weex_batch_cancel_body(
    requests: &[CancelOrderRequest],
    market_type: MarketType,
) -> ExchangeApiResult<Value> {
    let symbol = normalize_weex_symbol(&requests[0].symbol.exchange_symbol.symbol, market_type)?;
    let mut order_ids = Vec::new();
    let mut client_order_ids = Vec::new();
    for request in requests {
        let request_symbol =
            normalize_weex_symbol(&request.symbol.exchange_symbol.symbol, market_type)?;
        if request_symbol != symbol {
            return Err(ExchangeApiError::InvalidRequest {
                message: "weex batch_cancel_orders requires one symbol".to_string(),
            });
        }
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            order_ids.push(order_id.to_string());
        } else if let Some(client_order_id) = request.client_order_id.as_deref() {
            client_order_ids.push(client_order_id.to_string());
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message:
                    "weex batch_cancel_orders item requires exchange_order_id or client_order_id"
                        .to_string(),
            });
        }
    }
    if !order_ids.is_empty() && !client_order_ids.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "weex batch_cancel_orders cannot mix exchange_order_id and client_order_id"
                .to_string(),
        });
    }
    Ok(match market_type {
        MarketType::Spot if !order_ids.is_empty() => {
            json!({ "symbol": symbol, "orderIds": order_ids })
        }
        MarketType::Spot => json!({ "symbol": symbol, "origClientOrderIds": client_order_ids }),
        MarketType::Perpetual if !order_ids.is_empty() => {
            json!({ "symbol": symbol, "orderIdList": order_ids })
        }
        MarketType::Perpetual => {
            json!({ "symbol": symbol, "origClientOrderIdList": client_order_ids })
        }
        _ => unreachable!("checked by caller"),
    })
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
        client_order_id: value
            .get("clientOrderId")
            .or_else(|| value.get("clientOrderID"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value
            .get("orderId")
            .map(|value| value.to_string().trim_matches('"').to_string()),
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

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value
            .get("clientOrderId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value
            .get("orderId")
            .map(|value| value.to_string().trim_matches('"').to_string())
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

fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn order_type_text(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "MARKET",
        OrderType::StopMarket => "STOP_MARKET",
        OrderType::StopLimit => "STOP",
        _ => "LIMIT",
    }
}

fn tif_text(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTC",
    }
}

fn spot_force_text(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::IOC => "ioc",
        TimeInForce::FOK => "fok",
        TimeInForce::GTX => "post_only",
        TimeInForce::GTC => "gtc",
    }
}

fn position_side_text(
    side: Option<PositionSide>,
    order_side: OrderSide,
) -> ExchangeApiResult<&'static str> {
    match side {
        Some(PositionSide::Long) => Ok("LONG"),
        Some(PositionSide::Short) => Ok("SHORT"),
        Some(PositionSide::Net | PositionSide::None) | None => match order_side {
            OrderSide::Buy => Ok("LONG"),
            OrderSide::Sell => Ok("SHORT"),
        },
    }
}
