use std::collections::HashMap;

use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse,
    PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_xt_symbol;
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_fills, parse_order, parse_order_state, parse_orders,
    parse_positions,
};
use super::XtGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl XtGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context, "xt.get_balances")?;
        let endpoint = match market_type {
            MarketType::Spot => "/v4/balances",
            MarketType::Perpetual => "/future/user/v1/balance/list",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("xt.get_balances", endpoint, &HashMap::new())
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
                operation: "xt.positions_non_perpetual",
            });
        }
        let (tenant_id, account_id) = self.context_account(&request.context, "xt.get_positions")?;
        let mut params = HashMap::new();
        if let Some(symbol) = request.symbols.first() {
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "xt adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
            params.insert(
                "symbol".to_string(),
                normalize_xt_symbol(&symbol.symbol, MarketType::Perpetual)?,
            );
        }
        let value = self
            .send_signed_get("xt.get_positions", "/future/user/v1/position/list", &params)
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
                message: "xt get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            let endpoint = match symbol.market_type {
                MarketType::Spot => "/v4/public/symbol",
                MarketType::Perpetual => "/future/market/v3/public/symbol/list",
                _ => unreachable!("checked by ensure_supported_market"),
            };
            let mut params = HashMap::new();
            if symbol.market_type == MarketType::Spot {
                params.insert(
                    "symbol".to_string(),
                    normalize_xt_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
                );
            }
            let value = self
                .rest
                .send_public_request(symbol.market_type, endpoint, &params)
                .await?;
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
        let params = xt_place_order_params(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/v4/order",
            MarketType::Perpetual => "/future/trade/v1/order/create",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post("xt.place_order", endpoint, &params)
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
        let params = xt_cancel_order_params(&request)?;
        let (method, endpoint) = match request.symbol.market_type {
            MarketType::Spot => ("DELETE", spot_order_path(&request)?),
            MarketType::Perpetual => ("POST", "/future/trade/v1/order/cancel".to_string()),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = if method == "POST" {
            self.send_signed_post("xt.cancel_order", &endpoint, &params)
                .await?
        } else {
            self.send_signed_delete("xt.cancel_order", &endpoint, &HashMap::new())
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

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "xt amend_order requires exchange_order_id".to_string(),
            }
        })?;
        let current = self
            .query_order_impl(QueryOrderRequest {
                schema_version: request.schema_version,
                context: request.context.clone(),
                symbol: request.symbol.clone(),
                client_order_id: request.client_order_id.clone(),
                exchange_order_id: Some(order_id.to_string()),
            })
            .await?
            .order
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("xt amend_order could not load current order {order_id}"),
            })?;
        let price = current
            .price
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "xt amend_order requires an existing limit order price".to_string(),
            })?;
        let mut params = HashMap::new();
        params.insert("orderId".to_string(), order_id.to_string());
        params.insert("price".to_string(), price.clone());
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => {
                params.insert("quantity".to_string(), request.new_quantity.clone());
                format!("/v4/order/{order_id}")
            }
            MarketType::Perpetual => {
                params.insert("origQty".to_string(), request.new_quantity.clone());
                "/future/trade/v1/order/update".to_string()
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = match request.symbol.market_type {
            MarketType::Spot => {
                self.send_signed_put("xt.amend_order", &endpoint, &params)
                    .await?
            }
            MarketType::Perpetual => {
                self.send_signed_post("xt.amend_order", &endpoint, &params)
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let mut order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            data_or_root(&value),
        )
        .unwrap_or_else(|_| {
            let mut order = current;
            order.quantity = request.new_quantity.clone();
            order.price = Some(price);
            order.client_order_id = request
                .new_client_order_id
                .clone()
                .or(order.client_order_id);
            order.updated_at = Utc::now();
            order
        });
        order.quantity = request.new_quantity;
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
                message: "xt batch_place_orders requires at least one order".to_string(),
            });
        }
        let market_type = request.orders[0].symbol.market_type;
        self.ensure_supported_market(market_type)?;
        for order in &request.orders {
            ensure_exchange_api_schema(order.schema_version)?;
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market(order.symbol.market_type)?;
            if order.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "xt batch_place_orders requires a single market_type".to_string(),
                });
            }
        }
        let value = match market_type {
            MarketType::Spot => {
                let items = request
                    .orders
                    .iter()
                    .map(|order| xt_place_order_params(order))
                    .collect::<ExchangeApiResult<Vec<_>>>()?;
                self.send_signed_json(
                    "xt.batch_place_orders",
                    reqwest::Method::POST,
                    "/v4/batch-order",
                    &json!({ "items": items }),
                )
                .await?
            }
            MarketType::Perpetual => {
                let items = request
                    .orders
                    .iter()
                    .map(|order| {
                        xt_place_order_params(order).and_then(|params| {
                            serde_json::to_value(params).map_err(|error| {
                                ExchangeApiError::InvalidRequest {
                                    message: format!(
                                        "failed to serialize XT batch order item: {error}"
                                    ),
                                }
                            })
                        })
                    })
                    .collect::<ExchangeApiResult<Vec<_>>>()?;
                self.send_signed_json(
                    "xt.batch_place_orders",
                    reqwest::Method::POST,
                    "/future/trade/v2/order/atomic-create-batch",
                    &Value::Array(items),
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let orders = order_states_from_batch_place_ack(&self.exchange_id, &request.orders, &value);
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
                message: "xt batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        let market_type = request.cancels[0].symbol.market_type;
        self.ensure_supported_market(market_type)?;
        for cancel in &request.cancels {
            ensure_exchange_api_schema(cancel.schema_version)?;
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market(cancel.symbol.market_type)?;
            if cancel.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "xt batch_cancel_orders requires a single market_type".to_string(),
                });
            }
        }
        let order_ids = request
            .cancels
            .iter()
            .filter_map(|cancel| cancel.exchange_order_id.clone())
            .collect::<Vec<_>>();
        if order_ids.len() != request.cancels.len() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "xt batch_cancel_orders requires exchange_order_id for every cancel"
                    .to_string(),
            });
        }
        match market_type {
            MarketType::Spot => {
                self.send_signed_json(
                    "xt.batch_cancel_orders",
                    reqwest::Method::DELETE,
                    "/v4/batch-order",
                    &json!({ "orderIds": order_ids }),
                )
                .await?
            }
            MarketType::Perpetual => {
                let mut params = HashMap::new();
                params.insert("orderIds".to_string(), order_ids.join(","));
                let client_order_ids = request
                    .cancels
                    .iter()
                    .filter_map(|cancel| cancel.client_order_id.clone())
                    .collect::<Vec<_>>();
                if !client_order_ids.is_empty() {
                    params.insert("clientOrderIds".to_string(), client_order_ids.join(","));
                }
                self.send_signed_post(
                    "xt.batch_cancel_orders",
                    "/future/trade/v1/order/cancel-batch",
                    &params,
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let orders = request
            .cancels
            .iter()
            .map(|cancel| order_state_from_cancel_ack(&self.exchange_id, cancel, &Value::Null))
            .collect::<Vec<_>>();
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
                message: "xt cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        if request
            .market_type
            .is_some_and(|market_type| market_type != symbol.market_type)
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "xt cancel_all_orders market_type does not match symbol".to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_xt_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        let value = match symbol.market_type {
            MarketType::Spot => {
                params.insert("bizType".to_string(), "SPOT".to_string());
                self.send_signed_delete("xt.cancel_all_orders", "/v4/open-order", &params)
                    .await?
            }
            MarketType::Perpetual => {
                self.send_signed_post(
                    "xt.cancel_all_orders",
                    "/future/trade/v1/order/cancel-all",
                    &params,
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let orders = parse_orders(&self.exchange_id, Some(symbol), symbol.market_type, &value)
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
        let params = xt_query_order_params(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => {
                if let Some(order_id) = request.exchange_order_id.as_deref() {
                    format!("/v4/order/{order_id}")
                } else {
                    "/v4/order".to_string()
                }
            }
            MarketType::Perpetual => "/future/trade/v1/order/detail".to_string(),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let params = if request.symbol.market_type == MarketType::Spot
            && request.exchange_order_id.is_some()
        {
            HashMap::new()
        } else {
            params
        };
        let value = self
            .send_signed_get("xt.query_order", &endpoint, &params)
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
                normalize_xt_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
            );
        }
        let endpoint = match market_type {
            MarketType::Spot => "/v4/open-order",
            MarketType::Perpetual => "/future/trade/v1/order/list-open-order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        if market_type == MarketType::Spot {
            params.insert("bizType".to_string(), "SPOT".to_string());
        }
        let value = if market_type == MarketType::Perpetual {
            self.send_signed_post("xt.get_open_orders", endpoint, &params)
                .await?
        } else {
            self.send_signed_get("xt.get_open_orders", endpoint, &params)
                .await?
        };
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
                message: "xt get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "xt.get_recent_fills")?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_xt_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
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
            params.insert("limit".to_string(), limit.min(1000).to_string());
        }
        let endpoint = match symbol.market_type {
            MarketType::Spot => "/v4/trade",
            MarketType::Perpetual => "/future/trade/v1/order/trade-list",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("xt.get_recent_fills", endpoint, &params)
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

fn xt_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    if request.symbol.market_type == MarketType::Spot && request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "xt spot order does not support reduce_only".to_string(),
        });
    }
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_xt_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    if request.symbol.market_type == MarketType::Spot {
        params.insert("side".to_string(), side_text(request.side).to_string());
        params.insert(
            "type".to_string(),
            order_type_text(request.order_type).to_string(),
        );
        params.insert("bizType".to_string(), "SPOT".to_string());
    } else {
        params.insert("orderSide".to_string(), side_text(request.side).to_string());
        params.insert(
            "orderType".to_string(),
            order_type_text(request.order_type).to_string(),
        );
    }
    if request.order_type == OrderType::Market
        && request.symbol.market_type == MarketType::Spot
        && request.side == OrderSide::Buy
    {
        let quote_quantity =
            request
                .quote_quantity
                .as_deref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "xt spot market buy requires quote_quantity".to_string(),
                })?;
        params.insert("quoteQty".to_string(), quote_quantity.to_string());
    } else {
        let quantity_key = if request.symbol.market_type == MarketType::Perpetual {
            "origQty"
        } else {
            "quantity"
        };
        params.insert(quantity_key.to_string(), request.quantity.clone());
    }
    if request.order_type.requires_limit_price() {
        params.insert(
            "price".to_string(),
            request
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "xt limit-like order requires price".to_string(),
                })?,
        );
    } else if let Some(price) = &request.price {
        params.insert("price".to_string(), price.clone());
    }
    if let Some(tif) = request
        .time_in_force
        .or_else(|| request.post_only.then_some(TimeInForce::GTX))
    {
        params.insert("timeInForce".to_string(), tif_text(tif).to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("clientOrderId".to_string(), client_order_id.to_string());
    }
    if request.symbol.market_type == MarketType::Perpetual {
        params.insert(
            "positionSide".to_string(),
            position_side_text(request.position_side.unwrap_or(PositionSide::Net)).to_string(),
        );
        if request.reduce_only {
            return Err(ExchangeApiError::Unsupported {
                operation: "xt.perpetual_reduce_only_order",
            });
        }
    }
    Ok(params)
}

fn xt_cancel_order_params(
    request: &CancelOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_xt_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if request.symbol.market_type == MarketType::Perpetual {
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("clientOrderId".to_string(), client_order_id.to_string());
        }
    } else if request.exchange_order_id.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "xt spot cancel_order requires exchange_order_id".to_string(),
        });
    }
    if !params.contains_key("orderId") && !params.contains_key("clientOrderId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: "xt cancel_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    Ok(params)
}

fn spot_order_path(request: &CancelOrderRequest) -> ExchangeApiResult<String> {
    let order_id =
        request
            .exchange_order_id
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "xt spot cancel_order requires exchange_order_id".to_string(),
            })?;
    Ok(format!("/v4/order/{order_id}"))
}

fn xt_query_order_params(
    request: &QueryOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    if request.symbol.market_type != MarketType::Spot || request.exchange_order_id.is_none() {
        params.insert(
            "symbol".to_string(),
            normalize_xt_symbol(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?,
        );
    }
    if request.symbol.market_type == MarketType::Spot {
        params.insert("bizType".to_string(), "SPOT".to_string());
    }
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("clientOrderId".to_string(), client_order_id.to_string());
    }
    if !params.contains_key("orderId") && !params.contains_key("clientOrderId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: "xt cancel_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    Ok(params)
}

fn data_or_root(value: &Value) -> &Value {
    value
        .get("result")
        .or_else(|| value.get("data"))
        .unwrap_or(value)
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

fn order_states_from_batch_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    requests: &[PlaceOrderRequest],
    value: &Value,
) -> Vec<OrderState> {
    let items = data_or_root(value)
        .get("items")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| data_or_root(value).as_array().map(Vec::as_slice))
        .unwrap_or(&[]);
    requests
        .iter()
        .enumerate()
        .map(|(index, request)| {
            let ack = items
                .iter()
                .find(|item| {
                    item.get("index")
                        .map(|value| value.to_string().trim_matches('"').to_string())
                        .is_some_and(|value| value == index.to_string())
                })
                .or_else(|| items.get(index))
                .unwrap_or(&Value::Null);
            let mut order = order_state_from_place_ack(exchange_id, request, ack);
            if ack
                .get("reject")
                .and_then(|value| match value {
                    Value::Bool(flag) => Some(*flag),
                    Value::String(text) => Some(text.eq_ignore_ascii_case("true")),
                    _ => None,
                })
                .unwrap_or(false)
            {
                order.status = OrderStatus::Rejected;
            }
            order
        })
        .collect()
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
        TimeInForce::GTX => "GTX",
    }
}

fn position_side_text(side: PositionSide) -> &'static str {
    match side {
        PositionSide::Long => "LONG",
        PositionSide::Short => "SHORT",
        _ => "BOTH",
    }
}
