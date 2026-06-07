use std::collections::HashMap;

use chrono::Utc;
use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderState, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    QueryOrderRequest, QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, SymbolScope,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::Value;

use super::capabilities::BITRUE_COMPOSED_BATCH_MAX_ITEMS;
use super::parser::normalize_bitrue_symbol;
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_fills, parse_order, parse_order_state, parse_orders,
    parse_positions,
};
use super::BitrueGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BitrueGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bitrue.get_balances")?;
        let endpoint = match market_type {
            MarketType::Spot => "/api/v1/account",
            MarketType::Perpetual => "/fapi/v2/account",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("bitrue.get_balances", endpoint, &HashMap::new())
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
                operation: "bitrue.positions_non_perpetual",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bitrue.get_positions")?;
        let mut params = HashMap::new();
        if let Some(symbol) = request.symbols.first() {
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "bitrue adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
            params.insert(
                "contractName".to_string(),
                normalize_bitrue_symbol(&symbol.symbol, MarketType::Perpetual)?,
            );
        }
        let value = self
            .send_signed_get("bitrue.get_positions", "/fapi/v2/account", &params)
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
                message: "bitrue get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            let endpoint = match symbol.market_type {
                MarketType::Spot => "/api/v1/account",
                MarketType::Perpetual => "/fapi/v2/commissionRate",
                _ => unreachable!("checked by ensure_supported_market"),
            };
            let mut params = HashMap::new();
            if symbol.market_type == MarketType::Perpetual {
                params.insert(
                    "contractName".to_string(),
                    normalize_bitrue_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
                );
            }
            let value = self
                .send_signed_get("bitrue.get_fees", endpoint, &params)
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
        let params = bitrue_place_order_params(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/api/v1/order",
            MarketType::Perpetual => "/fapi/v2/order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post("bitrue.place_order", endpoint, &params)
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
        let params = bitrue_cancel_order_params(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/api/v1/order",
            MarketType::Perpetual => "/fapi/v2/cancel",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = match request.symbol.market_type {
            MarketType::Spot => {
                self.send_signed_delete("bitrue.cancel_order", endpoint, &params)
                    .await?
            }
            MarketType::Perpetual => {
                self.send_signed_post("bitrue.cancel_order", endpoint, &params)
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
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
                message: "bitrue batch_place_orders requires at least one order".to_string(),
            });
        }
        if request.orders.len() > BITRUE_COMPOSED_BATCH_MAX_ITEMS as usize {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitrue.batch_place_orders_too_large",
            });
        }

        let mut orders = Vec::with_capacity(request.orders.len());
        for order_request in request.orders {
            self.ensure_exchange(&order_request.symbol.exchange)?;
            self.ensure_supported_market(order_request.symbol.market_type)?;
            orders.push(self.place_order_impl(order_request).await?.order);
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
                message: "bitrue batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        if request.cancels.len() > BITRUE_COMPOSED_BATCH_MAX_ITEMS as usize {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitrue.batch_cancel_orders_too_large",
            });
        }

        let mut orders = Vec::with_capacity(request.cancels.len());
        let mut cancelled_count = 0_u32;
        for cancel_request in request.cancels {
            self.ensure_exchange(&cancel_request.symbol.exchange)?;
            self.ensure_supported_market(cancel_request.symbol.market_type)?;
            let response = self.cancel_order_impl(cancel_request).await?;
            if response.cancelled {
                cancelled_count += 1;
            }
            orders.push(response.order);
        }

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
                message: "bitrue cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        if request
            .market_type
            .is_some_and(|market_type| market_type != symbol.market_type)
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitrue cancel_all_orders market_type does not match symbol".to_string(),
            });
        }

        let open_orders = self
            .get_open_orders_impl(OpenOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request.context.clone(),
                exchange: request.exchange.clone(),
                market_type: Some(symbol.market_type),
                symbol: Some(symbol.clone()),
                page: None,
            })
            .await?
            .orders;

        let mut cancelled = Vec::with_capacity(open_orders.len());
        for open_order in open_orders {
            let cancel_symbol = symbol_scope_from_order(&request.exchange, symbol, &open_order);
            let response = self
                .cancel_order_impl(CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: request.context.clone(),
                    symbol: cancel_symbol,
                    client_order_id: open_order.client_order_id.clone(),
                    exchange_order_id: open_order.exchange_order_id.clone(),
                })
                .await?;
            if response.cancelled {
                cancelled.push(response.order);
            }
        }

        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: cancelled.len() as u32,
            orders: cancelled,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let params = bitrue_query_order_params(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/api/v1/order",
            MarketType::Perpetual => "/fapi/v2/order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("bitrue.query_order", endpoint, &params)
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
                (if symbol.market_type == MarketType::Perpetual {
                    "contractName"
                } else {
                    "symbol"
                })
                .to_string(),
                normalize_bitrue_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
            );
        }
        let endpoint = match market_type {
            MarketType::Spot => "/api/v1/openOrders",
            MarketType::Perpetual => "/fapi/v2/openOrders",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("bitrue.get_open_orders", endpoint, &params)
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
                message: "bitrue get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bitrue.get_recent_fills")?;
        let mut params = HashMap::new();
        params.insert(
            (if symbol.market_type == MarketType::Perpetual {
                "contractName"
            } else {
                "symbol"
            })
            .to_string(),
            normalize_bitrue_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
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
            MarketType::Spot => "/api/v2/myTrades",
            MarketType::Perpetual => "/fapi/v2/myTrades",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("bitrue.get_recent_fills", endpoint, &params)
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

fn bitrue_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    if request.symbol.market_type == MarketType::Spot && request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitrue spot order does not support reduce_only".to_string(),
        });
    }
    if request.quote_quantity.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitrue.quote_sized_order",
        });
    }
    if matches!(
        request.order_type,
        OrderType::StopMarket | OrderType::StopLimit
    ) {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitrue.stop_order",
        });
    }
    if request.symbol.market_type == MarketType::Spot {
        if matches!(
            request.order_type,
            OrderType::PostOnly | OrderType::IOC | OrderType::FOK
        ) || request.post_only
            || request
                .time_in_force
                .is_some_and(|tif| tif != TimeInForce::GTC)
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitrue.spot_advanced_time_in_force",
            });
        }
    }
    let mut params = HashMap::new();
    let symbol_key = if request.symbol.market_type == MarketType::Perpetual {
        "contractName"
    } else {
        "symbol"
    };
    params.insert(
        symbol_key.to_string(),
        normalize_bitrue_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    params.insert("side".to_string(), side_text(request.side).to_string());
    params.insert(
        "type".to_string(),
        order_type_text(request.order_type).to_string(),
    );
    let quantity_key = if request.symbol.market_type == MarketType::Perpetual {
        "volume"
    } else {
        "quantity"
    };
    params.insert(quantity_key.to_string(), request.quantity.clone());
    if request.order_type.requires_limit_price() {
        params.insert(
            "price".to_string(),
            request
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitrue limit-like order requires price".to_string(),
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
        let client_key = if request.symbol.market_type == MarketType::Perpetual {
            "clientOrderId"
        } else {
            "newClientOrderId"
        };
        params.insert(client_key.to_string(), client_order_id.to_string());
    }
    if request.symbol.market_type == MarketType::Perpetual {
        params.insert(
            "open".to_string(),
            if request.reduce_only { "CLOSE" } else { "OPEN" }.to_string(),
        );
        params.insert("positionType".to_string(), "1".to_string());
    }
    Ok(params)
}

fn bitrue_cancel_order_params(
    request: &CancelOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    let symbol_key = if request.symbol.market_type == MarketType::Perpetual {
        "contractName"
    } else {
        "symbol"
    };
    params.insert(
        symbol_key.to_string(),
        normalize_bitrue_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        let client_key = if request.symbol.market_type == MarketType::Perpetual {
            "clientOrderId"
        } else {
            "origClientOrderId"
        };
        params.insert(client_key.to_string(), client_order_id.to_string());
    }
    if !params.contains_key("orderId")
        && !params.contains_key("clientOrderId")
        && !params.contains_key("origClientOrderId")
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitrue cancel_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(params)
}

fn bitrue_query_order_params(
    request: &QueryOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    let symbol_key = if request.symbol.market_type == MarketType::Perpetual {
        "contractName"
    } else {
        "symbol"
    };
    params.insert(
        symbol_key.to_string(),
        normalize_bitrue_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        let client_key = if request.symbol.market_type == MarketType::Perpetual {
            "clientOrderId"
        } else {
            "origClientOrderId"
        };
        params.insert(client_key.to_string(), client_order_id.to_string());
    }
    if !params.contains_key("orderId")
        && !params.contains_key("clientOrderId")
        && !params.contains_key("origClientOrderId")
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitrue query_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    Ok(params)
}

fn data_or_root(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn symbol_scope_from_order(
    exchange: &rustcta_types::ExchangeId,
    fallback: &SymbolScope,
    order: &OrderState,
) -> SymbolScope {
    SymbolScope {
        exchange: exchange.clone(),
        market_type: order.market_type,
        canonical_symbol: order
            .canonical_symbol
            .clone()
            .or_else(|| fallback.canonical_symbol.clone()),
        exchange_symbol: order.exchange_symbol.clone(),
    }
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
        TimeInForce::GTX => "POST_ONLY",
    }
}
