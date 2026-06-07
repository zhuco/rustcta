use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeeRateSnapshot, FeesRequest,
    FeesResponse, OpenOrdersRequest, OpenOrdersResponse, OrderState, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, SymbolScope, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeSymbol, MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::Value;

use super::parser::{normalize_toobit_symbol, symbol_scope_from_exchange_symbol};
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_fills, parse_order, parse_order_state, parse_orders,
    parse_positions,
};
use super::ToobitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl ToobitGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "toobit.get_balances")?;
        let endpoint = match market_type {
            MarketType::Spot => "/api/v1/account",
            MarketType::Perpetual => "/api/v1/futures/balance",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("toobit.get_balances", endpoint, &HashMap::new())
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                market_type,
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
                operation: "toobit.positions_non_perpetual",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "toobit.get_positions")?;
        let value = self
            .send_signed_get(
                "toobit.get_positions",
                "/api/v1/futures/positions",
                &HashMap::new(),
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
                message: "toobit get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            match symbol.market_type {
                MarketType::Spot => {
                    let (Some(maker), Some(taker)) = (
                        self.config.spot_maker_fee_override.clone(),
                        self.config.spot_taker_fee_override.clone(),
                    ) else {
                        return Err(ExchangeApiError::Unsupported {
                            operation: "toobit.spot_fee_rate_requires_config_override",
                        });
                    };
                    fees.push(FeeRateSnapshot {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        symbol: symbol.clone(),
                        maker_rate: maker,
                        taker_rate: taker,
                        source: Some("toobit.config_override".to_string()),
                        updated_at: chrono::Utc::now(),
                    });
                }
                MarketType::Perpetual => {
                    let mut params = HashMap::new();
                    params.insert(
                        "symbol".to_string(),
                        normalize_toobit_symbol(
                            &symbol.exchange_symbol.symbol,
                            MarketType::Perpetual,
                        )?,
                    );
                    let value = self
                        .send_signed_get(
                            "toobit.get_fees",
                            "/api/v1/futures/commissionRate",
                            &params,
                        )
                        .await?;
                    fees.extend(parse_fee_snapshots(
                        &self.exchange_id,
                        std::slice::from_ref(symbol),
                        symbol.market_type,
                        &value,
                    )?);
                }
                _ => unreachable!("checked by ensure_supported_market"),
            }
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        mut request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        if request.client_order_id.is_none() {
            request.client_order_id =
                Some(self.generate_client_order_id(request.symbol.market_type));
        }
        let params = toobit_order_params(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/api/v1/spot/order",
            MarketType::Perpetual => "/api/v2/futures/order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post("toobit.place_order", endpoint, &params)
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            super::parser::data_payload(&value),
        )
        .unwrap_or_else(|_| order_state_from_place_request(&self.exchange_id, &request, &value));
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
        let params = toobit_cancel_order_params(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/api/v1/spot/order",
            MarketType::Perpetual => "/api/v2/futures/order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_delete("toobit.cancel_order", endpoint, &params)
            .await?;
        let order = parse_order(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )
        .ok()
        .flatten()
        .unwrap_or_else(|| order_state_from_cancel_request(&self.exchange_id, &request, &value));
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
        if request.symbol.market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "toobit.spot_amend_order",
            });
        }
        let exchange_order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "toobit amend_order requires exchange_order_id".to_string(),
            }
        })?;
        if request.new_client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "toobit.amend_new_client_order_id",
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_toobit_symbol(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?,
        );
        params.insert("orderId".to_string(), exchange_order_id.to_string());
        params.insert("quantity".to_string(), request.new_quantity.clone());
        let value = self
            .send_signed_post(
                "toobit.amend_order",
                "/api/v2/futures/order/update",
                &params,
            )
            .await?;
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_order_state(
                &self.exchange_id,
                Some(&request.symbol),
                request.symbol.market_type,
                super::parser::data_payload(&value),
            )?,
        })
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        mut request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.orders.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "toobit batch_place_orders requires at least one order".to_string(),
            });
        }
        let market_type = request.orders[0].symbol.market_type;
        self.ensure_supported_market(market_type)?;
        let mut order_items = Vec::new();
        let first_symbol = request.orders[0].symbol.clone();
        for order in &mut request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market(order.symbol.market_type)?;
            if order.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "toobit batch_place_orders cannot mix market types".to_string(),
                });
            }
            if market_type == MarketType::Spot
                && order.symbol.exchange_symbol.symbol != first_symbol.exchange_symbol.symbol
            {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "toobit Spot batch_place_orders requires one symbol".to_string(),
                });
            }
            if order.client_order_id.is_none() {
                order.client_order_id = Some(self.generate_client_order_id(market_type));
            }
            order_items.push(Value::Object(
                toobit_order_params(order)?
                    .into_iter()
                    .map(|(key, value)| (key, Value::String(value)))
                    .collect(),
            ));
        }
        let mut params = HashMap::new();
        if market_type == MarketType::Spot {
            params.insert(
                "symbol".to_string(),
                normalize_toobit_symbol(&first_symbol.exchange_symbol.symbol, MarketType::Spot)?,
            );
            params.insert("orders".to_string(), Value::Array(order_items).to_string());
            let value = self
                .send_signed_post(
                    "toobit.batch_place_orders",
                    "/api/v1/spot/batchOrders",
                    &params,
                )
                .await?;
            return Ok(BatchPlaceOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: parse_orders(&self.exchange_id, Some(&first_symbol), market_type, &value)?,
                report: None,
            });
        }
        params.insert("orders".to_string(), Value::Array(order_items).to_string());
        let value = self
            .send_signed_post(
                "toobit.batch_place_orders",
                "/api/v2/futures/batch-orders",
                &params,
            )
            .await?;
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, None, market_type, &value)?,
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
                message: "toobit batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        let market_type = request.cancels[0].symbol.market_type;
        self.ensure_supported_market(market_type)?;
        let mut ids = Vec::new();
        let mut fallback_symbol = request.cancels[0].symbol.clone();
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market(cancel.symbol.market_type)?;
            if cancel.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "toobit batch_cancel_orders cannot mix market types".to_string(),
                });
            }
            if market_type == MarketType::Spot
                && cancel.symbol.exchange_symbol.symbol != fallback_symbol.exchange_symbol.symbol
            {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "toobit Spot batch_cancel_orders requires one symbol".to_string(),
                });
            }
            ids.push(cancel.exchange_order_id.clone().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "toobit batch_cancel_orders requires exchange_order_id".to_string(),
                }
            })?);
            fallback_symbol = cancel.symbol.clone();
        }
        let endpoint = match market_type {
            MarketType::Spot => "/api/v1/spot/cancelOrderByIds",
            MarketType::Perpetual => "/api/v1/futures/cancelOrderByIds",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let mut params = HashMap::new();
        params.insert("ids".to_string(), ids.join(","));
        let value = self
            .send_signed_delete("toobit.batch_cancel_orders", endpoint, &params)
            .await?;
        let orders = parse_orders(
            &self.exchange_id,
            Some(&fallback_symbol),
            market_type,
            &value,
        )
        .unwrap_or_else(|_| {
            request
                .cancels
                .iter()
                .map(|cancel| order_state_from_cancel_request(&self.exchange_id, cancel, &value))
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
        let market_type = request.market_type.unwrap_or_else(|| {
            request
                .symbol
                .as_ref()
                .map(|symbol| symbol.market_type)
                .unwrap_or(MarketType::Spot)
        });
        self.ensure_supported_market(market_type)?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_toobit_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
            );
        } else if market_type == MarketType::Perpetual {
            return Err(ExchangeApiError::InvalidRequest {
                message: "toobit futures cancel_all_orders requires symbol".to_string(),
            });
        }
        let endpoint = match market_type {
            MarketType::Spot => "/api/v1/spot/openOrders",
            MarketType::Perpetual => "/api/v2/futures/batch-orders",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_delete("toobit.cancel_all_orders", endpoint, &params)
            .await?;
        let orders = request
            .symbol
            .as_ref()
            .and_then(|symbol| {
                parse_orders(&self.exchange_id, Some(symbol), market_type, &value).ok()
            })
            .unwrap_or_default();
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: if orders.is_empty() {
                0
            } else {
                orders.len() as u32
            },
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
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_toobit_symbol(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?,
        );
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("orderId".to_string(), order_id.clone());
        } else if let Some(client_order_id) = &request.client_order_id {
            let key = if request.symbol.market_type == MarketType::Perpetual {
                "origClientOrderId"
            } else {
                "clientOrderId"
            };
            params.insert(key.to_string(), client_order_id.clone());
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "toobit query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/api/v1/spot/order",
            MarketType::Perpetual => "/api/v2/futures/order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("toobit.query_order", endpoint, &params)
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
        let market_type = request.market_type.unwrap_or_else(|| {
            request
                .symbol
                .as_ref()
                .map(|symbol| symbol.market_type)
                .unwrap_or(MarketType::Spot)
        });
        self.ensure_supported_market(market_type)?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_toobit_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
            );
        }
        let endpoint = match market_type {
            MarketType::Spot => "/api/v1/spot/openOrders",
            MarketType::Perpetual => "/api/v2/futures/open-orders",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("toobit.get_open_orders", endpoint, &params)
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
        let market_type = request.market_type.unwrap_or_else(|| {
            request
                .symbol
                .as_ref()
                .map(|symbol| symbol.market_type)
                .unwrap_or(MarketType::Spot)
        });
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "toobit.get_recent_fills")?;
        let symbol = request
            .symbol
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "toobit get_recent_fills requires symbol".to_string(),
            })?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_toobit_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(1000).to_string());
        }
        let endpoint = match market_type {
            MarketType::Spot => "/api/v1/account/trades",
            MarketType::Perpetual => "/api/v2/futures/user-trades",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("toobit.get_recent_fills", endpoint, &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                Some(&symbol),
                market_type,
                &value,
            )?,
        })
    }
}

fn toobit_order_params(request: &PlaceOrderRequest) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_toobit_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    params.insert(
        "side".to_string(),
        match request.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
        .to_string(),
    );
    params.insert("type".to_string(), toobit_order_type(request).to_string());
    params.insert("quantity".to_string(), request.quantity.clone());
    if let Some(price) = &request.price {
        params.insert("price".to_string(), price.clone());
    }
    if let Some(client_order_id) = &request.client_order_id {
        params.insert("newClientOrderId".to_string(), client_order_id.clone());
    }
    if let Some(tif) = request.time_in_force {
        params.insert("timeInForce".to_string(), tif_to_toobit(tif).to_string());
    }
    if request.symbol.market_type == MarketType::Perpetual {
        params.insert(
            "positionSide".to_string(),
            match request.position_side.unwrap_or(PositionSide::Net) {
                PositionSide::Long => "LONG",
                PositionSide::Short => "SHORT",
                PositionSide::Net | PositionSide::None => match request.side {
                    OrderSide::Buy => "LONG",
                    OrderSide::Sell => "SHORT",
                },
            }
            .to_string(),
        );
        if request.reduce_only {
            params.insert("reduceOnly".to_string(), "true".to_string());
        }
    }
    Ok(params)
}

fn toobit_cancel_order_params(
    request: &CancelOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_toobit_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    if let Some(order_id) = &request.exchange_order_id {
        params.insert("orderId".to_string(), order_id.clone());
    }
    if let Some(client_order_id) = &request.client_order_id {
        let key = if request.symbol.market_type == MarketType::Perpetual {
            "origClientOrderId"
        } else {
            "clientOrderId"
        };
        params.insert(key.to_string(), client_order_id.clone());
    }
    if request.exchange_order_id.is_none() && request.client_order_id.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "toobit cancel_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(params)
}

fn toobit_order_type(request: &PlaceOrderRequest) -> &'static str {
    match request.order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit | OrderType::IOC | OrderType::FOK => "LIMIT",
        OrderType::PostOnly => "LIMIT_MAKER",
        _ => "LIMIT",
    }
}

fn tif_to_toobit(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "POST_ONLY",
    }
}

fn order_state_from_place_request(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    let exchange_symbol = request.symbol.exchange_symbol.clone();
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol,
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: super::parser::value_as_string(
            super::parser::data_payload(value)
                .get("orderId")
                .or_else(|| super::parser::data_payload(value).get("id")),
        ),
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
        post_only: request.post_only || matches!(request.order_type, OrderType::PostOnly),
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    }
}

fn order_state_from_cancel_request(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> OrderState {
    let data = super::parser::data_payload(value);
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request
            .client_order_id
            .clone()
            .or_else(|| super::parser::value_as_string(data.get("clientOrderId"))),
        exchange_order_id: request
            .exchange_order_id
            .clone()
            .or_else(|| super::parser::value_as_string(data.get("orderId"))),
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

#[allow(dead_code)]
fn synthetic_symbol(
    exchange_id: &rustcta_types::ExchangeId,
    market_type: MarketType,
    exchange_symbol: &str,
) -> SymbolScope {
    symbol_scope_from_exchange_symbol(exchange_id, market_type, exchange_symbol).unwrap_or_else(
        |_| SymbolScope {
            exchange: exchange_id.clone(),
            market_type,
            canonical_symbol: None,
            exchange_symbol: ExchangeSymbol::new(
                exchange_id.clone(),
                market_type,
                exchange_symbol.to_string(),
            )
            .expect("synthetic exchange symbol"),
        },
    )
}
