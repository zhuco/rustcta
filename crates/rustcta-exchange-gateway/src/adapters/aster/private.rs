use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchItemResult, BatchOperationReport,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, MarginMode, OpenOrdersRequest,
    OpenOrdersResponse, PlaceOrderRequest, PlaceOrderResponse, PositionMode, PositionsRequest,
    PositionsResponse, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, SetLeverageRequest, SetLeverageResponse, SetPositionModeRequest,
    SetPositionModeResponse, SymbolAccountConfig, SymbolAccountConfigRequest,
    SymbolAccountConfigResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeError, ExchangeErrorClass, MarketType, OrderSide, OrderType};
use serde_json::Value;

use super::parser::normalize_aster_symbol;
use super::private_parser::{
    parse_account_balances, parse_aster_cancel_all_orders, parse_fee_snapshots, parse_open_orders,
    parse_order_state, parse_positions, parse_recent_fills,
};
use super::AsterGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl AsterGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(request.symbol.market_type)?;
        let mut params = aster_place_order_params(&request)?;
        params.insert(
            "symbol".to_string(),
            normalize_aster_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_signed_post("aster.place_order", "/fapi/v3/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
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
        self.ensure_perpetual(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_aster_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        insert_order_identifier(
            &mut params,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "cancel_order",
        )?;
        let value = self
            .send_signed_delete("aster.cancel_order", "/fapi/v3/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "aster cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_aster_symbol(&symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_signed_delete("aster.cancel_all_orders", "/fapi/v3/allOpenOrders", &params)
            .await?;
        let orders = parse_aster_cancel_all_orders(&self.exchange_id, symbol, &value)?;
        let cancelled_count = orders.len() as u32;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(request.symbol.market_type)?;
        if request.new_client_order_id.is_some() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "aster amend_order does not support changing client_order_id".to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_aster_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        insert_order_identifier(
            &mut params,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "amend_order",
        )?;
        insert_non_empty(&mut params, "quantity", &request.new_quantity)?;
        let value = self
            .send_signed_put("aster.amend_order", "/fapi/v3/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
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
                message: "aster batch_place_orders requires at least one order".to_string(),
            });
        }
        if request.orders.len() > 5 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "aster batch_place_orders supports at most 5 orders".to_string(),
            });
        }
        let mut batch_orders = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            ensure_exchange_api_schema(order.schema_version)?;
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_perpetual(order.symbol.market_type)?;
            let mut params = aster_place_order_params(order)?;
            params.insert(
                "symbol".to_string(),
                normalize_aster_symbol(&order.symbol.exchange_symbol.symbol)?,
            );
            batch_orders.push(hashmap_to_json_object(params));
        }
        let mut params = HashMap::new();
        params.insert(
            "batchOrders".to_string(),
            serde_json::to_string(&batch_orders).map_err(|error| {
                ExchangeApiError::InvalidRequest {
                    message: format!("aster batch_place_orders serialize failed: {error}"),
                }
            })?,
        );
        let value = self
            .send_signed_post("aster.batch_place_orders", "/fapi/v3/batchOrders", &params)
            .await?;
        let (orders, report) =
            parse_aster_batch_place_response(&self.exchange_id, &request, &value);
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
                message: "aster batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        if request.cancels.len() > 10 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "aster batch_cancel_orders supports at most 10 cancels".to_string(),
            });
        }
        let first_symbol = &request.cancels[0].symbol;
        self.ensure_exchange(&first_symbol.exchange)?;
        self.ensure_perpetual(first_symbol.market_type)?;
        let mut order_ids = Vec::new();
        let mut client_order_ids = Vec::new();
        for cancel in &request.cancels {
            ensure_exchange_api_schema(cancel.schema_version)?;
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_perpetual(cancel.symbol.market_type)?;
            if cancel.symbol.exchange_symbol.symbol != first_symbol.exchange_symbol.symbol {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "aster batch_cancel_orders requires one symbol".to_string(),
                });
            }
            match (
                cancel.exchange_order_id.as_deref(),
                cancel.client_order_id.as_deref(),
            ) {
                (Some(order_id), _) if !order_id.trim().is_empty() => {
                    order_ids.push(order_id.trim().to_string())
                }
                (_, Some(client_order_id)) if !client_order_id.trim().is_empty() => {
                    client_order_ids.push(client_order_id.trim().to_string())
                }
                _ => return Err(ExchangeApiError::InvalidRequest {
                    message:
                        "aster batch_cancel_orders requires exchange_order_id or client_order_id"
                            .to_string(),
                }),
            }
        }
        if !order_ids.is_empty() && !client_order_ids.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message:
                    "aster batch_cancel_orders cannot mix orderIdList and origClientOrderIdList"
                        .to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_aster_symbol(&first_symbol.exchange_symbol.symbol)?,
        );
        if !order_ids.is_empty() {
            params.insert(
                "orderIdList".to_string(),
                serde_json::to_string(&order_ids).map_err(|error| {
                    ExchangeApiError::InvalidRequest {
                        message: format!(
                            "aster batch_cancel_orders serialize order ids failed: {error}"
                        ),
                    }
                })?,
            );
        } else {
            params.insert(
                "origClientOrderIdList".to_string(),
                serde_json::to_string(&client_order_ids).map_err(|error| {
                    ExchangeApiError::InvalidRequest {
                        message: format!(
                            "aster batch_cancel_orders serialize client order ids failed: {error}"
                        ),
                    }
                })?,
            );
        }
        let value = self
            .send_signed_delete("aster.batch_cancel_orders", "/fapi/v3/batchOrders", &params)
            .await?;
        let (orders, report) =
            parse_aster_batch_cancel_response(&self.exchange_id, &request, &value);
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: Some(report),
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
            .send_signed_get("aster.get_balances", "/fapi/v3/balance", &HashMap::new())
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
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "aster adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        if request.symbols.len() == 1 {
            params.insert(
                "symbol".to_string(),
                normalize_aster_symbol(&request.symbols[0].symbol)?,
            );
        }
        let value = self
            .send_signed_get("aster.get_positions", "/fapi/v3/positionRisk", &params)
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
                message: "aster get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_perpetual(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_aster_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .send_signed_get("aster.get_fees", "/fapi/v3/commissionRate", &params)
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
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_aster_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("origClientOrderId".to_string(), client_order_id.to_string());
        }
        if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "aster query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .send_signed_get("aster.query_order", "/fapi/v3/order", &params)
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
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_perpetual(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_aster_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get("aster.get_open_orders", "/fapi/v3/openOrders", &params)
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
                message: "aster get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_aster_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(from_trade_id) = request.from_trade_id.as_deref() {
            params.insert("fromId".to_string(), from_trade_id.to_string());
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
        } else {
            params.insert("limit".to_string(), "1000".to_string());
        }
        let value = self
            .send_signed_get("aster.get_recent_fills", "/fapi/v3/userTrades", &params)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }

    pub(super) async fn get_symbol_account_config_impl(
        &self,
        request: SymbolAccountConfigRequest,
    ) -> ExchangeApiResult<SymbolAccountConfigResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(request.symbol.market_type)?;
        let value = self
            .send_signed_get(
                "aster.get_position_mode",
                "/fapi/v3/positionSide/dual",
                &HashMap::new(),
            )
            .await?;
        Ok(SymbolAccountConfigResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            config: SymbolAccountConfig {
                symbol: request.symbol,
                position_mode: Some(parse_aster_position_mode(&value)?),
                margin_mode: Some(MarginMode::Unknown),
                leverage: None,
                max_leverage: None,
                updated_at: chrono::Utc::now(),
            },
        })
    }

    pub(super) async fn set_leverage_impl(
        &self,
        request: SetLeverageRequest,
    ) -> ExchangeApiResult<SetLeverageResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_perpetual(request.symbol.market_type)?;
        if request.leverage == 0 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "aster set_leverage requires leverage greater than zero".to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_aster_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert("leverage".to_string(), request.leverage.to_string());
        let value = self
            .send_signed_post("aster.set_leverage", "/fapi/v3/leverage", &params)
            .await?;
        let accepted_leverage = value
            .get("leverage")
            .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(request.leverage);
        Ok(SetLeverageResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            symbol: request.symbol,
            leverage: accepted_leverage,
            accepted: true,
            message: None,
        })
    }

    pub(super) async fn set_position_mode_impl(
        &self,
        request: SetPositionModeRequest,
    ) -> ExchangeApiResult<SetPositionModeResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let mut params = HashMap::new();
        params.insert(
            "dualSidePosition".to_string(),
            if request.mode.is_hedge() {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        self.send_signed_post(
            "aster.set_position_mode",
            "/fapi/v3/positionSide/dual",
            &params,
        )
        .await?;
        Ok(SetPositionModeResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            mode: request.mode,
            accepted: true,
            message: None,
        })
    }
}

fn aster_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert("side".to_string(), aster_side(request.side).to_string());
    params.insert(
        "type".to_string(),
        aster_order_type(request.order_type, request.post_only).to_string(),
    );
    if request.order_type == OrderType::Market {
        if let Some(quote_quantity) = request.quote_quantity.as_deref() {
            insert_non_empty(&mut params, "quoteOrderQty", quote_quantity)?;
        } else {
            insert_non_empty(&mut params, "quantity", &request.quantity)?;
        }
    } else {
        insert_non_empty(&mut params, "quantity", &request.quantity)?;
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "aster limit-style order requires price".to_string(),
            })?;
        insert_non_empty(&mut params, "price", price)?;
        let time_in_force = if request.post_only {
            "GTX"
        } else {
            aster_time_in_force(request.order_type, request.time_in_force)
        };
        params.insert("timeInForce".to_string(), time_in_force.to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        insert_non_empty(&mut params, "newClientOrderId", client_order_id)?;
    }
    if request.reduce_only {
        params.insert("reduceOnly".to_string(), "true".to_string());
    }
    if let Some(position_side) = request.position_side {
        params.insert(
            "positionSide".to_string(),
            aster_position_side(position_side).to_string(),
        );
    }
    Ok(params)
}

fn hashmap_to_json_object(params: HashMap<String, String>) -> Value {
    Value::Object(
        params
            .into_iter()
            .map(|(key, value)| (key, Value::String(value)))
            .collect(),
    )
}

fn parse_aster_batch_place_response(
    exchange_id: &rustcta_types::ExchangeId,
    request: &BatchPlaceOrdersRequest,
    value: &Value,
) -> (Vec<rustcta_exchange_api::OrderState>, BatchOperationReport) {
    let rows = value.as_array().map(Vec::as_slice).unwrap_or(&[]);
    let mut orders = Vec::new();
    let mut results = Vec::new();
    for (index, order_request) in request.orders.iter().enumerate() {
        let row = rows.get(index).unwrap_or(&Value::Null);
        if let Some(error) = aster_batch_item_error(exchange_id, row) {
            results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                None,
                error,
                None,
            ));
            continue;
        }
        match parse_order_state(exchange_id, Some(&order_request.symbol), row) {
            Ok(order) => {
                orders.push(order.clone());
                results.push(BatchItemResult::success(index, order));
            }
            Err(error) => results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                None,
                aster_decode_batch_error(exchange_id, row, error),
                None,
            )),
        }
    }
    (
        orders,
        BatchOperationReport {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            total_items: request.orders.len(),
            results,
        },
    )
}

fn parse_aster_batch_cancel_response(
    exchange_id: &rustcta_types::ExchangeId,
    request: &BatchCancelOrdersRequest,
    value: &Value,
) -> (Vec<rustcta_exchange_api::OrderState>, BatchOperationReport) {
    let rows = value.as_array().map(Vec::as_slice).unwrap_or(&[]);
    let mut orders = Vec::new();
    let mut results = Vec::new();
    for (index, cancel_request) in request.cancels.iter().enumerate() {
        let row = rows.get(index).unwrap_or(&Value::Null);
        if let Some(error) = aster_batch_item_error(exchange_id, row) {
            results.push(BatchItemResult::failed(
                index,
                cancel_request.client_order_id.clone(),
                cancel_request.exchange_order_id.clone(),
                error,
                None,
            ));
            continue;
        }
        match parse_order_state(exchange_id, Some(&cancel_request.symbol), row) {
            Ok(order) => {
                orders.push(order.clone());
                results.push(BatchItemResult::success(index, order));
            }
            Err(error) => results.push(BatchItemResult::failed(
                index,
                cancel_request.client_order_id.clone(),
                cancel_request.exchange_order_id.clone(),
                aster_decode_batch_error(exchange_id, row, error),
                None,
            )),
        }
    }
    (
        orders,
        BatchOperationReport {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            total_items: request.cancels.len(),
            results,
        },
    )
}

fn aster_batch_item_error(
    exchange_id: &rustcta_types::ExchangeId,
    value: &Value,
) -> Option<ExchangeError> {
    let code = value.get("code").and_then(|value| {
        value
            .as_i64()
            .map(|code| code.to_string())
            .or_else(|| value.as_str().map(str::to_string))
    })?;
    if code == "0" {
        return None;
    }
    let message = value
        .get("msg")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("Aster batch item failed");
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::OrderRejected,
        message,
        chrono::Utc::now(),
    );
    error.code = Some(code);
    error.raw = Some(value.clone());
    Some(error)
}

fn aster_decode_batch_error(
    exchange_id: &rustcta_types::ExchangeId,
    value: &Value,
    error: ExchangeApiError,
) -> ExchangeError {
    let mut exchange_error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::Decode,
        format!("Aster batch item could not be decoded: {error}"),
        chrono::Utc::now(),
    );
    exchange_error.raw = Some(value.clone());
    exchange_error
}

fn insert_order_identifier(
    params: &mut HashMap<String, String>,
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
    operation: &str,
) -> ExchangeApiResult<()> {
    if let Some(order_id) = exchange_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_id) = client_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        params.insert("origClientOrderId".to_string(), client_id.to_string());
    }
    if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("aster {operation} requires exchange_order_id or client_order_id"),
        });
    }
    Ok(())
}

fn insert_non_empty(
    params: &mut HashMap<String, String>,
    key: &str,
    value: &str,
) -> ExchangeApiResult<()> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("aster parameter {key} must not be empty"),
        });
    }
    params.insert(key.to_string(), value.to_string());
    Ok(())
}

fn aster_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn aster_order_type(order_type: OrderType, post_only: bool) -> &'static str {
    match order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit if post_only => "LIMIT",
        OrderType::Limit => "LIMIT",
        OrderType::PostOnly => "LIMIT",
        OrderType::IOC | OrderType::FOK => "LIMIT",
        _ => "LIMIT",
    }
}

fn aster_time_in_force(order_type: OrderType, tif: Option<TimeInForce>) -> &'static str {
    if order_type == OrderType::PostOnly {
        return "GTX";
    }
    if order_type == OrderType::IOC {
        return "IOC";
    }
    if order_type == OrderType::FOK {
        return "FOK";
    }
    tif.map(aster_time_in_force_from_tif).unwrap_or("GTC")
}

fn aster_time_in_force_from_tif(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTX",
    }
}

fn aster_position_side(side: rustcta_types::PositionSide) -> &'static str {
    match side {
        rustcta_types::PositionSide::Long => "LONG",
        rustcta_types::PositionSide::Short => "SHORT",
        rustcta_types::PositionSide::Net | rustcta_types::PositionSide::None => "BOTH",
    }
}

pub(super) fn parse_aster_position_mode(
    value: &serde_json::Value,
) -> ExchangeApiResult<PositionMode> {
    let dual = value
        .get("dualSidePosition")
        .or_else(|| value.get("dual"))
        .and_then(|value| {
            value
                .as_bool()
                .or_else(|| value.as_str().map(|text| text.eq_ignore_ascii_case("true")))
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("aster positionSide/dual response missing dualSidePosition: {value}"),
        })?;
    Ok(if dual {
        PositionMode::Hedge
    } else {
        PositionMode::OneWay
    })
}
