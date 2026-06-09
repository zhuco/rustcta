use std::collections::HashMap;

use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchItemResult, BatchOperationReport,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderState, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, ReconcilePlan, ReconcileTrigger, RetryReconcilePolicy, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    ExchangeError, ExchangeErrorClass, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
};
use serde_json::Value;

use super::parser::{
    is_okx_derivative_market, normalize_okx_symbol, normalize_okx_symbol_for_market, okx_inst_type,
    okx_td_mode,
};
use super::types::{
    parse_balances, parse_fees, parse_fills, parse_order, parse_orders, parse_positions,
};
use super::OkxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl OkxGatewayAdapter {
    pub(super) async fn place_order_private_rest(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        self.ensure_private_rest(self.profile_operation(
            "okx.place_order",
            "okxus.place_order",
            "myokx.place_order",
        ))?;
        let body = okx_place_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("/api/v5/trade/order", &body)
            .await?;
        let ack = okx_ack_item(&self.exchange_id, &value, "place_order")?;
        let order = order_state_from_place_ack(&self.exchange_id, &request, ack);
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_private_rest(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest(self.profile_operation(
            "okx.place_quote_market_order",
            "okxus.place_quote_market_order",
            "myokx.place_quote_market_order",
        ))?;
        let body = okx_quote_market_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("/api/v5/trade/order", &body)
            .await?;
        let ack = okx_ack_item(&self.exchange_id, &value, "place_quote_market_order")?;
        let order = order_state_from_quote_ack(&self.exchange_id, &request, ack);
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn cancel_order_private_rest(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        self.ensure_private_rest(self.profile_operation(
            "okx.cancel_order",
            "okxus.cancel_order",
            "myokx.cancel_order",
        ))?;
        let body = okx_cancel_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("/api/v5/trade/cancel-order", &body)
            .await?;
        let ack = okx_ack_item(&self.exchange_id, &value, "cancel_order")?;
        let order = order_state_from_cancel_ack(&self.exchange_id, &request, ack);
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn batch_place_orders_private_rest(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest(self.profile_operation(
            "okx.batch_place_orders",
            "okxus.batch_place_orders",
            "myokx.batch_place_orders",
        ))?;
        if request.orders.is_empty() {
            return Ok(BatchPlaceOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                report: None,
            });
        }
        if request.orders.len() > 20 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "okx.batch_place_orders supports at most 20 orders".to_string(),
            });
        }
        let market_type = request.orders[0].symbol.market_type;
        self.ensure_market_type(market_type)?;
        let mut body = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            ensure_exchange_api_schema(order.schema_version)?;
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_market_type(order.symbol.market_type)?;
            if order.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "okx.batch_place_orders requires one market type".to_string(),
                });
            }
            body.push(okx_place_order_body(order)?);
        }
        let value = self
            .rest
            .send_signed_post("/api/v5/trade/batch-orders", &Value::Array(body))
            .await?;
        let (orders, report) = parse_okx_batch_place_response(&self.exchange_id, &request, &value)?;
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            report: Some(report),
        })
    }

    pub(super) async fn batch_cancel_orders_private_rest(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest(self.profile_operation(
            "okx.batch_cancel_orders",
            "okxus.batch_cancel_orders",
            "myokx.batch_cancel_orders",
        ))?;
        if request.cancels.is_empty() {
            return Ok(BatchCancelOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                cancelled_count: 0,
                report: None,
            });
        }
        if request.cancels.len() > 20 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "okx.batch_cancel_orders supports at most 20 cancels".to_string(),
            });
        }
        let market_type = request.cancels[0].symbol.market_type;
        self.ensure_market_type(market_type)?;
        let mut body = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            ensure_exchange_api_schema(cancel.schema_version)?;
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_market_type(cancel.symbol.market_type)?;
            if cancel.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "okx.batch_cancel_orders requires one market type".to_string(),
                });
            }
            body.push(okx_cancel_order_body(cancel)?);
        }
        let value = self
            .rest
            .send_signed_post("/api/v5/trade/cancel-batch-orders", &Value::Array(body))
            .await?;
        let (orders, report) =
            parse_okx_batch_cancel_response(&self.exchange_id, &request, &value)?;
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: Some(report),
        })
    }

    pub(super) async fn cancel_all_orders_private_rest(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_market_type(request.market_type)?;
        self.ensure_private_rest(self.profile_operation(
            "okx.cancel_all_orders",
            "okxus.cancel_all_orders",
            "myokx.cancel_all_orders",
        ))?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "okx.cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_market_type(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "instType".to_string(),
            okx_inst_type(symbol.market_type)?.to_string(),
        );
        params.insert(
            "instId".to_string(),
            normalize_okx_symbol_for_market(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        let pending = self
            .rest
            .send_signed_get("/api/v5/trade/orders-pending", &params)
            .await?;
        let cancel_body = okx_cancel_all_body(symbol, &pending)?;
        if cancel_body.as_array().is_some_and(Vec::is_empty) {
            return Ok(CancelAllOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                cancelled_count: 0,
            });
        }
        let value = self
            .rest
            .send_signed_post("/api/v5/trade/cancel-batch-orders", &cancel_body)
            .await?;
        let orders = order_states_from_cancel_all_ack(&self.exchange_id, symbol, &value)?;
        let cancelled_count = orders.len() as u32;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
        })
    }

    pub(super) async fn amend_order_private_rest(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        self.ensure_private_rest(self.profile_operation(
            "okx.amend_order",
            "okxus.amend_order",
            "myokx.amend_order",
        ))?;
        let body = okx_amend_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("/api/v5/trade/amend-order", &body)
            .await?;
        let ack = okx_ack_item(&self.exchange_id, &value, "amend_order")?;
        let order = order_state_from_amend_ack(&self.exchange_id, &request, ack);
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn get_balances_private_rest(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_market_type(request.market_type)?;
        self.ensure_private_rest(self.profile_operation(
            "okx.get_balances",
            "okxus.get_balances",
            "myokx.get_balances",
        ))?;
        let value = self
            .rest
            .send_signed_get("/api/v5/account/balance", &HashMap::new())
            .await?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "okx.get_balances requires tenant_id in request context".to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "okx.get_balances requires account_id in request context".to_string(),
                })?;
        let requested_assets = request
            .assets
            .iter()
            .map(|asset| asset.to_ascii_uppercase())
            .collect::<Vec<_>>();
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        let mut balances = parse_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            market_type,
            &value,
        )?;
        if !requested_assets.is_empty() {
            for balance in &mut balances {
                balance
                    .balances
                    .retain(|asset| requested_assets.contains(&asset.asset));
            }
            balances.retain(|balance| !balance.balances.is_empty());
        }
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances,
        })
    }

    pub(super) async fn get_positions_private_rest(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request
            .market_type
            .or_else(|| request.symbols.first().map(|symbol| symbol.market_type))
            .unwrap_or(MarketType::Perpetual);
        self.ensure_market_type(market_type)?;
        if market_type == MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: self.profile_operation(
                    "okx.spot_positions",
                    "okxus.spot_positions",
                    "myokx.spot_positions",
                ),
            });
        }
        self.ensure_private_rest(self.profile_operation(
            "okx.get_positions",
            "okxus.get_positions",
            "myokx.get_positions",
        ))?;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange_id)?;
            self.ensure_market_type(symbol.market_type)?;
            if symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "okx.get_positions symbols must match request market_type".to_string(),
                });
            }
        }
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "okx.get_positions requires tenant_id in request context".to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "okx.get_positions requires account_id in request context".to_string(),
                })?;
        let mut params = HashMap::new();
        params.insert(
            "instType".to_string(),
            okx_inst_type(market_type)?.to_string(),
        );
        if request.symbols.len() == 1 {
            params.insert(
                "instId".to_string(),
                normalize_okx_symbol_for_market(&request.symbols[0].symbol, market_type)?,
            );
        }
        let value = self
            .rest
            .send_signed_get("/api/v5/account/positions", &params)
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions: parse_positions(
                &self.exchange_id,
                tenant_id,
                account_id,
                market_type,
                &request.symbols,
                &value,
            )?,
        })
    }

    pub(super) async fn get_fees_private_rest(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_private_rest(self.profile_operation(
            "okx.get_fees",
            "okxus.get_fees",
            "myokx.get_fees",
        ))?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "okx.get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_market_type(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "instType".to_string(),
                okx_inst_type(symbol.market_type)?.to_string(),
            );
            params.insert(
                "instId".to_string(),
                normalize_okx_symbol_for_market(
                    &symbol.exchange_symbol.symbol,
                    symbol.market_type,
                )?,
            );
            let value = self
                .rest
                .send_signed_get("/api/v5/account/trade-fee", &params)
                .await?;
            fees.extend(parse_fees(&self.exchange_id, Some(symbol), &value)?);
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees,
        })
    }

    pub(super) async fn query_order_private_rest(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        self.ensure_private_rest(self.profile_operation(
            "okx.query_order",
            "okxus.query_order",
            "myokx.query_order",
        ))?;
        let mut params = HashMap::new();
        params.insert(
            "instId".to_string(),
            normalize_okx_symbol_for_market(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?,
        );
        match (&request.exchange_order_id, &request.client_order_id) {
            (Some(exchange_order_id), _) => {
                params.insert("ordId".to_string(), exchange_order_id.clone());
            }
            (None, Some(client_order_id)) => {
                params.insert("clOrdId".to_string(), client_order_id.clone());
            }
            (None, None) => {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "okx.query_order requires exchange_order_id or client_order_id"
                        .to_string(),
                });
            }
        }
        let value = self
            .rest
            .send_signed_get("/api/v5/trade/order", &params)
            .await?;
        let order = parse_order(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order,
        })
    }

    pub(super) async fn get_open_orders_private_rest(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_market_type(request.market_type)?;
        self.ensure_private_rest(self.profile_operation(
            "okx.get_open_orders",
            "okxus.get_open_orders",
            "myokx.get_open_orders",
        ))?;
        let mut params = HashMap::new();
        let market_type = request
            .market_type
            .or_else(|| request.symbol.as_ref().map(|symbol| symbol.market_type))
            .unwrap_or(MarketType::Spot);
        params.insert(
            "instType".to_string(),
            okx_inst_type(market_type)?.to_string(),
        );
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_market_type(symbol.market_type)?;
            if symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message:
                        "okx.get_open_orders symbol market_type conflicts with request market_type"
                            .to_string(),
                });
            }
            params.insert(
                "instId".to_string(),
                normalize_okx_symbol_for_market(
                    &symbol.exchange_symbol.symbol,
                    symbol.market_type,
                )?,
            );
        }
        let value = self
            .rest
            .send_signed_get("/api/v5/trade/orders-pending", &params)
            .await?;
        let orders = parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
        })
    }

    pub(super) async fn get_recent_fills_private_rest(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_market_type(request.market_type)?;
        self.ensure_private_rest(self.profile_operation(
            "okx.get_recent_fills",
            "okxus.get_recent_fills",
            "myokx.get_recent_fills",
        ))?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "okx.get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_market_type(symbol.market_type)?;
        if let Some(request_market_type) = request.market_type {
            if request_market_type != symbol.market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message:
                        "okx.get_recent_fills symbol market_type conflicts with request market_type"
                            .to_string(),
                });
            }
        }
        let mut params = HashMap::new();
        params.insert(
            "instType".to_string(),
            okx_inst_type(symbol.market_type)?.to_string(),
        );
        params.insert(
            "instId".to_string(),
            normalize_okx_symbol_for_market(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        if let Some(exchange_order_id) = &request.exchange_order_id {
            params.insert("ordId".to_string(), exchange_order_id.clone());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "begin".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert("end".to_string(), end_time.timestamp_millis().to_string());
        }
        params.insert(
            "limit".to_string(),
            request.limit.unwrap_or(100).clamp(1, 100).to_string(),
        );
        let value = self
            .rest
            .send_signed_get("/api/v5/trade/fills-history", &params)
            .await?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "okx.get_recent_fills requires tenant_id in request context"
                        .to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "okx.get_recent_fills requires account_id in request context"
                        .to_string(),
                })?;
        let fills = parse_fills(
            &self.exchange_id,
            tenant_id,
            account_id,
            Some(symbol),
            &value,
        )?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn okx_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        Value::String(normalize_okx_symbol_for_market(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?),
    );
    body.insert(
        "tdMode".to_string(),
        Value::String(okx_td_mode(request.symbol.market_type).to_string()),
    );
    body.insert(
        "side".to_string(),
        Value::String(okx_side(request.side).to_string()),
    );
    body.insert(
        "ordType".to_string(),
        Value::String(
            okx_order_type(request.order_type, request.time_in_force, request.post_only)
                .to_string(),
        ),
    );
    if request.order_type == OrderType::Market {
        body.insert(
            "sz".to_string(),
            Value::String(non_empty("quantity", &request.quantity)?),
        );
        if request.symbol.market_type == MarketType::Spot {
            body.insert(
                "tgtCcy".to_string(),
                Value::String(okx_market_target(&request.symbol)?),
            );
        }
    } else {
        body.insert(
            "sz".to_string(),
            Value::String(non_empty("quantity", &request.quantity)?),
        );
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "okx limit-style order requires price".to_string(),
            })?;
        body.insert("px".to_string(), Value::String(non_empty("price", price)?));
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body.insert(
            "clOrdId".to_string(),
            Value::String(non_empty("client_order_id", client_order_id)?),
        );
    }
    if request.reduce_only && !is_okx_derivative_market(request.symbol.market_type) {
        return Err(ExchangeApiError::InvalidRequest {
            message: "okx reduce_only is only supported for derivative orders".to_string(),
        });
    }
    if request.reduce_only {
        body.insert("reduceOnly".to_string(), Value::Bool(true));
    }
    if is_okx_derivative_market(request.symbol.market_type) {
        if let Some(pos_side) = request.position_side.and_then(okx_position_side) {
            body.insert("posSide".to_string(), Value::String(pos_side.to_string()));
        }
    }
    Ok(Value::Object(body))
}

fn okx_quote_market_order_body(request: &QuoteMarketOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        Value::String(normalize_okx_symbol_for_market(
            &request.symbol.exchange_symbol.symbol,
            MarketType::Spot,
        )?),
    );
    body.insert("tdMode".to_string(), Value::String("cash".to_string()));
    body.insert(
        "side".to_string(),
        Value::String(okx_side(request.side).to_string()),
    );
    body.insert("ordType".to_string(), Value::String("market".to_string()));
    body.insert(
        "sz".to_string(),
        Value::String(non_empty("quote_quantity", &request.quote_quantity)?),
    );
    body.insert("tgtCcy".to_string(), Value::String("quote_ccy".to_string()));
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body.insert(
            "clOrdId".to_string(),
            Value::String(non_empty("client_order_id", client_order_id)?),
        );
    }
    Ok(Value::Object(body))
}

fn okx_cancel_order_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        Value::String(normalize_okx_symbol_for_market(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?),
    );
    insert_okx_order_identity(
        &mut body,
        request.exchange_order_id.as_deref(),
        request.client_order_id.as_deref(),
        "cancel_order",
    )?;
    Ok(Value::Object(body))
}

fn okx_amend_order_body(request: &AmendOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "instId".to_string(),
        Value::String(normalize_okx_symbol_for_market(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?),
    );
    insert_okx_order_identity(
        &mut body,
        request.exchange_order_id.as_deref(),
        request.client_order_id.as_deref(),
        "amend_order",
    )?;
    body.insert(
        "newSz".to_string(),
        Value::String(non_empty("new_quantity", &request.new_quantity)?),
    );
    if let Some(new_client_order_id) = request.new_client_order_id.as_deref() {
        body.insert(
            "newClOrdId".to_string(),
            Value::String(non_empty("new_client_order_id", new_client_order_id)?),
        );
    }
    Ok(Value::Object(body))
}

fn okx_cancel_all_body(
    symbol: &rustcta_exchange_api::SymbolScope,
    pending: &Value,
) -> ExchangeApiResult<Value> {
    let orders = pending
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "okx pending orders response is not an array".to_string(),
        })?;
    let mut rows = Vec::new();
    for order in orders {
        let inst_id = order
            .get("instId")
            .and_then(Value::as_str)
            .unwrap_or(symbol.exchange_symbol.symbol.as_str());
        let mut row = serde_json::Map::new();
        row.insert(
            "instId".to_string(),
            Value::String(normalize_okx_symbol_for_market(
                inst_id,
                symbol.market_type,
            )?),
        );
        if let Some(ord_id) = value_text(order.get("ordId")) {
            row.insert("ordId".to_string(), Value::String(ord_id));
        } else if let Some(client_order_id) = value_text(order.get("clOrdId")) {
            row.insert("clOrdId".to_string(), Value::String(client_order_id));
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("okx pending order missing ordId/clOrdId: {order}"),
            });
        }
        rows.push(Value::Object(row));
    }
    Ok(Value::Array(rows))
}

fn order_states_from_cancel_all_ack(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let items = value
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "okx cancel batch response is not an array".to_string(),
        })?;
    Ok(items
        .iter()
        .map(|ack| order_state_from_cancel_ack_fields(exchange_id, symbol, ack))
        .collect())
}

fn okx_ack_item<'a>(
    exchange_id: &rustcta_types::ExchangeId,
    value: &'a Value,
    operation: &str,
) -> ExchangeApiResult<&'a Value> {
    let item = value
        .as_array()
        .and_then(|items| items.first())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("okx {operation} response missing ack item"),
        })?;
    let code = item.get("sCode").and_then(Value::as_str).unwrap_or("0");
    if code != "0" {
        return Err(ExchangeApiError::Exchange(
            rustcta_types::ExchangeError::new(
                exchange_id.clone(),
                rustcta_types::ExchangeErrorClass::OrderRejected,
                item.get("sMsg")
                    .and_then(Value::as_str)
                    .unwrap_or("OKX order mutation rejected"),
                Utc::now(),
            ),
        ));
    }
    Ok(item)
}

fn parse_okx_batch_place_response(
    exchange_id: &rustcta_types::ExchangeId,
    request: &BatchPlaceOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<(Vec<OrderState>, BatchOperationReport)> {
    let rows = okx_batch_response_rows(exchange_id, "batch place response is not an array", value)?;
    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(request.orders.len());
    for (index, order_request) in request.orders.iter().enumerate() {
        let Some(row) = rows.get(index) else {
            let error =
                okx_missing_batch_item_error(exchange_id, "missing batch place response item");
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
                    "OKX did not return a batch place result for this request item",
                )),
            ));
            continue;
        };
        if let Some(error) = okx_batch_item_error(
            exchange_id,
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
                    "OKX batch place item failed and requires order readback",
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

fn parse_okx_batch_cancel_response(
    exchange_id: &rustcta_types::ExchangeId,
    request: &BatchCancelOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<(Vec<OrderState>, BatchOperationReport)> {
    let rows =
        okx_batch_response_rows(exchange_id, "batch cancel response is not an array", value)?;
    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(request.cancels.len());
    for (index, cancel_request) in request.cancels.iter().enumerate() {
        let Some(row) = rows.get(index) else {
            let error =
                okx_missing_batch_item_error(exchange_id, "missing batch cancel response item");
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
                    "OKX did not return a batch cancel result for this request item",
                )),
            ));
            continue;
        };
        if let Some(error) = okx_batch_item_error(
            exchange_id,
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
                    "OKX batch cancel item failed and requires order readback",
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

fn okx_batch_response_rows<'a>(
    exchange_id: &rustcta_types::ExchangeId,
    message: &str,
    value: &'a Value,
) -> ExchangeApiResult<&'a [Value]> {
    value.as_array().map(Vec::as_slice).ok_or_else(|| {
        ExchangeApiError::Exchange(ExchangeError::new(
            exchange_id.clone(),
            ExchangeErrorClass::Decode,
            format!("{message}: {value}"),
            Utc::now(),
        ))
    })
}

fn okx_batch_item_error(
    exchange_id: &rustcta_types::ExchangeId,
    value: &Value,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
) -> Option<ExchangeError> {
    let code = value_text(value.get("sCode")).unwrap_or_else(|| "0".to_string());
    if code == "0" {
        return None;
    }
    let message = value
        .get("sMsg")
        .or_else(|| value.get("msg"))
        .and_then(Value::as_str)
        .unwrap_or("OKX batch item failed");
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        ExchangeErrorClass::OrderRejected,
        message,
        Utc::now(),
    );
    error.code = Some(code);
    error.client_order_id = client_order_id;
    error.order_id = exchange_order_id;
    error.raw = Some(value.clone());
    Some(error)
}

fn okx_missing_batch_item_error(
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
    let ord_type = okx_order_type(request.order_type, request.time_in_force, request.post_only);
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clOrdId")).or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("ordId")),
        side: request.side,
        position_side: request.position_side.or(Some(PositionSide::None)),
        order_type: request.order_type,
        time_in_force: okx_time_in_force(ord_type),
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: ord_type == "post_only",
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

fn order_state_from_quote_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &QuoteMarketOrderRequest,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clOrdId")).or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("ordId")),
        side: request.side,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Market,
        time_in_force: None,
        status: OrderStatus::New,
        quantity: request.quote_quantity.clone(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    ack: &Value,
) -> OrderState {
    order_state_from_cancel_ack_fields(exchange_id, &request.symbol, ack)
}

fn order_state_from_cancel_ack_fields(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clOrdId")),
        exchange_order_id: value_text(ack.get("ordId")),
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
        updated_at: Utc::now(),
    }
}

fn order_state_from_amend_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &AmendOrderRequest,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clOrdId"))
            .or_else(|| request.new_client_order_id.clone())
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("ordId"))
            .or_else(|| request.exchange_order_id.clone()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
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

fn insert_okx_order_identity(
    body: &mut serde_json::Map<String, Value>,
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
    operation: &str,
) -> ExchangeApiResult<()> {
    if let Some(order_id) = exchange_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        body.insert("ordId".to_string(), Value::String(order_id.to_string()));
    }
    if let Some(client_id) = client_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        body.insert("clOrdId".to_string(), Value::String(client_id.to_string()));
    }
    if !body.contains_key("ordId") && !body.contains_key("clOrdId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("okx {operation} requires exchange_order_id or client_order_id"),
        });
    }
    Ok(())
}

fn okx_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn okx_order_type(
    order_type: OrderType,
    tif: Option<TimeInForce>,
    post_only: bool,
) -> &'static str {
    if post_only
        || matches!(order_type, OrderType::PostOnly)
        || matches!(tif, Some(TimeInForce::GTX))
    {
        return "post_only";
    }
    match tif {
        Some(TimeInForce::IOC) => "ioc",
        Some(TimeInForce::FOK) => "fok",
        _ => match order_type {
            OrderType::Market => "market",
            OrderType::IOC => "ioc",
            OrderType::FOK => "fok",
            _ => "limit",
        },
    }
}

fn okx_time_in_force(ord_type: &str) -> Option<TimeInForce> {
    match ord_type {
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        "post_only" => Some(TimeInForce::GTX),
        "limit" => Some(TimeInForce::GTC),
        _ => None,
    }
}

fn okx_position_side(position_side: PositionSide) -> Option<&'static str> {
    match position_side {
        PositionSide::Long => Some("long"),
        PositionSide::Short => Some("short"),
        PositionSide::Net => Some("net"),
        PositionSide::None => None,
    }
}

fn okx_market_target(symbol: &rustcta_exchange_api::SymbolScope) -> ExchangeApiResult<String> {
    let normalized = normalize_okx_symbol(&symbol.exchange_symbol.symbol)?;
    let (base, _) = normalized
        .split_once('-')
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("OKX Spot symbol missing dash: {normalized}"),
        })?;
    Ok(base.to_ascii_lowercase())
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("okx {field} must not be empty"),
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
