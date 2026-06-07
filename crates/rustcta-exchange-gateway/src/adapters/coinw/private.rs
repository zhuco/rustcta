use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderListRequest, OrderListResponse, OrderState,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::{normalize_coinw_perp_base, normalize_coinw_spot_symbol};
use super::private_parser::{
    order_state_from_cancel_ack, order_state_from_futures_place_ack, order_state_from_quote_ack,
    order_state_from_spot_place_ack, parse_balances, parse_fee_snapshots, parse_fills, parse_order,
    parse_orders, parse_positions,
};
use super::CoinwGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

const COINW_PERP_CANCEL_POS_TYPES: [&str; 5] = ["plan", "execute", "PostOnly", "IOC", "FOK"];

impl CoinwGatewayAdapter {
    pub(super) async fn get_balances_private_rest(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("coinw.get_balances")?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "coinw.get_balances")?;
        let value = match market_type {
            MarketType::Spot => {
                self.rest
                    .send_signed_spot_post(
                        "/api/v1/private?command=returnCompleteBalances",
                        &HashMap::new(),
                    )
                    .await?
            }
            MarketType::Perpetual => {
                self.rest
                    .send_signed_futures_get("/v1/perpum/account/getUserAssets", &HashMap::new())
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
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

    pub(super) async fn get_positions_private_rest(
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
                operation: "coinw.positions_non_perpetual",
            });
        }
        self.ensure_private_rest("coinw.get_positions")?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "coinw.get_positions")?;
        let mut positions = Vec::new();
        if request.symbols.is_empty() {
            let value = self
                .rest
                .send_signed_futures_get("/v1/perpum/positions/all", &HashMap::new())
                .await?;
            positions.extend(parse_positions(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.symbols,
                &value,
            )?);
        } else {
            for symbol in &request.symbols {
                if symbol.exchange_id != self.exchange_id {
                    return Err(ExchangeApiError::InvalidRequest {
                        message: format!(
                            "coinw adapter cannot serve position request for exchange {}",
                            symbol.exchange_id
                        ),
                    });
                }
                let mut params = HashMap::new();
                params.insert(
                    "instrument".to_string(),
                    normalize_coinw_perp_base(&symbol.symbol)?,
                );
                let value = self
                    .rest
                    .send_signed_futures_get("/v1/perpum/positions", &params)
                    .await?;
                positions.extend(parse_positions(
                    &self.exchange_id,
                    tenant_id.clone(),
                    account_id.clone(),
                    std::slice::from_ref(symbol),
                    &value,
                )?);
            }
        }
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions,
        })
    }

    pub(super) async fn get_fees_private_rest(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinw.get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        let mut futures_fees: Option<Value> = None;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            let value = if symbol.market_type == MarketType::Perpetual
                && self.config.private_rest_available()
            {
                if futures_fees.is_none() {
                    futures_fees = Some(
                        self.rest
                            .send_signed_futures_get("/v1/perpum/account/fees", &HashMap::new())
                            .await?,
                    );
                }
                futures_fees.as_ref()
            } else {
                None
            };
            fees.extend(parse_fee_snapshots(&self.exchange_id, symbol, value)?);
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees,
        })
    }

    pub(super) async fn place_order_private_rest(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("coinw.place_order")?;
        let value = match request.symbol.market_type {
            MarketType::Spot => {
                let params = coinw_spot_place_order_params(&request)?;
                self.rest
                    .send_signed_spot_post("/api/v1/private?command=doTrade", &params)
                    .await?
            }
            MarketType::Perpetual => {
                let body = coinw_futures_place_order_body(&request)?;
                self.rest
                    .send_signed_futures_post("/v1/perpum/order", &body)
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        let order = if request.symbol.market_type == MarketType::Spot {
            order_state_from_spot_place_ack(&self.exchange_id, &request, &value)
        } else {
            order_state_from_futures_place_ack(&self.exchange_id, &request, &value)
        };
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_private_rest(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        if request.symbol.market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinw.perp_quote_market_order",
            });
        }
        self.ensure_private_rest("coinw.place_quote_market_order")?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_coinw_spot_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert("type".to_string(), spot_side_code(request.side).to_string());
        params.insert("isMarket".to_string(), "true".to_string());
        params.insert("funds".to_string(), request.quote_quantity.clone());
        params.insert(
            "out_trade_no".to_string(),
            required_client_order_id(
                request.client_order_id.as_deref(),
                "coinw.place_quote_market_order",
            )?
            .to_string(),
        );
        let value = self
            .rest
            .send_signed_spot_post("/api/v1/private?command=doTrade", &params)
            .await?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: order_state_from_quote_ack(&self.exchange_id, &request, &value),
        })
    }

    pub(super) async fn cancel_order_private_rest(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("coinw.cancel_order")?;
        let order_id =
            required_exchange_order_id(&request.exchange_order_id, "coinw.cancel_order")?;
        match request.symbol.market_type {
            MarketType::Spot => {
                let mut params = HashMap::new();
                params.insert("orderNumber".to_string(), order_id.to_string());
                self.rest
                    .send_signed_spot_post("/api/v1/private?command=cancelOrder", &params)
                    .await?;
            }
            MarketType::Perpetual => {
                self.rest
                    .send_signed_futures_delete("/v1/perpum/order", &json!({ "id": order_id }))
                    .await?;
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        }
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: order_state_from_cancel_ack(&self.exchange_id, &request),
            cancelled: true,
        })
    }

    pub(super) async fn cancel_all_orders_private_rest(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("coinw.cancel_all_orders")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinw.cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        if request
            .market_type
            .is_some_and(|market_type| market_type != symbol.market_type)
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinw cancel_all_orders market_type does not match symbol".to_string(),
            });
        }
        let orders = match symbol.market_type {
            MarketType::Spot => {
                let mut params = HashMap::new();
                params.insert(
                    "currencyPair".to_string(),
                    normalize_coinw_spot_symbol(&symbol.exchange_symbol.symbol)?,
                );
                let value = self
                    .rest
                    .send_signed_spot_post("/api/v1/private?command=cancelAllOrder", &params)
                    .await?;
                cancelled_order_states_from_spot_ack(&self.exchange_id, symbol, &value)
            }
            MarketType::Perpetual => {
                let open = self
                    .get_open_orders_private_rest(OpenOrdersRequest {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        context: request.context.clone(),
                        exchange: request.exchange.clone(),
                        market_type: Some(MarketType::Perpetual),
                        symbol: Some(symbol.clone()),
                        page: None,
                    })
                    .await?;
                let ids = open
                    .orders
                    .iter()
                    .filter_map(|order| order.exchange_order_id.clone())
                    .collect::<Vec<_>>();
                for pos_type in COINW_PERP_CANCEL_POS_TYPES {
                    for chunk in ids.chunks(20) {
                        self.rest
                            .send_signed_futures_delete(
                                "/v1/perpum/batchOrders",
                                &json!({ "posType": pos_type, "sourceIds": chunk }),
                            )
                            .await?;
                    }
                }
                open.orders
                    .into_iter()
                    .map(|mut order| {
                        order.status = OrderStatus::Cancelled;
                        order.updated_at = chrono::Utc::now();
                        order
                    })
                    .collect()
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn amend_order_private_rest(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported_private("coinw.amend_order")
    }

    pub(super) async fn place_order_list_private_rest(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.unsupported_private("coinw.place_order_list")
    }

    pub(super) async fn batch_place_orders_private_rest(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("coinw.batch_place_orders")?;
        let mut orders = Vec::with_capacity(request.orders.len());
        for order in request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            let response = self.place_order_private_rest(order).await?;
            orders.push(response.order);
        }
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            report: None,
        })
    }

    pub(super) async fn batch_cancel_orders_private_rest(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("coinw.batch_cancel_orders")?;
        let mut orders = Vec::new();
        let mut futures_groups: HashMap<String, Vec<CancelOrderRequest>> = HashMap::new();
        for cancel in request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market_type(cancel.symbol.market_type)?;
            if cancel.symbol.market_type == MarketType::Perpetual {
                let symbol = cancel.symbol.exchange_symbol.symbol.clone();
                futures_groups.entry(symbol).or_default().push(cancel);
            } else {
                let response = self.cancel_order_private_rest(cancel).await?;
                orders.push(response.order);
            }
        }
        for cancels in futures_groups.into_values() {
            for chunk in cancels.chunks(20) {
                let ids = chunk
                    .iter()
                    .map(|cancel| {
                        required_exchange_order_id(
                            &cancel.exchange_order_id,
                            "coinw.batch_cancel_orders",
                        )
                        .map(ToString::to_string)
                    })
                    .collect::<ExchangeApiResult<Vec<_>>>()?;
                for pos_type in COINW_PERP_CANCEL_POS_TYPES {
                    self.rest
                        .send_signed_futures_delete(
                            "/v1/perpum/batchOrders",
                            &json!({ "posType": pos_type, "sourceIds": ids }),
                        )
                        .await?;
                }
                orders.extend(
                    chunk
                        .iter()
                        .map(|cancel| order_state_from_cancel_ack(&self.exchange_id, cancel)),
                );
            }
        }
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: None,
        })
    }

    pub(super) async fn query_order_private_rest(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("coinw.query_order")?;
        let order_id = required_exchange_order_id(&request.exchange_order_id, "coinw.query_order")?;
        let value = match request.symbol.market_type {
            MarketType::Spot => {
                let mut params = HashMap::new();
                params.insert("orderNumber".to_string(), order_id.to_string());
                self.rest
                    .send_signed_spot_post("/api/v1/private?command=returnOrderStatus", &params)
                    .await?
            }
            MarketType::Perpetual => {
                let mut params = HashMap::new();
                params.insert("sourceIds".to_string(), order_id.to_string());
                params.insert("positionType".to_string(), "plan".to_string());
                params.insert(
                    "instrument".to_string(),
                    normalize_coinw_perp_base(&request.symbol.exchange_symbol.symbol)?,
                );
                self.rest
                    .send_signed_futures_get("/v1/perpum/order", &params)
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order: parse_order(
                &self.exchange_id,
                Some(&request.symbol),
                request.symbol.market_type,
                &value,
            )?,
        })
    }

    pub(super) async fn get_open_orders_private_rest(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market_type(market_type)?;
        }
        self.ensure_private_rest("coinw.get_open_orders")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinw.get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let value = match symbol.market_type {
            MarketType::Spot => {
                let mut params = HashMap::new();
                params.insert(
                    "currencyPair".to_string(),
                    normalize_coinw_spot_symbol(&symbol.exchange_symbol.symbol)?,
                );
                self.rest
                    .send_signed_spot_post("/api/v1/private?command=returnOpenOrders", &params)
                    .await?
            }
            MarketType::Perpetual => {
                let mut params = HashMap::new();
                params.insert(
                    "instrument".to_string(),
                    normalize_coinw_perp_base(&symbol.exchange_symbol.symbol)?,
                );
                params.insert("positionType".to_string(), "plan".to_string());
                self.rest
                    .send_signed_futures_get("/v1/perpum/orders/open", &params)
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, Some(symbol), symbol.market_type, &value)?,
        })
    }

    pub(super) async fn get_recent_fills_private_rest(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("coinw.get_recent_fills")?;
        let market_type = request
            .market_type
            .or_else(|| request.symbol.as_ref().map(|symbol| symbol.market_type))
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "coinw.get_recent_fills")?;
        let value = match market_type {
            MarketType::Spot => {
                let symbol =
                    request
                        .symbol
                        .as_ref()
                        .ok_or_else(|| ExchangeApiError::InvalidRequest {
                            message: "coinw spot get_recent_fills requires symbol".to_string(),
                        })?;
                let mut params = HashMap::new();
                params.insert(
                    "currencyPair".to_string(),
                    normalize_coinw_spot_symbol(&symbol.exchange_symbol.symbol)?,
                );
                if let Some(limit) = request.limit {
                    params.insert("limit".to_string(), limit.min(100).to_string());
                }
                self.rest
                    .send_signed_spot_post("/api/v1/private?command=returnUTradeHistory", &params)
                    .await?
            }
            MarketType::Perpetual => {
                let symbol =
                    request
                        .symbol
                        .as_ref()
                        .ok_or_else(|| ExchangeApiError::InvalidRequest {
                            message: "coinw perpetual get_recent_fills requires symbol".to_string(),
                        })?;
                let mut params = HashMap::new();
                params.insert(
                    "instrument".to_string(),
                    normalize_coinw_perp_base(&symbol.exchange_symbol.symbol)?,
                );
                params.insert(
                    "pageSize".to_string(),
                    request.limit.unwrap_or(100).min(100).to_string(),
                );
                self.rest
                    .send_signed_futures_get("/v1/perpum/orders/deals", &params)
                    .await?
            }
            _ => unreachable!("checked by ensure_supported_market_type"),
        };
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.symbol.as_ref(),
                market_type,
                &value,
            )?,
        })
    }
}

fn coinw_spot_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinw.spot_reduce_only",
        });
    }
    if request.post_only || matches!(request.time_in_force, Some(TimeInForce::GTX)) {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinw.spot_post_only",
        });
    }
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_coinw_spot_symbol(&request.symbol.exchange_symbol.symbol)?,
    );
    params.insert("type".to_string(), spot_side_code(request.side).to_string());
    params.insert(
        "out_trade_no".to_string(),
        required_client_order_id(request.client_order_id.as_deref(), "coinw.place_order")?
            .to_string(),
    );
    match request.order_type {
        OrderType::Market => {
            params.insert("isMarket".to_string(), "true".to_string());
            if let Some(quote_quantity) = &request.quote_quantity {
                params.insert("funds".to_string(), quote_quantity.clone());
            } else {
                params.insert("amount".to_string(), request.quantity.clone());
            }
        }
        OrderType::Limit => {
            params.insert("isMarket".to_string(), "false".to_string());
            params.insert("amount".to_string(), request.quantity.clone());
            params.insert(
                "rate".to_string(),
                request
                    .price
                    .clone()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "coinw spot limit order requires price".to_string(),
                    })?,
            );
        }
        OrderType::IOC | OrderType::FOK | OrderType::PostOnly => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinw.spot_advanced_time_in_force",
            });
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinw.spot_order_type",
            });
        }
    }
    Ok(params)
}

fn coinw_futures_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinw.perp_reduce_only_requires_close_position_api",
        });
    }
    let position_type = match (request.order_type, request.post_only, request.time_in_force) {
        (OrderType::Market, _, _) => "execute",
        (_, true, _) | (_, _, Some(TimeInForce::GTX)) => "PostOnly",
        (OrderType::IOC, _, _) | (_, _, Some(TimeInForce::IOC)) => "IOC",
        (OrderType::FOK, _, _) | (_, _, Some(TimeInForce::FOK)) => "FOK",
        _ => "plan",
    };
    let mut body = json!({
        "instrument": normalize_coinw_perp_base(&request.symbol.exchange_symbol.symbol)?,
        "direction": futures_direction(request),
        "leverage": 1,
        "quantityUnit": 2,
        "quantity": request.quantity,
        "positionModel": 0,
        "positionType": position_type,
    });
    if let Some(client_order_id) = &request.client_order_id {
        body["thirdOrderId"] = json!(client_order_id);
    }
    if position_type != "execute" {
        body["openPrice"] =
            json!(request
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "coinw futures limit order requires price".to_string(),
                })?);
    }
    Ok(body)
}

fn spot_side_code(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "0",
        OrderSide::Sell => "1",
    }
}

fn futures_direction(request: &PlaceOrderRequest) -> &'static str {
    match request.position_side {
        Some(PositionSide::Short) => "short",
        Some(PositionSide::Long) => "long",
        _ => match request.side {
            OrderSide::Buy => "long",
            OrderSide::Sell => "short",
        },
    }
}

fn required_client_order_id<'a>(
    client_order_id: Option<&'a str>,
    operation: &str,
) -> ExchangeApiResult<&'a str> {
    client_order_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("{operation} requires client_order_id for CoinW"),
        })
}

fn required_exchange_order_id<'a>(
    exchange_order_id: &'a Option<String>,
    operation: &str,
) -> ExchangeApiResult<&'a str> {
    exchange_order_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("{operation} requires exchange_order_id"),
        })
}

fn cancelled_order_states_from_spot_ack(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> Vec<OrderState> {
    value
        .get("data")
        .and_then(|data| data.get("orderNumbers"))
        .and_then(Value::as_array)
        .map(|orders| {
            orders
                .iter()
                .filter_map(|order| match order {
                    Value::String(text) => Some(text.clone()),
                    Value::Number(number) => Some(number.to_string()),
                    _ => None,
                })
                .map(|order_id| OrderState {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    exchange: exchange_id.clone(),
                    market_type: symbol.market_type,
                    canonical_symbol: symbol.canonical_symbol.clone(),
                    exchange_symbol: symbol.exchange_symbol.clone(),
                    client_order_id: None,
                    exchange_order_id: Some(order_id),
                    side: OrderSide::Buy,
                    position_side: None,
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
                })
                .collect()
        })
        .unwrap_or_default()
}
