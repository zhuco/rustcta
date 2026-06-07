use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderListRequest, OrderListResponse, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Map, Value};

use super::parser::normalize_bitmex_symbol;
use super::private_parser::{
    parse_fee_snapshots, parse_margin_balances, parse_order_state, parse_orders, parse_positions,
    parse_recent_fills,
};
use super::BitmexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BitmexGatewayAdapter {
    pub(super) async fn get_balances_private_rest(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_supported_market_type(request.market_type)?;
        self.ensure_private_rest("bitmex.get_balances")?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let value = self
            .rest
            .send_signed_get("/api/v1/user/margin", &HashMap::new())
            .await?;
        let balances = parse_margin_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            &request.assets,
            &value,
        )?;
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
        self.ensure_optional_supported_market_type(request.market_type)?;
        self.ensure_private_rest("bitmex.get_positions")?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        let mut filter = Map::new();
        filter.insert("isOpen".to_string(), Value::Bool(true));
        if let Some(symbol) = request.symbols.first() {
            filter.insert(
                "symbol".to_string(),
                Value::String(normalize_bitmex_symbol(&symbol.symbol)?),
            );
        }
        params.insert("filter".to_string(), Value::Object(filter).to_string());
        let value = self
            .rest
            .send_signed_get("/api/v1/position", &params)
            .await?;
        let positions = parse_positions(&self.exchange_id, tenant_id, account_id, &value)?;
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
                message: "bitmex.get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        let value = self
            .rest
            .send_public_request("/api/v1/instrument/active", &HashMap::new())
            .await?;
        let fees = parse_fee_snapshots(&self.exchange_id, &request.symbols, &value)?;
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
        self.ensure_private_rest("bitmex.place_order")?;
        let body = bitmex_place_order_body(&request)?;
        let value = self.rest.send_signed_post("/api/v1/order", &body).await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_private_rest(
        &self,
        _request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.unsupported_private("bitmex.place_quote_market_order")
    }

    pub(super) async fn batch_place_orders_private_rest(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("bitmex.batch_place_orders")?;
        if request.orders.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitmex.batch_place_orders requires at least one order".to_string(),
            });
        }

        let mut orders = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market_type(order.symbol.market_type)?;
            orders.push(bitmex_place_order_body(order)?);
        }

        let body = json!({ "orders": orders });
        let value = self
            .rest
            .send_signed_post("/api/v1/order/bulk", &body)
            .await?;
        let order_states = parse_orders(&self.exchange_id, None, &value)?;
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: order_states,
            report: None,
        })
    }

    pub(super) async fn cancel_order_private_rest(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("bitmex.cancel_order")?;
        let mut params = HashMap::new();
        match (&request.exchange_order_id, &request.client_order_id) {
            (Some(order_id), _) => {
                params.insert("orderID".to_string(), order_id.clone());
            }
            (None, Some(client_order_id)) => {
                params.insert("clOrdID".to_string(), client_order_id.clone());
            }
            (None, None) => {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bitmex.cancel_order requires exchange_order_id or client_order_id"
                        .to_string(),
                });
            }
        }
        let value = self
            .rest
            .send_signed_delete("/api/v1/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn batch_cancel_orders_private_rest(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("bitmex.batch_cancel_orders")?;
        if request.cancels.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitmex.batch_cancel_orders requires at least one cancel".to_string(),
            });
        }

        let mut order_ids = Vec::new();
        let mut client_order_ids = Vec::new();
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market_type(cancel.symbol.market_type)?;
            if let Some(order_id) = &cancel.exchange_order_id {
                order_ids.push(order_id.clone());
            } else if let Some(client_order_id) = &cancel.client_order_id {
                client_order_ids.push(client_order_id.clone());
            } else {
                return Err(ExchangeApiError::InvalidRequest {
                    message:
                        "bitmex.batch_cancel_orders requires exchange_order_id or client_order_id"
                            .to_string(),
                });
            }
        }

        let mut orders = Vec::new();
        if !order_ids.is_empty() {
            let mut params = HashMap::new();
            params.insert(
                "orderID".to_string(),
                serde_json::to_string(&order_ids).map_err(|error| {
                    ExchangeApiError::Serialization {
                        message: error.to_string(),
                    }
                })?,
            );
            let value = self
                .rest
                .send_signed_delete("/api/v1/order", &params)
                .await?;
            orders.extend(parse_orders(&self.exchange_id, None, &value)?);
        }
        if !client_order_ids.is_empty() {
            let mut params = HashMap::new();
            params.insert(
                "clOrdID".to_string(),
                serde_json::to_string(&client_order_ids).map_err(|error| {
                    ExchangeApiError::Serialization {
                        message: error.to_string(),
                    }
                })?,
            );
            let value = self
                .rest
                .send_signed_delete("/api/v1/order", &params)
                .await?;
            orders.extend(parse_orders(&self.exchange_id, None, &value)?);
        }
        let cancelled_count = orders.len() as u32;
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
            report: None,
        })
    }

    pub(super) async fn cancel_all_orders_private_rest(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_supported_market_type(request.market_type)?;
        self.ensure_private_rest("bitmex.cancel_all_orders")?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_bitmex_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .rest
            .send_signed_delete("/api/v1/order/all", &params)
            .await?;
        let orders = if let Some(symbol) = &request.symbol {
            parse_orders(&self.exchange_id, Some(symbol), &value)?
        } else {
            parse_orders(&self.exchange_id, None, &value)?
        };
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
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("bitmex.amend_order")?;
        let body = bitmex_amend_order_body(&request)?;
        let value = self.rest.send_signed_put("/api/v1/order", &body).await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn place_order_list_private_rest(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.unsupported_private("bitmex.place_order_list")
    }

    pub(super) async fn query_order_private_rest(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.ensure_private_rest("bitmex.query_order")?;
        let mut params = HashMap::new();
        params.insert("count".to_string(), "1".to_string());
        params.insert("reverse".to_string(), "true".to_string());
        params.insert(
            "filter".to_string(),
            bitmex_order_filter(
                &request.symbol.exchange_symbol.symbol,
                request.exchange_order_id.as_deref(),
                request.client_order_id.as_deref(),
                None,
            )?,
        );
        let value = self.rest.send_signed_get("/api/v1/order", &params).await?;
        let orders = parse_orders(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order: orders.into_iter().next(),
        })
    }

    pub(super) async fn get_open_orders_private_rest(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_supported_market_type(request.market_type)?;
        self.ensure_private_rest("bitmex.get_open_orders")?;
        let mut params = HashMap::new();
        let symbol_text = request
            .symbol
            .as_ref()
            .map(|symbol| {
                self.ensure_exchange(&symbol.exchange)?;
                self.ensure_supported_market_type(symbol.market_type)?;
                normalize_bitmex_symbol(&symbol.exchange_symbol.symbol)
            })
            .transpose()?;
        let mut filter = Map::new();
        filter.insert("open".to_string(), Value::Bool(true));
        if let Some(symbol) = symbol_text {
            filter.insert("symbol".to_string(), Value::String(symbol));
        }
        params.insert("filter".to_string(), Value::Object(filter).to_string());
        params.insert("count".to_string(), "100".to_string());
        params.insert("reverse".to_string(), "true".to_string());
        let value = self.rest.send_signed_get("/api/v1/order", &params).await?;
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
        self.ensure_optional_supported_market_type(request.market_type)?;
        self.ensure_private_rest("bitmex.get_recent_fills")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitmex.get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bitmex_symbol(&symbol.exchange_symbol.symbol)?,
        );
        params.insert("reverse".to_string(), "true".to_string());
        params.insert(
            "count".to_string(),
            request.limit.unwrap_or(100).min(500).to_string(),
        );
        if let Some(start_time) = request.start_time {
            params.insert("startTime".to_string(), start_time.to_rfc3339());
        }
        if let Some(end_time) = request.end_time {
            params.insert("endTime".to_string(), end_time.to_rfc3339());
        }
        let value = self
            .rest
            .send_signed_get("/api/v1/execution/tradeHistory", &params)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn bitmex_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = Map::new();
    body.insert(
        "symbol".to_string(),
        Value::String(normalize_bitmex_symbol(
            &request.symbol.exchange_symbol.symbol,
        )?),
    );
    body.insert(
        "side".to_string(),
        Value::String(map_side(request.side).to_string()),
    );
    body.insert(
        "orderQty".to_string(),
        decimal_param(&request.quantity, "quantity")?,
    );
    body.insert(
        "ordType".to_string(),
        Value::String(map_order_type(request.order_type).to_string()),
    );
    match request.order_type {
        OrderType::Market => {}
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => {
            let price = request
                .price
                .as_ref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitmex limit order requires price".to_string(),
                })?;
            body.insert("price".to_string(), decimal_param(price, "price")?);
        }
        OrderType::StopMarket => {
            let stop_px =
                request
                    .price
                    .as_ref()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "bitmex stop-market order requires price as stopPx".to_string(),
                    })?;
            body.insert("stopPx".to_string(), decimal_param(stop_px, "stopPx")?);
        }
        OrderType::StopLimit => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitmex.stop_limit_order_requires_price_and_stop_px",
            });
        }
    }
    if let Some(client_order_id) = &request.client_order_id {
        body.insert(
            "clOrdID".to_string(),
            Value::String(client_order_id.clone()),
        );
    }
    if let Some(time_in_force) = request.time_in_force {
        body.insert(
            "timeInForce".to_string(),
            Value::String(map_time_in_force(time_in_force).to_string()),
        );
    }
    let exec_inst = exec_inst(request);
    if !exec_inst.is_empty() {
        body.insert("execInst".to_string(), Value::String(exec_inst.join(",")));
    }
    Ok(Value::Object(body))
}

fn bitmex_amend_order_body(request: &AmendOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = Map::new();
    match (&request.exchange_order_id, &request.client_order_id) {
        (Some(order_id), _) => {
            body.insert("orderID".to_string(), Value::String(order_id.clone()));
        }
        (None, Some(client_order_id)) => {
            body.insert(
                "origClOrdID".to_string(),
                Value::String(client_order_id.clone()),
            );
        }
        (None, None) => {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitmex.amend_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
    }
    if let Some(new_client_order_id) = &request.new_client_order_id {
        body.insert(
            "clOrdID".to_string(),
            Value::String(new_client_order_id.clone()),
        );
    }
    body.insert(
        "orderQty".to_string(),
        decimal_param(&request.new_quantity, "new_quantity")?,
    );
    Ok(Value::Object(body))
}

fn bitmex_order_filter(
    symbol: &str,
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
    open: Option<bool>,
) -> ExchangeApiResult<String> {
    let mut filter = Map::new();
    filter.insert(
        "symbol".to_string(),
        Value::String(normalize_bitmex_symbol(symbol)?),
    );
    if let Some(order_id) = exchange_order_id {
        filter.insert("orderID".to_string(), Value::String(order_id.to_string()));
    } else if let Some(client_order_id) = client_order_id {
        filter.insert(
            "clOrdID".to_string(),
            Value::String(client_order_id.to_string()),
        );
    } else if open.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitmex.query_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    if let Some(open) = open {
        filter.insert("open".to_string(), Value::Bool(open));
    }
    Ok(Value::Object(filter).to_string())
}

fn exec_inst(request: &PlaceOrderRequest) -> Vec<&'static str> {
    let mut values = Vec::new();
    if request.reduce_only {
        values.push("ReduceOnly");
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        values.push("ParticipateDoNotInitiate");
    }
    values
}

fn map_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "Buy",
        OrderSide::Sell => "Sell",
    }
}

fn map_order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "Market",
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => "Limit",
        OrderType::StopMarket => "Stop",
        OrderType::StopLimit => "StopLimit",
    }
}

fn map_time_in_force(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::GTC | TimeInForce::GTX => "GoodTillCancel",
        TimeInForce::IOC => "ImmediateOrCancel",
        TimeInForce::FOK => "FillOrKill",
    }
}

fn decimal_param(value: &str, field: &'static str) -> ExchangeApiResult<Value> {
    let parsed = value
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid BitMEX {field} value {value}: {error}"),
        })?;
    Ok(json!(parsed))
}
