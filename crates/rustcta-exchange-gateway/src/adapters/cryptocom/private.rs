use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderListConditionalLeg, OrderListLegType,
    OrderListOrderLeg, OrderListRequest, OrderListResponse, PlaceOrderRequest, PlaceOrderResponse,
    PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderStatus, OrderType};
use serde_json::{json, Value};

use super::parser::{normalize_cryptocom_symbol, value_as_string};
use super::private_parser::{
    cancelled_order_state, parse_balances, parse_fee_snapshots, parse_open_orders,
    parse_order_state, parse_recent_fills,
};
use super::CryptoComGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CryptoComGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_public_market(request.symbol.market_type)?;
        let body = cryptocom_order_params(&request)?;
        let value = self
            .send_private_post("cryptocom.place_order", "private/create-order", body)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .ok()
            .filter(|order| order.status != OrderStatus::Unknown)
            .unwrap_or_else(|| ack_order_state(&self.exchange_id, &request, &value));
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_impl(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_public_market(request.symbol.market_type)?;
        if request.side != OrderSide::Buy {
            return Err(ExchangeApiError::Unsupported {
                operation: "cryptocom.quote_market_sell",
            });
        }
        let mut body = json!({
            "instrument_name": normalize_cryptocom_symbol(&request.symbol)?,
            "side": "BUY",
            "type": "MARKET",
            "notional": non_empty("quote_quantity", &request.quote_quantity)?,
        });
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            body["client_oid"] = Value::String(non_empty("client_order_id", client_order_id)?);
        }
        let value = self
            .send_private_post(
                "cryptocom.place_quote_market_order",
                "private/create-order",
                body,
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .ok()
            .filter(|order| order.status != OrderStatus::Unknown)
            .unwrap_or_else(|| quote_ack_order_state(&self.exchange_id, &request, &value));
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
        self.ensure_public_market(request.symbol.market_type)?;
        let body = cryptocom_cancel_order_params(&request)?;
        let value = self
            .send_private_post("cryptocom.cancel_order", "private/cancel-order", body)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .ok()
            .filter(|order| order.status != OrderStatus::Unknown)
            .unwrap_or_else(|| {
                cancelled_order_state(
                    &self.exchange_id,
                    &request.symbol,
                    value_as_string(value.get("order_id"))
                        .or_else(|| request.exchange_order_id.clone()),
                    value_as_string(value.get("client_oid"))
                        .or_else(|| request.client_order_id.clone()),
                )
            });
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
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
        self.ensure_public_market(request.symbol.market_type)?;
        if request
            .new_client_order_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "cryptocom.amend_new_client_order_id",
            });
        }
        let lookup_body = cryptocom_order_lookup_params(
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "amend_order",
        )?;
        let current_value = self
            .send_private_post(
                "cryptocom.amend_order.lookup",
                "private/get-order-detail",
                lookup_body,
            )
            .await?;
        let current = parse_order_state(&self.exchange_id, Some(&request.symbol), &current_value)?;
        let current_price =
            current
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "cryptocom amend_order requires current order price".to_string(),
                })?;
        let body = cryptocom_amend_order_params(&request, &current_price)?;
        let value = self
            .send_private_post("cryptocom.amend_order", "private/amend-order", body)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .ok()
            .filter(|order| order.status != OrderStatus::Unknown)
            .unwrap_or_else(|| amend_ack_order_state(current, &request, &current_price, &value));
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn place_order_list_impl(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        let schema_version = order_list_schema_version(&request);
        let request_id = order_list_context_request_id(&request);
        let list_client_order_id = order_list_client_order_id(&request);
        ensure_exchange_api_schema(schema_version)?;
        let symbol = request.symbol();
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_public_market(symbol.market_type)?;
        let method = cryptocom_order_list_method(&request)?;
        let body = cryptocom_order_list_params(&request)?;
        let value = self
            .send_private_post("cryptocom.place_order_list", method, body)
            .await?;
        Ok(OrderListResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(symbol.exchange.clone(), request_id),
            symbol: symbol.clone(),
            kind: request.kind(),
            order_list_id: value_as_string(value.get("list_id")),
            list_client_order_id,
            list_status_type: Some("QUEUED".to_string()),
            list_order_status: Some("EXECUTING".to_string()),
            orders: Vec::new(),
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
                message: "cryptocom batch_place_orders requires at least one order".to_string(),
            });
        }
        if request.orders.len() > 10 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "cryptocom batch_place_orders supports at most 10 orders".to_string(),
            });
        }
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_public_market(order.symbol.market_type)?;
        }
        let body = cryptocom_batch_order_params(&request.orders)?;
        let value = self
            .send_private_post(
                "cryptocom.batch_place_orders",
                "private/create-order-list",
                body,
            )
            .await?;
        let orders = batch_place_order_states(&self.exchange_id, &request.orders, &value)?;
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
                message: "cryptocom batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        if request.cancels.len() > 10 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "cryptocom batch_cancel_orders supports at most 10 cancels".to_string(),
            });
        }
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_public_market(cancel.symbol.market_type)?;
        }
        let body = cryptocom_batch_cancel_params(&request.cancels)?;
        let value = self
            .send_private_post(
                "cryptocom.batch_cancel_orders",
                "private/cancel-order-list",
                body,
            )
            .await?;
        let orders = batch_cancel_order_states(&self.exchange_id, &request.cancels, &value)?;
        let cancelled_count = orders
            .iter()
            .filter(|order| order.status == OrderStatus::Cancelled)
            .count() as u32;
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
        if let Some(market_type) = request.market_type {
            self.ensure_public_market(market_type)?;
        }
        let mut body = json!({});
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_public_market(symbol.market_type)?;
            body["instrument_name"] = Value::String(normalize_cryptocom_symbol(symbol)?);
        }
        let value = self
            .send_private_post(
                "cryptocom.cancel_all_orders",
                "private/cancel-all-orders",
                body,
            )
            .await?;
        let orders = value
            .get("data")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        request.symbol.as_ref().map(|symbol| {
                            parse_order_state(&self.exchange_id, Some(symbol), item).unwrap_or_else(
                                |_| {
                                    cancelled_order_state(
                                        &self.exchange_id,
                                        symbol,
                                        value_as_string(item.get("order_id")),
                                        value_as_string(item.get("client_oid")),
                                    )
                                },
                            )
                        })
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

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let value = self
            .send_private_post("cryptocom.get_balances", "private/user-balance", json!({}))
            .await?;
        let balances = parse_balances(
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

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_public_market(market_type)?;
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut body = json!({});
        if request.symbols.len() == 1 {
            body["instrument_name"] =
                Value::String(non_empty("instrument_name", &request.symbols[0].symbol)?);
        } else if request.symbols.len() > 1 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "cryptocom get_positions supports at most one symbol filter".to_string(),
            });
        }
        let value = self
            .send_private_post("cryptocom.get_positions", "private/get-positions", body)
            .await?;
        let positions = super::private_parser::parse_positions(
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
                message: "cryptocom get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let value = self
            .send_private_post("cryptocom.get_fees", "private/get-fee-rate", json!({}))
            .await?;
        let fees = parse_fee_snapshots(&request.symbols, &value)?;
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
        self.ensure_public_market(request.symbol.market_type)?;
        let body = cryptocom_order_lookup_params(
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "query_order",
        )?;
        let value = self
            .send_private_post("cryptocom.query_order", "private/get-order-detail", body)
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
            self.ensure_public_market(market_type)?;
        }
        let mut body = json!({});
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_public_market(symbol.market_type)?;
            body["instrument_name"] = Value::String(normalize_cryptocom_symbol(symbol)?);
        }
        let value = self
            .send_private_post("cryptocom.get_open_orders", "private/get-open-orders", body)
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
            self.ensure_public_market(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "cryptocom get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_public_market(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut body = json!({
            "instrument_name": normalize_cryptocom_symbol(symbol)?,
        });
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            body["order_id"] = Value::String(order_id.to_string());
        }
        if let Some(start_time) = request.start_time {
            body["start_time"] = Value::Number(start_time.timestamp_millis().into());
        }
        if let Some(end_time) = request.end_time {
            body["end_time"] = Value::Number(end_time.timestamp_millis().into());
        }
        if let Some(limit) = request.limit {
            body["limit"] = Value::Number(limit.min(100).into());
        }
        let value = self
            .send_private_post("cryptocom.get_recent_fills", "private/get-trades", body)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn cryptocom_order_params(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only && request.symbol.market_type != rustcta_types::MarketType::Perpetual {
        return Err(ExchangeApiError::InvalidRequest {
            message: "cryptocom reduce_only requires perpetual market".to_string(),
        });
    }
    let mut body = json!({
        "instrument_name": normalize_cryptocom_symbol(&request.symbol)?,
        "side": cryptocom_side(request.side),
        "type": cryptocom_order_type(request.order_type)?,
        "quantity": non_empty("quantity", &request.quantity)?,
    });
    if let Some(tif) = cryptocom_time_in_force(request.time_in_force) {
        body["time_in_force"] = Value::String(tif.to_string());
    }
    let mut exec_inst = Vec::new();
    if request.post_only
        || request.order_type == OrderType::PostOnly
        || request.time_in_force == Some(TimeInForce::GTX)
    {
        exec_inst.push("POST_ONLY");
    }
    if request.reduce_only {
        exec_inst.push("REDUCE_ONLY");
    }
    if !exec_inst.is_empty() {
        body["exec_inst"] = json!(exec_inst);
    }
    if request.order_type != OrderType::Market {
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "cryptocom limit-style order requires price".to_string(),
            })?;
        body["price"] = Value::String(non_empty("price", price)?);
    } else if let Some(quote_quantity) = request.quote_quantity.as_deref() {
        body["notional"] = Value::String(non_empty("quote_quantity", quote_quantity)?);
        body.as_object_mut()
            .expect("body object")
            .remove("quantity");
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["client_oid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn cryptocom_cancel_order_params(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "instrument_name": normalize_cryptocom_symbol(&request.symbol)?,
    });
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body["order_id"] = Value::String(non_empty("exchange_order_id", order_id)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["client_oid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if body.get("order_id").is_none() && body.get("client_oid").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "cryptocom cancel_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(body)
}

fn cryptocom_order_lookup_params(
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
    operation: &str,
) -> ExchangeApiResult<Value> {
    let mut body = json!({});
    if let Some(order_id) = exchange_order_id {
        body["order_id"] = Value::String(non_empty("exchange_order_id", order_id)?);
    }
    if let Some(client_order_id) = client_order_id {
        body["client_oid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if body.get("order_id").is_none() && body.get("client_oid").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("cryptocom {operation} requires exchange_order_id or client_order_id"),
        });
    }
    Ok(body)
}

fn cryptocom_amend_order_params(
    request: &AmendOrderRequest,
    current_price: &str,
) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "instrument_name": normalize_cryptocom_symbol(&request.symbol)?,
        "new_price": non_empty("new_price", current_price)?,
        "new_quantity": non_empty("new_quantity", &request.new_quantity)?,
    });
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body["order_id"] = Value::String(non_empty("exchange_order_id", order_id)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["client_oid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if body.get("order_id").is_none() && body.get("client_oid").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "cryptocom amend_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(body)
}

fn cryptocom_batch_order_params(orders: &[PlaceOrderRequest]) -> ExchangeApiResult<Value> {
    Ok(json!({
        "contingency_type": "LIST",
        "order_list": orders
            .iter()
            .map(cryptocom_order_params)
            .collect::<ExchangeApiResult<Vec<_>>>()?,
    }))
}

fn cryptocom_batch_cancel_params(cancels: &[CancelOrderRequest]) -> ExchangeApiResult<Value> {
    Ok(json!({
        "contingency_type": "LIST",
        "order_list": cancels
            .iter()
            .map(cryptocom_cancel_order_params)
            .collect::<ExchangeApiResult<Vec<_>>>()?,
    }))
}

fn cryptocom_order_list_method(request: &OrderListRequest) -> ExchangeApiResult<&'static str> {
    match request {
        OrderListRequest::Oco { .. } => Ok("private/advanced/create-oco"),
        OrderListRequest::Oto { .. } => Ok("private/advanced/create-oto"),
    }
}

fn order_list_schema_version(request: &OrderListRequest) -> u16 {
    match request {
        OrderListRequest::Oco { schema_version, .. }
        | OrderListRequest::Oto { schema_version, .. } => *schema_version,
    }
}

fn order_list_context_request_id(request: &OrderListRequest) -> Option<String> {
    match request {
        OrderListRequest::Oco { context, .. } | OrderListRequest::Oto { context, .. } => {
            context.request_id.clone()
        }
    }
}

fn order_list_client_order_id(request: &OrderListRequest) -> Option<String> {
    match request {
        OrderListRequest::Oco {
            list_client_order_id,
            ..
        }
        | OrderListRequest::Oto {
            list_client_order_id,
            ..
        } => list_client_order_id.clone(),
    }
}

fn cryptocom_order_list_params(request: &OrderListRequest) -> ExchangeApiResult<Value> {
    match request {
        OrderListRequest::Oco {
            symbol,
            side,
            quantity,
            above,
            below,
            ..
        } => {
            let instrument_name = normalize_cryptocom_symbol(symbol)?;
            let legs = [
                cryptocom_oco_leg(&instrument_name, *side, quantity, above)?,
                cryptocom_oco_leg(&instrument_name, *side, quantity, below)?,
            ];
            ensure_one_limit_one_trigger(&legs)?;
            Ok(json!({ "order_list": legs }))
        }
        OrderListRequest::Oto {
            symbol,
            working,
            pending,
            ..
        } => {
            let instrument_name = normalize_cryptocom_symbol(symbol)?;
            Ok(json!({
                "order_list": [
                    cryptocom_working_leg(&instrument_name, working)?,
                    cryptocom_pending_leg(&instrument_name, pending)?,
                ],
            }))
        }
    }
}

fn cryptocom_oco_leg(
    instrument_name: &str,
    side: OrderSide,
    quantity: &str,
    leg: &OrderListConditionalLeg,
) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "instrument_name": instrument_name,
        "side": cryptocom_side(side),
        "type": cryptocom_order_list_leg_type(leg.order_type)?,
        "quantity": non_empty("quantity", quantity)?,
    });
    apply_order_list_prices(
        &mut body,
        leg.order_type,
        leg.price.as_deref(),
        leg.stop_price.as_deref(),
    )?;
    if let Some(tif) = cryptocom_time_in_force(leg.time_in_force) {
        body["time_in_force"] = Value::String(tif.to_string());
    }
    if let Some(client_order_id) = leg.client_order_id.as_deref() {
        body["client_oid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn cryptocom_working_leg(
    instrument_name: &str,
    leg: &OrderListOrderLeg,
) -> ExchangeApiResult<Value> {
    let order_type = match leg.order_type {
        OrderListLegType::Market => "MARKET",
        OrderListLegType::Limit | OrderListLegType::LimitMaker => "LIMIT",
        _ => {
            return Err(ExchangeApiError::InvalidRequest {
                message: "cryptocom OTO working leg must be MARKET, LIMIT, or LIMIT_MAKER"
                    .to_string(),
            });
        }
    };
    let mut body = json!({
        "instrument_name": instrument_name,
        "side": cryptocom_side(leg.side),
        "type": order_type,
        "quantity": non_empty("quantity", &leg.quantity)?,
    });
    if leg.order_type != OrderListLegType::Market {
        body["price"] = Value::String(non_empty(
            "working.price",
            leg.price
                .as_deref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "cryptocom OTO working limit leg requires price".to_string(),
                })?,
        )?);
    }
    if leg.order_type == OrderListLegType::LimitMaker {
        body["exec_inst"] = json!(["POST_ONLY"]);
    }
    if let Some(tif) = cryptocom_time_in_force(leg.time_in_force) {
        body["time_in_force"] = Value::String(tif.to_string());
    }
    if let Some(client_order_id) = leg.client_order_id.as_deref() {
        body["client_oid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn cryptocom_pending_leg(
    instrument_name: &str,
    leg: &OrderListOrderLeg,
) -> ExchangeApiResult<Value> {
    if !is_trigger_leg(leg.order_type) {
        return Err(ExchangeApiError::InvalidRequest {
            message: "cryptocom OTO pending leg must be a trigger order".to_string(),
        });
    }
    let mut body = json!({
        "instrument_name": instrument_name,
        "side": cryptocom_side(leg.side),
        "type": cryptocom_order_list_leg_type(leg.order_type)?,
        "quantity": non_empty("quantity", &leg.quantity)?,
    });
    apply_order_list_prices(
        &mut body,
        leg.order_type,
        leg.price.as_deref(),
        leg.stop_price.as_deref(),
    )?;
    if let Some(tif) = cryptocom_time_in_force(leg.time_in_force) {
        body["time_in_force"] = Value::String(tif.to_string());
    }
    if let Some(client_order_id) = leg.client_order_id.as_deref() {
        body["client_oid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn cryptocom_order_list_leg_type(order_type: OrderListLegType) -> ExchangeApiResult<&'static str> {
    Ok(match order_type {
        OrderListLegType::Limit | OrderListLegType::LimitMaker => "LIMIT",
        OrderListLegType::StopLoss => "STOP_LOSS",
        OrderListLegType::StopLossLimit => "STOP_LIMIT",
        OrderListLegType::TakeProfit => "TAKE_PROFIT",
        OrderListLegType::TakeProfitLimit => "TAKE_PROFIT_LIMIT",
        OrderListLegType::Market => {
            return Err(ExchangeApiError::InvalidRequest {
                message: "cryptocom OCO leg cannot be MARKET".to_string(),
            });
        }
    })
}

fn apply_order_list_prices(
    body: &mut Value,
    order_type: OrderListLegType,
    price: Option<&str>,
    stop_price: Option<&str>,
) -> ExchangeApiResult<()> {
    match order_type {
        OrderListLegType::Limit | OrderListLegType::LimitMaker => {
            body["price"] = Value::String(non_empty(
                "price",
                price.ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "cryptocom order-list limit leg requires price".to_string(),
                })?,
            )?);
            if order_type == OrderListLegType::LimitMaker {
                body["exec_inst"] = json!(["POST_ONLY"]);
            }
        }
        OrderListLegType::StopLoss | OrderListLegType::TakeProfit => {
            body["ref_price"] = Value::String(non_empty(
                "stop_price",
                stop_price.ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "cryptocom trigger leg requires stop_price".to_string(),
                })?,
            )?);
        }
        OrderListLegType::StopLossLimit | OrderListLegType::TakeProfitLimit => {
            body["price"] = Value::String(non_empty(
                "price",
                price.ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "cryptocom trigger limit leg requires price".to_string(),
                })?,
            )?);
            body["ref_price"] = Value::String(non_empty(
                "stop_price",
                stop_price.ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "cryptocom trigger limit leg requires stop_price".to_string(),
                })?,
            )?);
        }
        OrderListLegType::Market => {
            return Err(ExchangeApiError::InvalidRequest {
                message: "cryptocom order-list leg cannot be MARKET here".to_string(),
            });
        }
    }
    Ok(())
}

fn ensure_one_limit_one_trigger(legs: &[Value; 2]) -> ExchangeApiResult<()> {
    let limit_count = legs
        .iter()
        .filter(|leg| leg.get("type").and_then(Value::as_str) == Some("LIMIT"))
        .count();
    if limit_count != 1 {
        return Err(ExchangeApiError::InvalidRequest {
            message: "cryptocom OCO requires exactly one LIMIT leg and one trigger leg".to_string(),
        });
    }
    Ok(())
}

fn is_trigger_leg(order_type: OrderListLegType) -> bool {
    matches!(
        order_type,
        OrderListLegType::StopLoss
            | OrderListLegType::StopLossLimit
            | OrderListLegType::TakeProfit
            | OrderListLegType::TakeProfitLimit
    )
}

fn batch_place_order_states(
    exchange_id: &rustcta_types::ExchangeId,
    requests: &[PlaceOrderRequest],
    value: &Value,
) -> ExchangeApiResult<Vec<rustcta_exchange_api::OrderState>> {
    let items = batch_result_items(value)?;
    let mut orders = Vec::with_capacity(requests.len());
    for (index, request) in requests.iter().enumerate() {
        let item = batch_item_for_index(items, index).unwrap_or(&Value::Null);
        let mut order = ack_order_state(exchange_id, request, item);
        if item.get("code").and_then(value_as_i64).unwrap_or(0) != 0 {
            order.status = OrderStatus::Rejected;
        }
        orders.push(order);
    }
    Ok(orders)
}

fn batch_cancel_order_states(
    exchange_id: &rustcta_types::ExchangeId,
    requests: &[CancelOrderRequest],
    value: &Value,
) -> ExchangeApiResult<Vec<rustcta_exchange_api::OrderState>> {
    let items = batch_result_items(value)?;
    let mut orders = Vec::with_capacity(requests.len());
    for (index, request) in requests.iter().enumerate() {
        let item = batch_item_for_index(items, index).unwrap_or(&Value::Null);
        let mut order = cancelled_order_state(
            exchange_id,
            &request.symbol,
            value_as_string(item.get("order_id")).or_else(|| request.exchange_order_id.clone()),
            value_as_string(item.get("client_oid")).or_else(|| request.client_order_id.clone()),
        );
        if item.get("code").and_then(value_as_i64).unwrap_or(0) != 0 {
            order.status = OrderStatus::Rejected;
        }
        orders.push(order);
    }
    Ok(orders)
}

fn batch_result_items(value: &Value) -> ExchangeApiResult<&Vec<Value>> {
    value
        .get("data")
        .or_else(|| value.get("result"))
        .unwrap_or(value)
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("cryptocom batch response is not an array: {value}"),
        })
}

fn batch_item_for_index(items: &[Value], index: usize) -> Option<&Value> {
    items
        .iter()
        .find(|item| item.get("index").and_then(value_as_i64) == Some(index as i64))
        .or_else(|| items.get(index))
}

fn ack_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_as_string(value.get("client_oid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_as_string(value.get("order_id")),
        side: request.side,
        position_side: Some(rustcta_types::PositionSide::None),
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only || request.order_type == OrderType::PostOnly,
        created_at: None,
        updated_at: chrono::Utc::now(),
    }
}

fn quote_ack_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    request: &QuoteMarketOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_as_string(value.get("client_oid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_as_string(value.get("order_id")),
        side: request.side,
        position_side: Some(rustcta_types::PositionSide::None),
        order_type: OrderType::Market,
        time_in_force: None,
        status: OrderStatus::New,
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

fn amend_ack_order_state(
    mut current: rustcta_exchange_api::OrderState,
    request: &AmendOrderRequest,
    current_price: &str,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    current.client_order_id = value_as_string(value.get("client_oid"))
        .or_else(|| request.client_order_id.clone())
        .or(current.client_order_id);
    current.exchange_order_id = value_as_string(value.get("order_id"))
        .or_else(|| request.exchange_order_id.clone())
        .or(current.exchange_order_id);
    current.quantity = request.new_quantity.clone();
    current.price = Some(current_price.to_string());
    if current.status == OrderStatus::Unknown {
        current.status = OrderStatus::New;
    }
    current.updated_at = chrono::Utc::now();
    current
}

fn cryptocom_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn cryptocom_order_type(order_type: OrderType) -> ExchangeApiResult<&'static str> {
    Ok(match order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => "LIMIT",
        OrderType::StopMarket | OrderType::StopLimit => {
            return Err(ExchangeApiError::Unsupported {
                operation: "cryptocom.stop_order",
            });
        }
    })
}

fn cryptocom_time_in_force(tif: Option<TimeInForce>) -> Option<&'static str> {
    match tif {
        Some(TimeInForce::IOC) => Some("IMMEDIATE_OR_CANCEL"),
        Some(TimeInForce::FOK) => Some("FILL_OR_KILL"),
        Some(TimeInForce::GTC) | Some(TimeInForce::GTX) => Some("GOOD_TILL_CANCEL"),
        None => None,
    }
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("cryptocom {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}
