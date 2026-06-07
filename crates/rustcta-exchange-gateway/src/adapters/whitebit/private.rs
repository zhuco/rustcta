use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, PlaceOrderRequest, PlaceOrderResponse,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_whitebit_symbol;
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_open_orders, parse_order_state, parse_positions,
    parse_recent_fills,
};
use super::WhiteBitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl WhiteBitGatewayAdapter {
    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.orders.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "whitebit batch_place_orders requires at least one order".to_string(),
            });
        }
        if request.orders.len() > 20 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "whitebit batch_place_orders supports at most 20 orders".to_string(),
            });
        }
        let market_type = whitebit_batch_market_type(&request.orders)?;
        let endpoint = match market_type {
            MarketType::Spot => "/api/v4/order/bulk",
            MarketType::Perpetual => "/api/v4/order/collateral/bulk",
            _ => unreachable!("checked by whitebit_batch_market_type"),
        };
        let mut orders = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            orders.push(whitebit_batch_order_body(order)?);
        }
        let value = self
            .send_signed_post(
                "whitebit.batch_place_orders",
                endpoint,
                &json!({
                    "orders": orders,
                    "stopOnFail": false,
                }),
            )
            .await?;
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: whitebit_batch_place_orders(&self.exchange_id, &request.orders, &value),
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
                message: "whitebit batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        if request.cancels.len() > 100 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "whitebit batch_cancel_orders supports at most 100 orders".to_string(),
            });
        }
        let mut orders = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            orders.push(whitebit_batch_cancel_item(cancel)?);
        }
        let value = self
            .send_signed_post(
                "whitebit.batch_cancel_orders",
                "/api/v4/order/cancel/bulk",
                &json!({ "orders": orders }),
            )
            .await?;
        let orders = whitebit_batch_cancel_orders(&self.exchange_id, &request.cancels, &value);
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: None,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let (endpoint, body) = whitebit_order_endpoint_and_body(&request)?;
        let value = self
            .send_signed_post("whitebit.place_order", endpoint, &body)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
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
        self.ensure_spot(request.symbol.market_type)?;
        let body = whitebit_quote_market_order_body(&request)?;
        let value = self
            .send_signed_post(
                "whitebit.place_quote_market_order",
                "/api/v4/order/market",
                &body,
            )
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
        self.ensure_supported_market(request.symbol.market_type)?;
        let body = whitebit_cancel_order_body(&request)?;
        let value = self
            .send_signed_post("whitebit.cancel_order", "/api/v4/order/cancel", &body)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| whitebit_cancel_order_state(&self.exchange_id, &request, &value));
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
            self.ensure_supported_market(market_type)?;
        }
        let body = whitebit_cancel_all_body(&request)?;
        let value = self
            .send_signed_post(
                "whitebit.cancel_all_orders",
                "/api/v4/order/cancel/all",
                &body,
            )
            .await?;
        let orders = if let Some(symbol) = request.symbol.as_ref() {
            whitebit_cancel_all_orders(&self.exchange_id, symbol, &value)
        } else {
            Vec::new()
        };
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "whitebit.amend_order_unverified",
        })
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut body = json!({});
        if let Some(asset) = request.assets.first() {
            body["ticker"] = Value::String(non_empty("asset", asset)?.to_ascii_uppercase());
        }
        let endpoint = if market_type == MarketType::Perpetual {
            "/api/v4/collateral-account/balance"
        } else {
            "/api/v4/trade-account/balance"
        };
        let value = self
            .send_signed_post("whitebit.get_balances", endpoint, &body)
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

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "whitebit get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
        }
        let value = self
            .send_signed_post("whitebit.get_fees", "/api/v4/market/fee", &json!({}))
            .await?;
        let fees = parse_fee_snapshots(&self.exchange_id, &request.symbols, &value)?;
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
        self.ensure_supported_market(request.symbol.market_type)?;
        let mut body = json!({
            "market": normalize_whitebit_symbol(&request.symbol.exchange_symbol.symbol)?,
            "limit": 1,
            "offset": 0,
        });
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            body["orderId"] = Value::String(non_empty("exchange_order_id", order_id)?);
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            body["clientOrderId"] = Value::String(non_empty("client_order_id", client_order_id)?);
        }
        if body.get("orderId").is_none() && body.get("clientOrderId").is_none() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "whitebit query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .send_signed_post("whitebit.query_order", "/api/v4/orders", &body)
            .await?;
        let order = parse_open_orders(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_default()
            .into_iter()
            .next()
            .or_else(|| parse_order_state(&self.exchange_id, Some(&request.symbol), &value).ok());
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market(market_type)?;
        }
        let mut body = json!({
            "limit": 100,
            "offset": 0,
        });
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            body["market"] =
                Value::String(normalize_whitebit_symbol(&symbol.exchange_symbol.symbol)?);
        }
        let value = self
            .send_signed_post("whitebit.get_open_orders", "/api/v4/orders", &body)
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
            self.ensure_supported_market(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "whitebit get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut body = json!({
            "market": normalize_whitebit_symbol(&symbol.exchange_symbol.symbol)?,
            "limit": request.limit.unwrap_or(100).min(100),
            "offset": 0,
        });
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            body["orderId"] = Value::String(non_empty("exchange_order_id", order_id)?);
        }
        if let Some(start_time) = request.start_time {
            body["start"] = Value::String(start_time.timestamp().to_string());
        }
        if let Some(end_time) = request.end_time {
            body["end"] = Value::String(end_time.timestamp().to_string());
        }
        let endpoint = "/api/v4/trade-account/order";
        let value = self
            .send_signed_post("whitebit.get_recent_fills", endpoint, &body)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }

    pub(super) async fn get_positions_impl(
        &self,
        request: rustcta_exchange_api::PositionsRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            if market_type != MarketType::Perpetual {
                return Err(ExchangeApiError::Unsupported {
                    operation: "whitebit.non_perpetual_positions",
                });
            }
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut positions = Vec::new();
        if request.symbols.is_empty() {
            let value = self
                .send_signed_post(
                    "whitebit.get_positions",
                    "/api/v4/collateral-account/positions/open",
                    &json!({}),
                )
                .await?;
            positions.extend(parse_positions(
                &self.exchange_id,
                tenant_id,
                account_id,
                None,
                &value,
            )?);
        } else {
            for symbol in &request.symbols {
                if symbol.exchange_id != self.exchange_id {
                    return Err(ExchangeApiError::InvalidRequest {
                        message: format!(
                            "whitebit adapter cannot serve request for exchange {}",
                            symbol.exchange_id
                        ),
                    });
                }
                let value = self
                    .send_signed_post(
                        "whitebit.get_positions",
                        "/api/v4/collateral-account/positions/open",
                        &json!({ "market": normalize_whitebit_symbol(&symbol.symbol)? }),
                    )
                    .await?;
                positions.extend(parse_positions(
                    &self.exchange_id,
                    tenant_id.clone(),
                    account_id.clone(),
                    Some(symbol),
                    &value,
                )?);
            }
        }
        Ok(rustcta_exchange_api::PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions,
        })
    }
}

fn whitebit_order_endpoint_and_body(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<(&'static str, Value)> {
    if request.reduce_only && request.symbol.market_type == MarketType::Spot {
        return Err(ExchangeApiError::InvalidRequest {
            message: "whitebit spot order does not support reduce_only".to_string(),
        });
    }
    let endpoint = whitebit_order_endpoint(request.symbol.market_type, request.order_type)?;
    let mut body = json!({
        "market": normalize_whitebit_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": whitebit_side(request.side),
        "amount": non_empty("quantity", &request.quantity)?,
    });
    if matches!(
        request.order_type,
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC
    ) {
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "whitebit limit-style order requires price".to_string(),
            })?;
        body["price"] = Value::String(non_empty("price", price)?);
    }
    if request.order_type == OrderType::PostOnly
        || request.post_only
        || request.time_in_force == Some(TimeInForce::GTX)
    {
        body["postOnly"] = Value::Bool(true);
    }
    if request.order_type == OrderType::IOC || request.time_in_force == Some(TimeInForce::IOC) {
        body["ioc"] = Value::Bool(true);
    }
    if request.time_in_force == Some(TimeInForce::FOK) || request.order_type == OrderType::FOK {
        return Err(ExchangeApiError::Unsupported {
            operation: "whitebit.fok_order",
        });
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOrderId"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if request.symbol.market_type == MarketType::Perpetual {
        body["reduceOnly"] = Value::Bool(request.reduce_only);
        body["positionSide"] = Value::String(whitebit_position_side(request.position_side));
    }
    Ok((endpoint, body))
}

fn whitebit_batch_market_type(orders: &[PlaceOrderRequest]) -> ExchangeApiResult<MarketType> {
    let first = orders
        .first()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "whitebit batch_place_orders requires at least one order".to_string(),
        })?;
    let market_type = first.symbol.market_type;
    if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
        return Err(ExchangeApiError::Unsupported {
            operation: "whitebit.batch_place_orders_market_type",
        });
    }
    let market = normalize_whitebit_symbol(&first.symbol.exchange_symbol.symbol)?;
    for order in orders {
        if order.symbol.market_type != market_type {
            return Err(ExchangeApiError::InvalidRequest {
                message: "whitebit batch_place_orders cannot mix market types".to_string(),
            });
        }
        if normalize_whitebit_symbol(&order.symbol.exchange_symbol.symbol)? != market {
            return Err(ExchangeApiError::InvalidRequest {
                message: "whitebit batch_place_orders requires one market pair".to_string(),
            });
        }
    }
    Ok(market_type)
}

fn whitebit_batch_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.order_type == OrderType::Market {
        return Err(ExchangeApiError::Unsupported {
            operation: "whitebit.batch_market_order",
        });
    }
    let (endpoint, mut body) = whitebit_order_endpoint_and_body(request)?;
    if !matches!(
        endpoint,
        "/api/v4/order/new" | "/api/v4/order/collateral/limit"
    ) {
        return Err(ExchangeApiError::Unsupported {
            operation: "whitebit.batch_order_type",
        });
    }
    if request.symbol.market_type == MarketType::Perpetual {
        body["reduceOnly"] = Value::Bool(request.reduce_only);
        body["positionSide"] = Value::String(whitebit_position_side(request.position_side));
    }
    Ok(body)
}

fn whitebit_batch_cancel_item(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "market": normalize_whitebit_symbol(&request.symbol.exchange_symbol.symbol)?,
    });
    match (
        request.exchange_order_id.as_deref(),
        request.client_order_id.as_deref(),
    ) {
        (Some(order_id), None) => {
            body["orderId"] = Value::String(non_empty("exchange_order_id", order_id)?);
        }
        (None, Some(client_order_id)) => {
            body["clientOrderId"] = Value::String(non_empty("client_order_id", client_order_id)?);
        }
        (Some(_), Some(_)) => {
            return Err(ExchangeApiError::InvalidRequest {
                message: "whitebit batch_cancel_orders requires exactly one order id per item"
                    .to_string(),
            });
        }
        (None, None) => {
            return Err(ExchangeApiError::InvalidRequest {
                message:
                    "whitebit batch_cancel_orders requires exchange_order_id or client_order_id"
                        .to_string(),
            });
        }
    }
    Ok(body)
}

fn whitebit_batch_place_orders(
    exchange_id: &rustcta_types::ExchangeId,
    requests: &[PlaceOrderRequest],
    value: &Value,
) -> Vec<rustcta_exchange_api::OrderState> {
    whitebit_batch_items(value)
        .into_iter()
        .enumerate()
        .filter_map(|(index, item)| {
            if item.get("error").is_some_and(|error| !error.is_null()) {
                return None;
            }
            let result = item.get("result").unwrap_or(item);
            let request = requests.get(index).or_else(|| requests.first())?;
            Some(
                parse_order_state(exchange_id, Some(&request.symbol), result).unwrap_or_else(
                    |_| whitebit_order_state_from_place_request(exchange_id, request, result),
                ),
            )
        })
        .collect()
}

fn whitebit_batch_cancel_orders(
    exchange_id: &rustcta_types::ExchangeId,
    requests: &[CancelOrderRequest],
    value: &Value,
) -> Vec<rustcta_exchange_api::OrderState> {
    whitebit_batch_items(value)
        .into_iter()
        .enumerate()
        .filter_map(|(index, item)| {
            if item.get("error").is_some_and(|error| !error.is_null()) {
                return None;
            }
            let result = item.get("result").unwrap_or(item);
            let request = requests.get(index).or_else(|| requests.first())?;
            Some(
                parse_order_state(exchange_id, Some(&request.symbol), result)
                    .unwrap_or_else(|_| whitebit_cancel_order_state(exchange_id, request, result)),
            )
        })
        .collect()
}

fn whitebit_batch_items(value: &Value) -> Vec<&Value> {
    if let Some(items) = value.as_array() {
        return items.iter().collect();
    }
    value
        .get("records")
        .or_else(|| value.get("orders"))
        .and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .unwrap_or_else(|| vec![value])
}

fn whitebit_order_state_from_place_request(
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
        client_order_id: value_text(value.get("clientOrderId"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(value.get("orderId").or_else(|| value.get("id"))),
        side: request.side,
        position_side: Some(request.position_side.unwrap_or(
            if request.symbol.market_type == MarketType::Perpetual {
                PositionSide::Net
            } else {
                PositionSide::None
            },
        )),
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only,
        created_at: None,
        updated_at: chrono::Utc::now(),
    }
}

fn whitebit_quote_market_order_body(request: &QuoteMarketOrderRequest) -> ExchangeApiResult<Value> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeApiError::Unsupported {
            operation: "whitebit.quote_market_sell",
        });
    }
    let mut body = json!({
        "market": normalize_whitebit_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": "buy",
        "amount": non_empty("quote_quantity", &request.quote_quantity)?,
    });
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOrderId"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn whitebit_cancel_order_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "market": normalize_whitebit_symbol(&request.symbol.exchange_symbol.symbol)?,
    });
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body["orderId"] = Value::String(non_empty("exchange_order_id", order_id)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOrderId"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if body.get("orderId").is_none() && body.get("clientOrderId").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "whitebit cancel_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(body)
}

fn whitebit_cancel_all_body(request: &CancelAllOrdersRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({});
    if let Some(symbol) = &request.symbol {
        body["market"] = Value::String(normalize_whitebit_symbol(&symbol.exchange_symbol.symbol)?);
    }
    let market_type = request
        .symbol
        .as_ref()
        .map(|symbol| symbol.market_type)
        .or(request.market_type)
        .unwrap_or(MarketType::Spot);
    body["type"] = json!([match market_type {
        MarketType::Perpetual => "futures",
        MarketType::Spot => "spot",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "whitebit.cancel_all_market_type",
            });
        }
    }]);
    Ok(body)
}

fn whitebit_cancel_all_orders(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> Vec<rustcta_exchange_api::OrderState> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    parse_order_state(exchange_id, Some(symbol), item).unwrap_or_else(|_| {
                        whitebit_cancel_order_state_from_fields(
                            exchange_id,
                            symbol,
                            value_text(item.get("orderId").or_else(|| item.get("id"))),
                            value_text(item.get("clientOrderId")),
                        )
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn whitebit_cancel_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    whitebit_cancel_order_state_from_fields(
        exchange_id,
        &request.symbol,
        value_text(value.get("orderId").or_else(|| value.get("id")))
            .or_else(|| request.exchange_order_id.clone()),
        value_text(value.get("clientOrderId")).or_else(|| request.client_order_id.clone()),
    )
}

fn whitebit_cancel_order_state_from_fields(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
        side: OrderSide::Buy,
        position_side: Some(rustcta_types::PositionSide::None),
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
        updated_at: chrono::Utc::now(),
    }
}

fn whitebit_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn whitebit_order_endpoint(
    market_type: MarketType,
    order_type: OrderType,
) -> ExchangeApiResult<&'static str> {
    Ok(match order_type {
        _ if market_type == MarketType::Spot && order_type == OrderType::Market => {
            "/api/v4/order/market"
        }
        _ if market_type == MarketType::Spot
            && matches!(
                order_type,
                OrderType::Limit | OrderType::PostOnly | OrderType::IOC
            ) =>
        {
            "/api/v4/order/new"
        }
        _ if market_type == MarketType::Perpetual && order_type == OrderType::Market => {
            "/api/v4/order/collateral/market"
        }
        _ if market_type == MarketType::Perpetual
            && matches!(
                order_type,
                OrderType::Limit | OrderType::PostOnly | OrderType::IOC
            ) =>
        {
            "/api/v4/order/collateral/limit"
        }
        _ if matches!(order_type, OrderType::StopMarket | OrderType::StopLimit) => {
            return Err(ExchangeApiError::Unsupported {
                operation: "whitebit.stop_order",
            });
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "whitebit.order_type",
            });
        }
    })
}

fn whitebit_position_side(side: Option<PositionSide>) -> String {
    match side.unwrap_or(PositionSide::Net) {
        PositionSide::Long => "LONG",
        PositionSide::Short => "SHORT",
        PositionSide::Net | PositionSide::None => "BOTH",
    }
    .to_string()
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("whitebit {field} must not be empty"),
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
