use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest,
    OpenOrdersResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_kucoin_symbol;
use super::private_parser::{
    parse_account_balances, parse_fee_snapshots, parse_open_orders, parse_order_state,
    parse_recent_fills,
};
use super::KuCoinGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl KuCoinGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = kucoin_order_body(&request)?;
        let value = self
            .send_signed_post(
                "kucoin.place_order",
                "/api/v1/hf/orders",
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = order_state_from_place_ack(&self.exchange_id, &request, &value);
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
        let body = kucoin_quote_market_order_body(&request)?;
        let value = self
            .send_signed_post(
                "kucoin.place_quote_market_order",
                "/api/v1/hf/orders",
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = order_state_from_quote_ack(&self.exchange_id, &request, &value);
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
        self.ensure_spot(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_kucoin_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        let endpoint = if let Some(order_id) = request
            .exchange_order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            format!("/api/v1/hf/orders/{order_id}")
        } else if let Some(client_order_id) = request
            .client_order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            format!("/api/v1/hf/orders/client-order/{client_order_id}")
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "kucoin cancel_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        };
        let value = self
            .send_signed_delete("kucoin.cancel_order", &endpoint, &params)
            .await?;
        let order = order_state_from_cancel_ack(&self.exchange_id, &request, &value);
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
            self.ensure_spot(market_type)?;
        }
        let mut params = HashMap::new();
        let endpoint = if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_kucoin_symbol(&symbol.exchange_symbol.symbol)?,
            );
            "/api/v1/hf/orders"
        } else {
            "/api/v1/hf/orders/cancelAll"
        };
        let value = self
            .send_signed_delete("kucoin.cancel_all_orders", endpoint, &params)
            .await?;
        let orders = kucoin_cancel_all_orders(&self.exchange_id, request.symbol.as_ref(), &value);
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = kucoin_amend_order_body(&request)?;
        let value = self
            .send_signed_post(
                "kucoin.amend_order",
                "/api/v1/hf/orders/alter",
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = order_state_from_amend_ack(&self.exchange_id, &request, &value);
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
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
            .send_signed_get("kucoin.get_balances", "/api/v1/accounts", &HashMap::new())
            .await?;
        let balances = parse_account_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            MarketType::Spot,
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
                message: "kucoin get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "symbols".to_string(),
                normalize_kucoin_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .send_signed_get("kucoin.get_fees", "/api/v1/trade-fees", &params)
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
        self.ensure_spot(request.symbol.market_type)?;
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoin query_order requires exchange_order_id".to_string(),
            })?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_kucoin_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_signed_get(
                "kucoin.query_order",
                &format!("/api/v1/hf/orders/{order_id}"),
                &params,
            )
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
            self.ensure_spot(market_type)?;
        }
        let mut params = HashMap::new();
        params.insert("status".to_string(), "active".to_string());
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_kucoin_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get(
                "kucoin.get_open_orders",
                "/api/v1/hf/orders/active",
                &params,
            )
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
            self.ensure_spot(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoin get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_kucoin_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "startAt".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert("endAt".to_string(), end_time.timestamp_millis().to_string());
        }
        if let Some(limit) = request.limit {
            params.insert("pageSize".to_string(), limit.min(500).to_string());
        }
        let value = self
            .send_signed_get("kucoin.get_recent_fills", "/api/v1/hf/fills", &params)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn kucoin_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "kucoin spot order does not support reduce_only".to_string(),
        });
    }
    let client_order_id =
        request
            .client_order_id
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoin place_order requires client_order_id".to_string(),
            })?;
    let mut body = json!({
        "clientOid": non_empty("client_order_id", client_order_id)?,
        "symbol": normalize_kucoin_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": kucoin_side(request.side),
        "size": non_empty("quantity", &request.quantity)?,
        "tradeType": "TRADE",
    });
    match request.order_type {
        OrderType::Market => {
            body["type"] = Value::String("market".to_string());
        }
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => {
            body["type"] = Value::String("limit".to_string());
            let price =
                request
                    .price
                    .as_deref()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "kucoin limit-style order requires price".to_string(),
                    })?;
            body["price"] = Value::String(non_empty("price", price)?);
            if request.order_type == OrderType::PostOnly
                || matches!(request.time_in_force, Some(TimeInForce::GTX))
                || request.post_only
            {
                body["postOnly"] = Value::Bool(true);
            }
            if let Some(tif) = kucoin_time_in_force(request.order_type, request.time_in_force) {
                body["timeInForce"] = Value::String(tif.to_string());
            }
        }
        OrderType::StopMarket | OrderType::StopLimit => {
            return Err(ExchangeApiError::Unsupported {
                operation: "kucoin.stop_order",
            });
        }
    }
    Ok(body)
}

fn kucoin_quote_market_order_body(request: &QuoteMarketOrderRequest) -> ExchangeApiResult<Value> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeApiError::Unsupported {
            operation: "kucoin.quote_market_sell",
        });
    }
    let client_order_id =
        request
            .client_order_id
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoin place_quote_market_order requires client_order_id".to_string(),
            })?;
    Ok(json!({
        "clientOid": non_empty("client_order_id", client_order_id)?,
        "symbol": normalize_kucoin_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": "buy",
        "type": "market",
        "funds": non_empty("quote_quantity", &request.quote_quantity)?,
        "tradeType": "TRADE",
    }))
}

fn kucoin_amend_order_body(request: &AmendOrderRequest) -> ExchangeApiResult<Value> {
    if request
        .new_client_order_id
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "kucoin.amend_new_client_order_id",
        });
    }
    let mut body = json!({
        "symbol": normalize_kucoin_symbol(&request.symbol.exchange_symbol.symbol)?,
        "newSize": non_empty("new_quantity", &request.new_quantity)?,
    });
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body["orderId"] = Value::String(non_empty("exchange_order_id", order_id)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if body.get("orderId").is_none() && body.get("clientOid").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "kucoin amend_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    Ok(body)
}

fn order_state_from_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(value.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(value.get("orderId").or_else(|| value.get("id"))),
        side: request.side,
        position_side: request.position_side.or(Some(PositionSide::None)),
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: request.post_only || request.order_type == OrderType::PostOnly,
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    }
}

fn order_state_from_quote_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &QuoteMarketOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(value.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(value.get("orderId").or_else(|| value.get("id"))),
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
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    }
}

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> OrderState {
    cancel_order_state_from_fields(
        exchange_id,
        Some(&request.symbol),
        value_text(
            value
                .get("orderId")
                .or_else(|| value.get("cancelledOrderId")),
        )
        .or_else(|| {
            value
                .get("cancelledOrderIds")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(|item| value_text(Some(item)))
        })
        .or_else(|| request.exchange_order_id.clone()),
        request.client_order_id.clone(),
    )
}

fn order_state_from_amend_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &AmendOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(value.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(value.get("newOrderId").or_else(|| value.get("orderId")))
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
        updated_at: chrono::Utc::now(),
    }
}

fn kucoin_cancel_all_orders(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: Option<&rustcta_exchange_api::SymbolScope>,
    value: &Value,
) -> Vec<OrderState> {
    if let Some(ids) = value.get("cancelledOrderIds").and_then(Value::as_array) {
        return ids
            .iter()
            .filter_map(|id| value_text(Some(id)))
            .map(|id| cancel_order_state_from_fields(exchange_id, symbol, Some(id), None))
            .collect();
    }
    value
        .get("items")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    cancel_order_state_from_fields(
                        exchange_id,
                        symbol,
                        value_text(item.get("orderId").or_else(|| item.get("id"))),
                        value_text(item.get("clientOid")),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn cancel_order_state_from_fields(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: Option<&rustcta_exchange_api::SymbolScope>,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> OrderState {
    let exchange_symbol = symbol
        .map(|symbol| symbol.exchange_symbol.clone())
        .unwrap_or_else(|| {
            rustcta_types::ExchangeSymbol::new(exchange_id.clone(), MarketType::Spot, "UNKNOWN")
                .expect("fallback symbol")
        });
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.and_then(|symbol| symbol.canonical_symbol.clone()),
        exchange_symbol,
        client_order_id,
        exchange_order_id,
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
        updated_at: chrono::Utc::now(),
    }
}

fn kucoin_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn kucoin_time_in_force(order_type: OrderType, tif: Option<TimeInForce>) -> Option<&'static str> {
    match (order_type, tif) {
        (OrderType::IOC, _) | (_, Some(TimeInForce::IOC)) => Some("IOC"),
        (OrderType::FOK, _) | (_, Some(TimeInForce::FOK)) => Some("FOK"),
        _ => None,
    }
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("kucoin {field} must not be empty"),
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
