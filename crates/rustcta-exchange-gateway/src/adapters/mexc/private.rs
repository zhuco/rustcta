use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelAllOrdersRequest, CancelAllOrdersResponse,
    CancelOrderRequest, CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest,
    FeesResponse, OpenOrdersRequest, OpenOrdersResponse, OrderState, PlaceOrderRequest,
    PlaceOrderResponse, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::Value;

use super::parser::normalize_mexc_symbol;
use super::private_parser::{
    parse_account_balances, parse_fee_snapshots, parse_open_orders, parse_order_state,
    parse_recent_fills,
};
use super::MexcGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl MexcGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let params = mexc_place_order_params(&request)?;
        let value = self
            .send_signed_post("mexc.place_order", "/api/v3/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| order_state_from_place_ack(&self.exchange_id, &request, &value));
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
        let params = mexc_quote_market_order_params(&request)?;
        let value = self
            .send_signed_post("mexc.place_quote_market_order", "/api/v3/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| order_state_from_quote_ack(&self.exchange_id, &request, &value));
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
        let params = mexc_cancel_order_params(&request)?;
        let value = self
            .send_signed_delete("mexc.cancel_order", "/api/v3/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| order_state_from_cancel_ack(&self.exchange_id, &request, &value));
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_mexc_symbol(&symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_signed_delete("mexc.cancel_all_orders", "/api/v3/openOrders", &params)
            .await?;
        let orders = mexc_cancel_all_orders(&self.exchange_id, symbol, &value)?;
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
            .send_signed_get("mexc.get_balances", "/api/v3/account", &HashMap::new())
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
                message: "mexc get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_mexc_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .send_signed_get("mexc.get_fees", "/api/v3/tradeFee", &params)
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
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_mexc_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("origClientOrderId".to_string(), client_order_id.to_string());
        }
        if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "mexc query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .send_signed_get("mexc.query_order", "/api/v3/order", &params)
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
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_mexc_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get("mexc.get_open_orders", "/api/v3/openOrders", &params)
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
                message: "mexc get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_mexc_symbol(&symbol.exchange_symbol.symbol)?,
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
            .send_signed_get("mexc.get_recent_fills", "/api/v3/myTrades", &params)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn mexc_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "mexc spot order does not support reduce_only".to_string(),
        });
    }
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_mexc_symbol(&request.symbol.exchange_symbol.symbol)?,
    );
    params.insert("side".to_string(), mexc_side(request.side).to_string());
    params.insert(
        "type".to_string(),
        mexc_order_type(request.order_type, request.time_in_force)?.to_string(),
    );
    params.insert(
        "quantity".to_string(),
        non_empty("quantity", &request.quantity)?,
    );
    if request.order_type != OrderType::Market {
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc limit-style order requires price".to_string(),
            })?;
        params.insert("price".to_string(), non_empty("price", price)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert(
            "newClientOrderId".to_string(),
            non_empty("client_order_id", client_order_id)?,
        );
    }
    if let Some(time_in_force) = request.time_in_force {
        params.insert(
            "timeInForce".to_string(),
            mexc_time_in_force(time_in_force).to_string(),
        );
    }
    Ok(params)
}

fn mexc_quote_market_order_params(
    request: &QuoteMarketOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeApiError::Unsupported {
            operation: "mexc.quote_market_sell",
        });
    }
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_mexc_symbol(&request.symbol.exchange_symbol.symbol)?,
    );
    params.insert("side".to_string(), "BUY".to_string());
    params.insert("type".to_string(), "MARKET".to_string());
    params.insert(
        "quoteOrderQty".to_string(),
        non_empty("quote_quantity", &request.quote_quantity)?,
    );
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert(
            "newClientOrderId".to_string(),
            non_empty("client_order_id", client_order_id)?,
        );
    }
    Ok(params)
}

fn mexc_cancel_order_params(
    request: &CancelOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_mexc_symbol(&request.symbol.exchange_symbol.symbol)?,
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        params.insert(
            "orderId".to_string(),
            non_empty("exchange_order_id", order_id)?,
        );
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert(
            "origClientOrderId".to_string(),
            non_empty("client_order_id", client_order_id)?,
        );
    }
    if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: "mexc cancel_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    Ok(params)
}

fn mexc_cancel_all_orders(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<Vec<OrderState>> {
    let items = value
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mexc cancel-all response is not an array".to_string(),
        })?;
    let mut orders = Vec::new();
    for item in items {
        if let Some(reports) = item.get("orderReports").and_then(Value::as_array) {
            for report in reports {
                orders.push(
                    parse_order_state(exchange_id, Some(symbol), report).unwrap_or_else(|_| {
                        order_state_from_cancel_fields(
                            exchange_id,
                            symbol,
                            value_text(report.get("orderId")),
                            value_text(report.get("clientOrderId")),
                        )
                    }),
                );
            }
        } else {
            orders.push(
                parse_order_state(exchange_id, Some(symbol), item).unwrap_or_else(|_| {
                    order_state_from_cancel_fields(
                        exchange_id,
                        symbol,
                        value_text(item.get("orderId")),
                        value_text(item.get("clientOrderId")),
                    )
                }),
            );
        }
    }
    Ok(orders)
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
        client_order_id: value_text(value.get("clientOrderId"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(value.get("orderId")),
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
        created_at: chrono::Utc::now().into(),
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
        client_order_id: value_text(value.get("clientOrderId"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(value.get("orderId")),
        side: request.side,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Market,
        time_in_force: None,
        status: OrderStatus::New,
        quantity: value_text(value.get("origQty"))
            .unwrap_or_else(|| request.quote_quantity.clone()),
        price: None,
        filled_quantity: value_text(value.get("executedQty")).unwrap_or_else(|| "0".to_string()),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: chrono::Utc::now().into(),
        updated_at: chrono::Utc::now(),
    }
}

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> OrderState {
    order_state_from_cancel_fields(
        exchange_id,
        &request.symbol,
        value_text(value.get("orderId")).or_else(|| request.exchange_order_id.clone()),
        value_text(value.get("clientOrderId")).or_else(|| request.client_order_id.clone()),
    )
}

fn order_state_from_cancel_fields(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
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

fn mexc_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn mexc_order_type(
    order_type: OrderType,
    tif: Option<TimeInForce>,
) -> ExchangeApiResult<&'static str> {
    Ok(match (order_type, tif) {
        (OrderType::PostOnly, _) | (_, Some(TimeInForce::GTX)) => "LIMIT_MAKER",
        (OrderType::Market, _) => "MARKET",
        (OrderType::Limit | OrderType::IOC | OrderType::FOK, _) => "LIMIT",
        (OrderType::StopMarket | OrderType::StopLimit, _) => {
            return Err(ExchangeApiError::Unsupported {
                operation: "mexc.stop_order",
            });
        }
    })
}

fn mexc_time_in_force(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTX",
    }
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("mexc {field} must not be empty"),
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
