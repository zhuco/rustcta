use std::collections::HashMap;

use chrono::Utc;
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

use super::parser::normalize_bitget_symbol;
use super::private_parser::{parse_balances, parse_fees, parse_fills, parse_order, parse_orders};
use super::BitgetGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BitgetGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("bitget.place_order")?;
        let body = bitget_place_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post(
                "bitget.place_order",
                "/api/v2/spot/trade/place-order",
                &body,
            )
            .await?;
        let ack = bitget_ack_data(&value);
        let order = order_state_from_place_ack(&self.exchange_id, &request, ack);
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
        self.ensure_private_rest("bitget.place_quote_market_order")?;
        let body = bitget_quote_market_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post(
                "bitget.place_quote_market_order",
                "/api/v2/spot/trade/place-order",
                &body,
            )
            .await?;
        let ack = bitget_ack_data(&value);
        let order = order_state_from_quote_ack(&self.exchange_id, &request, ack);
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
        self.ensure_private_rest("bitget.cancel_order")?;
        let body = bitget_cancel_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post(
                "bitget.cancel_order",
                "/api/v2/spot/trade/cancel-order",
                &body,
            )
            .await?;
        let ack = bitget_ack_data(&value);
        let order = order_state_from_cancel_ack(&self.exchange_id, &request, ack);
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
        self.ensure_private_rest("bitget.cancel_all_orders")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitget.cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let body = json!({
            "symbol": normalize_bitget_symbol(&symbol.exchange_symbol.symbol)?,
        });
        let value = self
            .rest
            .send_signed_post(
                "bitget.cancel_all_orders",
                "/api/v2/spot/trade/cancel-symbol-order",
                &body,
            )
            .await?;
        let ack = bitget_ack_data(&value);
        let order_id = value_text(ack.get("orderId"));
        let client_order_id = value_text(ack.get("clientOid"));
        let mut orders = Vec::new();
        if order_id.is_some() || client_order_id.is_some() {
            orders.push(order_state_from_cancel_fields(
                &self.exchange_id,
                symbol,
                order_id,
                client_order_id,
            ));
        }
        let cancelled_count = if orders.is_empty() {
            0
        } else {
            orders.len() as u32
        };
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
        self.ensure_spot(request.symbol.market_type)?;
        self.ensure_private_rest("bitget.amend_order")?;
        let body = bitget_amend_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("bitget.amend_order", "/api/v3/trade/modify-order", &body)
            .await?;
        let ack = bitget_ack_data(&value);
        let order = order_state_from_amend_ack(&self.exchange_id, &request, ack);
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
        self.ensure_private_rest("bitget.get_balances")?;
        let value = self
            .rest
            .send_signed_get(
                "bitget.get_balances",
                "/api/v2/spot/account/assets",
                &HashMap::new(),
            )
            .await?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_balances requires tenant_id in request context"
                        .to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_balances requires account_id in request context"
                        .to_string(),
                })?;
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

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_private_rest("bitget.get_fees")?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitget.get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_bitget_symbol(&symbol.exchange_symbol.symbol)?,
            );
            params.insert("category".to_string(), "SPOT".to_string());
            let value = self
                .rest
                .send_signed_get("bitget.get_fees", "/api/v3/account/fee-rate", &params)
                .await?;
            fees.extend(parse_fees(&self.exchange_id, symbol, &value)?);
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
        self.ensure_private_rest("bitget.query_order")?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bitget_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        match (&request.exchange_order_id, &request.client_order_id) {
            (Some(exchange_order_id), _) => {
                params.insert("orderId".to_string(), exchange_order_id.clone());
            }
            (None, Some(client_order_id)) => {
                params.insert("clientOid".to_string(), client_order_id.clone());
            }
            (None, None) => {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bitget.query_order requires exchange_order_id or client_order_id"
                        .to_string(),
                });
            }
        }
        let value = self
            .rest
            .send_signed_get(
                "bitget.query_order",
                "/api/v2/spot/trade/orderInfo",
                &params,
            )
            .await?;
        let order = parse_order(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
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
            self.ensure_spot(market_type)?;
        }
        self.ensure_private_rest("bitget.get_open_orders")?;
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_bitget_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .rest
            .send_signed_get(
                "bitget.get_open_orders",
                "/api/v2/spot/trade/unfilled-orders",
                &params,
            )
            .await?;
        let orders = parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?;
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
        self.ensure_private_rest("bitget.get_recent_fills")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitget.get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bitget_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(exchange_order_id) = &request.exchange_order_id {
            params.insert("orderId".to_string(), exchange_order_id.clone());
        }
        if let Some(client_order_id) = &request.client_order_id {
            params.insert("clientOid".to_string(), client_order_id.clone());
        }
        if let Some(from_trade_id) = &request.from_trade_id {
            params.insert("idLessThan".to_string(), from_trade_id.clone());
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
        params.insert(
            "limit".to_string(),
            request.limit.unwrap_or(100).clamp(1, 100).to_string(),
        );
        let value = self
            .rest
            .send_signed_get(
                "bitget.get_recent_fills",
                "/api/v2/spot/trade/fills",
                &params,
            )
            .await?;
        let tenant_id =
            request
                .context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_recent_fills requires tenant_id in request context"
                        .to_string(),
                })?;
        let account_id =
            request
                .context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitget.get_recent_fills requires account_id in request context"
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

fn bitget_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "symbol": normalize_bitget_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": bitget_side(request.side),
        "orderType": if request.order_type == OrderType::Market { "market" } else { "limit" },
        "size": non_empty("quantity", &request.quantity)?,
    });
    if request.order_type != OrderType::Market {
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitget limit-style order requires price".to_string(),
            })?;
        body["price"] = Value::String(non_empty("price", price)?);
    }
    body["force"] = Value::String(bitget_time_in_force(request.time_in_force).to_string());
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitget spot order does not support reduce_only".to_string(),
        });
    }
    Ok(body)
}

fn bitget_quote_market_order_body(request: &QuoteMarketOrderRequest) -> ExchangeApiResult<Value> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitget.quote_market_sell",
        });
    }
    let mut body = json!({
        "symbol": normalize_bitget_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": "buy",
        "orderType": "market",
        "size": non_empty("quote_quantity", &request.quote_quantity)?,
    });
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOid"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn bitget_cancel_order_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "symbol": normalize_bitget_symbol(&request.symbol.exchange_symbol.symbol)?,
    });
    insert_order_identity(
        body.as_object_mut().expect("object"),
        request.exchange_order_id.as_deref(),
        request.client_order_id.as_deref(),
        "cancel_order",
    )?;
    Ok(body)
}

fn bitget_amend_order_body(request: &AmendOrderRequest) -> ExchangeApiResult<Value> {
    if request
        .new_client_order_id
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitget.amend_new_client_order_id",
        });
    }
    let mut body = json!({
        "category": "SPOT",
        "symbol": normalize_bitget_symbol(&request.symbol.exchange_symbol.symbol)?,
        "qty": non_empty("new_quantity", &request.new_quantity)?,
        "autoCancel": "no",
    });
    insert_order_identity(
        body.as_object_mut().expect("object"),
        request.exchange_order_id.as_deref(),
        request.client_order_id.as_deref(),
        "amend_order",
    )?;
    Ok(body)
}

fn order_state_from_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("orderId")),
        side: request.side,
        position_side: request.position_side.or(Some(PositionSide::None)),
        order_type: request.order_type,
        time_in_force: request.time_in_force.or(Some(TimeInForce::GTC)),
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: matches!(request.time_in_force, Some(TimeInForce::GTX)) || request.post_only,
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
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("orderId")),
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
    order_state_from_cancel_fields(
        exchange_id,
        &request.symbol,
        value_text(ack.get("orderId")).or_else(|| request.exchange_order_id.clone()),
        value_text(ack.get("clientOid")).or_else(|| request.client_order_id.clone()),
    )
}

fn order_state_from_cancel_fields(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    order_id: Option<String>,
    client_order_id: Option<String>,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id: order_id,
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
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clientOid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("orderId"))
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

fn bitget_ack_data(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn insert_order_identity(
    body: &mut serde_json::Map<String, Value>,
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
    operation: &str,
) -> ExchangeApiResult<()> {
    if let Some(order_id) = exchange_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        body.insert("orderId".to_string(), Value::String(order_id.to_string()));
    }
    if let Some(client_id) = client_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        body.insert(
            "clientOid".to_string(),
            Value::String(client_id.to_string()),
        );
    }
    if !body.contains_key("orderId") && !body.contains_key("clientOid") {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("bitget {operation} requires exchange_order_id or client_order_id"),
        });
    }
    Ok(())
}

fn bitget_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn bitget_time_in_force(tif: Option<TimeInForce>) -> &'static str {
    match tif {
        Some(TimeInForce::IOC) => "ioc",
        Some(TimeInForce::FOK) => "fok",
        Some(TimeInForce::GTX) => "post_only",
        Some(TimeInForce::GTC) | None => "gtc",
    }
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("bitget {field} must not be empty"),
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
