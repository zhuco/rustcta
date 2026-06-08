use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderState, PageCursor, PageRequest, PlaceOrderRequest,
    PlaceOrderResponse, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_coinbaseexchange_symbol;
use super::private_parser::{
    parse_account_balances, parse_fee_snapshots, parse_open_orders, parse_order_state,
    parse_recent_fills,
};
use super::CoinbaseExchangeGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinbaseExchangeGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = coinbaseexchange_order_body(&request)?;
        let value = self
            .send_signed_post(
                "coinbaseexchange.place_order",
                "/orders",
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
        if request.side != OrderSide::Buy {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinbaseexchange.quote_market_sell",
            });
        }
        let body = json!({
            "product_id": normalize_coinbaseexchange_symbol(&request.symbol.exchange_symbol.symbol)?,
            "side": "buy",
            "type": "market",
            "funds": request.quote_quantity,
            "client_oid": request.client_order_id.clone().unwrap_or_default()
        });
        let value = self
            .send_signed_post(
                "coinbaseexchange.place_quote_market_order",
                "/orders",
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
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinbaseexchange cancel_order requires exchange_order_id".to_string(),
            })?;
        let endpoint = format!("/orders/{order_id}");
        let value = self
            .send_signed_delete("coinbaseexchange.cancel_order", &endpoint, &HashMap::new())
            .await?;
        let order = cancel_order_state(
            &self.exchange_id,
            &request.symbol,
            Some(order_id.to_string()),
            request.client_order_id,
            &value,
        );
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
        })
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.orders.is_empty() || request.orders.len() > 20 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinbaseexchange batch_place_orders requires 1..=20 orders".to_string(),
            });
        }
        let mut orders = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            orders.push(self.place_order_impl(order.clone()).await?.order);
        }
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
        if request.cancels.is_empty() || request.cancels.len() > 20 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinbaseexchange batch_cancel_orders requires 1..=20 cancels".to_string(),
            });
        }
        let mut orders = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            orders.push(self.cancel_order_impl(cancel.clone()).await?.order);
        }
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
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinbaseexchange cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "product_id".to_string(),
            normalize_coinbaseexchange_symbol(&symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_signed_delete("coinbaseexchange.cancel_all_orders", "/orders", &params)
            .await?;
        let orders = value
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|id| id.as_str().map(str::to_string))
            .map(|id| cancel_order_state(&self.exchange_id, symbol, Some(id), None, &Value::Null))
            .collect::<Vec<_>>();
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
        self.unsupported_private("coinbaseexchange.amend_order")
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
            .send_signed_get(
                "coinbaseexchange.get_balances",
                "/accounts",
                &HashMap::new(),
            )
            .await?;
        let balances = parse_account_balances(
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
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let value = self
            .send_signed_get("coinbaseexchange.get_fees", "/fees", &HashMap::new())
            .await?;
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_snapshots(&self.exchange_id, &request.symbols, &value),
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
            .or(request.client_order_id.as_deref())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinbaseexchange query_order requires exchange_order_id".to_string(),
            })?;
        let value = self
            .send_signed_get(
                "coinbaseexchange.query_order",
                &format!("/orders/{order_id}"),
                &HashMap::new(),
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
        params.insert("status".to_string(), "open".to_string());
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "product_id".to_string(),
                normalize_coinbaseexchange_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        apply_coinbaseexchange_page_params(&mut params, request.page.as_ref(), None)?;
        let value = self
            .send_signed_get("coinbaseexchange.get_open_orders", "/orders", &params)
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
                message: "coinbaseexchange get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "product_id".to_string(),
            normalize_coinbaseexchange_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("order_id".to_string(), order_id.to_string());
        }
        apply_coinbaseexchange_page_params(&mut params, request.page.as_ref(), request.limit)?;
        let value = self
            .send_signed_get("coinbaseexchange.get_recent_fills", "/fills", &params)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn apply_coinbaseexchange_page_params(
    params: &mut HashMap<String, String>,
    page: Option<&PageRequest>,
    legacy_limit: Option<u32>,
) -> ExchangeApiResult<()> {
    if let Some(page) = page {
        page.validate(Some(100))
            .map_err(|message| ExchangeApiError::InvalidRequest { message })?;
    }
    let limit = page.and_then(|page| page.limit).or(legacy_limit);
    if let Some(limit) = limit {
        if limit == 0 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinbaseexchange limit must be positive".to_string(),
            });
        }
        params.insert("limit".to_string(), limit.min(100).to_string());
    }
    match page.and_then(|page| page.cursor.as_ref()) {
        Some(PageCursor::Token { token }) => {
            params.insert("after".to_string(), token.clone());
        }
        Some(PageCursor::Id { id }) => {
            params.insert("after".to_string(), id.clone());
        }
        Some(_) => {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinbaseexchange pagination supports token/id cursor".to_string(),
            });
        }
        None => {}
    }
    Ok(())
}

fn coinbaseexchange_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinbaseexchange spot order does not support reduce_only".to_string(),
        });
    }
    let mut body = json!({
        "product_id": normalize_coinbaseexchange_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": side(request.side),
        "type": order_type(request.order_type),
        "size": non_empty("quantity", &request.quantity)?,
    });
    if let Some(client_oid) = request
        .client_order_id
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        body["client_oid"] = Value::String(client_oid.to_string());
    }
    if matches!(
        request.order_type,
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK
    ) {
        body["price"] = Value::String(non_empty(
            "price",
            request
                .price
                .as_deref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "coinbaseexchange limit-style order requires price".to_string(),
                })?,
        )?);
    }
    if request.order_type == OrderType::PostOnly
        || request.post_only
        || matches!(request.time_in_force, Some(TimeInForce::GTX))
    {
        body["post_only"] = Value::Bool(true);
    }
    if let Some(tif) = request.time_in_force.and_then(time_in_force) {
        body["time_in_force"] = Value::String(tif.to_string());
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
        client_order_id: value_text(value.get("client_oid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(value.get("id")),
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
        client_order_id: value_text(value.get("client_oid"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(value.get("id")),
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

fn cancel_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    fallback_order_id: Option<String>,
    client_order_id: Option<String>,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id: value_text(value.get("id")).or(fallback_order_id),
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

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("coinbaseexchange {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "market",
        _ => "limit",
    }
}

fn time_in_force(tif: TimeInForce) -> Option<&'static str> {
    match tif {
        TimeInForce::GTC => Some("GTC"),
        TimeInForce::IOC => Some("IOC"),
        TimeInForce::FOK => Some("FOK"),
        TimeInForce::GTX => None,
    }
}

fn value_text(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}
