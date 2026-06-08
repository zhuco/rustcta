use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelAllOrdersRequest, CancelAllOrdersResponse,
    CancelOrderRequest, CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest,
    FeesResponse, OpenOrdersRequest, OpenOrdersResponse, PageCursor, PageRequest,
    PlaceOrderRequest, PlaceOrderResponse, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_open_orders, parse_order_state, parse_recent_fills,
};
use super::CoinoneGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinoneGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = coinone_order_body(
            &request,
            self.access_token("coinone.place_order")?,
            self.next_nonce(),
        )?;
        let value = self
            .send_signed_post("coinone.place_order", "/v2.1/order", &body)
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
        if request.side != OrderSide::Buy {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinone.quote_market_sell",
            });
        }
        let (base, quote) = self.ensure_krw_symbol(&request.symbol.exchange_symbol.symbol)?;
        let body = json!({
            "access_token": self.access_token("coinone.place_quote_market_order")?,
            "nonce": self.next_nonce(),
            "quote_currency": quote,
            "target_currency": base,
            "type": "MARKET",
            "side": "BUY",
            "amount": non_empty("quote_quantity", &request.quote_quantity)?,
            "user_order_id": request.client_order_id.as_deref().unwrap_or_default(),
        });
        let value = self
            .send_signed_post("coinone.place_quote_market_order", "/v2.1/order", &body)
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
        let body = coinone_cancel_order_body(self, &request)?;
        let value = self
            .send_signed_post("coinone.cancel_order", "/v2.1/order/cancel", &body)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .ok()
            .filter(|order| order.status != OrderStatus::Unknown)
            .unwrap_or_else(|| coinone_cancel_order_state(&self.exchange_id, &request, &value));
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
                message: "coinone cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (base, quote) = self.ensure_krw_symbol(&symbol.exchange_symbol.symbol)?;
        let body = json!({
            "access_token": self.access_token("coinone.cancel_all_orders")?,
            "nonce": self.next_nonce(),
            "quote_currency": quote,
            "target_currency": base,
        });
        let value = self
            .send_signed_post("coinone.cancel_all_orders", "/v2.1/order/cancel/all", &body)
            .await?;
        let orders = parse_open_orders(&self.exchange_id, Some(symbol), &value).unwrap_or_default();
        let cancelled_count = value
            .get("cancelled_count")
            .and_then(Value::as_u64)
            .map(|value| value as u32)
            .unwrap_or(orders.len() as u32);
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count,
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
        let body = json!({
            "access_token": self.access_token("coinone.get_balances")?,
            "nonce": self.next_nonce(),
        });
        let value = self
            .send_signed_post("coinone.get_balances", "/v2.1/account/balance/all", &body)
            .await?;
        let balances = parse_balances(
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
                message: "coinone get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            let (base, quote) = self.ensure_krw_symbol(&symbol.exchange_symbol.symbol)?;
            let body = json!({
                "access_token": self.access_token("coinone.get_fees")?,
                "nonce": self.next_nonce(),
                "quote_currency": quote,
                "target_currency": base,
            });
            let value = self
                .send_signed_post("coinone.get_fees", "/v2.1/account/trade_fee", &body)
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
        let (base, quote) = self.ensure_krw_symbol(&request.symbol.exchange_symbol.symbol)?;
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinone query_order requires exchange_order_id".to_string(),
            })?;
        let body = json!({
            "access_token": self.access_token("coinone.query_order")?,
            "nonce": self.next_nonce(),
            "quote_currency": quote,
            "target_currency": base,
            "order_id": order_id,
        });
        let value = self
            .send_signed_post("coinone.query_order", "/v2.1/order/detail", &body)
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinone get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (base, quote) = self.ensure_krw_symbol(&symbol.exchange_symbol.symbol)?;
        let mut body = json!({
            "access_token": self.access_token("coinone.get_open_orders")?,
            "nonce": self.next_nonce(),
            "quote_currency": quote,
            "target_currency": base,
        });
        apply_coinone_page_params(&mut body, request.page.as_ref(), None)?;
        let value = self
            .send_signed_post("coinone.get_open_orders", "/v2.1/order/open_orders", &body)
            .await?;
        let orders = parse_open_orders(&self.exchange_id, Some(symbol), &value)?;
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
                message: "coinone get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let (base, quote) = self.ensure_krw_symbol(&symbol.exchange_symbol.symbol)?;
        let mut body = json!({
            "access_token": self.access_token("coinone.get_recent_fills")?,
            "nonce": self.next_nonce(),
            "quote_currency": quote,
            "target_currency": base,
        });
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            body["order_id"] = Value::String(non_empty("exchange_order_id", order_id)?);
        }
        if let Some(start_time) = request.start_time {
            body["from_ts"] = Value::Number(start_time.timestamp_millis().into());
        }
        if let Some(end_time) = request.end_time {
            body["to_ts"] = Value::Number(end_time.timestamp_millis().into());
        }
        apply_coinone_page_params(&mut body, request.page.as_ref(), request.limit)?;
        let value = self
            .send_signed_post(
                "coinone.get_recent_fills",
                "/v2.1/order/complete_orders",
                &body,
            )
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn apply_coinone_page_params(
    body: &mut Value,
    page: Option<&PageRequest>,
    legacy_limit: Option<u32>,
) -> ExchangeApiResult<()> {
    if let Some(page) = page {
        page.validate(Some(500))
            .map_err(|message| ExchangeApiError::InvalidRequest { message })?;
    }
    if legacy_limit == Some(0) {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinone pagination limit must be positive".to_string(),
        });
    }
    if let Some(limit) = page.and_then(|page| page.limit).or(legacy_limit) {
        body["size"] = Value::Number(limit.min(500).into());
    }
    let Some(cursor) = page.and_then(|page| page.cursor.as_ref()) else {
        return Ok(());
    };
    match cursor {
        PageCursor::Offset { offset } => {
            body["page"] = Value::Number((offset + 1).into());
        }
        PageCursor::Token { token } => {
            let page = token
                .parse::<u64>()
                .map_err(|_| ExchangeApiError::InvalidRequest {
                    message: "coinone pagination token cursor must be a page number".to_string(),
                })?;
            body["page"] = Value::Number(page.into());
        }
        PageCursor::Id { id } => {
            body["from_order_id"] = Value::String(id.clone());
        }
        PageCursor::Timestamp { millis } => {
            body["from_ts"] = Value::Number((*millis).into());
        }
        PageCursor::TimeRange { start_ms, end_ms } => {
            body["from_ts"] = Value::Number((*start_ms).into());
            if let Some(end) = end_ms {
                body["to_ts"] = Value::Number((*end).into());
            }
        }
    }
    Ok(())
}

fn coinone_order_body(
    request: &PlaceOrderRequest,
    access_token: &str,
    nonce: String,
) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinone spot order does not support reduce_only".to_string(),
        });
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinone.post_only_order",
        });
    }
    let (base, quote) =
        super::parser::normalize_coinone_symbol(&request.symbol.exchange_symbol.symbol)?;
    let mut body = json!({
        "access_token": access_token,
        "nonce": nonce,
        "quote_currency": quote,
        "target_currency": base,
        "side": coinone_side(request.side),
        "qty": non_empty("quantity", &request.quantity)?,
    });
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["user_order_id"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    match request.order_type {
        OrderType::Market => {
            body["type"] = Value::String("MARKET".to_string());
        }
        OrderType::Limit | OrderType::IOC | OrderType::FOK => {
            body["type"] = Value::String("LIMIT".to_string());
            let price =
                request
                    .price
                    .as_deref()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "coinone limit-style order requires price".to_string(),
                    })?;
            body["price"] = Value::String(non_empty("price", price)?);
            if let Some(tif) = coinone_time_in_force(request.order_type, request.time_in_force) {
                body["time_in_force"] = Value::String(tif.to_string());
            }
        }
        OrderType::PostOnly | OrderType::StopMarket | OrderType::StopLimit => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinone.unsupported_order_type",
            });
        }
    }
    Ok(body)
}

fn coinone_cancel_order_body(
    adapter: &CoinoneGatewayAdapter,
    request: &CancelOrderRequest,
) -> ExchangeApiResult<Value> {
    let (base, quote) = adapter.ensure_krw_symbol(&request.symbol.exchange_symbol.symbol)?;
    let order_id = request
        .exchange_order_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinone cancel_order requires exchange_order_id".to_string(),
        })?;
    Ok(json!({
        "access_token": adapter.access_token("coinone.cancel_order")?,
        "nonce": adapter.next_nonce(),
        "quote_currency": quote,
        "target_currency": base,
        "order_id": order_id,
    }))
}

fn order_state_from_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(value.get("user_order_id"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(value.get("order_id").or_else(|| value.get("id"))),
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
        post_only: false,
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    }
}

fn order_state_from_quote_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &QuoteMarketOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(value.get("user_order_id"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(value.get("order_id").or_else(|| value.get("id"))),
        side: request.side,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Market,
        time_in_force: None,
        status: OrderStatus::New,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    }
}

fn coinone_cancel_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: value_text(value.get("order_id"))
            .or_else(|| request.exchange_order_id.clone()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
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
    }
}

fn coinone_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn coinone_time_in_force(order_type: OrderType, tif: Option<TimeInForce>) -> Option<&'static str> {
    match (order_type, tif) {
        (OrderType::IOC, _) | (_, Some(TimeInForce::IOC)) => Some("IOC"),
        (OrderType::FOK, _) | (_, Some(TimeInForce::FOK)) => Some("FOK"),
        (_, Some(TimeInForce::GTC)) | (_, None) => None,
        (_, Some(TimeInForce::GTX)) => None,
    }
}

fn value_text(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("coinone {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}
