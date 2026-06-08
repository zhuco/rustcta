use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PageCursor, PageRequest, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::Value;

use super::parser::normalize_exmo_exchange_symbol;
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_open_orders, parse_order_state,
    parse_order_trades_as_order_state, parse_recent_fills,
};
use super::ExmoGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl ExmoGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let params = self.order_create_params(&request)?;
        let value = self
            .send_signed_post("exmo.place_order", "/v1.1/order_create", params)
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
        let mut params = vec![
            (
                "pair".to_string(),
                normalize_exmo_exchange_symbol(&request.symbol.exchange_symbol.symbol)?,
            ),
            (
                "quantity".to_string(),
                non_empty("quote_quantity", &request.quote_quantity)?,
            ),
            ("price".to_string(), "0".to_string()),
            (
                "type".to_string(),
                match request.side {
                    OrderSide::Buy => "market_buy_total",
                    OrderSide::Sell => "market_sell_total",
                }
                .to_string(),
            ),
            ("nonce".to_string(), self.next_nonce()),
        ];
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert(
                params.len() - 1,
                (
                    "client_id".to_string(),
                    positive_integer("client_order_id", client_order_id)?,
                ),
            );
        }
        let value = self
            .send_signed_post(
                "exmo.place_quote_market_order",
                "/v1.1/order_create",
                params,
            )
            .await?;
        let order = rustcta_exchange_api::OrderState {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: self.exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: request.symbol.canonical_symbol.clone(),
            exchange_symbol: request.symbol.exchange_symbol.clone(),
            client_order_id: value_as_text(value.get("client_id"))
                .or_else(|| request.client_order_id.clone()),
            exchange_order_id: value_as_text(value.get("order_id")),
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
        };
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
                message: "EXMO cancel_order requires exchange_order_id".to_string(),
            })?;
        let params = vec![
            ("order_id".to_string(), order_id.to_string()),
            ("nonce".to_string(), self.next_nonce()),
        ];
        let value = self
            .send_signed_post("exmo.cancel_order", "/v1.1/order_cancel", params)
            .await?;
        let order = exmo_cancel_order_state(&self.exchange_id, &request, &value);
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
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
            .send_signed_post(
                "exmo.get_balances",
                "/v1.1/user_info",
                vec![("nonce".to_string(), self.next_nonce())],
            )
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
        _request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.unsupported_private("exmo.get_positions")
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "EXMO get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let value = self
            .rest
            .send_public_post("/v1.1/pair_settings", &[])
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
        self.ensure_spot(request.symbol.market_type)?;
        let order_id = request
            .exchange_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "EXMO query_order requires exchange_order_id".to_string(),
            })?;
        let value = self
            .send_signed_post(
                "exmo.query_order",
                "/v1.1/order_trades",
                vec![
                    ("order_id".to_string(), order_id.to_string()),
                    ("nonce".to_string(), self.next_nonce()),
                ],
            )
            .await?;
        let order = parse_order_trades_as_order_state(&self.exchange_id, &request.symbol, &value)?;
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
        if request.page.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "exmo.open_orders_pagination",
            });
        }
        let symbol = request.symbol.as_ref();
        if let Some(symbol) = symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let mut params = Vec::new();
        if let Some(symbol) = symbol {
            params.push((
                "pair".to_string(),
                normalize_exmo_exchange_symbol(&symbol.exchange_symbol.symbol)?,
            ));
        }
        params.push(("nonce".to_string(), self.next_nonce()));
        let value = self
            .send_signed_post("exmo.get_open_orders", "/v1.1/user_open_orders", params)
            .await?;
        let orders = parse_open_orders(&self.exchange_id, symbol, &value)?;
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
        if request.start_time.is_some() || request.end_time.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "exmo.user_trades_time_range",
            });
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "EXMO get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = vec![(
            "pair".to_string(),
            normalize_exmo_exchange_symbol(&symbol.exchange_symbol.symbol)?,
        )];
        apply_exmo_page_params(&mut params, request.page.as_ref(), request.limit)?;
        params.push(("nonce".to_string(), self.next_nonce()));
        let value = self
            .send_signed_post("exmo.get_recent_fills", "/v1.1/user_trades", params)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }

    fn order_create_params(
        &self,
        request: &PlaceOrderRequest,
    ) -> ExchangeApiResult<Vec<(String, String)>> {
        if request.reduce_only {
            return Err(ExchangeApiError::InvalidRequest {
                message: "EXMO spot order does not support reduce_only".to_string(),
            });
        }
        let pair = normalize_exmo_exchange_symbol(&request.symbol.exchange_symbol.symbol)?;
        let order_type = match (request.order_type, request.side) {
            (OrderType::Market, OrderSide::Buy) => "market_buy",
            (OrderType::Market, OrderSide::Sell) => "market_sell",
            (_, OrderSide::Buy) => "buy",
            (_, OrderSide::Sell) => "sell",
        };
        let mut params = vec![
            ("pair".to_string(), pair),
            (
                "quantity".to_string(),
                non_empty("quantity", &request.quantity)?,
            ),
        ];
        if request.order_type != OrderType::Market {
            params.push((
                "price".to_string(),
                non_empty(
                    "price",
                    request
                        .price
                        .as_deref()
                        .ok_or_else(|| ExchangeApiError::InvalidRequest {
                            message: "EXMO limit order requires price".to_string(),
                        })?,
                )?,
            ));
            if let Some(exec_type) =
                exmo_exec_type(request.order_type, request.time_in_force, request.post_only)
            {
                params.push(("exec_type".to_string(), exec_type.to_string()));
            }
        }
        params.push(("type".to_string(), order_type.to_string()));
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.push((
                "client_id".to_string(),
                positive_integer("client_order_id", client_order_id)?,
            ));
        }
        params.push(("nonce".to_string(), self.next_nonce()));
        Ok(params)
    }
}

fn apply_exmo_page_params(
    params: &mut Vec<(String, String)>,
    page: Option<&PageRequest>,
    legacy_limit: Option<u32>,
) -> ExchangeApiResult<()> {
    if let Some(page) = page {
        page.validate(Some(100))
            .map_err(|message| ExchangeApiError::InvalidRequest { message })?;
    }
    if legacy_limit == Some(0) {
        return Err(ExchangeApiError::InvalidRequest {
            message: "EXMO pagination limit must be positive".to_string(),
        });
    }
    if let Some(limit) = page.and_then(|page| page.limit).or(legacy_limit) {
        params.push(("limit".to_string(), limit.min(100).to_string()));
    }
    let Some(cursor) = page.and_then(|page| page.cursor.as_ref()) else {
        return Ok(());
    };
    match cursor {
        PageCursor::Offset { offset } => {
            params.push(("offset".to_string(), offset.to_string()));
        }
        PageCursor::Token { token } => {
            let offset = token
                .parse::<u64>()
                .map_err(|_| ExchangeApiError::InvalidRequest {
                    message: "EXMO pagination token cursor must be an offset".to_string(),
                })?;
            params.push(("offset".to_string(), offset.to_string()));
        }
        PageCursor::Id { .. } | PageCursor::Timestamp { .. } | PageCursor::TimeRange { .. } => {
            return Err(ExchangeApiError::Unsupported {
                operation: "exmo.user_trades_cursor_type",
            });
        }
    }
    Ok(())
}

fn exmo_exec_type(
    order_type: OrderType,
    time_in_force: Option<TimeInForce>,
    post_only: bool,
) -> Option<&'static str> {
    if post_only || order_type == OrderType::PostOnly || time_in_force == Some(TimeInForce::GTX) {
        return Some("post_only");
    }
    match (order_type, time_in_force) {
        (OrderType::IOC, _) | (_, Some(TimeInForce::IOC)) => Some("ioc"),
        (OrderType::FOK, _) | (_, Some(TimeInForce::FOK)) => Some("fok"),
        _ => None,
    }
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
        client_order_id: value_as_text(value.get("client_id"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_as_text(value.get("order_id")),
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

fn exmo_cancel_order_state(
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
        exchange_order_id: value_as_text(value.get("order_id"))
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

fn value_as_text(value: Option<&Value>) -> Option<String> {
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
            message: format!("EXMO {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn positive_integer(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = non_empty(field, value)?;
    let parsed = value
        .parse::<u64>()
        .map_err(|_| ExchangeApiError::InvalidRequest {
            message: format!("EXMO {field} must be a positive integer"),
        })?;
    if parsed == 0 {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("EXMO {field} must be a positive integer"),
        });
    }
    Ok(value)
}
