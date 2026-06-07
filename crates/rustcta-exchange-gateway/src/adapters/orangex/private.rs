#![cfg_attr(not(test), allow(dead_code))]

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::{market_currency, market_kind, normalize_orangex_symbol};
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_order_state, parse_positions, parse_recent_fills,
};
use super::signing::sign_client_signature;
use super::OrangeXGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, missing_order_identity, response_metadata};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrangeXMarginMode {
    Cross,
    Isolated,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrangeXPrivateAck {
    pub operation: &'static str,
    pub data: Value,
}

impl OrangeXGatewayAdapter {
    pub async fn adjust_perpetual_margin_type(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        margin_type: OrangeXMarginMode,
    ) -> ExchangeApiResult<OrangeXPrivateAck> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        if symbol.market_type != rustcta_types::MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "orangex.adjust_margin_type_non_perpetual",
            });
        }
        let value = self
            .send_private_rpc(
                "orangex.adjust_perpetual_margin_type",
                "/private/adjust_perpetual_margin_type",
                json!({
                    "instrument_name": normalize_symbol_scope(&symbol)?,
                    "margin_type": orangex_margin_mode_text(margin_type),
                }),
            )
            .await?;
        Ok(OrangeXPrivateAck {
            operation: "orangex.adjust_perpetual_margin_type",
            data: value,
        })
    }

    pub async fn adjust_perpetual_leverage(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        leverage: u32,
    ) -> ExchangeApiResult<OrangeXPrivateAck> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        if symbol.market_type != rustcta_types::MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "orangex.adjust_leverage_non_perpetual",
            });
        }
        if leverage == 0 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "orangex leverage must be greater than zero".to_string(),
            });
        }
        let value = self
            .send_private_rpc(
                "orangex.adjust_perpetual_leverage",
                "/private/adjust_perpetual_leverage",
                json!({
                    "instrument_name": normalize_symbol_scope(&symbol)?,
                    "leverage": leverage.to_string(),
                }),
            )
            .await?;
        Ok(OrangeXPrivateAck {
            operation: "orangex.adjust_perpetual_leverage",
            data: value,
        })
    }

    pub(super) async fn get_balances_private_rest(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request
            .market_type
            .unwrap_or(rustcta_types::MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let value = self
            .send_private_rpc(
                "orangex.get_balances",
                "/private/get_assets_info",
                json!({ "asset_type": [market_currency(market_type)?] }),
            )
            .await?;
        let balances = parse_balances(
            tenant_id,
            account_id,
            &self.exchange_id,
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

    pub(super) async fn get_positions_private_rest(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let value = self
            .send_private_rpc(
                "orangex.get_positions",
                "/private/get_positions",
                json!({ "currency": "PERPETUAL", "kind": "perpetual" }),
            )
            .await?;
        let mut positions = parse_positions(tenant_id, account_id, &self.exchange_id, &value)?;
        if !request.symbols.is_empty() {
            positions.retain(|position| {
                position
                    .exchange_symbol
                    .as_ref()
                    .is_some_and(|exchange_symbol| {
                        request.symbols.iter().any(|requested| {
                            requested
                                .symbol
                                .eq_ignore_ascii_case(&exchange_symbol.symbol)
                        })
                    })
            });
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
                message: "orangex get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            let value = self
                .rest
                .send_public_rpc(
                    "/public/get_instruments",
                    json!({
                        "currency": market_currency(symbol.market_type)?,
                        "kind": market_kind(symbol.market_type)?,
                    }),
                )
                .await?;
            fees.extend(parse_fee_snapshots(&self.exchange_id, symbol, &value)?);
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
        let method = match request.side {
            OrderSide::Buy => "/private/buy",
            OrderSide::Sell => "/private/sell",
        };
        let params = orangex_order_params(&request)?;
        let value = self
            .send_private_rpc("orangex.place_order", method, params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| ack_order_state(&self.exchange_id, &request, &value));
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_private_rest(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let mut params = json!({
            "instrument_name": normalize_symbol_scope(&request.symbol)?,
            "amount": non_empty("quote_quantity", &request.quote_quantity)?,
            "type": "market",
            "market_amount_order": true,
        });
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            validate_client_order_id(client_order_id)?;
            params["custom_order_id"] = Value::String(client_order_id.to_string());
        }
        let method = match request.side {
            OrderSide::Buy => "/private/buy",
            OrderSide::Sell => "/private/sell",
        };
        let value = self
            .send_private_rpc("orangex.place_quote_market_order", method, params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| quote_ack_order_state(&self.exchange_id, &request, &value));
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn batch_place_orders_private_rest(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.orders.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "orangex.batch_place_orders requires at least one order".to_string(),
            });
        }

        let mut orders = Vec::with_capacity(request.orders.len());
        for order in request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market_type(order.symbol.market_type)?;
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

    pub(super) async fn cancel_order_private_rest(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        if missing_order_identity(&request) {
            return Err(ExchangeApiError::InvalidRequest {
                message: "orangex cancel_order requires exchange_order_id".to_string(),
            });
        }
        let order_id =
            request
                .exchange_order_id
                .as_deref()
                .ok_or(ExchangeApiError::Unsupported {
                    operation: "orangex.cancel_by_client_order_id",
                })?;
        let value = self
            .send_private_rpc(
                "orangex.cancel_order",
                "/private/cancel",
                json!({ "order_id": non_empty("exchange_order_id", order_id)? }),
            )
            .await?;
        let mut order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| cancelled_order_state(&self.exchange_id, &request, &value));
        order.status = OrderStatus::Cancelled;
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
        if request.cancels.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "orangex.batch_cancel_orders requires at least one cancel".to_string(),
            });
        }

        let mut orders = Vec::with_capacity(request.cancels.len());
        for cancel in request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market_type(cancel.symbol.market_type)?;
            let response = self.cancel_order_private_rest(cancel).await?;
            orders.push(response.order);
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
        let (method, params) = if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            (
                "/private/cancel_all_by_instrument",
                json!({ "instrument_name": normalize_symbol_scope(symbol)? }),
            )
        } else {
            let market_type = request
                .market_type
                .unwrap_or(rustcta_types::MarketType::Spot);
            (
                "/private/cancel_all_by_currency",
                json!({
                    "currency": market_currency(market_type)?,
                    "kind": market_kind(market_type)?,
                }),
            )
        };
        let value = self
            .send_private_rpc("orangex.cancel_all_orders", method, params)
            .await?;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: value.as_u64().unwrap_or(0) as u32,
            orders: Vec::new(),
        })
    }

    pub(super) async fn query_order_private_rest(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        let order_id =
            request
                .exchange_order_id
                .as_deref()
                .ok_or(ExchangeApiError::Unsupported {
                    operation: "orangex.query_by_client_order_id",
                })?;
        let value = self
            .send_private_rpc(
                "orangex.query_order",
                "/private/get_order_state",
                json!({ "order_id": non_empty("exchange_order_id", order_id)? }),
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value).ok();
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn get_open_orders_private_rest(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let (method, params) = if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            (
                "/private/get_open_orders_by_instrument",
                json!({ "instrument_name": normalize_symbol_scope(symbol)? }),
            )
        } else {
            let market_type = request
                .market_type
                .unwrap_or(rustcta_types::MarketType::Spot);
            (
                "/private/get_open_orders_by_currency",
                json!({
                    "currency": market_currency(market_type)?,
                    "kind": market_kind(market_type)?,
                }),
            )
        };
        let value = self
            .send_private_rpc("orangex.get_open_orders", method, params)
            .await?;
        let orders = value
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| {
                        parse_order_state(&self.exchange_id, request.symbol.as_ref(), row).ok()
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
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
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let (method, mut params) = if let Some(order_id) = request.exchange_order_id.as_deref() {
            (
                "/private/get_user_trades_by_order",
                json!({ "order_id": order_id }),
            )
        } else if let Some(symbol) = &request.symbol {
            (
                "/private/get_user_trades_by_instrument",
                json!({ "instrument_name": normalize_symbol_scope(symbol)? }),
            )
        } else {
            let market_type = request
                .market_type
                .unwrap_or(rustcta_types::MarketType::Spot);
            (
                "/private/get_user_trades_by_currency",
                json!({
                    "currency": market_currency(market_type)?,
                    "kind": market_kind(market_type)?,
                }),
            )
        };
        if let Some(limit) = request.limit {
            params["count"] = Value::Number(serde_json::Number::from(limit.min(100)));
        }
        let value = self
            .send_private_rpc("orangex.get_recent_fills", method, params)
            .await?;
        let fills = parse_recent_fills(
            tenant_id,
            account_id,
            &self.exchange_id,
            request.symbol.as_ref(),
            &value,
        )?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }

    pub(super) async fn send_private_rpc(
        &self,
        operation: &'static str,
        method: &str,
        params: Value,
    ) -> ExchangeApiResult<Value> {
        let token = self.private_access_token(operation).await?;
        self.rest.send_private_rpc(method, params, &token).await
    }

    pub(super) async fn private_access_token(
        &self,
        operation: &'static str,
    ) -> ExchangeApiResult<String> {
        if !self.config.enabled_private_rest {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        if let Some(token) = self.config.access_token_value() {
            return Ok(token.trim().to_string());
        }
        let client_id = self
            .config
            .client_id_value()
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let client_secret = self
            .config
            .client_secret_value()
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let timestamp = chrono::Utc::now().timestamp_millis().to_string();
        let nonce = format!("rustcta-{timestamp}");
        let value = self
            .rest
            .send_public_rpc(
                "/public/auth",
                orangex_auth_client_signature_params(
                    client_id,
                    client_secret,
                    &timestamp,
                    &nonce,
                    "account:read trade:read_write",
                ),
            )
            .await?;
        parse_auth_access_token(&self.exchange_id, &value)
    }

    fn context_account(
        &self,
        context: &rustcta_exchange_api::RequestContext,
    ) -> ExchangeApiResult<(
        rustcta_exchange_api::TenantId,
        rustcta_exchange_api::AccountId,
    )> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "orangex private REST requires context.tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "orangex private REST requires context.account_id".to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

fn parse_auth_access_token(
    exchange_id: &rustcta_exchange_api::ExchangeId,
    value: &Value,
) -> ExchangeApiResult<String> {
    value
        .get("access_token")
        .or_else(|| value.get("token"))
        .and_then(Value::as_str)
        .filter(|token| !token.trim().is_empty())
        .map(|token| token.trim().to_string())
        .ok_or_else(|| {
            let mut error = rustcta_types::ExchangeError::new(
                exchange_id.clone(),
                rustcta_types::ExchangeErrorClass::Authentication,
                "OrangeX auth response missing access_token",
                chrono::Utc::now(),
            );
            error.raw = Some(value.clone());
            ExchangeApiError::Exchange(error)
        })
}

fn normalize_symbol_scope(symbol: &rustcta_exchange_api::SymbolScope) -> ExchangeApiResult<String> {
    normalize_orangex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)
}

pub(super) fn orangex_auth_client_signature_params(
    client_id: &str,
    client_secret: &str,
    timestamp: &str,
    nonce: &str,
    scope: &str,
) -> Value {
    json!({
        "grant_type": "client_signature",
        "client_id": client_id,
        "timestamp": timestamp,
        "nonce": nonce,
        "signature": sign_client_signature(client_secret, client_id, timestamp, nonce),
        "scope": scope,
    })
}

fn orangex_order_params(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        validate_client_order_id(client_order_id)?;
    }
    let order_type = match request.order_type {
        OrderType::Market => "market",
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => "limit",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "orangex.conditional_order",
            });
        }
    };
    let mut params = json!({
        "instrument_name": normalize_symbol_scope(&request.symbol)?,
        "amount": non_empty("quantity", &request.quantity)?,
        "type": order_type,
        "post_only": request.post_only || request.order_type == OrderType::PostOnly,
        "reduce_only": request.reduce_only,
    });
    if let Some(price) = request.price.as_deref() {
        params["price"] = Value::String(non_empty("price", price)?);
    } else if request.order_type.requires_limit_price() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "orangex limit order requires price".to_string(),
        });
    }
    if let Some(time_in_force) = request
        .time_in_force
        .or_else(|| order_type_time_in_force(request.order_type))
    {
        params["time_in_force"] = Value::String(orangex_time_in_force(time_in_force).to_string());
    }
    if let Some(position_side) = request.position_side {
        params["position_side"] = Value::String(
            match position_side {
                PositionSide::Long => "LONG",
                PositionSide::Short => "SHORT",
                _ => "BOTH",
            }
            .to_string(),
        );
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params["custom_order_id"] = Value::String(client_order_id.to_string());
    }
    Ok(params)
}

fn ack_order_state(
    exchange_id: &rustcta_exchange_api::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    let order_id = value
        .get("order")
        .or_else(|| value.get("result").and_then(|result| result.get("order")))
        .and_then(|order| order.get("order_id"))
        .and_then(Value::as_str)
        .map(str::to_string);
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: order_id,
        side: request.side,
        position_side: request.position_side,
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only,
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    }
}

fn quote_ack_order_state(
    exchange_id: &rustcta_exchange_api::ExchangeId,
    request: &QuoteMarketOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    let order_id = value
        .get("order")
        .or_else(|| value.get("result").and_then(|result| result.get("order")))
        .and_then(|order| order.get("order_id"))
        .and_then(Value::as_str)
        .map(str::to_string);
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: order_id,
        side: request.side,
        position_side: None,
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

fn cancelled_order_state(
    exchange_id: &rustcta_exchange_api::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: value
            .get("order_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| request.exchange_order_id.clone()),
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
    }
}

fn validate_client_order_id(value: &str) -> ExchangeApiResult<()> {
    if value.is_empty() || value.len() > 36 {
        return Err(ExchangeApiError::InvalidRequest {
            message: "orangex custom_order_id must be 1..36 characters".to_string(),
        });
    }
    if !value.chars().all(|ch| {
        ch.is_ascii_lowercase()
            || ch.is_ascii_digit()
            || ch.is_ascii_uppercase()
            || matches!(ch, '.' | ':' | '/' | '_' | '-')
    }) {
        return Err(ExchangeApiError::InvalidRequest {
            message: "orangex custom_order_id contains unsupported characters".to_string(),
        });
    }
    Ok(())
}

fn non_empty(field: &'static str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("orangex {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn order_type_time_in_force(order_type: OrderType) -> Option<TimeInForce> {
    match order_type {
        OrderType::IOC => Some(TimeInForce::IOC),
        OrderType::FOK => Some(TimeInForce::FOK),
        _ => None,
    }
}

fn orangex_time_in_force(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::GTC | TimeInForce::GTX => "good_til_cancelled",
        TimeInForce::IOC => "immediate_or_cancel",
        TimeInForce::FOK => "fill_or_kill",
    }
}

fn orangex_margin_mode_text(mode: OrangeXMarginMode) -> &'static str {
    match mode {
        OrangeXMarginMode::Cross => "cross",
        OrangeXMarginMode::Isolated => "isolated",
    }
}
