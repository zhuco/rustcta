use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelAllOrdersRequest, CancelAllOrdersResponse,
    CancelOrderRequest, CancelOrderResponse, ExchangeApiError, ExchangeApiResult,
    OpenOrdersRequest, OpenOrdersResponse, PlaceOrderRequest, PlaceOrderResponse,
    QueryOrderRequest, QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderStatus, OrderType};
use serde_json::{json, Map, Value};

use super::parser::normalize_gemini_symbol;
use super::private_parser::{
    parse_balances, parse_open_orders, parse_order_state, parse_recent_fills,
};
use super::GeminiGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl GeminiGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let payload = gemini_order_payload(&request)?;
        let value = self
            .send_private_post("gemini.place_order", "/v1/order/new", payload)
            .await?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let order_id =
            request
                .exchange_order_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "gemini cancel_order requires exchange_order_id".to_string(),
                })?;
        let mut payload = Map::new();
        payload.insert("order_id".to_string(), json!(order_id));
        let value = self
            .send_private_post("gemini.cancel_order", "/v1/order/cancel", payload)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| gemini_cancelled_order(&self.exchange_id, &request));
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
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
        let value = self
            .send_private_post(
                "gemini.cancel_all_orders",
                "/v1/order/cancel/session",
                Map::new(),
            )
            .await?;
        let orders = parse_open_orders(&self.exchange_id, request.symbol.as_ref(), &value)
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
            .send_private_post("gemini.get_balances", "/v1/balances", Map::new())
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.assets,
                &value,
            )?,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let order_id =
            request
                .exchange_order_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "gemini query_order requires exchange_order_id".to_string(),
                })?;
        let mut payload = Map::new();
        payload.insert("order_id".to_string(), json!(order_id));
        let value = self
            .send_private_post("gemini.query_order", "/v1/order/status", payload)
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: Some(parse_order_state(
                &self.exchange_id,
                Some(&request.symbol),
                &value,
            )?),
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
        let value = self
            .send_private_post("gemini.get_open_orders", "/v1/orders", Map::new())
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_open_orders(&self.exchange_id, request.symbol.as_ref(), &value)?,
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
                message: "gemini get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut payload = Map::new();
        payload.insert(
            "symbol".to_string(),
            json!(normalize_gemini_symbol(&symbol.exchange_symbol.symbol)?),
        );
        if let Some(limit) = request
            .limit
            .or_else(|| request.page.as_ref().and_then(|p| p.limit))
        {
            payload.insert("limit_trades".to_string(), json!(limit));
        }
        let value = self
            .send_private_post("gemini.get_recent_fills", "/v1/mytrades", payload)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }
}

pub fn gemini_order_payload(request: &PlaceOrderRequest) -> ExchangeApiResult<Map<String, Value>> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "gemini.reduce_only_unsupported_spot",
        });
    }
    if request.order_type == OrderType::Market || request.quote_quantity.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "gemini.market_order_unsupported",
        });
    }
    let mut payload = Map::new();
    payload.insert(
        "symbol".to_string(),
        json!(normalize_gemini_symbol(
            &request.symbol.exchange_symbol.symbol
        )?),
    );
    payload.insert(
        "amount".to_string(),
        json!(request.quote_quantity.as_ref().unwrap_or(&request.quantity)),
    );
    payload.insert(
        "side".to_string(),
        json!(match request.side {
            rustcta_types::OrderSide::Buy => "buy",
            rustcta_types::OrderSide::Sell => "sell",
        }),
    );
    payload.insert("type".to_string(), json!("exchange limit"));
    let price = request
        .price
        .as_ref()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "gemini limit order requires price".to_string(),
        })?;
    payload.insert("price".to_string(), json!(price));
    if let Some(client_order_id) = &request.client_order_id {
        payload.insert("client_order_id".to_string(), json!(client_order_id));
    }
    let mut options = Vec::new();
    if request.post_only || request.order_type == OrderType::PostOnly {
        options.push("maker-or-cancel");
    }
    if request.time_in_force == Some(TimeInForce::IOC) {
        options.push("immediate-or-cancel");
    }
    if !options.is_empty() {
        payload.insert("options".to_string(), json!(options));
    }
    Ok(payload)
}

fn gemini_cancelled_order(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: request.exchange_order_id.clone(),
        side: rustcta_types::OrderSide::Buy,
        position_side: Some(rustcta_types::PositionSide::None),
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
