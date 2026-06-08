#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderState, PlaceOrderRequest, PlaceOrderResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::cryptomus_symbol;
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_open_orders, parse_order_state, parse_recent_fills,
};
use super::CryptomusGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

pub const BALANCES_PATH: &str = "/v2/user-api/exchange/account/balance";
pub const FEES_PATH: &str = "/v2/user-api/account/tariffs";
pub const PLACE_LIMIT_ORDER_PATH: &str = "/v2/user-api/exchange/orders";
pub const PLACE_MARKET_ORDER_PATH: &str = "/v2/user-api/exchange/orders/market";
pub const OPEN_ORDERS_PATH: &str = "/v2/user-api/exchange/orders";
pub const ORDER_HISTORY_PATH: &str = "/v2/user-api/exchange/orders/history";
pub const CANCEL_ORDER_PATH_PREFIX: &str = "/v2/user-api/exchange/orders";

impl CryptomusGatewayAdapter {
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
        let mut params = HashMap::new();
        if request.assets.len() == 1 {
            params.insert("ticker".to_string(), request.assets[0].to_ascii_uppercase());
        }
        let value = self
            .send_signed_get("cryptomus.get_balances", BALANCES_PATH, &params)
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

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "cryptomus get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let value = self
            .send_signed_get("cryptomus.get_fees", FEES_PATH, &HashMap::new())
            .await?;
        let fees = parse_fee_snapshots(&request.symbols, &value)?;
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = cryptomus_limit_order_body(&request)?;
        let value = self
            .send_signed_post("cryptomus.place_order", PLACE_LIMIT_ORDER_PATH, &body)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| order_state_from_place_ack(&self.exchange_id, &request, &value));
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
                message: "cryptomus cancel_order requires exchange_order_id".to_string(),
            })?;
        let endpoint = format!("{CANCEL_ORDER_PATH_PREFIX}/{order_id}");
        let value = self
            .send_signed_delete("cryptomus.cancel_order", &endpoint)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .ok()
            .filter(|order| order.status != OrderStatus::Unknown)
            .unwrap_or_else(|| order_state_from_cancel_ack(&self.exchange_id, &request));
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
            cancelled: true,
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
        params.insert("market".to_string(), cryptomus_symbol(&request.symbol));
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("order_id".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("client_order_id".to_string(), client_order_id.to_string());
        }
        let value = self
            .send_signed_get("cryptomus.query_order", OPEN_ORDERS_PATH, &params)
            .await?;
        let mut orders = parse_open_orders(&self.exchange_id, Some(&request.symbol), &value)?;
        if orders.is_empty() {
            let value = self
                .send_signed_get("cryptomus.query_order_history", ORDER_HISTORY_PATH, &params)
                .await?;
            orders = parse_open_orders(&self.exchange_id, Some(&request.symbol), &value)?;
        }
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order: orders.into_iter().next(),
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
        if let Some(symbol) = request.symbol.as_ref() {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert("market".to_string(), cryptomus_symbol(symbol));
        }
        if let Some(page) = request.page {
            if let Some(limit) = page.limit {
                params.insert("limit".to_string(), limit.min(100).to_string());
            }
        }
        let value = self
            .send_signed_get("cryptomus.get_open_orders", OPEN_ORDERS_PATH, &params)
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
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        if let Some(symbol) = request.symbol.as_ref() {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert("market".to_string(), cryptomus_symbol(symbol));
        }
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("order_id".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("client_order_id".to_string(), client_order_id.to_string());
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(100).to_string());
        }
        let value = self
            .send_signed_get("cryptomus.get_recent_fills", ORDER_HISTORY_PATH, &params)
            .await?;
        let fills = parse_recent_fills(
            &self.exchange_id,
            tenant_id,
            account_id,
            request.symbol.as_ref(),
            &value,
        )?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

pub fn place_limit_order_request_spec_fixture() -> Value {
    super::transport::signed_json_request_spec(
        "POST",
        PLACE_LIMIT_ORDER_PATH,
        json!({
            "market": "TRX_USDT",
            "quantity": "20",
            "price": "0.2964",
            "direction": "buy",
            "client_order_id": "CLIENT-LIMIT-1"
        }),
    )
}

pub fn cancel_order_request_spec_fixture() -> Value {
    super::transport::signed_json_request_spec(
        "DELETE",
        "/v2/user-api/exchange/orders/01JEXAFCCC5ZVJPZAAHHDKQBNG",
        json!({}),
    )
}

fn cryptomus_limit_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "cryptomus.reduce_only_unsupported_for_spot",
        });
    }
    if request.post_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "cryptomus.post_only_unverified",
        });
    }
    if request
        .time_in_force
        .is_some_and(|tif| tif != TimeInForce::GTC)
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "cryptomus.time_in_force_unverified",
        });
    }
    if request.order_type != OrderType::Limit {
        return Err(ExchangeApiError::Unsupported {
            operation: "cryptomus.market_order_live_write_gated",
        });
    }
    let price = request
        .price
        .as_deref()
        .filter(|price| !price.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "cryptomus limit order requires price".to_string(),
        })?;
    let mut body = json!({
        "market": cryptomus_symbol(&request.symbol),
        "quantity": request.quantity,
        "price": price,
        "direction": match request.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        }
    });
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["client_order_id"] = Value::String(client_order_id.to_string());
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
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: value
            .get("order_id")
            .or_else(|| value.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string),
        side: request.side,
        position_side: Some(PositionSide::None),
        order_type: request.order_type,
        time_in_force: Some(TimeInForce::GTC),
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

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: request.exchange_order_id.clone(),
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
