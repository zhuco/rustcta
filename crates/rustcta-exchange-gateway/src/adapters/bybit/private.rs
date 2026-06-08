use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelAllOrdersRequest, CancelAllOrdersResponse,
    CancelOrderRequest, CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest,
    FeesResponse, OpenOrdersRequest, OpenOrdersResponse, PlaceOrderRequest, PlaceOrderResponse,
    PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_bybit_symbol;
use super::private_parser::{
    parse_balances, parse_fills, parse_order_state, parse_orders, parse_positions,
};
use super::public::bybit_category;
use super::BybitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BybitGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let operation = self.profile_operation("bybit.get_balances", "bybiteu.get_balances");
        let (tenant_id, account_id) = self.context_account(&request.context, operation)?;
        let mut params = HashMap::new();
        params.insert("accountType".to_string(), "UNIFIED".to_string());
        let value = self
            .send_signed_get(operation, "/v5/account/wallet-balance", &params)
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.market_type.unwrap_or(MarketType::Perpetual),
                &request.assets,
                &value,
            )?,
        })
    }

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_supported_market_type(market_type)?;
        let operation = self.profile_operation("bybit.get_positions", "bybiteu.get_positions");
        let (tenant_id, account_id) = self.context_account(&request.context, operation)?;
        let mut params = HashMap::new();
        params.insert(
            "category".to_string(),
            bybit_category(market_type).to_string(),
        );
        if request.symbols.len() == 1 {
            params.insert(
                "symbol".to_string(),
                normalize_bybit_symbol(&request.symbols[0].symbol)?,
            );
        } else if request.symbols.is_empty() {
            params.insert("settleCoin".to_string(), "USDT".to_string());
        }
        let value = self
            .send_signed_get(operation, "/v5/position/list", &params)
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions: parse_positions(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.symbols,
                &value,
            )?,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let operation = self.profile_operation("bybit.place_order", "bybiteu.place_order");
        let body = bybit_place_order_body(&request)?;
        let exchange = request.symbol.exchange.clone();
        let request_id = request.context.request_id.clone();
        let value = self
            .send_signed_post_json(operation, "/v5/order/create", &body)
            .await?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request_id),
            order: parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        let operation = self.profile_operation("bybit.cancel_order", "bybiteu.cancel_order");
        let mut body = json!({
            "category": bybit_category(request.symbol.market_type),
            "symbol": normalize_bybit_symbol(&request.symbol.exchange_symbol.symbol)?,
        });
        insert_order_id(
            &mut body,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
        )?;
        let value = self
            .send_signed_post_json(operation, "/v5/order/cancel", &body)
            .await?;
        let exchange = request.symbol.exchange.clone();
        let request_id = request.context.request_id.clone();
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange, request_id),
            order: parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?,
            cancelled: true,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        let operation = self.profile_operation("bybit.query_order", "bybiteu.query_order");
        let mut params = HashMap::new();
        params.insert(
            "category".to_string(),
            bybit_category(request.symbol.market_type).to_string(),
        );
        params.insert(
            "symbol".to_string(),
            normalize_bybit_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(order_link_id) = request.client_order_id.as_deref() {
            params.insert("orderLinkId".to_string(), order_link_id.to_string());
        }
        let value = self
            .send_signed_get(operation, "/v5/order/realtime", &params)
            .await?;
        let mut orders = parse_orders(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order: orders.pop(),
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let operation = self.profile_operation("bybit.get_open_orders", "bybiteu.get_open_orders");
        let market_type = request.market_type.unwrap_or(MarketType::Perpetual);
        let mut params = HashMap::new();
        params.insert(
            "category".to_string(),
            bybit_category(market_type).to_string(),
        );
        if let Some(symbol) = &request.symbol {
            params.insert(
                "symbol".to_string(),
                normalize_bybit_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get(operation, "/v5/order/realtime", &params)
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bybit get_recent_fills requires symbol".to_string(),
            })?;
        let operation =
            self.profile_operation("bybit.get_recent_fills", "bybiteu.get_recent_fills");
        let (tenant_id, account_id) = self.context_account(&request.context, operation)?;
        let mut params = HashMap::new();
        params.insert(
            "category".to_string(),
            bybit_category(symbol.market_type).to_string(),
        );
        params.insert(
            "symbol".to_string(),
            normalize_bybit_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(100).to_string());
        }
        let value = self
            .send_signed_get(operation, "/v5/execution/list", &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let operation =
            self.profile_operation("bybit.cancel_all_orders", "bybiteu.cancel_all_orders");
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bybit cancel_all_orders requires symbol".to_string(),
            })?;
        let body = json!({
            "category": bybit_category(symbol.market_type),
            "symbol": normalize_bybit_symbol(&symbol.exchange_symbol.symbol)?,
        });
        let value = self
            .send_signed_post_json(operation, "/v5/order/cancel-all", &body)
            .await?;
        let orders = parse_orders(&self.exchange_id, Some(symbol), &value).unwrap_or_default();
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        _request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        self.unsupported_private(self.profile_operation("bybit.get_fees", "bybiteu.get_fees"))
    }
}

fn bybit_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "category": bybit_category(request.symbol.market_type),
        "symbol": normalize_bybit_symbol(&request.symbol.exchange_symbol.symbol)?,
        "side": bybit_side(request.side),
        "orderType": bybit_order_type(request.order_type),
        "qty": request.quantity,
    });
    if let Some(price) = request.price.as_deref() {
        body["price"] = Value::String(price.to_string());
    }
    if let Some(client_id) = request.client_order_id.as_deref() {
        body["orderLinkId"] = Value::String(client_id.to_string());
    }
    if request.reduce_only {
        body["reduceOnly"] = Value::Bool(true);
    }
    if request.post_only || request.order_type == OrderType::PostOnly {
        body["timeInForce"] = Value::String("PostOnly".to_string());
    }
    if let Some(position_side) = request.position_side {
        body["positionIdx"] = Value::Number(bybit_position_idx(position_side).into());
    }
    Ok(body)
}

fn insert_order_id(
    body: &mut Value,
    order_id: Option<&str>,
    order_link_id: Option<&str>,
) -> ExchangeApiResult<()> {
    if let Some(order_id) = order_id {
        body["orderId"] = Value::String(order_id.to_string());
    }
    if let Some(order_link_id) = order_link_id {
        body["orderLinkId"] = Value::String(order_link_id.to_string());
    }
    if body.get("orderId").is_none() && body.get("orderLinkId").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bybit order request requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(())
}

fn bybit_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "Buy",
        OrderSide::Sell => "Sell",
    }
}

fn bybit_order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "Market",
        _ => "Limit",
    }
}

fn bybit_position_idx(position_side: PositionSide) -> u64 {
    match position_side {
        PositionSide::Long => 1,
        PositionSide::Short => 2,
        PositionSide::Net | PositionSide::None => 0,
    }
}
