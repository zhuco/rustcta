use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_coincheck_symbol;
use super::private_parser::{parse_account_balances, parse_open_orders, parse_recent_fills};
use super::CoincheckGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoincheckGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = coincheck_order_body(&request)?;
        let value = self
            .send_signed_post(
                "coincheck.place_order",
                "/api/exchange/orders",
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = order_state_from_ack(&self.exchange_id, &request.symbol, &value);
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
                operation: "coincheck.quote_market_sell",
            });
        }
        let body = json!({
            "pair": normalize_coincheck_symbol(&request.symbol.exchange_symbol.symbol)?,
            "order_type": "market_buy",
            "market_buy_amount": request.quote_quantity
        });
        let value = self
            .send_signed_post(
                "coincheck.place_quote_market_order",
                "/api/exchange/orders",
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
                message: "coincheck cancel_order requires exchange_order_id".to_string(),
            })?;
        let value = self
            .send_signed_delete(
                "coincheck.cancel_order",
                &format!("/api/exchange/orders/{order_id}"),
                &HashMap::new(),
            )
            .await?;
        let order = cancel_order_state(&self.exchange_id, &request.symbol, order_id, &value);
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
                message: "coincheck batch_place_orders requires 1..=20 orders".to_string(),
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
                message: "coincheck batch_cancel_orders requires 1..=20 cancels".to_string(),
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
        _request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.unsupported_private("coincheck.cancel_all_orders")
    }

    pub(super) async fn amend_order_impl(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported_private("coincheck.amend_order")
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
                "coincheck.get_balances",
                "/api/accounts/balance",
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
        _request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        self.unsupported_private("coincheck.get_fees")
    }

    pub(super) async fn query_order_impl(
        &self,
        _request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.unsupported_private("coincheck.query_order")
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
                "pair".to_string(),
                normalize_coincheck_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get(
                "coincheck.get_open_orders",
                "/api/exchange/orders/opens",
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
                message: "coincheck get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "pair".to_string(),
            normalize_coincheck_symbol(&symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_signed_get(
                "coincheck.get_recent_fills",
                "/api/exchange/orders/transactions",
                &params,
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

fn coincheck_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coincheck spot order does not support reduce_only".to_string(),
        });
    }
    let order_type = match (request.side, request.order_type) {
        (OrderSide::Buy, OrderType::Market) => "market_buy",
        (OrderSide::Sell, OrderType::Market) => "market_sell",
        (OrderSide::Buy, _) => "buy",
        (OrderSide::Sell, _) => "sell",
    };
    let mut body = json!({
        "pair": normalize_coincheck_symbol(&request.symbol.exchange_symbol.symbol)?,
        "order_type": order_type,
        "amount": request.quantity
    });
    if !matches!(request.order_type, OrderType::Market) {
        body["rate"] = Value::String(request.price.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "coincheck limit order requires price".to_string(),
            }
        })?);
    }
    Ok(body)
}

fn order_state_from_ack(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: None,
        exchange_order_id: text(value.get("id")),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: None,
        status: OrderStatus::New,
        quantity: text(value.get("amount")).unwrap_or_else(|| "0".to_string()),
        price: text(value.get("rate")),
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
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: None,
        exchange_order_id: text(value.get("id")),
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
    order_id: &str,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id: None,
        exchange_order_id: text(value.get("id")).or_else(|| Some(order_id.to_string())),
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

fn text(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}
