use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderStatus, OrderType};

use super::parser::normalize_bequant_symbol;
use super::private_parser::{
    parse_account_balances, parse_fee_snapshots, parse_open_orders, parse_order_state,
    parse_recent_fills,
};
use super::BequantGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BequantGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = bequant_order_form(&request)?;
        let value = self
            .send_signed_post("bequant.place_order", "/spot/order", &HashMap::new(), &body)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_impl(
        &self,
        _request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.unsupported_private("bequant.quote_sized_market_order")
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let client_order_id = request
            .client_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bequant cancel_order requires client_order_id".to_string(),
            })?;
        let value = self
            .send_signed_delete(
                "bequant.cancel_order",
                &format!("/spot/order/{client_order_id}"),
                &HashMap::new(),
            )
            .await?;
        let mut order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        order.status = OrderStatus::Cancelled;
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
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_bequant_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_delete("bequant.cancel_all_orders", "/spot/order", &params)
            .await?;
        let orders = parse_open_orders(&self.exchange_id, request.symbol.as_ref(), &value)
            .unwrap_or_else(|_| Vec::new());
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
        self.unsupported_private("bequant.amend_order")
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        _request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.unsupported_private("bequant.batch_place_orders")
    }

    pub(super) async fn batch_cancel_orders_impl(
        &self,
        _request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.unsupported_private("bequant.batch_cancel_orders")
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
            .send_signed_get("bequant.get_balances", "/spot/balance", &HashMap::new())
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

    pub(super) async fn get_positions_impl(
        &self,
        _request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.unsupported_private("bequant.get_positions_spot_adapter")
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
        let value = if let Some(symbol) = request.symbols.first() {
            let bequant_symbol = normalize_bequant_symbol(&symbol.exchange_symbol.symbol)?;
            self.send_signed_get(
                "bequant.get_fees",
                &format!("/spot/fee/{bequant_symbol}"),
                &HashMap::new(),
            )
            .await?
        } else {
            self.send_signed_get("bequant.get_fees", "/spot/fee", &HashMap::new())
                .await?
        };
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
        let client_order_id = request
            .client_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bequant query_order requires client_order_id".to_string(),
            })?;
        let value = self
            .send_signed_get(
                "bequant.query_order",
                &format!("/spot/order/{client_order_id}"),
                &HashMap::new(),
            )
            .await?;
        let metadata_exchange = request.symbol.exchange.clone();
        let request_id = request.context.request_id.clone();
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(metadata_exchange, request_id),
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
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_bequant_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        if let Some(page) = &request.page {
            if let Some(limit) = page.limit {
                params.insert("limit".to_string(), limit.min(1000).to_string());
            }
        }
        let value = self
            .send_signed_get("bequant.get_open_orders", "/spot/order", &params)
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
                message: "bequant get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bequant_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("order_id".to_string(), order_id.clone());
        }
        if let Some(from) = &request.from_trade_id {
            params.insert("from".to_string(), from.clone());
        }
        if let Some(start) = request.start_time {
            params.insert("from".to_string(), start.to_rfc3339());
        }
        if let Some(end) = request.end_time {
            params.insert("till".to_string(), end.to_rfc3339());
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(1000).to_string());
        }
        let value = self
            .send_signed_get("bequant.get_recent_fills", "/spot/history/trade", &params)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn bequant_order_form(request: &PlaceOrderRequest) -> ExchangeApiResult<HashMap<String, String>> {
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bequant spot order does not support reduce_only".to_string(),
        });
    }
    let mut body = HashMap::new();
    body.insert(
        "symbol".to_string(),
        normalize_bequant_symbol(&request.symbol.exchange_symbol.symbol)?,
    );
    body.insert(
        "side".to_string(),
        match request.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        }
        .to_string(),
    );
    body.insert("quantity".to_string(), request.quantity.clone());
    if let Some(client_order_id) = &request.client_order_id {
        body.insert("client_order_id".to_string(), client_order_id.clone());
    }
    let order_type = match request.order_type {
        OrderType::Market => "market",
        OrderType::PostOnly => "limit",
        OrderType::Limit | OrderType::IOC | OrderType::FOK => "limit",
        OrderType::StopMarket | OrderType::StopLimit => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bequant.stop_orders",
            })
        }
    };
    body.insert("type".to_string(), order_type.to_string());
    if !matches!(request.order_type, OrderType::Market) {
        body.insert(
            "price".to_string(),
            request
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bequant limit order requires price".to_string(),
                })?,
        );
    }
    if request.post_only || matches!(request.order_type, OrderType::PostOnly) {
        body.insert("post_only".to_string(), "true".to_string());
    }
    if let Some(time_in_force) = request.time_in_force {
        let value = match time_in_force {
            TimeInForce::GTC => "GTC",
            TimeInForce::IOC => "IOC",
            TimeInForce::FOK => "FOK",
            TimeInForce::GTX => {
                body.insert("post_only".to_string(), "true".to_string());
                "GTC"
            }
        };
        body.insert("time_in_force".to_string(), value.to_string());
    }
    Ok(body)
}
