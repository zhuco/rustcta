use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, PlaceOrderResponse, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{OrderSide, OrderType};

use super::parser::{
    normalize_market_symbol, parse_balances, parse_fee_snapshot, parse_fills, parse_order,
    parse_orders,
};
use super::UpbitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl UpbitGatewayAdapter {
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
            .signed_get("upbit.get_balances", "/v1/accounts", &[])
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

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        let mut fees = Vec::new();
        for symbol in request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            let market = normalize_market_symbol(&symbol.exchange_symbol.symbol)?;
            let value = self
                .signed_get(
                    "upbit.get_fees",
                    "/v1/orders/chance",
                    &[("market".to_string(), market)],
                )
                .await?;
            fees.push(parse_fee_snapshot(&self.exchange_id, symbol, &value));
        }
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
        let params = upbit_order_params(&request)?;
        let value = self
            .signed_post("upbit.place_order", "/v1/orders", &params)
            .await?;
        let metadata_exchange = request.symbol.exchange.clone();
        let symbol = request.symbol.clone();
        let request_id = request.context.request_id;
        let order = parse_order(&self.exchange_id, Some(&symbol), &value)?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(metadata_exchange, request_id),
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
        let params = vec![
            (
                "market".to_string(),
                normalize_market_symbol(&request.symbol.exchange_symbol.symbol)?,
            ),
            ("side".to_string(), "bid".to_string()),
            ("ord_type".to_string(), "price".to_string()),
            ("price".to_string(), request.quote_quantity.clone()),
        ];
        let value = self
            .signed_post("upbit.place_quote_market_order", "/v1/orders", &params)
            .await?;
        let metadata_exchange = request.symbol.exchange.clone();
        let symbol = request.symbol.clone();
        let request_id = request.context.request_id;
        let order = parse_order(&self.exchange_id, Some(&symbol), &value)?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(metadata_exchange, request_id),
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
            .clone()
            .or(request.client_order_id.clone())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "upbit cancel_order requires exchange_order_id or client_order_id"
                    .to_string(),
            })?;
        let key = if request.exchange_order_id.is_some() {
            "uuid"
        } else {
            "identifier"
        };
        let value = self
            .signed_delete(
                "upbit.cancel_order",
                "/v1/order",
                &[(key.to_string(), order_id)],
            )
            .await?;
        let metadata_exchange = request.symbol.exchange.clone();
        let request_id = request.context.request_id.clone();
        let order = parse_order(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| cancelled_order(&self.exchange_id, &request));
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(metadata_exchange, request_id),
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
        let order_id = request
            .exchange_order_id
            .clone()
            .or(request.client_order_id.clone())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "upbit query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            })?;
        let key = if request.exchange_order_id.is_some() {
            "uuid"
        } else {
            "identifier"
        };
        let value = self
            .signed_get(
                "upbit.query_order",
                "/v1/order",
                &[(key.to_string(), order_id)],
            )
            .await?;
        let metadata_exchange = request.symbol.exchange.clone();
        let symbol = request.symbol.clone();
        let request_id = request.context.request_id;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(metadata_exchange, request_id),
            order: Some(parse_order(&self.exchange_id, Some(&symbol), &value)?),
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
        let mut params = vec![(
            "limit".to_string(),
            request
                .page
                .as_ref()
                .and_then(|page| page.limit)
                .unwrap_or(100)
                .to_string(),
        )];
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.push((
                "market".to_string(),
                normalize_market_symbol(&symbol.exchange_symbol.symbol)?,
            ));
        }
        let value = self
            .signed_get("upbit.get_open_orders", "/v1/orders/open", &params)
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
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = vec![(
            "limit".to_string(),
            request.limit.unwrap_or(100).to_string(),
        )];
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
            params.push((
                "market".to_string(),
                normalize_market_symbol(&symbol.exchange_symbol.symbol)?,
            ));
        }
        if let Some(order_id) = request.exchange_order_id.as_ref() {
            params.push(("uuid".to_string(), order_id.clone()));
        }
        let value = if request.exchange_order_id.is_some() || request.client_order_id.is_some() {
            self.signed_get("upbit.get_recent_fills", "/v1/order", &params)
                .await?
        } else {
            self.signed_get("upbit.get_recent_fills", "/v1/orders/closed", &params)
                .await?
        };
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.symbol.as_ref(),
                &value,
            )?,
        })
    }
}

fn upbit_order_params(request: &PlaceOrderRequest) -> ExchangeApiResult<Vec<(String, String)>> {
    let mut params = vec![
        (
            "market".to_string(),
            normalize_market_symbol(&request.symbol.exchange_symbol.symbol)?,
        ),
        (
            "side".to_string(),
            match request.side {
                OrderSide::Buy => "bid",
                OrderSide::Sell => "ask",
            }
            .to_string(),
        ),
    ];
    match request.order_type {
        OrderType::Limit => {
            params.push(("ord_type".to_string(), "limit".to_string()));
            params.push((
                "price".to_string(),
                request
                    .price
                    .clone()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "upbit limit order requires price".to_string(),
                    })?,
            ));
            params.push(("volume".to_string(), request.quantity.clone()));
        }
        OrderType::Market if request.side == OrderSide::Buy => {
            let quote =
                request
                    .quote_quantity
                    .clone()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "upbit market buy requires quote_quantity".to_string(),
                    })?;
            params.push(("ord_type".to_string(), "price".to_string()));
            params.push(("price".to_string(), quote));
        }
        OrderType::Market => {
            params.push(("ord_type".to_string(), "market".to_string()));
            params.push(("volume".to_string(), request.quantity.clone()));
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "upbit.place_order_type",
            })
        }
    }
    if let Some(identifier) = &request.client_order_id {
        params.push(("identifier".to_string(), identifier.clone()));
    }
    Ok(params)
}

fn cancelled_order(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: rustcta_types::MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: request.exchange_order_id.clone(),
        side: rustcta_types::OrderSide::Buy,
        position_side: None,
        order_type: rustcta_types::OrderType::Limit,
        time_in_force: None,
        status: rustcta_types::OrderStatus::Cancelled,
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
