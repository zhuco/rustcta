#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse,
    PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeSymbol, MarketType, OrderSide, OrderStatus, OrderType, PositionSide};

use super::parser::{currency_for_symbol, normalize_deribit_symbol};
use super::private_parser::{
    parse_balances, parse_fees, parse_fills, parse_order, parse_order_state, parse_orders,
    parse_positions,
};
use super::DeribitGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl DeribitGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("deribit.get_balances")?;
        let market_type = request.market_type.unwrap_or(MarketType::Futures);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "deribit.get_balances")?;
        let currencies = if request.assets.is_empty() {
            vec![
                "BTC".to_string(),
                "ETH".to_string(),
                "USDC".to_string(),
                "USDT".to_string(),
            ]
        } else {
            request
                .assets
                .iter()
                .map(|asset| asset.to_ascii_uppercase())
                .collect()
        };
        let mut balances = Vec::new();
        for currency in currencies {
            let mut params = HashMap::new();
            params.insert("currency".to_string(), currency);
            let value = self
                .rest
                .send_private_get(
                    "deribit.get_balances",
                    "/api/v2/private/get_account_summary",
                    &params,
                )
                .await?;
            balances.extend(parse_balances(
                &self.exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                &request.assets,
                market_type,
                &value,
            )?);
        }
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances,
        })
    }

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market(market_type)?;
        }
        self.ensure_private_rest("deribit.get_positions")?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "deribit.get_positions")?;
        let groups = position_groups(request.market_type, &request.symbols);
        let mut positions = Vec::new();
        for (currency, kind) in groups {
            let mut params = HashMap::new();
            params.insert("currency".to_string(), currency);
            if let Some(kind) = kind {
                params.insert("kind".to_string(), kind);
            }
            let value = self
                .rest
                .send_private_get(
                    "deribit.get_positions",
                    "/api/v2/private/get_positions",
                    &params,
                )
                .await?;
            let mut parsed = parse_positions(
                &self.exchange_id,
                tenant_id.clone(),
                account_id.clone(),
                &value,
            )?;
            if !request.symbols.is_empty() {
                parsed.retain(|position| {
                    position
                        .exchange_symbol
                        .as_ref()
                        .is_some_and(|position_symbol| {
                            request.symbols.iter().any(|symbol| {
                                symbol.symbol.eq_ignore_ascii_case(&position_symbol.symbol)
                            })
                        })
                });
            }
            positions.extend(parsed);
        }
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_private_rest("deribit.get_fees")?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "deribit.get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert("currency".to_string(), currency_for_symbol(symbol));
            params.insert("extended".to_string(), "true".to_string());
            let value = self
                .rest
                .send_private_get(
                    "deribit.get_fees",
                    "/api/v2/private/get_account_summary",
                    &params,
                )
                .await?;
            fees.extend(parse_fees(symbol, &value)?);
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
        self.ensure_supported_market(request.symbol.market_type)?;
        self.ensure_private_rest("deribit.place_order")?;
        let endpoint = match request.side {
            OrderSide::Buy => "/api/v2/private/buy",
            OrderSide::Sell => "/api/v2/private/sell",
        };
        let params = deribit_place_order_params(&request)?;
        let value = self
            .rest
            .send_private_get("deribit.place_order", endpoint, &params)
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            value
                .get("result")
                .and_then(|result| result.get("order"))
                .unwrap_or(&value),
        )?;
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_impl(
        &self,
        _request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "deribit.place_quote_market_order",
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        self.ensure_private_rest("deribit.cancel_order")?;
        let mut params = HashMap::new();
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("order_id".to_string(), order_id.clone());
        } else if let Some(client_order_id) = &request.client_order_id {
            params.insert("label".to_string(), client_order_id.clone());
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "deribit.cancel_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .rest
            .send_private_get("deribit.cancel_order", "/api/v2/private/cancel", &params)
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            value.get("result").unwrap_or(&value),
        )
        .unwrap_or_else(|_| cancelled_order_state(&self.exchange_id, &request));
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
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
        self.ensure_private_rest("deribit.batch_place_orders")?;
        let mut orders = Vec::with_capacity(request.orders.len());
        for order in request.orders {
            orders.push(self.place_order_impl(order).await?.order);
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
        self.ensure_private_rest("deribit.batch_cancel_orders")?;
        let mut orders = Vec::with_capacity(request.cancels.len());
        let mut cancelled_count = 0_u32;
        for cancel in request.cancels {
            let cancelled = self.cancel_order_impl(cancel).await?;
            if cancelled.cancelled {
                cancelled_count = cancelled_count.saturating_add(1);
            }
            orders.push(cancelled.order);
        }
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
            report: None,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        self.ensure_private_rest("deribit.amend_order")?;
        let mut params = HashMap::new();
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("order_id".to_string(), order_id.clone());
        } else if let Some(client_order_id) = &request.client_order_id {
            params.insert("label".to_string(), client_order_id.clone());
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "deribit.amend_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        params.insert("amount".to_string(), request.new_quantity.clone());
        let value = self
            .rest
            .send_private_get("deribit.amend_order", "/api/v2/private/edit", &params)
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            value
                .get("result")
                .and_then(|result| result.get("order"))
                .unwrap_or(&value),
        )?;
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market(market_type)?;
        }
        self.ensure_private_rest("deribit.cancel_all_orders")?;
        let mut params = HashMap::new();
        let endpoint = if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            params.insert(
                "instrument_name".to_string(),
                normalize_deribit_symbol(symbol)?,
            );
            "/api/v2/private/cancel_all_by_instrument"
        } else {
            "/api/v2/private/cancel_all"
        };
        let value = self
            .rest
            .send_private_get("deribit.cancel_all_orders", endpoint, &params)
            .await?;
        let cancelled_count = value
            .get("result")
            .and_then(|value| value.as_u64())
            .unwrap_or(0) as u32;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: Vec::new(),
            cancelled_count,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        self.ensure_private_rest("deribit.query_order")?;
        let mut params = HashMap::new();
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("order_id".to_string(), order_id.clone());
        } else {
            return Err(ExchangeApiError::Unsupported {
                operation: "deribit.query_order_by_client_order_id",
            });
        }
        let value = self
            .rest
            .send_private_get(
                "deribit.query_order",
                "/api/v2/private/get_order_state",
                &params,
            )
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order: parse_order(&self.exchange_id, Some(&request.symbol), &value)?,
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market(market_type)?;
        }
        self.ensure_private_rest("deribit.get_open_orders")?;
        let mut params = HashMap::new();
        let endpoint = if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            params.insert(
                "instrument_name".to_string(),
                normalize_deribit_symbol(symbol)?,
            );
            "/api/v2/private/get_open_orders_by_instrument"
        } else {
            params.insert("currency".to_string(), "BTC".to_string());
            if let Some(market_type) = request.market_type {
                params.insert("kind".to_string(), deribit_kind(market_type).to_string());
            }
            "/api/v2/private/get_open_orders_by_currency"
        };
        let value = self
            .rest
            .send_private_get("deribit.get_open_orders", endpoint, &params)
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
            self.ensure_supported_market(market_type)?;
        }
        self.ensure_private_rest("deribit.get_recent_fills")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "deribit.get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "instrument_name".to_string(),
            normalize_deribit_symbol(symbol)?,
        );
        params.insert(
            "count".to_string(),
            request.limit.unwrap_or(100).clamp(1, 1000).to_string(),
        );
        params.insert("include_old".to_string(), "true".to_string());
        if let Some(start) = request.start_time {
            params.insert(
                "start_timestamp".to_string(),
                start.timestamp_millis().to_string(),
            );
        }
        if let Some(end) = request.end_time {
            params.insert(
                "end_timestamp".to_string(),
                end.timestamp_millis().to_string(),
            );
        }
        let value = self
            .rest
            .send_private_get(
                "deribit.get_recent_fills",
                "/api/v2/private/get_user_trades_by_instrument",
                &params,
            )
            .await?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "deribit.get_recent_fills")?;
        let fills = parse_fills(
            &self.exchange_id,
            tenant_id,
            account_id,
            Some(symbol),
            &value,
        )?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }
}

fn deribit_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "instrument_name".to_string(),
        normalize_deribit_symbol(&request.symbol)?,
    );
    params.insert("amount".to_string(), request.quantity.clone());
    params.insert(
        "type".to_string(),
        deribit_order_type(request.order_type).to_string(),
    );
    if let Some(price) = &request.price {
        params.insert("price".to_string(), price.clone());
    }
    if let Some(client_order_id) = &request.client_order_id {
        params.insert("label".to_string(), client_order_id.clone());
    }
    if request.reduce_only {
        params.insert("reduce_only".to_string(), "true".to_string());
    }
    if request.post_only || matches!(request.order_type, OrderType::PostOnly) {
        params.insert("post_only".to_string(), "true".to_string());
    }
    if let Some(time_in_force) = request.time_in_force {
        params.insert(
            "time_in_force".to_string(),
            deribit_time_in_force(time_in_force).to_string(),
        );
    }
    if let Some(position_side) = request.position_side {
        if !matches!(position_side, PositionSide::Net | PositionSide::None) {
            return Err(ExchangeApiError::Unsupported {
                operation: "deribit.hedged_position_side",
            });
        }
    }
    Ok(params)
}

fn deribit_order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "market",
        OrderType::StopMarket => "stop_market",
        OrderType::StopLimit => "stop_limit",
        _ => "limit",
    }
}

fn deribit_time_in_force(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::GTC | TimeInForce::GTX => "good_til_cancelled",
        TimeInForce::IOC => "immediate_or_cancel",
        TimeInForce::FOK => "fill_or_kill",
    }
}

fn deribit_kind(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Option => "option",
        _ => "future",
    }
}

fn position_groups(
    market_type: Option<MarketType>,
    symbols: &[ExchangeSymbol],
) -> Vec<(String, Option<String>)> {
    if !symbols.is_empty() {
        let mut groups = Vec::new();
        for symbol in symbols {
            let currency = symbol
                .symbol
                .split('-')
                .next()
                .unwrap_or("BTC")
                .to_ascii_uppercase();
            let kind = match symbol.market_type {
                MarketType::Option => Some("option".to_string()),
                MarketType::Futures | MarketType::Perpetual => Some("future".to_string()),
                _ => None,
            };
            let group = (currency, kind);
            if !groups.contains(&group) {
                groups.push(group);
            }
        }
        return groups;
    }
    let kind = market_type.map(deribit_kind).map(str::to_string);
    vec![("BTC".to_string(), kind.clone()), ("ETH".to_string(), kind)]
}

fn cancelled_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: request.exchange_order_id.clone(),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::Net),
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
