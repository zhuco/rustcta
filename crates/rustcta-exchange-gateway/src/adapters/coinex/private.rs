use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, PageCursor, PlaceOrderRequest, PlaceOrderResponse,
    Position, PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeSymbol, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
    SchemaVersion,
};
use serde_json::{json, Value};

use super::parser::normalize_coinex_symbol;
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_open_orders, parse_order_state, parse_recent_fills,
};
use super::CoinExGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl CoinExGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        let body = coinex_order_body(&request)?;
        let endpoint = coinex_private_path(request.symbol.market_type, "order")?;
        let value = self
            .send_signed_post("coinex.place_order", endpoint, &HashMap::new(), &body)
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
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        let body = coinex_quote_market_order_body(&request)?;
        let value = self
            .send_signed_post(
                "coinex.place_quote_market_order",
                "/spot/order",
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
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
        self.ensure_market_type(request.symbol.market_type)?;
        let body = coinex_cancel_order_body(&request)?;
        let endpoint = coinex_private_path(request.symbol.market_type, "cancel_order")?;
        let value = if request.symbol.market_type == MarketType::Perpetual {
            self.send_signed_post("coinex.cancel_order", endpoint, &HashMap::new(), &body)
                .await?
        } else {
            self.send_signed_delete("coinex.cancel_order", endpoint, &HashMap::new(), &body)
                .await?
        };
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| coinex_cancel_order_state(&self.exchange_id, &request, &value));
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
        if request.orders.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinex batch_place_orders requires at least one order".to_string(),
            });
        }
        if request.orders.len() > super::capabilities::COINEX_COMPOSED_BATCH_MAX_ITEMS as usize {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "coinex batch_place_orders supports at most {} orders",
                    super::capabilities::COINEX_COMPOSED_BATCH_MAX_ITEMS
                ),
            });
        }

        let mut orders = Vec::with_capacity(request.orders.len());
        for order_request in request.orders {
            self.ensure_exchange(&order_request.symbol.exchange)?;
            self.ensure_market_type(order_request.symbol.market_type)?;
            orders.push(self.place_order_impl(order_request).await?.order);
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
        if request.cancels.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinex batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        if request.cancels.len() > super::capabilities::COINEX_COMPOSED_BATCH_MAX_ITEMS as usize {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "coinex batch_cancel_orders supports at most {} cancels",
                    super::capabilities::COINEX_COMPOSED_BATCH_MAX_ITEMS
                ),
            });
        }

        let mut orders = Vec::with_capacity(request.cancels.len());
        let mut cancelled_count = 0_u32;
        for cancel_request in request.cancels {
            self.ensure_exchange(&cancel_request.symbol.exchange)?;
            self.ensure_market_type(cancel_request.symbol.market_type)?;
            let response = self.cancel_order_impl(cancel_request).await?;
            if response.cancelled {
                cancelled_count += 1;
            }
            orders.push(response.order);
        }

        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
            report: None,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_optional_market_type(request.market_type)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinex cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_market_type(symbol.market_type)?;
        if let Some(market_type) = request.market_type {
            if market_type != symbol.market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message:
                        "coinex cancel_all_orders symbol market_type conflicts with request market_type"
                            .to_string(),
                });
            }
        }
        let body = json!({
            "market": normalize_coinex_symbol(&symbol.exchange_symbol.symbol)?,
            "market_type": coinex_market_type_text(symbol.market_type)?,
        });
        let value = self
            .send_signed_post(
                "coinex.cancel_all_orders",
                coinex_private_path(symbol.market_type, "cancel_all_orders")?,
                &HashMap::new(),
                &body,
            )
            .await?;
        let orders = coinex_cancel_all_orders(&self.exchange_id, symbol, &value);
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        let body = coinex_amend_order_body(&request)?;
        let value = self
            .send_signed_post(
                "coinex.amend_order",
                coinex_private_path(request.symbol.market_type, "amend_order")?,
                &HashMap::new(),
                &body,
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_market_type(market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let value = self
            .send_signed_get(
                "coinex.get_balances",
                coinex_balance_path(market_type)?,
                &HashMap::new(),
            )
            .await?;
        let balances = parse_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
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

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinex get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_market_type(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "market".to_string(),
                normalize_coinex_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .send_signed_get(
                    "coinex.get_fees",
                    coinex_private_path(symbol.market_type, "market")?,
                    &params,
                )
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
        self.ensure_market_type(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "market".to_string(),
            normalize_coinex_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert(
            "market_type".to_string(),
            coinex_market_type_text(request.symbol.market_type)?.to_string(),
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("order_id".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("client_id".to_string(), client_order_id.to_string());
        }
        if !params.contains_key("order_id") && !params.contains_key("client_id") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "coinex query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .send_signed_get(
                "coinex.query_order",
                coinex_private_path(request.symbol.market_type, "query_order")?,
                &params,
            )
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
        self.ensure_optional_market_type(request.market_type)?;
        let market_type = request
            .market_type
            .or_else(|| request.symbol.as_ref().map(|symbol| symbol.market_type))
            .unwrap_or(MarketType::Spot);
        let mut params = HashMap::new();
        params.insert(
            "market_type".to_string(),
            coinex_market_type_text(market_type)?.to_string(),
        );
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_market_type(symbol.market_type)?;
            if symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message:
                        "coinex get_open_orders symbol market_type conflicts with request market_type"
                            .to_string(),
                });
            }
            params.insert(
                "market".to_string(),
                normalize_coinex_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get(
                "coinex.get_open_orders",
                coinex_private_path(market_type, "open_orders")?,
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
        self.ensure_optional_market_type(request.market_type)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinex get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_market_type(symbol.market_type)?;
        if let Some(market_type) = request.market_type {
            if market_type != symbol.market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message:
                        "coinex get_recent_fills symbol market_type conflicts with request market_type"
                            .to_string(),
                });
            }
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "market".to_string(),
            normalize_coinex_symbol(&symbol.exchange_symbol.symbol)?,
        );
        params.insert(
            "market_type".to_string(),
            coinex_market_type_text(symbol.market_type)?.to_string(),
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("order_id".to_string(), order_id.to_string());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "start_time".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert(
                "end_time".to_string(),
                end_time.timestamp_millis().to_string(),
            );
        }
        apply_coinex_fills_pagination(&request, &mut params)?;
        if !params.contains_key("limit") {
            if let Some(limit) = request.limit {
                params.insert(
                    "limit".to_string(),
                    limit
                        .min(super::capabilities::COINEX_SPOT_MAX_PAGE_LIMIT)
                        .to_string(),
                );
            } else {
                params.insert(
                    "limit".to_string(),
                    super::capabilities::COINEX_SPOT_MAX_PAGE_LIMIT.to_string(),
                );
            }
        }
        let value = self
            .send_signed_get(
                "coinex.get_recent_fills",
                coinex_private_path(symbol.market_type, "fills")?,
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

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Perpetual);
        if market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinex.spot_positions",
            });
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert("market_type".to_string(), "FUTURES".to_string());
        if let Some(symbol) = request.symbols.first() {
            self.ensure_exchange(&symbol.exchange_id)?;
            if symbol.market_type != MarketType::Perpetual {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "coinex futures positions require perpetual symbols".to_string(),
                });
            }
            params.insert(
                "market".to_string(),
                normalize_coinex_symbol(&symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get("coinex.get_positions", "/futures/pending-position", &params)
            .await?;
        let positions = parse_coinex_positions(
            &self.exchange_id,
            tenant_id,
            account_id,
            request.symbols.as_slice(),
            &value,
        )?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions,
        })
    }
}

fn apply_coinex_fills_pagination(
    request: &RecentFillsRequest,
    params: &mut HashMap<String, String>,
) -> ExchangeApiResult<()> {
    let Some(page) = &request.page else {
        return Ok(());
    };
    page.validate(Some(super::capabilities::COINEX_SPOT_MAX_PAGE_LIMIT))
        .map_err(|message| ExchangeApiError::InvalidRequest { message })?;
    if let Some(limit) = page.limit {
        params.insert("limit".to_string(), limit.to_string());
    }
    let Some(cursor) = page.cursor.as_ref() else {
        return Ok(());
    };
    match cursor {
        PageCursor::Timestamp { millis } => {
            params.insert("start_time".to_string(), millis.to_string());
        }
        PageCursor::TimeRange { start_ms, end_ms } => {
            params.insert("start_time".to_string(), start_ms.to_string());
            if let Some(end_ms) = end_ms {
                params.insert("end_time".to_string(), end_ms.to_string());
            }
        }
        PageCursor::Id { id } | PageCursor::Token { token: id } => {
            if id.trim().is_empty() {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "coinex fills pagination cursor id must not be empty".to_string(),
                });
            }
            params.insert("last_id".to_string(), id.clone());
        }
        PageCursor::Offset { .. } => {
            return Err(ExchangeApiError::InvalidRequest {
                message:
                    "coinex fills pagination supports timestamp/time-range/id cursors, not offset"
                        .to_string(),
            });
        }
    }
    Ok(())
}

fn coinex_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only && request.symbol.market_type != MarketType::Perpetual {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinex spot order does not support reduce_only".to_string(),
        });
    }
    let (order_type, option) = coinex_order_type(request.order_type, request.time_in_force)?;
    let mut body = json!({
        "market": normalize_coinex_symbol(&request.symbol.exchange_symbol.symbol)?,
        "market_type": coinex_market_type_text(request.symbol.market_type)?,
        "side": coinex_side(request.side),
        "type": order_type,
        "amount": non_empty("quantity", &request.quantity)?,
    });
    if request.reduce_only {
        body["is_reduce_only"] = Value::Bool(true);
    }
    if let Some(option) = option {
        body["option"] = Value::String(option.to_string());
    }
    if request.order_type != OrderType::Market {
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "coinex limit-style order requires price".to_string(),
            })?;
        body["price"] = Value::String(non_empty("price", price)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["client_id"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn coinex_quote_market_order_body(request: &QuoteMarketOrderRequest) -> ExchangeApiResult<Value> {
    if request.side != OrderSide::Buy {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinex.quote_market_sell",
        });
    }
    let quote_asset = request
        .symbol
        .canonical_symbol
        .as_ref()
        .map(|symbol| symbol.quote_asset().to_string())
        .unwrap_or_else(|| "USDT".to_string());
    let mut body = json!({
        "market": normalize_coinex_symbol(&request.symbol.exchange_symbol.symbol)?,
        "market_type": "SPOT",
        "side": "buy",
        "type": "market",
        "amount": non_empty("quote_quantity", &request.quote_quantity)?,
        "ccy": quote_asset,
    });
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["client_id"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn coinex_cancel_order_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "market": normalize_coinex_symbol(&request.symbol.exchange_symbol.symbol)?,
        "market_type": coinex_market_type_text(request.symbol.market_type)?,
    });
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body["order_id"] = Value::String(non_empty("exchange_order_id", order_id)?);
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["client_id"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if body.get("order_id").is_none() && body.get("client_id").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "coinex cancel_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(body)
}

fn coinex_amend_order_body(request: &AmendOrderRequest) -> ExchangeApiResult<Value> {
    if request
        .new_client_order_id
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "coinex.amend_new_client_order_id",
        });
    }
    let order_id = request
        .exchange_order_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(ExchangeApiError::Unsupported {
            operation: "coinex.amend_by_client_order_id",
        })?;
    Ok(json!({
        "market": normalize_coinex_symbol(&request.symbol.exchange_symbol.symbol)?,
        "market_type": coinex_market_type_text(request.symbol.market_type)?,
        "order_id": coinex_numeric_order_id(order_id)?,
        "amount": non_empty("new_quantity", &request.new_quantity)?,
    }))
}

fn coinex_cancel_all_orders(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> Vec<rustcta_exchange_api::OrderState> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    parse_order_state(exchange_id, Some(symbol), item).unwrap_or_else(|_| {
                        coinex_cancel_order_state_from_fields(
                            exchange_id,
                            symbol,
                            value_text(item.get("order_id").or_else(|| item.get("id"))),
                            value_text(item.get("client_id")),
                        )
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn coinex_cancel_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> rustcta_exchange_api::OrderState {
    coinex_cancel_order_state_from_fields(
        exchange_id,
        &request.symbol,
        value_text(value.get("order_id").or_else(|| value.get("id")))
            .or_else(|| request.exchange_order_id.clone()),
        value_text(value.get("client_id")).or_else(|| request.client_order_id.clone()),
    )
}

fn coinex_cancel_order_state_from_fields(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
) -> rustcta_exchange_api::OrderState {
    rustcta_exchange_api::OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
        side: OrderSide::Buy,
        position_side: Some(if symbol.market_type == MarketType::Perpetual {
            PositionSide::Net
        } else {
            PositionSide::None
        }),
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

fn coinex_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn coinex_order_type(
    order_type: OrderType,
    tif: Option<TimeInForce>,
) -> ExchangeApiResult<(&'static str, Option<&'static str>)> {
    Ok(match (order_type, tif) {
        (OrderType::Market, _) => ("market", None),
        (OrderType::PostOnly, _) | (_, Some(TimeInForce::GTX)) => ("limit", Some("maker_only")),
        (OrderType::IOC, _) | (_, Some(TimeInForce::IOC)) => ("limit", Some("ioc")),
        (OrderType::FOK, _) | (_, Some(TimeInForce::FOK)) => ("limit", Some("fok")),
        (OrderType::Limit, _) => ("limit", Some("normal")),
        (OrderType::StopMarket | OrderType::StopLimit, _) => {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinex.stop_order",
            });
        }
    })
}

fn coinex_market_type_text(market_type: MarketType) -> ExchangeApiResult<&'static str> {
    match market_type {
        MarketType::Spot => Ok("SPOT"),
        MarketType::Perpetual => Ok("FUTURES"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinex.unsupported_market_type",
        }),
    }
}

fn coinex_private_path(
    market_type: MarketType,
    operation: &str,
) -> ExchangeApiResult<&'static str> {
    match (market_type, operation) {
        (MarketType::Spot, "order") => Ok("/spot/order"),
        (MarketType::Spot, "cancel_order") => Ok("/spot/order"),
        (MarketType::Spot, "cancel_all_orders") => Ok("/spot/cancel-all-order"),
        (MarketType::Spot, "amend_order") => Ok("/spot/modify-order"),
        (MarketType::Spot, "market") => Ok("/spot/market"),
        (MarketType::Spot, "query_order") => Ok("/spot/order-status"),
        (MarketType::Spot, "open_orders") => Ok("/spot/pending-order"),
        (MarketType::Spot, "fills") => Ok("/spot/finished-order"),
        (MarketType::Perpetual, "order") => Ok("/futures/order"),
        (MarketType::Perpetual, "cancel_order") => Ok("/futures/cancel-order"),
        (MarketType::Perpetual, "cancel_all_orders") => Ok("/futures/cancel-all-order"),
        (MarketType::Perpetual, "amend_order") => Ok("/futures/modify-order"),
        (MarketType::Perpetual, "market") => Ok("/futures/market"),
        (MarketType::Perpetual, "query_order") => Ok("/futures/order-status"),
        (MarketType::Perpetual, "open_orders") => Ok("/futures/pending-order"),
        (MarketType::Perpetual, "fills") => Ok("/futures/user-deals"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinex.unsupported_private_path",
        }),
    }
}

fn coinex_balance_path(market_type: MarketType) -> ExchangeApiResult<&'static str> {
    match market_type {
        MarketType::Spot => Ok("/assets/spot/balance"),
        MarketType::Perpetual => Ok("/assets/futures/balance"),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "coinex.unsupported_balance_market_type",
        }),
    }
}

fn parse_coinex_positions(
    exchange_id: &rustcta_types::ExchangeId,
    tenant_id: rustcta_exchange_api::TenantId,
    account_id: rustcta_exchange_api::AccountId,
    symbol_filters: &[ExchangeSymbol],
    value: &Value,
) -> ExchangeApiResult<Vec<Position>> {
    let items = value
        .as_array()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "coinex futures positions response is not an array".to_string(),
        })?;
    let filters = symbol_filters
        .iter()
        .map(|symbol| symbol.symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let mut positions = Vec::new();
    for item in items {
        let market = value_text(item.get("market")).unwrap_or_default();
        let normalized = normalize_coinex_symbol(&market)?;
        if !filters.is_empty() && !filters.contains(&normalized) {
            continue;
        }
        let (base, quote) =
            split_coinex_symbol(&normalized).ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("coinex futures position market missing assets: {market}"),
            })?;
        let quantity = value_text(
            item.get("amount")
                .or_else(|| item.get("quantity"))
                .or_else(|| item.get("open_interest")),
        )
        .unwrap_or_else(|| "0".to_string())
        .parse::<f64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid coinex position quantity: {error}"),
        })?
        .abs();
        if quantity <= 0.0 {
            continue;
        }
        let side = match value_text(item.get("side")).unwrap_or_default().as_str() {
            "long" | "LONG" => PositionSide::Long,
            "short" | "SHORT" => PositionSide::Short,
            _ => PositionSide::Net,
        };
        positions.push(Position {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id.clone(),
            account_id: account_id.clone(),
            exchange_id: exchange_id.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: CanonicalSymbol::new(base, quote).map_err(|error| {
                ExchangeApiError::InvalidRequest {
                    message: error.to_string(),
                }
            })?,
            exchange_symbol: Some(
                ExchangeSymbol::new(exchange_id.clone(), MarketType::Perpetual, normalized)
                    .map_err(|error| ExchangeApiError::InvalidRequest {
                        message: error.to_string(),
                    })?,
            ),
            side,
            quantity,
            entry_price: value_text(item.get("open_price").or_else(|| item.get("entry_price")))
                .and_then(|value| value.parse().ok()),
            mark_price: value_text(item.get("mark_price")).and_then(|value| value.parse().ok()),
            liquidation_price: value_text(item.get("liq_price"))
                .and_then(|value| value.parse().ok()),
            unrealized_pnl: value_text(item.get("unrealized_pnl").or_else(|| item.get("pnl")))
                .and_then(|value| value.parse().ok()),
            leverage: value_text(item.get("leverage")).and_then(|value| value.parse().ok()),
            observed_at: chrono::Utc::now(),
        });
    }
    Ok(positions)
}

fn split_coinex_symbol(symbol: &str) -> Option<(String, String)> {
    const QUOTES: [&str; 9] = [
        "USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "EUR", "TRY", "BNB",
    ];
    QUOTES.iter().find_map(|quote| {
        symbol
            .strip_suffix(quote)
            .filter(|base| !base.is_empty())
            .map(|base| (base.to_string(), quote.to_string()))
    })
}

fn coinex_numeric_order_id(order_id: &str) -> ExchangeApiResult<Value> {
    let numeric_id = order_id
        .parse::<u64>()
        .map_err(|_| ExchangeApiError::InvalidRequest {
            message: "coinex amend_order requires numeric exchange_order_id".to_string(),
        })?;
    Ok(Value::Number(numeric_id.into()))
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("coinex {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn value_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}
