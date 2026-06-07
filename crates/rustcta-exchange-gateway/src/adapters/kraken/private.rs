use std::collections::BTreeMap;

use reqwest::Method;
use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PageCursor, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};
use serde_json::{json, Value};

use super::parser::{normalize_futures_symbol, normalize_spot_symbol};
use super::private_parser::{
    ack_order_state, cancelled_order_state, parse_balances, parse_fee_snapshots, parse_order_list,
    parse_order_state, parse_positions, parse_recent_fills,
};
use super::KrakenGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl KrakenGatewayAdapter {
    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = validate_batch_place_request(self, &request)?;
        let value = match market_type {
            MarketType::Spot => {
                let mut params = BTreeMap::new();
                let orders = request
                    .orders
                    .iter()
                    .map(spot_order_params)
                    .collect::<ExchangeApiResult<Vec<_>>>()?;
                params.insert("orders".to_string(), json_string(json!(orders))?);
                self.send_spot_private("kraken.batch_place_orders", "AddOrderBatch", params)
                    .await?
            }
            MarketType::Perpetual => {
                let batch = request
                    .orders
                    .iter()
                    .map(|order| {
                        futures_order_params(order).map(|params| {
                            json!({
                                "order": "send",
                                "params": params,
                            })
                        })
                    })
                    .collect::<ExchangeApiResult<Vec<_>>>()?;
                let mut params = BTreeMap::new();
                params.insert("batchOrder".to_string(), json_string(json!(batch))?);
                self.send_futures_private(
                    "kraken.futures.batch_place_orders",
                    Method::POST,
                    "batchorder",
                    params,
                )
                .await?
            }
            _ => unreachable!("ensure_market_type checked"),
        };
        let exchange_order_ids = batch_order_ids(&value);
        let orders = request
            .orders
            .iter()
            .enumerate()
            .map(|(index, order_request)| {
                ack_order_state(
                    &self.exchange_id,
                    order_request,
                    exchange_order_ids.get(index).cloned().flatten(),
                )
            })
            .collect();
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
        let market_type = validate_batch_cancel_request(self, &request)?;
        match market_type {
            MarketType::Spot => {
                let ids = request
                    .cancels
                    .iter()
                    .map(cancel_id)
                    .collect::<ExchangeApiResult<Vec<_>>>()?;
                let mut params = BTreeMap::new();
                params.insert("orders".to_string(), json_string(json!(ids))?);
                self.send_spot_private("kraken.batch_cancel_orders", "CancelOrderBatch", params)
                    .await?;
            }
            MarketType::Perpetual => {
                let batch = request
                    .cancels
                    .iter()
                    .map(|cancel| {
                        let mut params = BTreeMap::new();
                        params.insert("order_id".to_string(), cancel_id(cancel)?);
                        Ok(json!({
                            "order": "cancel",
                            "params": params,
                        }))
                    })
                    .collect::<ExchangeApiResult<Vec<_>>>()?;
                let mut params = BTreeMap::new();
                params.insert("batchOrder".to_string(), json_string(json!(batch))?);
                self.send_futures_private(
                    "kraken.futures.batch_cancel_orders",
                    Method::POST,
                    "batchorder",
                    params,
                )
                .await?;
            }
            _ => unreachable!("ensure_market_type checked"),
        }
        let orders = request
            .cancels
            .iter()
            .map(|cancel| {
                cancelled_order_state(
                    &self.exchange_id,
                    &cancel.symbol,
                    cancel.exchange_order_id.clone(),
                    cancel.client_order_id.clone(),
                )
            })
            .collect::<Vec<_>>();
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: None,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        let mut params = match request.symbol.market_type {
            rustcta_types::MarketType::Spot => spot_order_params(&request)?,
            rustcta_types::MarketType::Perpetual => futures_order_params(&request)?,
            _ => unreachable!("ensure_market_type checked"),
        };
        let value = match request.symbol.market_type {
            rustcta_types::MarketType::Spot => {
                self.send_spot_private("kraken.place_order", "AddOrder", params.clone())
                    .await?
            }
            rustcta_types::MarketType::Perpetual => {
                self.send_futures_private(
                    "kraken.futures.place_order",
                    Method::POST,
                    "sendorder",
                    params.clone(),
                )
                .await?
            }
            _ => unreachable!("ensure_market_type checked"),
        };
        let exchange_order_id = value
            .get("txid")
            .and_then(serde_json::Value::as_array)
            .and_then(|ids| ids.first())
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                value
                    .get("sendStatus")
                    .and_then(|status| status.get("order_id"))
                    .and_then(serde_json::Value::as_str)
            })
            .map(str::to_string);
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            exchange_order_id.as_deref(),
            &value,
        )
        .unwrap_or_else(|_| ack_order_state(&self.exchange_id, &request, exchange_order_id));
        params.clear();
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
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        if request.side != OrderSide::Buy {
            return Err(ExchangeApiError::Unsupported {
                operation: "kraken.quote_market_sell",
            });
        }
        let mut params = BTreeMap::new();
        params.insert("pair".to_string(), normalize_spot_symbol(&request.symbol)?);
        params.insert("type".to_string(), "buy".to_string());
        params.insert("ordertype".to_string(), "market".to_string());
        params.insert("volume".to_string(), request.quote_quantity.clone());
        params.insert("oflags".to_string(), "viqc".to_string());
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("cl_ord_id".to_string(), client_order_id.to_string());
        }
        let value = self
            .send_spot_private("kraken.place_quote_market_order", "AddOrder", params)
            .await?;
        let exchange_order_id = value
            .get("txid")
            .and_then(serde_json::Value::as_array)
            .and_then(|ids| ids.first())
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let pseudo = PlaceOrderRequest {
            schema_version: request.schema_version,
            context: request.context.clone(),
            symbol: request.symbol.clone(),
            client_order_id: request.client_order_id.clone(),
            side: request.side,
            position_side: None,
            order_type: OrderType::Market,
            time_in_force: None,
            quantity: request.quote_quantity.clone(),
            price: None,
            quote_quantity: Some(request.quote_quantity),
            reduce_only: false,
            post_only: false,
        };
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: ack_order_state(&self.exchange_id, &pseudo, exchange_order_id),
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        let id = request
            .exchange_order_id
            .as_deref()
            .or(request.client_order_id.as_deref())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kraken cancel_order requires exchange_order_id or client_order_id"
                    .to_string(),
            })?;
        match request.symbol.market_type {
            rustcta_types::MarketType::Spot => {
                let mut params = BTreeMap::new();
                params.insert("txid".to_string(), id.to_string());
                self.send_spot_private("kraken.cancel_order", "CancelOrder", params)
                    .await?;
            }
            rustcta_types::MarketType::Perpetual => {
                let mut params = BTreeMap::new();
                params.insert("order_id".to_string(), id.to_string());
                self.send_futures_private(
                    "kraken.futures.cancel_order",
                    Method::POST,
                    "cancelorder",
                    params,
                )
                .await?;
            }
            _ => unreachable!("ensure_market_type checked"),
        }
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: cancelled_order_state(
                &self.exchange_id,
                &request.symbol,
                request.exchange_order_id,
                request.client_order_id,
            ),
            cancelled: true,
        })
    }

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_market_type(symbol.market_type)?;
        }
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(rustcta_types::MarketType::Spot);
        match market_type {
            rustcta_types::MarketType::Spot => {
                if request.symbol.is_some() {
                    return Err(ExchangeApiError::Unsupported {
                        operation: "kraken.spot_symbol_scoped_cancel_all",
                    });
                }
                self.send_spot_private("kraken.cancel_all_orders", "CancelAll", BTreeMap::new())
                    .await?;
            }
            rustcta_types::MarketType::Perpetual => {
                let mut params = BTreeMap::new();
                if let Some(symbol) = &request.symbol {
                    params.insert("symbol".to_string(), normalize_futures_symbol(symbol)?);
                }
                self.send_futures_private(
                    "kraken.futures.cancel_all_orders",
                    Method::POST,
                    "cancelallorders",
                    params,
                )
                .await?;
            }
            _ => self.ensure_market_type(market_type)?,
        }
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            orders: Vec::new(),
            cancelled_count: 0,
        })
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let (tenant_id, account_id) = self.context_account(&request.context, "get_balances")?;
        let market_type = request
            .market_type
            .unwrap_or(rustcta_types::MarketType::Spot);
        let value = match market_type {
            rustcta_types::MarketType::Spot => {
                match self
                    .send_spot_private("kraken.get_balances", "BalanceEx", BTreeMap::new())
                    .await
                {
                    Ok(value) => value,
                    Err(_) => {
                        self.send_spot_private("kraken.get_balances", "Balance", BTreeMap::new())
                            .await?
                    }
                }
            }
            rustcta_types::MarketType::Perpetual => {
                self.send_futures_private(
                    "kraken.futures.get_balances",
                    Method::GET,
                    "accounts",
                    BTreeMap::new(),
                )
                .await?
            }
            _ => {
                self.ensure_market_type(market_type)?;
                unreachable!()
            }
        };
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            balances: parse_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
                market_type,
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
        let (tenant_id, account_id) = self.context_account(&request.context, "get_positions")?;
        let value = self
            .send_futures_private(
                "kraken.futures.get_positions",
                Method::GET,
                "openpositions",
                BTreeMap::new(),
            )
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            positions: parse_positions(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.symbols,
                &value,
            )?,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "kraken get_fees requires at least one symbol".to_string(),
            });
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_market_type(symbol.market_type)?;
        }
        let mut params = BTreeMap::new();
        let spot_pairs = request
            .symbols
            .iter()
            .filter(|symbol| symbol.market_type == rustcta_types::MarketType::Spot)
            .map(normalize_spot_symbol)
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        if !spot_pairs.is_empty() {
            params.insert("pair".to_string(), spot_pairs.join(","));
        }
        let value = self
            .send_spot_private("kraken.get_fees", "TradeVolume", params)
            .await
            .unwrap_or_else(|_| serde_json::json!({}));
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_snapshots(&self.exchange_id, &request.symbols, &value),
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_market_type(request.symbol.market_type)?;
        let id = request
            .exchange_order_id
            .as_deref()
            .or(request.client_order_id.as_deref())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kraken query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            })?;
        let value = match request.symbol.market_type {
            rustcta_types::MarketType::Spot => {
                let mut params = BTreeMap::new();
                params.insert("txid".to_string(), id.to_string());
                self.send_spot_private("kraken.query_order", "QueryOrders", params)
                    .await?
            }
            rustcta_types::MarketType::Perpetual => {
                let mut params = BTreeMap::new();
                params.insert("orderIds".to_string(), id.to_string());
                self.send_futures_private(
                    "kraken.futures.query_order",
                    Method::GET,
                    "orders",
                    params,
                )
                .await?
            }
            _ => unreachable!("ensure_market_type checked"),
        };
        let order = parse_order_list(&self.exchange_id, Some(&request.symbol), &value)?
            .into_iter()
            .next();
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order,
        })
    }

    pub(super) async fn get_open_orders_impl(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(rustcta_types::MarketType::Spot);
        let value = match market_type {
            rustcta_types::MarketType::Spot => {
                self.send_spot_private("kraken.get_open_orders", "OpenOrders", BTreeMap::new())
                    .await?
            }
            rustcta_types::MarketType::Perpetual => {
                self.send_futures_private(
                    "kraken.futures.get_open_orders",
                    Method::GET,
                    "openorders",
                    BTreeMap::new(),
                )
                .await?
            }
            _ => {
                self.ensure_market_type(market_type)?;
                unreachable!()
            }
        };
        let mut orders = parse_order_list(&self.exchange_id, request.symbol.as_ref(), &value)?;
        if let Some(symbol) = &request.symbol {
            orders.retain(|order| {
                order
                    .exchange_symbol
                    .symbol
                    .eq_ignore_ascii_case(&symbol.exchange_symbol.symbol)
            });
        }
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            orders,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let (tenant_id, account_id) = self.context_account(&request.context, "get_recent_fills")?;
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(rustcta_types::MarketType::Spot);
        let value = match market_type {
            rustcta_types::MarketType::Spot => {
                self.send_spot_private(
                    "kraken.get_recent_fills",
                    "TradesHistory",
                    recent_fills_params(&request, MarketType::Spot)?,
                )
                .await?
            }
            rustcta_types::MarketType::Perpetual => {
                self.send_futures_private(
                    "kraken.futures.get_recent_fills",
                    Method::GET,
                    "fills",
                    recent_fills_params(&request, MarketType::Perpetual)?,
                )
                .await?
            }
            _ => {
                self.ensure_market_type(market_type)?;
                unreachable!()
            }
        };
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange.clone(), request.context.request_id),
            fills: parse_recent_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.symbol.as_ref(),
                &value,
            )?,
        })
    }
}

fn recent_fills_params(
    request: &RecentFillsRequest,
    market_type: MarketType,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    if let Some(page) = &request.page {
        page.validate(Some(1000))
            .map_err(|message| ExchangeApiError::InvalidRequest { message })?;
    }
    if request
        .limit
        .is_some_and(|limit| limit == 0 || limit > 1000)
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "kraken get_recent_fills limit must be between 1 and 1000".to_string(),
        });
    }
    let mut params = BTreeMap::new();
    if let Some(symbol) = &request.symbol {
        match market_type {
            MarketType::Spot => {
                params.insert("pair".to_string(), normalize_spot_symbol(symbol)?);
            }
            MarketType::Perpetual => {
                params.insert("symbol".to_string(), normalize_futures_symbol(symbol)?);
            }
            _ => {}
        }
    }
    if let Some(order_id) = request
        .exchange_order_id
        .as_deref()
        .or(request.client_order_id.as_deref())
    {
        match market_type {
            MarketType::Spot => {
                params.insert("txid".to_string(), order_id.to_string());
            }
            MarketType::Perpetual => {
                params.insert("orderIds".to_string(), order_id.to_string());
            }
            _ => {}
        }
    }
    let limit = request
        .page
        .as_ref()
        .and_then(|page| page.limit)
        .or(request.limit);
    if let Some(limit) = limit {
        params.insert("count".to_string(), limit.min(1000).to_string());
    }
    match market_type {
        MarketType::Spot => {
            if let Some(start_time) = request.start_time.as_ref() {
                params.insert(
                    "start".to_string(),
                    start_time
                        .timestamp_nanos_opt()
                        .unwrap_or_else(|| start_time.timestamp_millis().saturating_mul(1_000_000))
                        .to_string(),
                );
            }
            if let Some(end_time) = request.end_time.as_ref() {
                params.insert(
                    "end".to_string(),
                    end_time
                        .timestamp_nanos_opt()
                        .unwrap_or_else(|| end_time.timestamp_millis().saturating_mul(1_000_000))
                        .to_string(),
                );
            }
            if let Some(from_trade_id) = request.from_trade_id.as_deref() {
                params.insert("ofs".to_string(), from_trade_id.to_string());
            }
            if let Some(cursor) = request.page.as_ref().and_then(|page| page.cursor.as_ref()) {
                match cursor {
                    PageCursor::Offset { offset } => {
                        params.insert("ofs".to_string(), offset.to_string());
                    }
                    PageCursor::TimeRange { start_ms, end_ms } => {
                        params.insert(
                            "start".to_string(),
                            start_ms.saturating_mul(1_000_000).to_string(),
                        );
                        if let Some(end_ms) = end_ms {
                            params.insert(
                                "end".to_string(),
                                end_ms.saturating_mul(1_000_000).to_string(),
                            );
                        }
                    }
                    PageCursor::Timestamp { millis } => {
                        params.insert(
                            "start".to_string(),
                            millis.saturating_mul(1_000_000).to_string(),
                        );
                    }
                    PageCursor::Id { id } | PageCursor::Token { token: id } => {
                        params.insert("ofs".to_string(), id.clone());
                    }
                }
            }
        }
        MarketType::Perpetual => {
            if let Some(start_time) = request.start_time.as_ref() {
                params.insert("lastFillTime".to_string(), start_time.to_rfc3339());
            }
            if let Some(from_trade_id) = request.from_trade_id.as_deref() {
                params.insert("lastFillTime".to_string(), from_trade_id.to_string());
            }
            if let Some(cursor) = request.page.as_ref().and_then(|page| page.cursor.as_ref()) {
                match cursor {
                    PageCursor::Timestamp { millis } => {
                        let seconds = millis.div_euclid(1000);
                        let nanos = millis.rem_euclid(1000) as u32 * 1_000_000;
                        let timestamp =
                            chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, nanos)
                                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                                    message: format!(
                                        "invalid kraken futures fills timestamp cursor {millis}"
                                    ),
                                })?;
                        params.insert("lastFillTime".to_string(), timestamp.to_rfc3339());
                    }
                    PageCursor::Token { token } | PageCursor::Id { id: token } => {
                        params.insert("lastFillTime".to_string(), token.clone());
                    }
                    PageCursor::Offset { .. } | PageCursor::TimeRange { .. } => {
                        return Err(ExchangeApiError::InvalidRequest {
                            message: "kraken futures fills pagination supports timestamp/token cursors, not offset or time-range cursors".to_string(),
                        });
                    }
                }
            }
        }
        _ => {}
    }
    Ok(params)
}

fn validate_batch_place_request(
    adapter: &KrakenGatewayAdapter,
    request: &BatchPlaceOrdersRequest,
) -> ExchangeApiResult<MarketType> {
    if request.orders.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "kraken batch_place_orders requires at least one order".to_string(),
        });
    }
    let market_type = request.orders[0].symbol.market_type;
    adapter.ensure_market_type(market_type)?;
    for order in &request.orders {
        adapter.ensure_exchange(&order.symbol.exchange)?;
        adapter.ensure_market_type(order.symbol.market_type)?;
        if order.symbol.market_type != market_type {
            return Err(ExchangeApiError::InvalidRequest {
                message: "kraken batch_place_orders requires one market type".to_string(),
            });
        }
    }
    if market_type == MarketType::Spot {
        if !(2..=15).contains(&request.orders.len()) {
            return Err(ExchangeApiError::InvalidRequest {
                message: "kraken spot AddOrderBatch requires 2 to 15 orders".to_string(),
            });
        }
        let first_pair = normalize_spot_symbol(&request.orders[0].symbol)?;
        for order in &request.orders[1..] {
            let pair = normalize_spot_symbol(&order.symbol)?;
            if pair != first_pair {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "kraken spot AddOrderBatch requires one pair".to_string(),
                });
            }
        }
    }
    Ok(market_type)
}

fn validate_batch_cancel_request(
    adapter: &KrakenGatewayAdapter,
    request: &BatchCancelOrdersRequest,
) -> ExchangeApiResult<MarketType> {
    if request.cancels.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "kraken batch_cancel_orders requires at least one cancel".to_string(),
        });
    }
    let market_type = request.cancels[0].symbol.market_type;
    adapter.ensure_market_type(market_type)?;
    for cancel in &request.cancels {
        adapter.ensure_exchange(&cancel.symbol.exchange)?;
        adapter.ensure_market_type(cancel.symbol.market_type)?;
        if cancel.symbol.market_type != market_type {
            return Err(ExchangeApiError::InvalidRequest {
                message: "kraken batch_cancel_orders requires one market type".to_string(),
            });
        }
        let _ = cancel_id(cancel)?;
    }
    if market_type == MarketType::Spot && request.cancels.len() > 50 {
        return Err(ExchangeApiError::InvalidRequest {
            message: "kraken spot CancelOrderBatch supports at most 50 cancels".to_string(),
        });
    }
    Ok(market_type)
}

fn cancel_id(request: &CancelOrderRequest) -> ExchangeApiResult<String> {
    request
        .exchange_order_id
        .as_deref()
        .or(request.client_order_id.as_deref())
        .map(str::to_string)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "kraken batch cancel requires exchange_order_id or client_order_id"
                .to_string(),
        })
}

fn json_string(value: Value) -> ExchangeApiResult<String> {
    serde_json::to_string(&value).map_err(|error| ExchangeApiError::InvalidRequest {
        message: format!("failed to encode kraken batch payload: {error}"),
    })
}

fn batch_order_ids(value: &Value) -> Vec<Option<String>> {
    if let Some(txids) = value.get("txid").and_then(Value::as_array) {
        return txids.iter().map(|value| value_text(Some(value))).collect();
    }
    batch_rows(value)
        .iter()
        .map(|row| {
            value_text(row.get("txid"))
                .or_else(|| {
                    row.get("txid")
                        .and_then(Value::as_array)
                        .and_then(|ids| ids.first())
                        .and_then(|value| value_text(Some(value)))
                })
                .or_else(|| value_text(row.get("order_id")))
                .or_else(|| value_text(row.get("orderId")))
                .or_else(|| {
                    row.get("sendStatus")
                        .and_then(|status| value_text(status.get("order_id")))
                })
        })
        .collect()
}

fn batch_rows(value: &Value) -> Vec<&Value> {
    value
        .get("orders")
        .or_else(|| value.get("batchStatus"))
        .or_else(|| value.get("elements"))
        .or_else(|| value.get("result"))
        .and_then(Value::as_array)
        .map(|rows| rows.iter().collect())
        .unwrap_or_default()
}

fn value_text(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| {
        value
            .as_str()
            .map(str::to_string)
            .or_else(|| value.as_i64().map(|number| number.to_string()))
            .or_else(|| value.as_u64().map(|number| number.to_string()))
    })
}

fn spot_order_params(request: &PlaceOrderRequest) -> ExchangeApiResult<BTreeMap<String, String>> {
    let mut params = BTreeMap::new();
    params.insert("pair".to_string(), normalize_spot_symbol(&request.symbol)?);
    params.insert("type".to_string(), side_text(request.side).to_string());
    params.insert(
        "ordertype".to_string(),
        spot_order_type(request)?.to_string(),
    );
    params.insert("volume".to_string(), request.quantity.clone());
    if request.order_type.requires_limit_price() {
        params.insert(
            "price".to_string(),
            request
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "kraken limit-style spot order requires price".to_string(),
                })?,
        );
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("cl_ord_id".to_string(), client_order_id.to_string());
    }
    let mut flags = Vec::new();
    if request.post_only || request.order_type == OrderType::PostOnly {
        flags.push("post");
    }
    if request.quote_quantity.is_some() {
        flags.push("viqc");
    }
    if !flags.is_empty() {
        params.insert("oflags".to_string(), flags.join(","));
    }
    if let Some(tif) = request.time_in_force {
        params.insert("timeinforce".to_string(), tif_text(tif)?.to_string());
    }
    Ok(params)
}

fn futures_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<BTreeMap<String, String>> {
    let mut params = BTreeMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_futures_symbol(&request.symbol)?,
    );
    params.insert("side".to_string(), side_text(request.side).to_string());
    params.insert(
        "orderType".to_string(),
        futures_order_type(request)?.to_string(),
    );
    params.insert("size".to_string(), request.quantity.clone());
    if request.order_type.requires_limit_price() {
        params.insert(
            "limitPrice".to_string(),
            request
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "kraken futures limit-style order requires price".to_string(),
                })?,
        );
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("cliOrdId".to_string(), client_order_id.to_string());
    }
    if request.reduce_only {
        params.insert("reduceOnly".to_string(), "true".to_string());
    }
    Ok(params)
}

fn spot_order_type(request: &PlaceOrderRequest) -> ExchangeApiResult<&'static str> {
    Ok(match request.order_type {
        OrderType::Market => "market",
        OrderType::Limit | OrderType::PostOnly | OrderType::IOC | OrderType::FOK => "limit",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "kraken.spot_order_type",
            })
        }
    })
}

fn futures_order_type(request: &PlaceOrderRequest) -> ExchangeApiResult<&'static str> {
    Ok(match request.order_type {
        OrderType::Market => "mkt",
        OrderType::Limit => "lmt",
        OrderType::PostOnly => "post",
        OrderType::IOC => "ioc",
        OrderType::FOK => "fok",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "kraken.futures_order_type",
            })
        }
    })
}

fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn tif_text(tif: TimeInForce) -> ExchangeApiResult<&'static str> {
    Ok(match tif {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTC",
    })
}
