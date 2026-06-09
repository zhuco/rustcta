use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchItemResult, BatchOperationReport,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderListConditionalLeg, OrderListLegType, OrderListOrderLeg, OrderListRequest,
    OrderListResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, ReconcilePlan, ReconcileTrigger, RetryReconcilePolicy, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeError, MarketType, OrderSide, OrderType};
use serde_json::Value;

use super::parser::normalize_binancecoinm_symbol;
use super::private_parser::{
    parse_account_balances, parse_binancecoinm_cancel_all_orders, parse_fee_snapshots,
    parse_open_orders, parse_order_state, parse_positions, parse_recent_fills,
};
use super::transport::classify_binancecoinm_error;
use super::BinanceCoinMGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BinanceCoinMGatewayAdapter {
    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_coin_m(request.symbol.market_type)?;
        let mut params = binancecoinm_place_order_params(&request)?;
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_signed_post("binancecoinm.place_order", "/dapi/v1/order", &params)
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
        self.ensure_coin_m(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert(
            "side".to_string(),
            binancecoinm_side(request.side).to_string(),
        );
        params.insert("type".to_string(), "MARKET".to_string());
        insert_non_empty(&mut params, "quoteOrderQty", &request.quote_quantity)?;
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            insert_non_empty(&mut params, "newClientOrderId", client_order_id)?;
        }
        let value = self
            .send_signed_post(
                "binancecoinm.place_quote_market_order",
                "/dapi/v1/order",
                &params,
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
        self.ensure_coin_m(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        insert_order_identifier(
            &mut params,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "cancel_order",
        )?;
        let value = self
            .send_signed_delete("binancecoinm.cancel_order", "/dapi/v1/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
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
            self.ensure_coin_m(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "binancecoinm cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_coin_m(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_signed_delete(
                "binancecoinm.cancel_all_orders",
                "/dapi/v1/allOpenOrders",
                &params,
            )
            .await?;
        let orders = parse_binancecoinm_cancel_all_orders(&self.exchange_id, symbol, &value)?;
        let cancelled_count = orders.len() as u32;
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            cancelled_count,
        })
    }

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_coin_m(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        insert_order_identifier(
            &mut params,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "amend_order",
        )?;
        insert_non_empty(&mut params, "quantity", &request.new_quantity)?;
        if let Some(client_order_id) = request.new_client_order_id.as_deref() {
            insert_non_empty(&mut params, "newClientOrderId", client_order_id)?;
        }
        let value = self
            .send_signed_put("binancecoinm.amend_order", "/dapi/v1/order", &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
            order,
        })
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.orders.is_empty() {
            return Ok(BatchPlaceOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                report: None,
            });
        }
        if request.orders.len() > 5 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "binancecoinm batch_place_orders supports at most 5 orders".to_string(),
            });
        }

        let mut batch_orders = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            ensure_exchange_api_schema(order.schema_version)?;
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_coin_m(order.symbol.market_type)?;
            batch_orders.push(binancecoinm_batch_order_json(order)?);
        }

        let mut params = HashMap::new();
        params.insert(
            "batchOrders".to_string(),
            urlencoding::encode(&format!("[{}]", batch_orders.join(","))).into_owned(),
        );
        let value = self
            .send_signed_post(
                "binancecoinm.batch_place_orders",
                "/dapi/v1/batchOrders",
                &params,
            )
            .await?;
        let (orders, report) =
            parse_binancecoinm_batch_place_response(&self.exchange_id, &request, &value)?;
        Ok(BatchPlaceOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders,
            report: Some(report),
        })
    }

    pub(super) async fn batch_cancel_orders_impl(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request.cancels.is_empty() {
            return Ok(BatchCancelOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                cancelled_count: 0,
                report: None,
            });
        }
        if request.cancels.len() > 10 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "binancecoinm batch_cancel_orders supports at most 10 cancels".to_string(),
            });
        }

        let expected_symbol =
            normalize_binancecoinm_symbol(&request.cancels[0].symbol.exchange_symbol.symbol)?;
        for cancel in &request.cancels {
            ensure_exchange_api_schema(cancel.schema_version)?;
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_coin_m(cancel.symbol.market_type)?;
            let symbol = normalize_binancecoinm_symbol(&cancel.symbol.exchange_symbol.symbol)?;
            if symbol != expected_symbol {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "binancecoinm batch_cancel_orders requires one symbol".to_string(),
                });
            }
        }

        let params = binancecoinm_batch_cancel_params(&request.cancels)?;
        let value = self
            .send_signed_delete(
                "binancecoinm.batch_cancel_orders",
                "/dapi/v1/batchOrders",
                &params,
            )
            .await?;
        let (orders, report) =
            parse_binancecoinm_batch_cancel_response(&self.exchange_id, &request, &value)?;
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
            report: Some(report),
        })
    }

    pub(super) async fn place_order_list_impl(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        let symbol = request.symbol().clone();
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_coin_m(symbol.market_type)?;
        let (schema_version, context_request_id, endpoint, params, kind) =
            binancecoinm_order_list_params(&request)?;
        ensure_exchange_api_schema(schema_version)?;
        let value = self
            .send_signed_post("binancecoinm.place_order_list", endpoint, &params)
            .await?;
        let response = super::private_parser::parse_order_list_response(
            &self.exchange_id,
            &symbol,
            kind,
            &value,
        )?;
        Ok(OrderListResponse {
            metadata: response_metadata(symbol.exchange, context_request_id),
            ..response
        })
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_coin_m(market_type)?;
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let value = self
            .send_signed_get(
                "binancecoinm.get_balances",
                "/dapi/v1/balance",
                &HashMap::new(),
            )
            .await?;
        let balances = parse_account_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            MarketType::Perpetual,
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
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_coin_m(market_type)?;
        }
        for symbol in &request.symbols {
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "binancecoinm adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        if request.symbols.len() == 1 {
            params.insert(
                "pair".to_string(),
                normalize_binancecoinm_pair(&request.symbols[0].symbol)?,
            );
        }
        let value = self
            .send_signed_get(
                "binancecoinm.get_positions",
                "/dapi/v1/positionRisk",
                &params,
            )
            .await?;
        let positions = parse_positions(
            &self.exchange_id,
            tenant_id,
            account_id,
            &request.symbols,
            &value,
        )?;
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
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "binancecoinm get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_coin_m(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_binancecoinm_symbol(&symbol.exchange_symbol.symbol)?,
            );
            let value = self
                .send_signed_get(
                    "binancecoinm.get_fees",
                    "/dapi/v1/account/commission",
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
        self.ensure_coin_m(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("origClientOrderId".to_string(), client_order_id.to_string());
        }
        if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "binancecoinm query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .send_signed_get("binancecoinm.query_order", "/dapi/v1/order", &params)
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
        if let Some(market_type) = request.market_type {
            self.ensure_coin_m(market_type)?;
        }
        let mut params = HashMap::new();
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_coin_m(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_binancecoinm_symbol(&symbol.exchange_symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get(
                "binancecoinm.get_open_orders",
                "/dapi/v1/openOrders",
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
            self.ensure_coin_m(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "binancecoinm get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_coin_m(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_binancecoinm_symbol(&symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(from_trade_id) = request.from_trade_id.as_deref() {
            params.insert("fromId".to_string(), from_trade_id.to_string());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "startTime".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert(
                "endTime".to_string(),
                end_time.timestamp_millis().to_string(),
            );
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(1000).to_string());
        } else {
            params.insert("limit".to_string(), "1000".to_string());
        }
        let value = self
            .send_signed_get(
                "binancecoinm.get_recent_fills",
                "/dapi/v1/myTrades",
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

fn binancecoinm_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "side".to_string(),
        binancecoinm_side(request.side).to_string(),
    );
    params.insert(
        "type".to_string(),
        binancecoinm_order_type(request.order_type, request.post_only).to_string(),
    );
    if request.order_type == OrderType::Market {
        if let Some(quote_quantity) = request.quote_quantity.as_deref() {
            insert_non_empty(&mut params, "quoteOrderQty", quote_quantity)?;
        } else {
            insert_non_empty(&mut params, "quantity", &request.quantity)?;
        }
    } else {
        insert_non_empty(&mut params, "quantity", &request.quantity)?;
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "binancecoinm limit-style order requires price".to_string(),
            })?;
        insert_non_empty(&mut params, "price", price)?;
        let time_in_force = if request.post_only {
            "GTX"
        } else {
            binancecoinm_time_in_force(request.order_type, request.time_in_force)
        };
        params.insert("timeInForce".to_string(), time_in_force.to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        insert_non_empty(&mut params, "newClientOrderId", client_order_id)?;
    }
    if request.reduce_only {
        params.insert("reduceOnly".to_string(), "true".to_string());
    }
    if let Some(position_side) = request.position_side {
        params.insert(
            "positionSide".to_string(),
            binancecoinm_position_side(position_side).to_string(),
        );
    }
    Ok(params)
}

fn binancecoinm_batch_order_json(request: &PlaceOrderRequest) -> ExchangeApiResult<String> {
    let mut fields = vec![
        (
            "symbol",
            normalize_binancecoinm_symbol(&request.symbol.exchange_symbol.symbol)?,
        ),
        ("side", binancecoinm_side(request.side).to_string()),
    ];
    if let Some(position_side) = request.position_side {
        fields.push((
            "positionSide",
            binancecoinm_position_side(position_side).to_string(),
        ));
    }
    fields.push((
        "type",
        binancecoinm_order_type(request.order_type, request.post_only).to_string(),
    ));
    if request.order_type != OrderType::Market {
        let time_in_force = if request.post_only {
            "GTX"
        } else {
            binancecoinm_time_in_force(request.order_type, request.time_in_force)
        };
        fields.push(("timeInForce", time_in_force.to_string()));
    }
    if request.order_type == OrderType::Market {
        if let Some(quote_quantity) = request.quote_quantity.as_deref() {
            fields.push((
                "quoteOrderQty",
                non_empty_value("quoteOrderQty", quote_quantity)?,
            ));
        } else {
            fields.push(("quantity", non_empty_value("quantity", &request.quantity)?));
        }
    } else {
        fields.push(("quantity", non_empty_value("quantity", &request.quantity)?));
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "binancecoinm limit-style batch order requires price".to_string(),
            })?;
        fields.push(("price", non_empty_value("price", price)?));
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        fields.push((
            "newClientOrderId",
            non_empty_value("newClientOrderId", client_order_id)?,
        ));
    }
    if request.reduce_only {
        fields.push(("reduceOnly", "true".to_string()));
    }
    let json_fields = fields
        .into_iter()
        .map(|(key, value)| {
            let value =
                serde_json::to_string(&value).map_err(|error| ExchangeApiError::Serialization {
                    message: error.to_string(),
                })?;
            Ok(format!("\"{key}\":{value}"))
        })
        .collect::<ExchangeApiResult<Vec<_>>>()?;
    Ok(format!("{{{}}}", json_fields.join(",")))
}

fn non_empty_value(key: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("binancecoinm parameter {key} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn binancecoinm_batch_cancel_params(
    cancels: &[CancelOrderRequest],
) -> ExchangeApiResult<HashMap<String, String>> {
    let symbol = normalize_binancecoinm_symbol(&cancels[0].symbol.exchange_symbol.symbol)?;
    let order_ids = cancels
        .iter()
        .map(|cancel| {
            cancel
                .exchange_order_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(parse_order_id)
        })
        .collect::<Option<ExchangeApiResult<Vec<_>>>>()
        .transpose()?;
    let client_order_ids = cancels
        .iter()
        .map(|cancel| {
            cancel
                .client_order_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
        .collect::<Option<Vec<_>>>();

    let mut params = HashMap::new();
    params.insert("symbol".to_string(), symbol);
    if let Some(order_ids) = order_ids {
        params.insert(
            "orderIdList".to_string(),
            serialize_json_query_value(&order_ids)?,
        );
    } else if let Some(client_order_ids) = client_order_ids {
        params.insert(
            "origClientOrderIdList".to_string(),
            serialize_json_query_value(&client_order_ids)?,
        );
    } else {
        return Err(ExchangeApiError::InvalidRequest {
            message: "binancecoinm batch_cancel_orders requires either all exchange_order_id or all client_order_id".to_string(),
        });
    }
    Ok(params)
}

fn parse_binancecoinm_batch_place_response(
    exchange_id: &rustcta_types::ExchangeId,
    request: &BatchPlaceOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<(Vec<rustcta_exchange_api::OrderState>, BatchOperationReport)> {
    let rows = batch_response_rows(exchange_id, "batch place response is not an array", value)?;
    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(request.orders.len());
    for (index, order_request) in request.orders.iter().enumerate() {
        let Some(row) = rows.get(index) else {
            let error = missing_batch_item_error(exchange_id, "missing batch place response item");
            results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                None,
                error,
                Some(ReconcilePlan::for_place_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchResponseMissingItem,
                    order_request,
                    RetryReconcilePolicy::default(),
                    "Binance COIN-M did not return a batch place result for this request item",
                )),
            ));
            continue;
        };
        if let Some(error) = binancecoinm_batch_item_error(
            exchange_id,
            row,
            order_request.client_order_id.clone(),
            None,
        ) {
            let plan = error.requires_reconciliation().then(|| {
                ReconcilePlan::for_place_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchPlacePartialFailure,
                    order_request,
                    RetryReconcilePolicy::default(),
                    "Binance COIN-M batch place item failed and requires order readback",
                )
            });
            results.push(BatchItemResult::failed(
                index,
                order_request.client_order_id.clone(),
                None,
                error,
                plan,
            ));
            continue;
        }
        let order = parse_order_state(exchange_id, Some(&order_request.symbol), row)?;
        results.push(BatchItemResult::success(index, order.clone()));
        orders.push(order);
    }
    Ok((
        orders,
        BatchOperationReport {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            total_items: request.orders.len(),
            results,
        },
    ))
}

fn parse_binancecoinm_batch_cancel_response(
    exchange_id: &rustcta_types::ExchangeId,
    request: &BatchCancelOrdersRequest,
    value: &Value,
) -> ExchangeApiResult<(Vec<rustcta_exchange_api::OrderState>, BatchOperationReport)> {
    let rows = batch_response_rows(exchange_id, "batch cancel response is not an array", value)?;
    let mut orders = Vec::new();
    let mut results = Vec::with_capacity(request.cancels.len());
    for (index, cancel_request) in request.cancels.iter().enumerate() {
        let Some(row) = rows.get(index) else {
            let error = missing_batch_item_error(exchange_id, "missing batch cancel response item");
            results.push(BatchItemResult::failed(
                index,
                cancel_request.client_order_id.clone(),
                cancel_request.exchange_order_id.clone(),
                error,
                Some(ReconcilePlan::for_cancel_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchResponseMissingItem,
                    cancel_request,
                    RetryReconcilePolicy::default(),
                    "Binance COIN-M did not return a batch cancel result for this request item",
                )),
            ));
            continue;
        };
        if let Some(error) = binancecoinm_batch_item_error(
            exchange_id,
            row,
            cancel_request.client_order_id.clone(),
            cancel_request.exchange_order_id.clone(),
        ) {
            let plan = error.requires_reconciliation().then(|| {
                ReconcilePlan::for_cancel_request(
                    exchange_id.clone(),
                    ReconcileTrigger::BatchCancelPartialFailure,
                    cancel_request,
                    RetryReconcilePolicy::default(),
                    "Binance COIN-M batch cancel item failed and requires order readback",
                )
            });
            results.push(BatchItemResult::failed(
                index,
                cancel_request.client_order_id.clone(),
                cancel_request.exchange_order_id.clone(),
                error,
                plan,
            ));
            continue;
        }
        let order = parse_order_state(exchange_id, Some(&cancel_request.symbol), row)?;
        results.push(BatchItemResult::success(index, order.clone()));
        orders.push(order);
    }
    Ok((
        orders,
        BatchOperationReport {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            total_items: request.cancels.len(),
            results,
        },
    ))
}

fn batch_response_rows<'a>(
    exchange_id: &rustcta_types::ExchangeId,
    message: &str,
    value: &'a Value,
) -> ExchangeApiResult<&'a [Value]> {
    value.as_array().map(Vec::as_slice).ok_or_else(|| {
        ExchangeApiError::Exchange(ExchangeError::new(
            exchange_id.clone(),
            rustcta_types::ExchangeErrorClass::Decode,
            format!("{message}: {value}"),
            chrono::Utc::now(),
        ))
    })
}

fn binancecoinm_batch_item_error(
    exchange_id: &rustcta_types::ExchangeId,
    value: &Value,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
) -> Option<ExchangeError> {
    let code = value.get("code").and_then(json_value_text)?;
    let message = value
        .get("msg")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("Binance COIN-M batch item failed");
    let mut error = ExchangeError::new(
        exchange_id.clone(),
        classify_binancecoinm_error(Some(&code), message),
        message,
        chrono::Utc::now(),
    );
    error.code = Some(code);
    error.client_order_id = client_order_id;
    error.order_id = exchange_order_id;
    error.raw = Some(value.clone());
    Some(error)
}

fn missing_batch_item_error(
    exchange_id: &rustcta_types::ExchangeId,
    message: &str,
) -> ExchangeError {
    ExchangeError::new(
        exchange_id.clone(),
        rustcta_types::ExchangeErrorClass::UnknownOrderState,
        message,
        chrono::Utc::now(),
    )
}

fn json_value_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn serialize_json_query_value<T: serde::Serialize>(value: &T) -> ExchangeApiResult<String> {
    let json = serde_json::to_string(value).map_err(|error| ExchangeApiError::Serialization {
        message: error.to_string(),
    })?;
    Ok(urlencoding::encode(&json).into_owned())
}

fn parse_order_id(value: &str) -> ExchangeApiResult<u64> {
    value
        .parse::<u64>()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("binancecoinm orderId must be an unsigned integer: {error}"),
        })
}

fn binancecoinm_order_list_params(
    request: &OrderListRequest,
) -> ExchangeApiResult<(
    u16,
    Option<String>,
    &'static str,
    HashMap<String, String>,
    rustcta_exchange_api::OrderListKind,
)> {
    match request {
        OrderListRequest::Oco {
            schema_version,
            context,
            symbol,
            list_client_order_id,
            side,
            quantity,
            above,
            below,
        } => {
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_binancecoinm_symbol(&symbol.exchange_symbol.symbol)?,
            );
            params.insert("side".to_string(), binancecoinm_side(*side).to_string());
            insert_non_empty(&mut params, "quantity", quantity)?;
            if let Some(client_id) = list_client_order_id.as_deref() {
                insert_non_empty(&mut params, "listClientOrderId", client_id)?;
            }
            insert_conditional_leg(&mut params, "above", above)?;
            insert_conditional_leg(&mut params, "below", below)?;
            Ok((
                *schema_version,
                context.request_id.clone(),
                "/dapi/v1/orderList/oco",
                params,
                rustcta_exchange_api::OrderListKind::Oco,
            ))
        }
        OrderListRequest::Oto {
            schema_version,
            context,
            symbol,
            list_client_order_id,
            working,
            pending,
        } => {
            let mut params = HashMap::new();
            params.insert(
                "symbol".to_string(),
                normalize_binancecoinm_symbol(&symbol.exchange_symbol.symbol)?,
            );
            if let Some(client_id) = list_client_order_id.as_deref() {
                insert_non_empty(&mut params, "listClientOrderId", client_id)?;
            }
            insert_order_leg(&mut params, "working", working)?;
            insert_order_leg(&mut params, "pending", pending)?;
            Ok((
                *schema_version,
                context.request_id.clone(),
                "/dapi/v1/orderList/oto",
                params,
                rustcta_exchange_api::OrderListKind::Oto,
            ))
        }
    }
}

fn insert_conditional_leg(
    params: &mut HashMap<String, String>,
    prefix: &str,
    leg: &OrderListConditionalLeg,
) -> ExchangeApiResult<()> {
    params.insert(
        format!("{prefix}Type"),
        binancecoinm_order_list_type(leg.order_type).to_string(),
    );
    if let Some(price) = leg.price.as_deref() {
        insert_non_empty(params, &format!("{prefix}Price"), price)?;
    }
    if let Some(stop_price) = leg.stop_price.as_deref() {
        insert_non_empty(params, &format!("{prefix}StopPrice"), stop_price)?;
    }
    if let Some(time_in_force) = leg.time_in_force {
        params.insert(
            format!("{prefix}TimeInForce"),
            binancecoinm_time_in_force_from_tif(time_in_force).to_string(),
        );
    }
    if let Some(client_order_id) = leg.client_order_id.as_deref() {
        insert_non_empty(params, &format!("{prefix}ClientOrderId"), client_order_id)?;
    }
    Ok(())
}

fn insert_order_leg(
    params: &mut HashMap<String, String>,
    prefix: &str,
    leg: &OrderListOrderLeg,
) -> ExchangeApiResult<()> {
    params.insert(
        format!("{prefix}Side"),
        binancecoinm_side(leg.side).to_string(),
    );
    params.insert(
        format!("{prefix}Type"),
        binancecoinm_order_list_type(leg.order_type).to_string(),
    );
    insert_non_empty(params, &format!("{prefix}Quantity"), &leg.quantity)?;
    if let Some(price) = leg.price.as_deref() {
        insert_non_empty(params, &format!("{prefix}Price"), price)?;
    }
    if let Some(stop_price) = leg.stop_price.as_deref() {
        insert_non_empty(params, &format!("{prefix}StopPrice"), stop_price)?;
    }
    if let Some(time_in_force) = leg.time_in_force {
        params.insert(
            format!("{prefix}TimeInForce"),
            binancecoinm_time_in_force_from_tif(time_in_force).to_string(),
        );
    }
    if let Some(client_order_id) = leg.client_order_id.as_deref() {
        insert_non_empty(params, &format!("{prefix}ClientOrderId"), client_order_id)?;
    }
    Ok(())
}

fn insert_order_identifier(
    params: &mut HashMap<String, String>,
    exchange_order_id: Option<&str>,
    client_order_id: Option<&str>,
    operation: &str,
) -> ExchangeApiResult<()> {
    if let Some(order_id) = exchange_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_id) = client_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        params.insert("origClientOrderId".to_string(), client_id.to_string());
    }
    if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!(
                "binancecoinm {operation} requires exchange_order_id or client_order_id"
            ),
        });
    }
    Ok(())
}

fn insert_non_empty(
    params: &mut HashMap<String, String>,
    key: &str,
    value: &str,
) -> ExchangeApiResult<()> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("binancecoinm parameter {key} must not be empty"),
        });
    }
    params.insert(key.to_string(), value.to_string());
    Ok(())
}

fn binancecoinm_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn binancecoinm_order_type(order_type: OrderType, post_only: bool) -> &'static str {
    match order_type {
        OrderType::Market => "MARKET",
        OrderType::Limit if post_only => "LIMIT",
        OrderType::Limit => "LIMIT",
        OrderType::PostOnly => "LIMIT",
        OrderType::IOC | OrderType::FOK => "LIMIT",
        _ => "LIMIT",
    }
}

fn binancecoinm_time_in_force(order_type: OrderType, tif: Option<TimeInForce>) -> &'static str {
    if order_type == OrderType::PostOnly {
        return "GTX";
    }
    if order_type == OrderType::IOC {
        return "IOC";
    }
    if order_type == OrderType::FOK {
        return "FOK";
    }
    tif.map(binancecoinm_time_in_force_from_tif)
        .unwrap_or("GTC")
}

fn binancecoinm_time_in_force_from_tif(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "GTX",
    }
}

fn binancecoinm_position_side(side: rustcta_types::PositionSide) -> &'static str {
    match side {
        rustcta_types::PositionSide::Long => "LONG",
        rustcta_types::PositionSide::Short => "SHORT",
        rustcta_types::PositionSide::Net | rustcta_types::PositionSide::None => "BOTH",
    }
}

fn normalize_binancecoinm_pair(symbol: &str) -> ExchangeApiResult<String> {
    let symbol = normalize_binancecoinm_symbol(symbol)?;
    if let Some((pair, _delivery)) = symbol.split_once('_') {
        Ok(pair.to_string())
    } else {
        Ok(symbol.trim_end_matches("PERP").to_string())
    }
}

fn binancecoinm_order_list_type(order_type: OrderListLegType) -> &'static str {
    match order_type {
        OrderListLegType::Market => "MARKET",
        OrderListLegType::Limit => "LIMIT",
        OrderListLegType::LimitMaker => "LIMIT_MAKER",
        OrderListLegType::StopLoss => "STOP_LOSS",
        OrderListLegType::StopLossLimit => "STOP_LOSS_LIMIT",
        OrderListLegType::TakeProfit => "TAKE_PROFIT",
        OrderListLegType::TakeProfitLimit => "TAKE_PROFIT_LIMIT",
    }
}
