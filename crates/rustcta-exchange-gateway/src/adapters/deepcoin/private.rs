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
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::{normalize_deepcoin_symbol, product_inst_type};
use super::private_parser::{
    order_state_from_cancel_ack, order_state_from_place_ack, parse_balances, parse_fee_snapshots,
    parse_fills, parse_order, parse_order_state, parse_orders, parse_positions,
};
use super::DeepcoinGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, missing_order_identity, response_metadata};

impl DeepcoinGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "deepcoin.get_balances")?;
        let mut params = HashMap::new();
        params.insert(
            "accountType".to_string(),
            if market_type == MarketType::Spot {
                "spot"
            } else {
                "swapU"
            }
            .to_string(),
        );
        if !request.assets.is_empty() {
            params.insert("ccy".to_string(), request.assets.join(","));
        }
        let value = self
            .rest
            .send_signed_get("/deepcoin/account/all-balances", &params)
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

    pub(super) async fn get_positions_impl(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if request
            .market_type
            .is_some_and(|market| market != MarketType::Perpetual)
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "deepcoin.positions_non_perpetual",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "deepcoin.get_positions")?;
        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SWAP".to_string());
        if let Some(symbol) = request.symbols.first() {
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "deepcoin adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
            params.insert(
                "instId".to_string(),
                normalize_deepcoin_symbol(&symbol.symbol, MarketType::Perpetual)?,
            );
        }
        let value = self
            .rest
            .send_signed_get("/deepcoin/account/positions", &params)
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions: parse_positions(&self.exchange_id, tenant_id, account_id, &value)?,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "deepcoin get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert(
                "instType".to_string(),
                product_inst_type(symbol.market_type).to_string(),
            );
            params.insert(
                "instId".to_string(),
                normalize_deepcoin_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
            );
            let value = self
                .rest
                .send_signed_get("/deepcoin/account/trade-fee", &params)
                .await?;
            fees.extend(parse_fee_snapshots(
                &self.exchange_id,
                std::slice::from_ref(symbol),
                symbol.market_type,
                &value,
            )?);
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
        let body = deepcoin_place_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("/deepcoin/trade/order", &body)
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            super::parser::data_payload(&value),
        )
        .unwrap_or_else(|_| order_state_from_place_ack(&self.exchange_id, &request, &value));
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order,
        })
    }

    pub(super) async fn place_quote_market_order_impl(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.place_order_impl(PlaceOrderRequest {
            schema_version: request.schema_version,
            context: request.context,
            symbol: request.symbol,
            client_order_id: request.client_order_id,
            side: request.side,
            position_side: None,
            order_type: OrderType::Market,
            time_in_force: None,
            quantity: "0".to_string(),
            price: None,
            quote_quantity: Some(request.quote_quantity),
            reduce_only: false,
            post_only: false,
        })
        .await
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        if missing_order_identity(&request) || request.exchange_order_id.is_none() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "deepcoin cancel_order requires exchange_order_id".to_string(),
            });
        }
        let body = json!({
            "instId": normalize_deepcoin_symbol(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type
            )?,
            "ordId": request.exchange_order_id.clone().unwrap_or_default(),
        });
        let value = self
            .rest
            .send_signed_post("/deepcoin/trade/cancel-order", &body)
            .await?;
        let order = parse_order(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )
        .ok()
        .flatten()
        .unwrap_or_else(|| order_state_from_cancel_ack(&self.exchange_id, &request, &value));
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

    pub(super) async fn amend_order_impl(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let exchange_order_id =
            request
                .exchange_order_id
                .as_ref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "deepcoin amend_order requires exchange_order_id".to_string(),
                })?;
        if request.new_quantity.trim().is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "deepcoin amend_order requires non-empty new_quantity".to_string(),
            });
        }
        let mut body = json!({
            "OrderSysID": exchange_order_id,
            "volume": request.new_quantity,
        });
        if let Some(new_client_order_id) = &request.new_client_order_id {
            body["clOrdId"] = json!(new_client_order_id);
        }
        let value = self
            .rest
            .send_signed_post("/deepcoin/trade/replace-order", &body)
            .await?;
        let order = order_state_from_amend_ack(&self.exchange_id, &request, &value);
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
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
            return Err(ExchangeApiError::InvalidRequest {
                message: "deepcoin batch_place_orders requires at least one order".to_string(),
            });
        }
        if request.orders.len() > 5 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "deepcoin batch_place_orders supports at most 5 orders".to_string(),
            });
        }
        let mut order_bodies = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market(order.symbol.market_type)?;
            order_bodies.push(deepcoin_place_order_body(order)?);
        }
        let body = json!({ "orders": order_bodies });
        let value = self
            .rest
            .send_signed_post("/deepcoin/trade/batch-orders", &body)
            .await?;
        let ack_items = response_array(super::parser::data_payload(&value));
        let orders = request
            .orders
            .iter()
            .enumerate()
            .map(|(index, order_request)| {
                let ack = ack_items.get(index).copied().unwrap_or(&Value::Null);
                order_state_from_place_ack(&self.exchange_id, order_request, ack)
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
        if request.cancels.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "deepcoin batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        if request.cancels.len() > 50 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "deepcoin batch_cancel_orders supports at most 50 orders".to_string(),
            });
        }
        let mut order_ids = Vec::with_capacity(request.cancels.len());
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market(cancel.symbol.market_type)?;
            let order_id = cancel.exchange_order_id.as_ref().ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: "deepcoin batch_cancel_orders requires exchange_order_id".to_string(),
                }
            })?;
            order_ids.push(order_id.clone());
        }
        let value = self
            .rest
            .send_signed_post(
                "/deepcoin/trade/batch-cancel-order",
                &json!({ "OrderSysIDs": order_ids }),
            )
            .await?;
        let errors = batch_cancel_error_ids(super::parser::data_payload(&value));
        let orders = request
            .cancels
            .iter()
            .map(|cancel| {
                let mut order =
                    order_state_from_cancel_ack(&self.exchange_id, cancel, &Value::Null);
                if cancel
                    .exchange_order_id
                    .as_ref()
                    .is_some_and(|order_id| errors.contains(order_id))
                {
                    order.status = OrderStatus::Unknown;
                }
                order
            })
            .collect::<Vec<_>>();
        let cancelled_count = orders
            .iter()
            .filter(|order| order.status == OrderStatus::Cancelled)
            .count() as u32;
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "deepcoin cancel_all_orders requires symbol".to_string(),
            })?;
        if symbol.market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "deepcoin.cancel_all_spot_orders",
            });
        }
        let inst_id =
            normalize_deepcoin_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?;
        let body = json!({
            "InstrumentID": inst_id.replace("-SWAP", "").replace('-', ""),
            "ProductGroup": "SwapU",
            "IsCrossMargin": 1,
            "IsMergeMode": 1,
        });
        let value = self
            .rest
            .send_signed_post("/deepcoin/trade/swap/cancel-all", &body)
            .await?;
        let errors = super::parser::data_payload(&value)
            .get("errorList")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: Vec::new(),
            cancelled_count: if errors == 0 { 0 } else { errors as u32 },
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        if request.exchange_order_id.is_none() && request.client_order_id.is_none() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "deepcoin query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "instId".to_string(),
            normalize_deepcoin_symbol(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?,
        );
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("ordId".to_string(), order_id.clone());
        } else if let Some(client_id) = &request.client_order_id {
            params.insert("clOrdId".to_string(), client_id.clone());
        }
        let value = self
            .rest
            .send_signed_get("/deepcoin/trade/order", &params)
            .await?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id,
            ),
            order: parse_order(
                &self.exchange_id,
                Some(&request.symbol),
                request.symbol.market_type,
                &value,
            )?,
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
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let mut params = HashMap::new();
        params.insert("index".to_string(), "1".to_string());
        params.insert("limit".to_string(), "100".to_string());
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            params.insert(
                "instId".to_string(),
                normalize_deepcoin_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
            );
        }
        let value = self
            .rest
            .send_signed_get("/deepcoin/trade/v2/orders-pending", &params)
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(
                &self.exchange_id,
                request.symbol.as_ref(),
                market_type,
                &value,
            )?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request
            .symbol
            .as_ref()
            .map(|symbol| symbol.market_type)
            .or(request.market_type)
            .unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "deepcoin.get_recent_fills")?;
        let mut params = HashMap::new();
        params.insert(
            "instType".to_string(),
            product_inst_type(market_type).to_string(),
        );
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            params.insert(
                "instId".to_string(),
                normalize_deepcoin_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
            );
        }
        if let Some(order_id) = &request.exchange_order_id {
            params.insert("ordId".to_string(), order_id.clone());
        }
        if let Some(from_trade_id) = &request.from_trade_id {
            params.insert("after".to_string(), from_trade_id.clone());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "begin".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert("end".to_string(), end_time.timestamp_millis().to_string());
        }
        params.insert(
            "limit".to_string(),
            request.limit.unwrap_or(100).min(100).to_string(),
        );
        let value = self
            .rest
            .send_signed_get("/deepcoin/trade/fills", &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                request.symbol.as_ref(),
                market_type,
                &value,
            )?,
        })
    }
}

fn deepcoin_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.order_type == OrderType::FOK || request.time_in_force == Some(TimeInForce::FOK) {
        return Err(ExchangeApiError::Unsupported {
            operation: "deepcoin.fok_order",
        });
    }
    if request.symbol.market_type != MarketType::Spot && request.quote_quantity.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "deepcoin.quote_market_order_non_spot",
        });
    }
    let ord_type = match (request.order_type, request.post_only, request.time_in_force) {
        (OrderType::Market, _, _) => "market",
        (OrderType::PostOnly, _, _) | (_, true, _) => "post_only",
        (OrderType::IOC, _, _) | (_, _, Some(TimeInForce::IOC)) => "ioc",
        (OrderType::Limit, _, _) => "limit",
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "deepcoin.unsupported_order_type",
            })
        }
    };
    if matches!(ord_type, "limit" | "post_only" | "ioc") && request.price.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "deepcoin limit/post_only/ioc order requires price".to_string(),
        });
    }
    let mut body = json!({
        "instId": normalize_deepcoin_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type
        )?,
        "tdMode": if request.symbol.market_type == MarketType::Spot { "cash" } else { "cross" },
        "side": match request.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        },
        "ordType": ord_type,
        "sz": request.quote_quantity.as_ref().unwrap_or(&request.quantity),
    });
    if let Some(client_order_id) = &request.client_order_id {
        body["clOrdId"] = json!(client_order_id);
    }
    if let Some(price) = &request.price {
        body["px"] = json!(price);
    }
    if request.symbol.market_type == MarketType::Spot {
        if request.quote_quantity.is_some() {
            body["tgtCcy"] = json!("quote_ccy");
        } else {
            body["tgtCcy"] = json!("base_ccy");
        }
    } else {
        body["posSide"] = json!(deepcoin_position_side(request));
        body["mrgPosition"] = json!("merge");
        if request.reduce_only {
            body["reduceOnly"] = json!(true);
        }
    }
    Ok(body)
}

fn deepcoin_position_side(request: &PlaceOrderRequest) -> &'static str {
    match request.position_side {
        Some(PositionSide::Long) => "long",
        Some(PositionSide::Short) => "short",
        _ if request.side == OrderSide::Buy => "long",
        _ => "short",
    }
}

fn order_state_from_amend_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &AmendOrderRequest,
    value: &Value,
) -> OrderState {
    let data = super::parser::data_payload(value);
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request
            .new_client_order_id
            .clone()
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: data
            .get("ordId")
            .and_then(|value| match value {
                Value::String(text) => Some(text.clone()),
                Value::Number(number) => Some(number.to_string()),
                _ => None,
            })
            .or_else(|| request.exchange_order_id.clone()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: if data
            .get("errorCode")
            .and_then(|value| match value {
                Value::Number(number) => number.as_i64(),
                Value::String(text) => text.parse().ok(),
                _ => None,
            })
            .is_some_and(|code| code != 0)
        {
            OrderStatus::Unknown
        } else {
            OrderStatus::Open
        },
        quantity: request.new_quantity.clone(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: chrono::Utc::now(),
    }
}

fn response_array(value: &Value) -> Vec<&Value> {
    value
        .as_array()
        .map(|items| items.iter().collect())
        .unwrap_or_else(|| vec![value])
}

fn batch_cancel_error_ids(value: &Value) -> Vec<String> {
    value
        .get("errorList")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            item.get("orderSysId")
                .or_else(|| item.get("ordId"))
                .and_then(|value| match value {
                    Value::String(text) => Some(text.clone()),
                    Value::Number(number) => Some(number.to_string()),
                    _ => None,
                })
        })
        .collect()
}
