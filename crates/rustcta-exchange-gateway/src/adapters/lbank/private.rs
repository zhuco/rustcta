#![allow(dead_code)]

use std::collections::HashMap;

use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, TimeInForce};
use serde_json::Value;

use super::parser::{normalize_contract_symbol, normalize_spot_symbol};
use super::private_parser::{
    parse_balances, parse_contract_balances, parse_contract_order_state, parse_contract_positions,
    parse_fee_snapshots, parse_open_orders, parse_order_state, parse_order_states,
    parse_recent_fills,
};
use super::LBankGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, missing_order_identity, response_metadata};

#[derive(Debug, Clone, PartialEq)]
pub struct LBankPrivateAck {
    pub schema_version: u16,
    pub exchange: rustcta_types::ExchangeId,
    pub operation: String,
    pub data: Option<Value>,
    pub raw: Value,
}

impl LBankGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        match request.market_type.unwrap_or(MarketType::Spot) {
            MarketType::Spot => self.get_spot_balances(request).await,
            MarketType::Perpetual => self.get_contract_balances(request).await,
            _ => self.unsupported("lbank.get_balances.market_type"),
        }
    }

    async fn get_spot_balances(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let value = self
            .send_spot_signed_post(
                "lbank.get_balances",
                "/v2/supplement/user_info_account.do",
                &HashMap::new(),
            )
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

    async fn get_contract_balances(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "productGroup".to_string(),
            self.config.contract_product_group.clone(),
        );
        let asset = request
            .assets
            .first()
            .cloned()
            .unwrap_or_else(|| "USDT".to_string());
        params.insert("asset".to_string(), asset);
        let value = self
            .send_contract_signed_post(
                "lbank.contract.get_balances",
                "/cfd/openApi/v1/prv/account",
                &params,
            )
            .await?;
        Ok(BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            balances: parse_contract_balances(
                &self.exchange_id,
                tenant_id,
                account_id,
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
        match request.market_type.unwrap_or(MarketType::Perpetual) {
            MarketType::Perpetual => {}
            MarketType::Spot => return self.unsupported("lbank.spot_positions"),
            _ => return self.unsupported("lbank.get_positions.market_type"),
        }
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "productGroup".to_string(),
            self.config.contract_product_group.clone(),
        );
        params.insert("asset".to_string(), "USDT".to_string());
        let value = self
            .send_contract_signed_post(
                "lbank.get_positions",
                "/cfd/openApi/v1/prv/account",
                &params,
            )
            .await?;
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions: parse_contract_positions(
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
                message: "lbank get_fees requires at least one symbol".to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "category".to_string(),
            normalize_spot_symbol(&request.symbols[0].exchange_symbol.symbol)?,
        );
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        let value = self
            .send_spot_signed_post(
                "lbank.get_fees",
                "/v2/supplement/customer_trade_fee.do",
                &params,
            )
            .await?;
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_snapshots(&self.exchange_id, &request.symbols, &value)?,
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        let order = match request.symbol.market_type {
            MarketType::Spot => {
                let params = lbank_order_params(&request)?;
                let value = self
                    .send_spot_signed_post(
                        "lbank.place_order",
                        "/v2/supplement/create_order.do",
                        &params,
                    )
                    .await?;
                parse_order_state(&self.exchange_id, Some(&request.symbol), &value)?
            }
            MarketType::Perpetual => {
                let params = lbank_contract_order_params(&request)?;
                let value = self
                    .send_contract_signed_post(
                        "lbank.contract.place_order",
                        "/cfd/openApi/v1/prv/placeOrder",
                        &params,
                    )
                    .await?;
                parse_contract_order_state(&self.exchange_id, Some(&request.symbol), &value)?
            }
            _ => return self.unsupported("lbank.place_order.market_type"),
        };
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
                operation: "lbank.place_quote_market_order.sell_quote_quantity",
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_spot_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        params.insert("type".to_string(), "buy_market".to_string());
        params.insert("price".to_string(), request.quote_quantity.clone());
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("custom_id".to_string(), client_order_id.to_string());
        }
        let value = self
            .send_spot_signed_post(
                "lbank.place_quote_market_order",
                "/v2/supplement/create_order.do",
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
        self.ensure_spot(request.symbol.market_type)?;
        if missing_order_identity(&request) {
            return Err(ExchangeApiError::InvalidRequest {
                message: "lbank cancel_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_spot_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("origClientOrderId".to_string(), client_order_id.to_string());
        }
        let value = self
            .send_spot_signed_post(
                "lbank.cancel_order",
                "/v2/supplement/cancel_order.do",
                &params,
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| self.cancelled_order_from_request(&request));
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
                message: "lbank batch_place_orders requires at least one order".to_string(),
            });
        }
        let market_type = request.orders[0].symbol.market_type;
        if market_type == MarketType::Perpetual {
            for order in &request.orders {
                self.ensure_exchange(&order.symbol.exchange)?;
                if order.symbol.market_type != MarketType::Perpetual {
                    return Err(ExchangeApiError::InvalidRequest {
                        message: "lbank perpetual batch_place_orders requires homogeneous perpetual orders"
                            .to_string(),
                    });
                }
                lbank_contract_order_params(order)?;
            }
            let mut orders = Vec::with_capacity(request.orders.len());
            for order in request.orders {
                orders.push(self.place_order_impl(order).await?.order);
            }
            return Ok(BatchPlaceOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders,
                report: None,
            });
        }
        let mut native_orders = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_spot(order.symbol.market_type)?;
            let params = lbank_order_params(order)?;
            native_orders.push(serde_json::Value::Object(
                params
                    .into_iter()
                    .map(|(key, value)| (key, serde_json::Value::String(value)))
                    .collect(),
            ));
        }
        let mut params = HashMap::new();
        params.insert(
            "orders".to_string(),
            serde_json::to_string(&native_orders).map_err(|error| {
                ExchangeApiError::InvalidRequest {
                    message: format!("lbank batch_place_orders cannot encode orders: {error}"),
                }
            })?,
        );
        let value = self
            .send_spot_signed_post(
                "lbank.batch_place_orders",
                "/v2/batch_create_order.do",
                &params,
            )
            .await?;
        let orders = parse_order_states(&self.exchange_id, None, &value)
            .unwrap_or_else(|_| batch_place_orders_from_requests(&request.orders, &value));
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
                message: "lbank batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        let mut orders = Vec::with_capacity(request.cancels.len());
        let mut cancelled_count = 0_u32;
        for cancel in request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_spot(cancel.symbol.market_type)?;
            let response = self.cancel_order_impl(cancel).await?;
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
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "lbank cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_spot_symbol(&symbol.exchange_symbol.symbol)?,
        );
        let value = self
            .send_spot_signed_post(
                "lbank.cancel_all_orders",
                "/v2/supplement/cancel_order_by_symbol.do",
                &params,
            )
            .await?;
        let orders = parse_open_orders(&self.exchange_id, Some(symbol), &value).unwrap_or_default();
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
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
        params.insert(
            "symbol".to_string(),
            normalize_spot_symbol(&request.symbol.exchange_symbol.symbol)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert("origClientOrderId".to_string(), client_order_id.to_string());
        }
        if !params.contains_key("orderId") && !params.contains_key("origClientOrderId") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "lbank query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .send_spot_signed_post(
                "lbank.query_order",
                "/v2/supplement/orders_info.do",
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
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "lbank get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_spot_symbol(&symbol.exchange_symbol.symbol)?,
        );
        params.insert("current_page".to_string(), "1".to_string());
        params.insert("page_length".to_string(), "100".to_string());
        let value = self
            .send_spot_signed_post(
                "lbank.get_open_orders",
                "/v2/supplement/orders_info_no_deal.do",
                &params,
            )
            .await?;
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_open_orders(&self.exchange_id, Some(symbol), &value)?,
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
                message: "lbank get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_spot(symbol.market_type)?;
        let (tenant_id, account_id) = self.context_account(&request.context)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_spot_symbol(&symbol.exchange_symbol.symbol)?,
        );
        let endpoint = if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("order_id".to_string(), order_id.to_string());
            "/v2/order_transaction_detail.do"
        } else {
            if let Some(from_id) = request.from_trade_id.as_deref() {
                params.insert("fromId".to_string(), from_id.to_string());
            }
            if let Some(start_time) = request.start_time {
                params.insert(
                    "startTim".to_string(),
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
                params.insert("limit".to_string(), limit.min(100).to_string());
            }
            "/v2/supplement/transaction_history.do"
        };
        let value = self
            .send_spot_signed_post("lbank.get_recent_fills", endpoint, &params)
            .await?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?,
        })
    }

    pub async fn post_contract_signed_raw(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<LBankPrivateAck> {
        validate_contract_private_endpoint(endpoint)?;
        let value = self
            .send_contract_signed_post("lbank.contract.raw_signed_post", endpoint, params)
            .await?;
        Ok(lbank_private_ack(
            &self.exchange_id,
            "lbank.contract.raw_signed_post",
            value,
        ))
    }

    fn cancelled_order_from_request(
        &self,
        request: &CancelOrderRequest,
    ) -> rustcta_exchange_api::OrderState {
        rustcta_exchange_api::OrderState {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: self.exchange_id.clone(),
            market_type: MarketType::Spot,
            canonical_symbol: request.symbol.canonical_symbol.clone(),
            exchange_symbol: request.symbol.exchange_symbol.clone(),
            client_order_id: request.client_order_id.clone(),
            exchange_order_id: request.exchange_order_id.clone(),
            side: OrderSide::Buy,
            position_side: Some(rustcta_types::PositionSide::None),
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
}

fn validate_contract_private_endpoint(endpoint: &str) -> ExchangeApiResult<()> {
    if endpoint.starts_with("/cfd/openApi/v1/prv/")
        && !endpoint.contains('?')
        && !endpoint.contains("..")
    {
        return Ok(());
    }
    Err(ExchangeApiError::InvalidRequest {
        message:
            "lbank contract raw endpoint must be a /cfd/openApi/v1/prv/ path without query text"
                .to_string(),
    })
}

fn lbank_private_ack(
    exchange_id: &rustcta_types::ExchangeId,
    operation: &str,
    value: Value,
) -> LBankPrivateAck {
    LBankPrivateAck {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        operation: operation.to_string(),
        data: value.get("data").cloned(),
        raw: value,
    }
}

fn lbank_contract_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    if request.post_only || request.order_type == OrderType::PostOnly {
        return Err(ExchangeApiError::Unsupported {
            operation: "lbank.contract_post_only",
        });
    }
    if !matches!(request.order_type, OrderType::Limit) {
        return Err(ExchangeApiError::Unsupported {
            operation: "lbank.contract_order_type",
        });
    }
    let price = request
        .price
        .as_ref()
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "lbank contract limit order requires price".to_string(),
        })?;
    let mut params = HashMap::new();
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("clientOrderId".to_string(), client_order_id.to_string());
    }
    params.insert(
        "symbol".to_string(),
        normalize_contract_symbol(&request.symbol.exchange_symbol.symbol)?,
    );
    params.insert(
        "side".to_string(),
        match request.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
        .to_string(),
    );
    params.insert(
        "offsetFlag".to_string(),
        if request.reduce_only { "1" } else { "0" }.to_string(),
    );
    params.insert("orderPriceType".to_string(), "4".to_string());
    params.insert("origType".to_string(), "0".to_string());
    params.insert("price".to_string(), price.clone());
    params.insert("volume".to_string(), request.quantity.clone());
    Ok(params)
}

fn lbank_order_params(request: &PlaceOrderRequest) -> ExchangeApiResult<HashMap<String, String>> {
    if request.reduce_only {
        return Err(ExchangeApiError::Unsupported {
            operation: "lbank.spot_reduce_only",
        });
    }
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_spot_symbol(&request.symbol.exchange_symbol.symbol)?,
    );
    params.insert("type".to_string(), lbank_order_type(request)?.to_string());
    match request.order_type {
        OrderType::Market if request.side == OrderSide::Buy => {
            let quote = request
                .quote_quantity
                .as_ref()
                .or(request.price.as_ref())
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "lbank buy_market requires quote_quantity or price amount".to_string(),
                })?;
            params.insert("price".to_string(), quote.clone());
        }
        OrderType::Market => {
            params.insert("amount".to_string(), request.quantity.clone());
        }
        _ => {
            let price = request
                .price
                .as_ref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "lbank limit order requires price".to_string(),
                })?;
            params.insert("price".to_string(), price.clone());
            params.insert("amount".to_string(), request.quantity.clone());
        }
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("custom_id".to_string(), client_order_id.to_string());
    }
    Ok(params)
}

fn lbank_order_type(request: &PlaceOrderRequest) -> ExchangeApiResult<&'static str> {
    let tif = request.time_in_force;
    match (request.side, request.order_type, request.post_only, tif) {
        (OrderSide::Buy, OrderType::Market, _, _) => Ok("buy_market"),
        (OrderSide::Sell, OrderType::Market, _, _) => Ok("sell_market"),
        (OrderSide::Buy, OrderType::PostOnly, _, _) | (OrderSide::Buy, _, true, _) => {
            Ok("buy_maker")
        }
        (OrderSide::Sell, OrderType::PostOnly, _, _) | (OrderSide::Sell, _, true, _) => {
            Ok("sell_maker")
        }
        (OrderSide::Buy, OrderType::IOC, _, _) | (OrderSide::Buy, _, _, Some(TimeInForce::IOC)) => {
            Ok("buy_ioc")
        }
        (OrderSide::Sell, OrderType::IOC, _, _)
        | (OrderSide::Sell, _, _, Some(TimeInForce::IOC)) => Ok("sell_ioc"),
        (OrderSide::Buy, OrderType::FOK, _, _) | (OrderSide::Buy, _, _, Some(TimeInForce::FOK)) => {
            Ok("buy_fok")
        }
        (OrderSide::Sell, OrderType::FOK, _, _)
        | (OrderSide::Sell, _, _, Some(TimeInForce::FOK)) => Ok("sell_fok"),
        (OrderSide::Buy, OrderType::Limit, _, _) => Ok("buy"),
        (OrderSide::Sell, OrderType::Limit, _, _) => Ok("sell"),
        (_, unsupported, _, _) => Err(ExchangeApiError::Unsupported {
            operation: match unsupported {
                OrderType::StopMarket => "lbank.stop_market",
                OrderType::StopLimit => "lbank.stop_limit",
                _ => "lbank.order_type",
            },
        }),
    }
}

fn batch_place_orders_from_requests(
    requests: &[PlaceOrderRequest],
    value: &serde_json::Value,
) -> Vec<rustcta_exchange_api::OrderState> {
    let order_ids = value
        .get("data")
        .or_else(|| value.get("order_id"))
        .and_then(|value| {
            value
                .as_array()
                .map(|rows| {
                    rows.iter()
                        .filter_map(|row| {
                            row.get("order_id")
                                .or_else(|| row.get("orderId"))
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_string)
                                .or_else(|| row.as_str().map(str::to_string))
                        })
                        .collect::<Vec<_>>()
                })
                .or_else(|| {
                    value.as_str().map(|text| {
                        text.split(',')
                            .map(str::trim)
                            .filter(|text| !text.is_empty())
                            .map(str::to_string)
                            .collect::<Vec<_>>()
                    })
                })
        })
        .unwrap_or_default();
    requests
        .iter()
        .enumerate()
        .map(|(index, request)| {
            let mut order = rustcta_exchange_api::OrderState {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                exchange: request.symbol.exchange.clone(),
                market_type: MarketType::Spot,
                canonical_symbol: request.symbol.canonical_symbol.clone(),
                exchange_symbol: request.symbol.exchange_symbol.clone(),
                client_order_id: request.client_order_id.clone(),
                exchange_order_id: order_ids.get(index).cloned(),
                side: request.side,
                position_side: Some(rustcta_types::PositionSide::None),
                order_type: request.order_type,
                time_in_force: request.time_in_force.or(Some(TimeInForce::GTC)),
                status: OrderStatus::Open,
                quantity: request.quantity.clone(),
                price: request.price.clone(),
                filled_quantity: "0".to_string(),
                average_fill_price: None,
                reduce_only: false,
                post_only: request.post_only || request.order_type == OrderType::PostOnly,
                created_at: None,
                updated_at: chrono::Utc::now(),
            };
            if request.order_type == OrderType::Market {
                order.time_in_force = None;
            }
            order
        })
        .collect()
}
