#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use chrono::Utc;
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

use super::parser::normalize_woo_symbol;
use super::private_parser::{
    parse_balances, parse_fees, parse_fills, parse_order, parse_order_state, parse_orders,
    parse_positions,
};
use super::WooGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Clone, PartialEq)]
pub struct WooAlgoOrderRequest {
    pub symbol: rustcta_exchange_api::SymbolScope,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: String,
    pub price: Option<String>,
    pub trigger_price: String,
    pub trigger_price_type: Option<String>,
    pub client_order_id: Option<String>,
    pub reduce_only: bool,
    pub position_side: Option<PositionSide>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WooAlgoOrderQuery {
    pub symbol: rustcta_exchange_api::SymbolScope,
    pub algo_order_id: Option<String>,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WooAlgoOrdersQuery {
    pub symbol: Option<rustcta_exchange_api::SymbolScope>,
    pub order_type: Option<OrderType>,
    pub status: Option<String>,
    pub limit: Option<u16>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WooAlgoOrderAmendRequest {
    pub symbol: rustcta_exchange_api::SymbolScope,
    pub algo_order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub new_quantity: Option<String>,
    pub new_price: Option<String>,
    pub new_trigger_price: Option<String>,
}

impl WooGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        self.ensure_private_rest("woo.get_balances")?;
        let mut params = HashMap::new();
        if request.assets.len() == 1 {
            params.insert("token".to_string(), request.assets[0].to_ascii_uppercase());
        }
        let value = self
            .rest
            .send_signed_get("woo.get_balances", "/v3/asset/balances", &params)
            .await?;
        let (tenant_id, account_id) = self.context_account(&request.context, "woo.get_balances")?;
        let balances = parse_balances(
            &self.exchange_id,
            tenant_id,
            account_id,
            &request.assets,
            market_type,
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
            self.ensure_perp(market_type)?;
        }
        self.ensure_private_rest("woo.get_positions")?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "woo.get_positions")?;
        let mut all_positions = Vec::new();
        if request.symbols.is_empty() {
            let value = self
                .rest
                .send_signed_get(
                    "woo.get_positions",
                    "/v3/futures/positions",
                    &HashMap::new(),
                )
                .await?;
            all_positions.extend(parse_positions(
                &self.exchange_id,
                tenant_id,
                account_id,
                &request.symbols,
                &value,
            )?);
        } else {
            for symbol in &request.symbols {
                let mut params = HashMap::new();
                params.insert("symbol".to_string(), symbol.symbol.to_ascii_uppercase());
                let value = self
                    .rest
                    .send_signed_get("woo.get_positions", "/v3/futures/positions", &params)
                    .await?;
                all_positions.extend(parse_positions(
                    &self.exchange_id,
                    tenant_id.clone(),
                    account_id.clone(),
                    std::slice::from_ref(symbol),
                    &value,
                )?);
            }
        }
        Ok(PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            positions: all_positions,
        })
    }

    pub(super) async fn get_fees_impl(
        &self,
        request: FeesRequest,
    ) -> ExchangeApiResult<FeesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_private_rest("woo.get_fees")?;
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "woo.get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            let mut params = HashMap::new();
            params.insert("symbol".to_string(), normalize_woo_symbol(symbol)?);
            let value = self
                .rest
                .send_signed_get("woo.get_fees", "/v3/trade/tradingFee", &params)
                .await?;
            fees.extend(parse_fees(&self.exchange_id, symbol, &value)?);
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
        self.ensure_private_rest("woo.place_order")?;
        let body = woo_place_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("woo.place_order", "/v3/trade/order", &body)
            .await?;
        let order = order_state_from_place_ack(&self.exchange_id, &request, ack_data(&value))?;
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
        self.ensure_supported_market(request.symbol.market_type)?;
        if request.symbol.market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "woo.perp_quote_market_order",
            });
        }
        self.ensure_private_rest("woo.place_quote_market_order")?;
        let body = woo_quote_market_order_body(&request)?;
        let value = self
            .rest
            .send_signed_post("woo.place_quote_market_order", "/v3/trade/order", &body)
            .await?;
        let order = order_state_from_quote_ack(&self.exchange_id, &request, ack_data(&value));
        Ok(PlaceOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order,
        })
    }

    pub(super) async fn cancel_order_impl(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        self.ensure_private_rest("woo.cancel_order")?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), normalize_woo_symbol(&request.symbol)?);
        insert_order_identity(
            &mut params,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "cancel_order",
        )?;
        let value = self
            .rest
            .send_signed_delete("woo.cancel_order", "/v3/trade/order", &params)
            .await?;
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: order_state_from_cancel_ack(&self.exchange_id, &request, ack_data(&value)),
            cancelled: ack_data(&value)
                .get("status")
                .and_then(Value::as_str)
                .map(|status| status == "CANCELLED")
                .unwrap_or(true),
        })
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        self.ensure_private_rest("woo.batch_place_orders")?;
        if request.orders.is_empty() {
            return Ok(BatchPlaceOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                report: None,
            });
        }
        let market_type = request.orders[0].symbol.market_type;
        for order in &request.orders {
            ensure_exchange_api_schema(order.schema_version)?;
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market(order.symbol.market_type)?;
            if order.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "woo batch_place_orders requires one market type".to_string(),
                });
            }
        }

        let mut orders = Vec::with_capacity(request.orders.len());
        for order_request in request.orders {
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
        self.ensure_private_rest("woo.batch_cancel_orders")?;
        if request.cancels.is_empty() {
            return Ok(BatchCancelOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                cancelled_count: 0,
                report: None,
            });
        }
        let market_type = request.cancels[0].symbol.market_type;
        for cancel in &request.cancels {
            ensure_exchange_api_schema(cancel.schema_version)?;
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market(cancel.symbol.market_type)?;
            if cancel.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "woo batch_cancel_orders requires one market type".to_string(),
                });
            }
        }

        let mut orders = Vec::with_capacity(request.cancels.len());
        let mut cancelled_count = 0_u32;
        for cancel_request in request.cancels {
            let cancelled = self.cancel_order_impl(cancel_request).await?;
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

    pub(super) async fn cancel_all_orders_impl(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_supported_market(market_type)?;
        }
        self.ensure_private_rest("woo.cancel_all_orders")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "woo.cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), normalize_woo_symbol(symbol)?);
        let value = self
            .rest
            .send_signed_delete("woo.cancel_all_orders", "/v3/trade/allOrders", &params)
            .await?;
        let cancelled = ack_data(&value)
            .get("status")
            .and_then(Value::as_str)
            .map(|status| status == "CANCELLED")
            .unwrap_or(true);
        let orders = cancelled
            .then(|| cancel_all_order_state(&self.exchange_id, symbol))
            .into_iter()
            .collect::<Vec<_>>();
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
        self.ensure_supported_market(request.symbol.market_type)?;
        self.ensure_private_rest("woo.amend_order")?;
        let body = woo_amend_order_body(&request)?;
        let value = self
            .rest
            .send_signed_put("woo.amend_order", "/v3/trade/order", &body)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), ack_data(&value))
            .unwrap_or_else(|_| {
                order_state_from_amend_ack(&self.exchange_id, &request, ack_data(&value))
            });
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order,
        })
    }

    pub(super) async fn query_order_impl(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        self.ensure_private_rest("woo.query_order")?;
        let mut params = HashMap::new();
        insert_order_identity(
            &mut params,
            request.exchange_order_id.as_deref(),
            request.client_order_id.as_deref(),
            "query_order",
        )?;
        params.insert("withRealizedPnl".to_string(), "1".to_string());
        let value = self
            .rest
            .send_signed_get("woo.query_order", "/v3/trade/order", &params)
            .await?;
        let order = parse_order(&self.exchange_id, Some(&request.symbol), &value)?;
        Ok(QueryOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            order,
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
        self.ensure_private_rest("woo.get_open_orders")?;
        let mut params = HashMap::new();
        params.insert("status".to_string(), "INCOMPLETE".to_string());
        params.insert("size".to_string(), "500".to_string());
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            params.insert("symbol".to_string(), normalize_woo_symbol(symbol)?);
        }
        let value = self
            .rest
            .send_signed_get("woo.get_open_orders", "/v3/trade/orders", &params)
            .await?;
        let orders = parse_orders(&self.exchange_id, request.symbol.as_ref(), &value)?;
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
            self.ensure_supported_market(market_type)?;
        }
        self.ensure_private_rest("woo.get_recent_fills")?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "woo.get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), normalize_woo_symbol(symbol)?);
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
        if let Some(from_trade_id) = &request.from_trade_id {
            params.insert("fromId".to_string(), from_trade_id.clone());
        }
        if let Some(client_order_id) = &request.client_order_id {
            params.insert("clientOrderId".to_string(), client_order_id.clone());
        }
        if let Some(exchange_order_id) = &request.exchange_order_id {
            params.insert("orderId".to_string(), exchange_order_id.clone());
        }
        params.insert(
            "limit".to_string(),
            request.limit.unwrap_or(100).clamp(1, 500).to_string(),
        );
        let value = self
            .rest
            .send_signed_get(
                "woo.get_recent_fills",
                "/v3/trade/transactionHistory",
                &params,
            )
            .await?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "woo.get_recent_fills")?;
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

    pub async fn set_cancel_all_after(&self, trigger_after_ms: u64) -> ExchangeApiResult<Value> {
        self.ensure_private_rest("woo.set_cancel_all_after")?;
        if trigger_after_ms > 900_000 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "woo cancel-all-after trigger_after_ms must be <= 900000".to_string(),
            });
        }
        self.rest
            .send_signed_post(
                "woo.set_cancel_all_after",
                "/v3/trade/cancelAllAfter",
                &json!({ "triggerAfter": trigger_after_ms }),
            )
            .await
    }

    pub async fn set_futures_leverage(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        margin_mode: &str,
        leverage: u32,
        position_mode: Option<&str>,
    ) -> ExchangeApiResult<Value> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perp(symbol.market_type)?;
        self.ensure_private_rest("woo.set_futures_leverage")?;
        if leverage == 0 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "woo futures leverage must be greater than zero".to_string(),
            });
        }
        let margin_mode = normalize_margin_mode(margin_mode)?;
        let mut body = json!({
            "symbol": normalize_woo_symbol(&symbol)?,
            "marginMode": margin_mode,
            "leverage": leverage,
        });
        if let Some(position_mode) = position_mode {
            body["positionMode"] = Value::String(normalize_position_mode(position_mode)?);
        }
        self.rest
            .send_signed_put("woo.set_futures_leverage", "/v3/futures/leverage", &body)
            .await
    }

    pub async fn set_position_mode(&self, position_mode: &str) -> ExchangeApiResult<Value> {
        self.ensure_private_rest("woo.set_position_mode")?;
        self.rest
            .send_signed_put(
                "woo.set_position_mode",
                "/v3/futures/positionMode",
                &json!({ "positionMode": normalize_position_mode(position_mode)? }),
            )
            .await
    }

    pub async fn set_account_trading_mode(&self, trading_mode: &str) -> ExchangeApiResult<Value> {
        self.ensure_private_rest("woo.set_account_trading_mode")?;
        self.rest
            .send_signed_post(
                "woo.set_account_trading_mode",
                "/v3/account/tradingMode",
                &json!({ "tradingMode": normalize_trading_mode(trading_mode)? }),
            )
            .await
    }

    pub async fn get_default_margin_mode(
        &self,
        symbol: Option<rustcta_exchange_api::SymbolScope>,
    ) -> ExchangeApiResult<Value> {
        self.ensure_private_rest("woo.get_default_margin_mode")?;
        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_perp(symbol.market_type)?;
            params.insert("symbol".to_string(), normalize_woo_symbol(&symbol)?);
        }
        self.rest
            .send_signed_get(
                "woo.get_default_margin_mode",
                "/v3/futures/defaultMarginMode",
                &params,
            )
            .await
    }

    pub async fn place_algo_order(&self, request: WooAlgoOrderRequest) -> ExchangeApiResult<Value> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        self.ensure_private_rest("woo.place_algo_order")?;
        let body = woo_algo_order_body(&request)?;
        self.rest
            .send_signed_post("woo.place_algo_order", "/v3/trade/algoOrder", &body)
            .await
    }

    pub async fn amend_algo_order(
        &self,
        request: WooAlgoOrderAmendRequest,
    ) -> ExchangeApiResult<Value> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        self.ensure_private_rest("woo.amend_algo_order")?;
        let body = woo_algo_amend_order_body(&request)?;
        self.rest
            .send_signed_put("woo.amend_algo_order", "/v3/trade/algoOrder", &body)
            .await
    }

    pub async fn cancel_algo_order(&self, query: WooAlgoOrderQuery) -> ExchangeApiResult<Value> {
        self.ensure_exchange(&query.symbol.exchange)?;
        self.ensure_supported_market(query.symbol.market_type)?;
        self.ensure_private_rest("woo.cancel_algo_order")?;
        let params = woo_algo_order_query_params(&query, "cancel_algo_order")?;
        self.rest
            .send_signed_delete("woo.cancel_algo_order", "/v3/trade/algoOrder", &params)
            .await
    }

    pub async fn query_algo_order(&self, query: WooAlgoOrderQuery) -> ExchangeApiResult<Value> {
        self.ensure_exchange(&query.symbol.exchange)?;
        self.ensure_supported_market(query.symbol.market_type)?;
        self.ensure_private_rest("woo.query_algo_order")?;
        let params = woo_algo_order_query_params(&query, "query_algo_order")?;
        self.rest
            .send_signed_get("woo.query_algo_order", "/v3/trade/algoOrder", &params)
            .await
    }

    pub async fn get_algo_orders(&self, query: WooAlgoOrdersQuery) -> ExchangeApiResult<Value> {
        self.ensure_private_rest("woo.get_algo_orders")?;
        let params = woo_algo_orders_query_params(&query)?;
        self.rest
            .send_signed_get("woo.get_algo_orders", "/v3/trade/algoOrders", &params)
            .await
    }

    pub async fn cancel_algo_orders(&self, query: WooAlgoOrdersQuery) -> ExchangeApiResult<Value> {
        self.ensure_private_rest("woo.cancel_algo_orders")?;
        let params = woo_algo_orders_query_params(&query)?;
        self.rest
            .send_signed_delete("woo.cancel_algo_orders", "/v3/trade/algoOrders", &params)
            .await
    }
}

fn woo_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "symbol": normalize_woo_symbol(&request.symbol)?,
        "side": woo_side(request.side),
        "type": woo_order_type(request.order_type, request.time_in_force, request.post_only),
    });
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOrderId"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    match request.order_type {
        OrderType::Market => {
            if let Some(quote_quantity) = request.quote_quantity.as_deref() {
                body["amount"] = Value::String(non_empty("quote_quantity", quote_quantity)?);
            } else {
                body["quantity"] = Value::String(non_empty("quantity", &request.quantity)?);
            }
        }
        _ => {
            body["quantity"] = Value::String(non_empty("quantity", &request.quantity)?);
            body["price"] = Value::String(non_empty(
                "price",
                request
                    .price
                    .as_deref()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "woo limit-style order requires price".to_string(),
                    })?,
            )?);
        }
    }
    if request.symbol.market_type == MarketType::Perpetual {
        body["reduceOnly"] = Value::Bool(request.reduce_only);
        if let Some(position_side) = request.position_side {
            body["positionSide"] = Value::String(woo_position_side(position_side).to_string());
        }
    } else if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "woo spot order does not support reduce_only".to_string(),
        });
    }
    Ok(body)
}

fn woo_quote_market_order_body(request: &QuoteMarketOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "symbol": normalize_woo_symbol(&request.symbol)?,
        "side": woo_side(request.side),
        "type": "MARKET",
        "amount": non_empty("quote_quantity", &request.quote_quantity)?,
    });
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOrderId"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    Ok(body)
}

fn woo_algo_order_body(request: &WooAlgoOrderRequest) -> ExchangeApiResult<Value> {
    if !matches!(
        request.order_type,
        OrderType::StopMarket | OrderType::StopLimit
    ) {
        return Err(ExchangeApiError::InvalidRequest {
            message: "woo algo order supports stop_market or stop_limit".to_string(),
        });
    }
    let mut body = json!({
        "symbol": normalize_woo_symbol(&request.symbol)?,
        "side": woo_side(request.side),
        "type": woo_algo_order_type(request.order_type),
        "quantity": non_empty("quantity", &request.quantity)?,
        "triggerPrice": non_empty("trigger_price", &request.trigger_price)?,
    });
    if let Some(price) = request.price.as_deref() {
        body["price"] = Value::String(non_empty("price", price)?);
    } else if request.order_type == OrderType::StopLimit {
        return Err(ExchangeApiError::InvalidRequest {
            message: "woo stop_limit algo order requires price".to_string(),
        });
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOrderId"] = Value::String(non_empty("client_order_id", client_order_id)?);
    }
    if let Some(trigger_price_type) = request.trigger_price_type.as_deref() {
        body["triggerPriceType"] = Value::String(normalize_trigger_price_type(trigger_price_type)?);
    }
    if request.symbol.market_type == MarketType::Perpetual {
        body["reduceOnly"] = Value::Bool(request.reduce_only);
        if let Some(position_side) = request.position_side {
            body["positionSide"] = Value::String(woo_position_side(position_side).to_string());
        }
    } else if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "woo spot algo order does not support reduce_only".to_string(),
        });
    }
    Ok(body)
}

fn woo_algo_amend_order_body(request: &WooAlgoOrderAmendRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "symbol": normalize_woo_symbol(&request.symbol)?,
    });
    if let Some(order_id) = request.algo_order_id.as_deref() {
        body["algoOrderId"] = Value::String(non_empty("algo_order_id", order_id)?);
    } else if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOrderId"] = Value::String(non_empty("client_order_id", client_order_id)?);
    } else {
        return Err(ExchangeApiError::InvalidRequest {
            message: "woo amend_algo_order requires algo_order_id or client_order_id".to_string(),
        });
    }
    if let Some(quantity) = request.new_quantity.as_deref() {
        body["quantity"] = Value::String(non_empty("new_quantity", quantity)?);
    }
    if let Some(price) = request.new_price.as_deref() {
        body["price"] = Value::String(non_empty("new_price", price)?);
    }
    if let Some(trigger_price) = request.new_trigger_price.as_deref() {
        body["triggerPrice"] = Value::String(non_empty("new_trigger_price", trigger_price)?);
    }
    if request.new_quantity.is_none()
        && request.new_price.is_none()
        && request.new_trigger_price.is_none()
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "woo amend_algo_order requires at least one changed field".to_string(),
        });
    }
    Ok(body)
}

fn woo_algo_order_query_params(
    query: &WooAlgoOrderQuery,
    operation: &str,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert("symbol".to_string(), normalize_woo_symbol(&query.symbol)?);
    if let Some(order_id) = query
        .algo_order_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        params.insert("algoOrderId".to_string(), order_id.to_string());
    } else if let Some(client_order_id) = query
        .client_order_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        params.insert("clientOrderId".to_string(), client_order_id.to_string());
    } else {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("woo {operation} requires algo_order_id or client_order_id"),
        });
    }
    Ok(params)
}

fn woo_algo_orders_query_params(
    query: &WooAlgoOrdersQuery,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    if let Some(symbol) = &query.symbol {
        params.insert("symbol".to_string(), normalize_woo_symbol(symbol)?);
    }
    if let Some(order_type) = query.order_type {
        params.insert(
            "type".to_string(),
            woo_algo_order_type(order_type).to_string(),
        );
    }
    if let Some(status) = query.status.as_deref() {
        params.insert("status".to_string(), non_empty("status", status)?);
    }
    if let Some(limit) = query.limit {
        params.insert("limit".to_string(), limit.clamp(1, 500).to_string());
    }
    Ok(params)
}

fn woo_amend_order_body(request: &AmendOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = json!({
        "symbol": normalize_woo_symbol(&request.symbol)?,
        "quantity": non_empty("new_quantity", &request.new_quantity)?,
    });
    if let Some(new_client_order_id) = request.new_client_order_id.as_deref() {
        body["clientOrderId"] =
            Value::String(non_empty("new_client_order_id", new_client_order_id)?);
    }
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body["orderId"] = Value::String(non_empty("exchange_order_id", order_id)?);
    } else if let Some(client_order_id) = request.client_order_id.as_deref() {
        body["clientOrderId"] = Value::String(non_empty("client_order_id", client_order_id)?);
    } else {
        return Err(ExchangeApiError::InvalidRequest {
            message: "woo amend_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    Ok(body)
}

fn order_state_from_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    ack: &Value,
) -> ExchangeApiResult<OrderState> {
    if ack.get("symbol").is_some() && ack.get("side").is_some() {
        return parse_order_state(exchange_id, Some(&request.symbol), ack);
    }
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clientOrderId"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("orderId")),
        side: request.side,
        position_side: request.position_side.or(Some(PositionSide::None)),
        order_type: request.order_type,
        time_in_force: request.time_in_force.or(Some(TimeInForce::GTC)),
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only || matches!(request.time_in_force, Some(TimeInForce::GTX)),
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    })
}

fn order_state_from_quote_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &QuoteMarketOrderRequest,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clientOrderId"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("orderId")),
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
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    ack: &Value,
) -> OrderState {
    cancel_order_state(
        exchange_id,
        &request.symbol,
        value_text(ack.get("orderId")).or_else(|| request.exchange_order_id.clone()),
        value_text(ack.get("clientOrderId")).or_else(|| request.client_order_id.clone()),
    )
}

fn cancel_all_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
) -> OrderState {
    cancel_order_state(exchange_id, symbol, None, None)
}

fn cancel_order_state(
    exchange_id: &rustcta_types::ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    order_id: Option<String>,
    client_order_id: Option<String>,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: symbol.market_type,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol.clone(),
        client_order_id,
        exchange_order_id: order_id,
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
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
        updated_at: Utc::now(),
    }
}

fn order_state_from_amend_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &AmendOrderRequest,
    ack: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value_text(ack.get("clientOrderId"))
            .or_else(|| request.new_client_order_id.clone())
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_text(ack.get("orderId"))
            .or_else(|| request.exchange_order_id.clone()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        status: OrderStatus::New,
        quantity: request.new_quantity.clone(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: None,
        updated_at: Utc::now(),
    }
}

fn ack_data(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn insert_order_identity(
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
    } else if let Some(client_id) = client_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        params.insert("clientOrderId".to_string(), client_id.to_string());
    } else {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("woo {operation} requires exchange_order_id or client_order_id"),
        });
    }
    Ok(())
}

fn woo_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn woo_position_side(side: PositionSide) -> &'static str {
    match side {
        PositionSide::Long => "LONG",
        PositionSide::Short => "SHORT",
        PositionSide::Net | PositionSide::None => "BOTH",
    }
}

fn woo_order_type(
    order_type: OrderType,
    tif: Option<TimeInForce>,
    post_only: bool,
) -> &'static str {
    if post_only || matches!(tif, Some(TimeInForce::GTX)) {
        return "POST_ONLY";
    }
    match (order_type, tif) {
        (OrderType::Market, _) => "MARKET",
        (OrderType::IOC, _) | (_, Some(TimeInForce::IOC)) => "IOC",
        (OrderType::FOK, _) | (_, Some(TimeInForce::FOK)) => "FOK",
        (OrderType::PostOnly, _) => "POST_ONLY",
        _ => "LIMIT",
    }
}

fn woo_algo_order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::StopMarket => "STOP_MARKET",
        OrderType::StopLimit => "STOP_LIMIT",
        _ => "STOP_LIMIT",
    }
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("woo {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn normalize_trigger_price_type(value: &str) -> ExchangeApiResult<String> {
    match value.trim().to_ascii_uppercase().replace('-', "_").as_str() {
        "MARK_PRICE" | "MARK" => Ok("MARK_PRICE".to_string()),
        "INDEX_PRICE" | "INDEX" => Ok("INDEX_PRICE".to_string()),
        "LAST_PRICE" | "LAST" | "TRADE_PRICE" => Ok("LAST_PRICE".to_string()),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: "woo trigger_price_type must be MARK_PRICE, INDEX_PRICE, or LAST_PRICE"
                .to_string(),
        }),
    }
}

fn normalize_margin_mode(value: &str) -> ExchangeApiResult<String> {
    match value.trim().to_ascii_uppercase().as_str() {
        "CROSS" | "CROSSED" => Ok("CROSS".to_string()),
        "ISOLATED" => Ok("ISOLATED".to_string()),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: "woo margin_mode must be CROSS or ISOLATED".to_string(),
        }),
    }
}

fn normalize_position_mode(value: &str) -> ExchangeApiResult<String> {
    match value.trim().to_ascii_uppercase().replace('-', "_").as_str() {
        "ONE_WAY" | "ONEWAY" | "NET" => Ok("ONE_WAY".to_string()),
        "HEDGE_MODE" | "HEDGE" | "HEDGED" => Ok("HEDGE_MODE".to_string()),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: "woo position_mode must be ONE_WAY or HEDGE_MODE".to_string(),
        }),
    }
}

fn normalize_trading_mode(value: &str) -> ExchangeApiResult<String> {
    match value.trim().to_ascii_uppercase().replace('-', "_").as_str() {
        "PURE_SPOT" | "SPOT" => Ok("PURE_SPOT".to_string()),
        "MARGIN" => Ok("MARGIN".to_string()),
        "FUTURES" | "PERP" | "PERPETUAL" => Ok("FUTURES".to_string()),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: "woo trading_mode must be PURE_SPOT, MARGIN, or FUTURES".to_string(),
        }),
    }
}

fn value_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.is_empty() && text != "0" => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()).filter(|value| value != "0"),
        _ => None,
    }
}
