#![allow(dead_code)]

use std::collections::HashMap;

use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse,
    PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_bitunix_symbol;
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_fills, parse_order, parse_order_state, parse_orders,
    parse_positions,
};
use super::BitunixGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Clone, PartialEq)]
pub struct BitunixPrivateAck {
    pub operation: &'static str,
    pub data: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitunixMarginMode {
    Isolation,
    Cross,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitunixPositionMode {
    OneWay,
    Hedge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitunixStopType {
    LastPrice,
    MarkPrice,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitunixTriggerOrderType {
    Market,
    Limit,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BitunixTpslOrderRequest {
    pub symbol: rustcta_exchange_api::SymbolScope,
    pub position_id: String,
    pub take_profit_price: Option<String>,
    pub take_profit_stop_type: Option<BitunixStopType>,
    pub take_profit_order_type: Option<BitunixTriggerOrderType>,
    pub take_profit_order_price: Option<String>,
    pub take_profit_quantity: Option<String>,
    pub stop_loss_price: Option<String>,
    pub stop_loss_stop_type: Option<BitunixStopType>,
    pub stop_loss_order_type: Option<BitunixTriggerOrderType>,
    pub stop_loss_order_price: Option<String>,
    pub stop_loss_quantity: Option<String>,
}

impl BitunixGatewayAdapter {
    pub async fn get_leverage_margin_mode(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        margin_coin: &str,
    ) -> ExchangeApiResult<BitunixPrivateAck> {
        self.ensure_exchange(&symbol.exchange)?;
        ensure_bitunix_perpetual_symbol(self, &symbol)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bitunix_symbol(&symbol.exchange_symbol.symbol)?,
        );
        params.insert(
            "marginCoin".to_string(),
            non_empty("marginCoin", margin_coin)?,
        );
        let value = self
            .send_signed_get(
                "bitunix.get_leverage_margin_mode",
                MarketType::Perpetual,
                "/api/v1/futures/account/get_leverage_margin_mode",
                &params,
            )
            .await?;
        Ok(bitunix_private_ack(
            "bitunix.get_leverage_margin_mode",
            value,
        ))
    }

    pub async fn change_leverage(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        margin_coin: &str,
        leverage: u32,
    ) -> ExchangeApiResult<BitunixPrivateAck> {
        self.ensure_exchange(&symbol.exchange)?;
        ensure_bitunix_perpetual_symbol(self, &symbol)?;
        let body = json!({
            "symbol": normalize_bitunix_symbol(&symbol.exchange_symbol.symbol)?,
            "marginCoin": non_empty("marginCoin", margin_coin)?,
            "leverage": leverage,
        });
        let value = self
            .send_signed_post(
                "bitunix.change_leverage",
                MarketType::Perpetual,
                "/api/v1/futures/account/change_leverage",
                &HashMap::new(),
                body,
            )
            .await?;
        Ok(bitunix_private_ack("bitunix.change_leverage", value))
    }

    pub async fn change_margin_mode(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        margin_coin: &str,
        margin_mode: BitunixMarginMode,
    ) -> ExchangeApiResult<BitunixPrivateAck> {
        self.ensure_exchange(&symbol.exchange)?;
        ensure_bitunix_perpetual_symbol(self, &symbol)?;
        let body = json!({
            "symbol": normalize_bitunix_symbol(&symbol.exchange_symbol.symbol)?,
            "marginCoin": non_empty("marginCoin", margin_coin)?,
            "marginMode": bitunix_margin_mode_text(margin_mode),
        });
        let value = self
            .send_signed_post(
                "bitunix.change_margin_mode",
                MarketType::Perpetual,
                "/api/v1/futures/account/change_margin_mode",
                &HashMap::new(),
                body,
            )
            .await?;
        Ok(bitunix_private_ack("bitunix.change_margin_mode", value))
    }

    pub async fn change_position_mode(
        &self,
        position_mode: BitunixPositionMode,
    ) -> ExchangeApiResult<BitunixPrivateAck> {
        let body = json!({
            "positionMode": bitunix_position_mode_text(position_mode),
        });
        let value = self
            .send_signed_post(
                "bitunix.change_position_mode",
                MarketType::Perpetual,
                "/api/v1/futures/account/change_position_mode",
                &HashMap::new(),
                body,
            )
            .await?;
        Ok(bitunix_private_ack("bitunix.change_position_mode", value))
    }

    pub async fn adjust_position_margin(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        margin_coin: &str,
        amount: &str,
        position_side: Option<PositionSide>,
        position_id: Option<&str>,
    ) -> ExchangeApiResult<BitunixPrivateAck> {
        self.ensure_exchange(&symbol.exchange)?;
        ensure_bitunix_perpetual_symbol(self, &symbol)?;
        if position_side.is_none() && position_id.is_none() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitunix adjust_position_margin requires position_side or position_id"
                    .to_string(),
            });
        }
        let mut body = serde_json::Map::new();
        body.insert(
            "symbol".to_string(),
            json!(normalize_bitunix_symbol(&symbol.exchange_symbol.symbol)?),
        );
        body.insert(
            "marginCoin".to_string(),
            json!(non_empty("marginCoin", margin_coin)?),
        );
        body.insert("amount".to_string(), json!(non_empty("amount", amount)?));
        if let Some(side) = position_side {
            body.insert("side".to_string(), json!(position_side_text(side)));
        }
        if let Some(position_id) = position_id {
            body.insert(
                "positionId".to_string(),
                json!(non_empty("positionId", position_id)?),
            );
        }
        let value = self
            .send_signed_post(
                "bitunix.adjust_position_margin",
                MarketType::Perpetual,
                "/api/v1/futures/account/adjust_position_margin",
                &HashMap::new(),
                Value::Object(body),
            )
            .await?;
        Ok(bitunix_private_ack("bitunix.adjust_position_margin", value))
    }

    pub async fn place_tpsl_order(
        &self,
        request: BitunixTpslOrderRequest,
    ) -> ExchangeApiResult<BitunixPrivateAck> {
        self.ensure_exchange(&request.symbol.exchange)?;
        ensure_bitunix_perpetual_symbol(self, &request.symbol)?;
        let body = bitunix_tpsl_order_body(&request)?;
        let value = self
            .send_signed_post(
                "bitunix.place_tpsl_order",
                MarketType::Perpetual,
                "/api/v1/futures/tpsl/place_order",
                &HashMap::new(),
                body,
            )
            .await?;
        Ok(bitunix_private_ack("bitunix.place_tpsl_order", value))
    }

    pub async fn cancel_tpsl_order(
        &self,
        symbol: rustcta_exchange_api::SymbolScope,
        order_id: &str,
    ) -> ExchangeApiResult<BitunixPrivateAck> {
        self.ensure_exchange(&symbol.exchange)?;
        ensure_bitunix_perpetual_symbol(self, &symbol)?;
        let body = json!({
            "symbol": normalize_bitunix_symbol(&symbol.exchange_symbol.symbol)?,
            "orderId": non_empty("orderId", order_id)?,
        });
        let value = self
            .send_signed_post(
                "bitunix.cancel_tpsl_order",
                MarketType::Perpetual,
                "/api/v1/futures/tpsl/cancel_order",
                &HashMap::new(),
                body,
            )
            .await?;
        Ok(bitunix_private_ack("bitunix.cancel_tpsl_order", value))
    }

    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bitunix.get_balances")?;
        let value = match market_type {
            MarketType::Spot => {
                self.send_signed_get(
                    "bitunix.get_balances",
                    market_type,
                    "/api/spot/v1/user/account",
                    &HashMap::new(),
                )
                .await?
            }
            MarketType::Perpetual => {
                let mut params = HashMap::new();
                params.insert("marginCoin".to_string(), "USDT".to_string());
                self.send_signed_get(
                    "bitunix.get_balances",
                    market_type,
                    "/api/v1/futures/account",
                    &params,
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
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
                operation: "bitunix.positions_non_perpetual",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bitunix.get_positions")?;
        let mut params = HashMap::new();
        if let Some(symbol) = request.symbols.first() {
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "bitunix adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
            params.insert(
                "symbol".to_string(),
                normalize_bitunix_symbol(&symbol.symbol)?,
            );
        }
        let value = self
            .send_signed_get(
                "bitunix.get_positions",
                MarketType::Perpetual,
                "/api/v1/futures/position/get_pending_positions",
                &params,
            )
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
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
        }
        Ok(FeesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(self.exchange_id.clone(), request.context.request_id),
            fees: parse_fee_snapshots(
                &request.symbols,
                request
                    .symbols
                    .first()
                    .map_or(MarketType::Spot, |symbol| symbol.market_type),
            ),
        })
    }

    pub(super) async fn place_order_impl(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market(request.symbol.market_type)?;
        let body = bitunix_place_order_body(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/api/spot/v1/order/place_order",
            MarketType::Perpetual => "/api/v1/futures/trade/place_order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post(
                "bitunix.place_order",
                request.symbol.market_type,
                endpoint,
                &HashMap::new(),
                body,
            )
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            data_or_root(&value),
        )
        .unwrap_or_else(|_| {
            order_state_from_place_ack(&self.exchange_id, &request, data_or_root(&value))
        });
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
        let body = bitunix_cancel_body(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/api/spot/v1/order/cancel",
            MarketType::Perpetual => "/api/v1/futures/trade/cancel_orders",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post(
                "bitunix.cancel_order",
                request.symbol.market_type,
                endpoint,
                &HashMap::new(),
                body,
            )
            .await?;
        let order = parse_order(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            &value,
        )
        .ok()
        .flatten()
        .unwrap_or_else(|| {
            order_state_from_cancel_ack(&self.exchange_id, &request, data_or_root(&value))
        });
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
        if request.symbol.market_type == MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitunix.spot_amend_order",
            });
        }
        let body = bitunix_amend_body(&request)?;
        let value = self
            .send_signed_post(
                "bitunix.amend_order",
                MarketType::Perpetual,
                "/api/v1/futures/trade/modify_order",
                &HashMap::new(),
                body,
            )
            .await?;
        let order = parse_order_state(
            &self.exchange_id,
            Some(&request.symbol),
            request.symbol.market_type,
            data_or_root(&value),
        )
        .unwrap_or_else(|_| {
            order_state_from_amend_ack(&self.exchange_id, &request, data_or_root(&value))
        });
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
        let market_type = request.orders[0].symbol.market_type;
        self.ensure_supported_market(market_type)?;
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market(order.symbol.market_type)?;
            if order.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bitunix batch_place_orders requires one market type".to_string(),
                });
            }
        }
        let order_list = request
            .orders
            .iter()
            .map(bitunix_place_order_body)
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let endpoint = match market_type {
            MarketType::Spot => "/api/spot/v1/order/place_order/batch",
            MarketType::Perpetual => "/api/v1/futures/trade/batch_order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post(
                "bitunix.batch_place_orders",
                market_type,
                endpoint,
                &HashMap::new(),
                json!({ "orderList": order_list }),
            )
            .await?;
        let orders =
            parse_orders(&self.exchange_id, None, market_type, &value).unwrap_or_else(|_| {
                request
                    .orders
                    .iter()
                    .map(|order| {
                        order_state_from_place_ack(&self.exchange_id, order, data_or_root(&value))
                    })
                    .collect()
            });
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
            return Ok(BatchCancelOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                orders: Vec::new(),
                cancelled_count: 0,
                report: None,
            });
        }
        let market_type = request.cancels[0].symbol.market_type;
        self.ensure_supported_market(market_type)?;
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market(cancel.symbol.market_type)?;
            if cancel.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bitunix batch_cancel_orders requires one market type".to_string(),
                });
            }
        }
        let body = match market_type {
            MarketType::Spot => bitunix_spot_batch_cancel_body(&request.cancels)?,
            MarketType::Perpetual => bitunix_perp_batch_cancel_body(&request.cancels)?,
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let endpoint = match market_type {
            MarketType::Spot => "/api/spot/v1/order/cancel",
            MarketType::Perpetual => "/api/v1/futures/trade/cancel_orders",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post(
                "bitunix.batch_cancel_orders",
                market_type,
                endpoint,
                &HashMap::new(),
                body,
            )
            .await?;
        let orders =
            parse_orders(&self.exchange_id, None, market_type, &value).unwrap_or_else(|_| {
                request
                    .cancels
                    .iter()
                    .map(|cancel| {
                        order_state_from_cancel_ack(&self.exchange_id, cancel, data_or_root(&value))
                    })
                    .collect()
            });
        let cancelled_count = if orders.is_empty() {
            request.cancels.len() as u32
        } else {
            orders.len() as u32
        };
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
                message: "bitunix cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        if symbol.market_type == MarketType::Spot {
            let open_body = json!({
                "symbol": normalize_bitunix_symbol(&symbol.exchange_symbol.symbol)?,
                "page": 1,
                "pageSize": 100
            });
            let open_value = self
                .send_signed_post(
                    "bitunix.cancel_all_orders.open_orders",
                    MarketType::Spot,
                    "/api/spot/v1/order/pending/list",
                    &HashMap::new(),
                    open_body,
                )
                .await?;
            let open_orders = parse_orders(
                &self.exchange_id,
                Some(symbol),
                MarketType::Spot,
                &open_value,
            )?;
            if open_orders.is_empty() {
                return Ok(CancelAllOrdersResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: response_metadata(request.exchange, request.context.request_id),
                    cancelled_count: 0,
                    orders: Vec::new(),
                });
            }
            let order_ids = open_orders
                .iter()
                .map(|order| {
                    order.exchange_order_id.clone().ok_or_else(|| {
                        ExchangeApiError::InvalidRequest {
                            message: "bitunix spot cancel_all open order missing exchange_order_id"
                                .to_string(),
                        }
                    })
                })
                .collect::<ExchangeApiResult<Vec<_>>>()?;
            let cancel_body = json!({
                "symbol": normalize_bitunix_symbol(&symbol.exchange_symbol.symbol)?,
                "orderIdList": order_ids
            });
            let cancel_value = self
                .send_signed_post(
                    "bitunix.cancel_all_orders",
                    MarketType::Spot,
                    "/api/spot/v1/order/cancel",
                    &HashMap::new(),
                    cancel_body,
                )
                .await?;
            let mut orders = parse_orders(
                &self.exchange_id,
                Some(symbol),
                MarketType::Spot,
                &cancel_value,
            )
            .unwrap_or(open_orders);
            for order in &mut orders {
                order.status = OrderStatus::Cancelled;
                order.updated_at = Utc::now();
            }
            let cancelled_count = orders.len() as u32;
            return Ok(CancelAllOrdersResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: response_metadata(request.exchange, request.context.request_id),
                cancelled_count,
                orders,
            });
        }
        let body = json!({
            "symbol": normalize_bitunix_symbol(&symbol.exchange_symbol.symbol)?
        });
        let value = self
            .send_signed_post(
                "bitunix.cancel_all_orders",
                MarketType::Perpetual,
                "/api/v1/futures/trade/cancel_all_orders",
                &HashMap::new(),
                body,
            )
            .await?;
        let orders = parse_orders(&self.exchange_id, Some(symbol), symbol.market_type, &value)
            .unwrap_or_default();
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
        self.ensure_supported_market(request.symbol.market_type)?;
        let value = match request.symbol.market_type {
            MarketType::Spot => {
                let mut params = HashMap::new();
                params.insert(
                    "symbol".to_string(),
                    normalize_bitunix_symbol(&request.symbol.exchange_symbol.symbol)?,
                );
                if let Some(order_id) = request.exchange_order_id.as_deref() {
                    params.insert("orderId".to_string(), order_id.to_string());
                } else if let Some(client_id) = request.client_order_id.as_deref() {
                    params.insert("clientId".to_string(), client_id.to_string());
                } else {
                    return Err(ExchangeApiError::InvalidRequest {
                        message: "bitunix query_order requires order id".to_string(),
                    });
                }
                self.send_signed_get(
                    "bitunix.query_order",
                    MarketType::Spot,
                    "/api/spot/v1/order/detail",
                    &params,
                )
                .await?
            }
            MarketType::Perpetual => {
                let params = bitunix_query_order_params(&request)?;
                self.send_signed_get(
                    "bitunix.query_order",
                    MarketType::Perpetual,
                    "/api/v1/futures/trade/get_order_detail",
                    &params,
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitunix get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let value = match symbol.market_type {
            MarketType::Spot => {
                let body = json!({
                    "symbol": normalize_bitunix_symbol(&symbol.exchange_symbol.symbol)?,
                    "page": 1,
                    "pageSize": 100
                });
                self.send_signed_post(
                    "bitunix.get_open_orders",
                    MarketType::Spot,
                    "/api/spot/v1/order/pending/list",
                    &HashMap::new(),
                    body,
                )
                .await?
            }
            MarketType::Perpetual => {
                let mut params = HashMap::new();
                params.insert(
                    "symbol".to_string(),
                    normalize_bitunix_symbol(&symbol.exchange_symbol.symbol)?,
                );
                params.insert("limit".to_string(), "100".to_string());
                self.send_signed_get(
                    "bitunix.get_open_orders",
                    MarketType::Perpetual,
                    "/api/v1/futures/trade/get_pending_orders",
                    &params,
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        Ok(OpenOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            orders: parse_orders(&self.exchange_id, Some(symbol), symbol.market_type, &value)?,
        })
    }

    pub(super) async fn get_recent_fills_impl(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "bitunix get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bitunix.get_recent_fills")?;
        let value = match symbol.market_type {
            MarketType::Spot => {
                let order_id = request.exchange_order_id.as_deref().ok_or_else(|| {
                    ExchangeApiError::InvalidRequest {
                        message: "bitunix spot fills require exchange_order_id".to_string(),
                    }
                })?;
                let body = json!({
                    "symbol": normalize_bitunix_symbol(&symbol.exchange_symbol.symbol)?,
                    "orderId": order_id
                });
                self.send_signed_post(
                    "bitunix.get_recent_fills",
                    MarketType::Spot,
                    "/api/spot/v1/order/deal/list",
                    &HashMap::new(),
                    body,
                )
                .await?
            }
            MarketType::Perpetual => {
                let mut params = HashMap::new();
                params.insert(
                    "symbol".to_string(),
                    normalize_bitunix_symbol(&symbol.exchange_symbol.symbol)?,
                );
                if let Some(order_id) = request.exchange_order_id.as_deref() {
                    params.insert("orderId".to_string(), order_id.to_string());
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
                params.insert(
                    "limit".to_string(),
                    request.limit.unwrap_or(100).min(100).to_string(),
                );
                self.send_signed_get(
                    "bitunix.get_recent_fills",
                    MarketType::Perpetual,
                    "/api/v1/futures/trade/get_history_trades",
                    &params,
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills: parse_fills(
                &self.exchange_id,
                tenant_id,
                account_id,
                Some(symbol),
                symbol.market_type,
                &value,
            )?,
        })
    }
}

fn bitunix_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    match request.symbol.market_type {
        MarketType::Spot => bitunix_spot_place_order_body(request),
        MarketType::Perpetual => bitunix_perp_place_order_body(request),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "bitunix.place_order.market_type",
        }),
    }
}

fn bitunix_spot_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitunix spot order does not support reduce_only".to_string(),
        });
    }
    if request.post_only || matches!(request.order_type, OrderType::PostOnly) {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitunix.spot_post_only_order",
        });
    }
    if matches!(request.order_type, OrderType::IOC | OrderType::FOK)
        || matches!(
            request.time_in_force,
            Some(TimeInForce::IOC | TimeInForce::FOK | TimeInForce::GTX)
        )
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitunix.spot_time_in_force",
        });
    }
    if request.client_order_id.is_some() {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitunix.spot_client_order_id",
        });
    }

    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        json!(normalize_bitunix_symbol(
            &request.symbol.exchange_symbol.symbol
        )?),
    );
    body.insert("side".to_string(), json!(spot_side_code(request.side)));
    body.insert(
        "type".to_string(),
        json!(spot_order_type_code(request.order_type)),
    );
    match request.order_type {
        OrderType::Market => {
            body.insert(
                "price".to_string(),
                json!(request.price.as_deref().unwrap_or("0")),
            );
            if request.side == OrderSide::Buy {
                if let Some(quote_quantity) = request.quote_quantity.as_deref() {
                    body.insert("amount".to_string(), json!(quote_quantity));
                    body.insert("volume".to_string(), json!("0"));
                } else {
                    body.insert("volume".to_string(), json!(request.quantity));
                }
            } else {
                body.insert("volume".to_string(), json!(request.quantity));
            }
        }
        OrderType::Limit => {
            body.insert("volume".to_string(), json!(request.quantity));
            body.insert(
                "price".to_string(),
                json!(request
                    .price
                    .clone()
                    .ok_or_else(|| ExchangeApiError::InvalidRequest {
                        message: "bitunix spot limit order requires price".to_string(),
                    })?),
            );
        }
        _ => {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitunix.spot_order_type",
            });
        }
    }
    Ok(Value::Object(body))
}

fn bitunix_perp_place_order_body(request: &PlaceOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        json!(normalize_bitunix_symbol(
            &request.symbol.exchange_symbol.symbol
        )?),
    );
    body.insert("side".to_string(), json!(side_text(request.side)));
    body.insert(
        "orderType".to_string(),
        json!(order_type_text(request.order_type)),
    );
    body.insert("qty".to_string(), json!(request.quantity));
    if request.order_type.requires_limit_price() {
        body.insert(
            "price".to_string(),
            json!(request
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitunix limit-like order requires price".to_string(),
                })?),
        );
    } else if let Some(price) = &request.price {
        body.insert("price".to_string(), json!(price));
    }
    if let Some(tif) = request
        .time_in_force
        .or_else(|| request.post_only.then_some(TimeInForce::GTX))
    {
        body.insert("effect".to_string(), json!(tif_text(tif)));
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        body.insert("clientId".to_string(), json!(client_order_id));
    }
    body.insert(
        "tradeSide".to_string(),
        json!(if request.reduce_only { "CLOSE" } else { "OPEN" }),
    );
    body.insert("reduceOnly".to_string(), json!(request.reduce_only));
    Ok(Value::Object(body))
}

fn bitunix_cancel_body(request: &CancelOrderRequest) -> ExchangeApiResult<Value> {
    let mut order = serde_json::Map::new();
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        order.insert("orderId".to_string(), json!(order_id));
    }
    if let Some(client_id) = request.client_order_id.as_deref() {
        order.insert("clientId".to_string(), json!(client_id));
    }
    if order.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitunix cancel_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    let symbol = normalize_bitunix_symbol(&request.symbol.exchange_symbol.symbol)?;
    if request.symbol.market_type == MarketType::Spot {
        order.insert("symbol".to_string(), json!(symbol));
        Ok(Value::Object(order))
    } else {
        Ok(json!({
            "symbol": symbol,
            "orderList": [Value::Object(order)]
        }))
    }
}

fn bitunix_amend_body(request: &AmendOrderRequest) -> ExchangeApiResult<Value> {
    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        json!(normalize_bitunix_symbol(
            &request.symbol.exchange_symbol.symbol
        )?),
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        body.insert("orderId".to_string(), json!(order_id));
    }
    if let Some(client_id) = request.client_order_id.as_deref() {
        body.insert("clientId".to_string(), json!(client_id));
    }
    if body.get("orderId").is_none() && body.get("clientId").is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitunix amend_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    body.insert("qty".to_string(), json!(request.new_quantity));
    if let Some(new_client_id) = request.new_client_order_id.as_deref() {
        body.insert("newClientId".to_string(), json!(new_client_id));
    }
    Ok(Value::Object(body))
}

fn bitunix_spot_batch_cancel_body(cancels: &[CancelOrderRequest]) -> ExchangeApiResult<Value> {
    let first_symbol = normalize_bitunix_symbol(&cancels[0].symbol.exchange_symbol.symbol)?;
    let mut order_ids = Vec::with_capacity(cancels.len());
    for cancel in cancels {
        let symbol = normalize_bitunix_symbol(&cancel.symbol.exchange_symbol.symbol)?;
        if symbol != first_symbol {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitunix spot batch_cancel_orders requires one symbol".to_string(),
            });
        }
        let order_id = cancel.exchange_order_id.as_deref().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "bitunix spot batch_cancel_orders requires exchange_order_id".to_string(),
            }
        })?;
        order_ids.push(json!(order_id));
    }
    Ok(json!({
        "symbol": first_symbol,
        "orderIdList": order_ids
    }))
}

fn bitunix_perp_batch_cancel_body(cancels: &[CancelOrderRequest]) -> ExchangeApiResult<Value> {
    let first_symbol = normalize_bitunix_symbol(&cancels[0].symbol.exchange_symbol.symbol)?;
    let mut order_list = Vec::with_capacity(cancels.len());
    for cancel in cancels {
        let symbol = normalize_bitunix_symbol(&cancel.symbol.exchange_symbol.symbol)?;
        if symbol != first_symbol {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitunix perp batch_cancel_orders requires one symbol".to_string(),
            });
        }
        let mut order = serde_json::Map::new();
        if let Some(order_id) = cancel.exchange_order_id.as_deref() {
            order.insert("orderId".to_string(), json!(order_id));
        }
        if let Some(client_id) = cancel.client_order_id.as_deref() {
            order.insert("clientId".to_string(), json!(client_id));
        }
        if order.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bitunix perp batch_cancel_orders requires order id".to_string(),
            });
        }
        order_list.push(Value::Object(order));
    }
    Ok(json!({
        "symbol": first_symbol,
        "orderList": order_list
    }))
}

fn bitunix_query_order_params(
    request: &QueryOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_id) = request.client_order_id.as_deref() {
        params.insert("clientId".to_string(), client_id.to_string());
    }
    if params.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitunix query_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(params)
}

fn data_or_root(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn order_state_from_place_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &PlaceOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value
            .get("clientId")
            .or_else(|| value.get("clientOrderId"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value
            .get("orderId")
            .or_else(|| value.get("id"))
            .map(|value| value.to_string().trim_matches('"').to_string()),
        side: request.side,
        position_side: request.position_side,
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only,
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
}

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value
            .get("clientId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value
            .get("orderId")
            .map(|value| value.to_string().trim_matches('"').to_string())
            .or_else(|| request.exchange_order_id.clone()),
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
        updated_at: Utc::now(),
    }
}

fn order_state_from_amend_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &AmendOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: value
            .get("clientId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| request.new_client_order_id.clone())
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value
            .get("orderId")
            .map(|value| value.to_string().trim_matches('"').to_string())
            .or_else(|| request.exchange_order_id.clone()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: None,
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

fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn position_side_text(side: PositionSide) -> &'static str {
    match side {
        PositionSide::Long => "LONG",
        PositionSide::Short => "SHORT",
        _ => "BOTH",
    }
}

fn bitunix_margin_mode_text(mode: BitunixMarginMode) -> &'static str {
    match mode {
        BitunixMarginMode::Isolation => "ISOLATION",
        BitunixMarginMode::Cross => "CROSS",
    }
}

fn bitunix_position_mode_text(mode: BitunixPositionMode) -> &'static str {
    match mode {
        BitunixPositionMode::OneWay => "ONE_WAY",
        BitunixPositionMode::Hedge => "HEDGE",
    }
}

fn bitunix_stop_type_text(stop_type: BitunixStopType) -> &'static str {
    match stop_type {
        BitunixStopType::LastPrice => "LAST_PRICE",
        BitunixStopType::MarkPrice => "MARK_PRICE",
    }
}

fn bitunix_trigger_order_type_text(order_type: BitunixTriggerOrderType) -> &'static str {
    match order_type {
        BitunixTriggerOrderType::Market => "MARKET",
        BitunixTriggerOrderType::Limit => "LIMIT",
    }
}

fn ensure_bitunix_perpetual_symbol(
    adapter: &BitunixGatewayAdapter,
    symbol: &rustcta_exchange_api::SymbolScope,
) -> ExchangeApiResult<()> {
    adapter.ensure_supported_market(symbol.market_type)?;
    if symbol.market_type != MarketType::Perpetual {
        return Err(ExchangeApiError::Unsupported {
            operation: "bitunix.perpetual_only_operation",
        });
    }
    Ok(())
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("bitunix {field} must not be empty"),
        });
    }
    Ok(trimmed.to_string())
}

fn bitunix_private_ack(operation: &'static str, value: Value) -> BitunixPrivateAck {
    BitunixPrivateAck {
        operation,
        data: data_or_root(&value).clone(),
    }
}

fn bitunix_tpsl_order_body(request: &BitunixTpslOrderRequest) -> ExchangeApiResult<Value> {
    if request.take_profit_price.is_none() && request.stop_loss_price.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitunix tpsl requires take_profit_price or stop_loss_price".to_string(),
        });
    }
    if request.take_profit_quantity.is_none() && request.stop_loss_quantity.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bitunix tpsl requires take_profit_quantity or stop_loss_quantity".to_string(),
        });
    }
    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        json!(normalize_bitunix_symbol(
            &request.symbol.exchange_symbol.symbol
        )?),
    );
    body.insert(
        "positionId".to_string(),
        json!(non_empty("positionId", &request.position_id)?),
    );
    insert_optional_string(&mut body, "tpPrice", request.take_profit_price.as_deref())?;
    insert_optional_string(&mut body, "slPrice", request.stop_loss_price.as_deref())?;
    insert_optional_string(
        &mut body,
        "tpOrderPrice",
        request.take_profit_order_price.as_deref(),
    )?;
    insert_optional_string(
        &mut body,
        "slOrderPrice",
        request.stop_loss_order_price.as_deref(),
    )?;
    insert_optional_string(&mut body, "tpQty", request.take_profit_quantity.as_deref())?;
    insert_optional_string(&mut body, "slQty", request.stop_loss_quantity.as_deref())?;
    if let Some(stop_type) = request.take_profit_stop_type {
        body.insert(
            "tpStopType".to_string(),
            json!(bitunix_stop_type_text(stop_type)),
        );
    }
    if let Some(stop_type) = request.stop_loss_stop_type {
        body.insert(
            "slStopType".to_string(),
            json!(bitunix_stop_type_text(stop_type)),
        );
    }
    if let Some(order_type) = request.take_profit_order_type {
        body.insert(
            "tpOrderType".to_string(),
            json!(bitunix_trigger_order_type_text(order_type)),
        );
    }
    if let Some(order_type) = request.stop_loss_order_type {
        body.insert(
            "slOrderType".to_string(),
            json!(bitunix_trigger_order_type_text(order_type)),
        );
    }
    Ok(Value::Object(body))
}

fn insert_optional_string(
    body: &mut serde_json::Map<String, Value>,
    key: &str,
    value: Option<&str>,
) -> ExchangeApiResult<()> {
    if let Some(value) = value {
        body.insert(key.to_string(), json!(non_empty(key, value)?));
    }
    Ok(())
}

fn spot_side_code(side: OrderSide) -> i64 {
    match side {
        OrderSide::Sell => 1,
        OrderSide::Buy => 2,
    }
}

fn spot_order_type_code(order_type: OrderType) -> i64 {
    match order_type {
        OrderType::Market => 2,
        _ => 1,
    }
}

fn order_type_text(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "MARKET",
        OrderType::PostOnly => "LIMIT",
        OrderType::IOC | OrderType::FOK | OrderType::Limit => "LIMIT",
        _ => "LIMIT",
    }
}

fn tif_text(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "POST_ONLY",
        _ => "GTC",
    }
}
