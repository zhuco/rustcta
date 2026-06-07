#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;

use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse,
    PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
    RecentFillsResponse, SymbolScope, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_poloniex_symbol;
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_fills, parse_order, parse_order_state, parse_orders,
    parse_positions,
};
use super::PoloniexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoloniexPositionMode {
    OneWay,
    Hedge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoloniexMarginMode {
    Cross,
    Isolated,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PoloniexPrivateAck {
    pub schema_version: u16,
    pub exchange: rustcta_types::ExchangeId,
    pub operation: String,
    pub data: Option<Value>,
    pub raw: Value,
}

impl PoloniexGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "poloniex.get_balances")?;
        let endpoint = match market_type {
            MarketType::Spot => "/accounts/balances",
            MarketType::Perpetual => "/v3/account/balance",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let mut params = HashMap::new();
        if market_type == MarketType::Spot {
            params.insert("accountType".to_string(), "SPOT".to_string());
        }
        let value = self
            .send_signed_get("poloniex.get_balances", endpoint, &params)
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
                operation: "poloniex.positions_non_perpetual",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "poloniex.get_positions")?;
        let mut params = HashMap::new();
        if let Some(symbol) = request.symbols.first() {
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "poloniex adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
            params.insert(
                "symbol".to_string(),
                normalize_poloniex_symbol(&symbol.symbol, MarketType::Perpetual)?,
            );
        }
        let value = self
            .send_signed_get(
                "poloniex.get_positions",
                "/v3/trade/position/opens",
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
        if request.symbols.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "poloniex get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            let endpoint = match symbol.market_type {
                MarketType::Spot => "/feeinfo",
                MarketType::Perpetual => {
                    return Err(ExchangeApiError::Unsupported {
                        operation: "poloniex.futures_fees",
                    })
                }
                _ => unreachable!("checked by ensure_supported_market"),
            };
            let mut params = HashMap::new();
            if symbol.market_type == MarketType::Spot {
                params.insert(
                    "symbol".to_string(),
                    normalize_poloniex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
                );
            }
            let value = self
                .send_signed_get("poloniex.get_fees", endpoint, &params)
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
        let params = poloniex_place_order_params(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/orders",
            MarketType::Perpetual => "/v3/trade/order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post("poloniex.place_order", endpoint, &params)
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
        let params = poloniex_cancel_order_params(&request)?;
        let (method, endpoint) = match request.symbol.market_type {
            MarketType::Spot => {
                let id = spot_order_path_id(&request)?;
                ("DELETE", format!("/orders/{id}"))
            }
            MarketType::Perpetual => ("DELETE", "/v3/trade/order".to_string()),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = if method == "POST" {
            self.send_signed_post("poloniex.cancel_order", &endpoint, &params)
                .await?
        } else if request.symbol.market_type == MarketType::Spot {
            self.send_signed_delete("poloniex.cancel_order", &endpoint, &HashMap::new())
                .await?
        } else {
            self.send_signed_delete("poloniex.cancel_order", &endpoint, &params)
                .await?
        };
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
                request.context.request_id.clone(),
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
        if request.symbol.market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "poloniex.futures_amend_order",
            });
        }
        let id = spot_amend_path_id(&request)?;
        let mut params = HashMap::new();
        params.insert("quantity".to_string(), request.new_quantity.clone());
        if let Some(client_order_id) = request.new_client_order_id.as_deref() {
            params.insert("clientOrderId".to_string(), client_order_id.to_string());
        }
        let value = self
            .send_signed_put("poloniex.amend_order", &format!("/orders/{id}"), &params)
            .await?;
        Ok(AmendOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                request.symbol.exchange.clone(),
                request.context.request_id.clone(),
            ),
            order: order_state_from_amend_ack(&self.exchange_id, &request, data_or_root(&value)),
        })
    }

    pub(super) async fn batch_place_orders_impl(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = batch_order_market_type(&request.orders)?;
        let max_orders = if market_type == MarketType::Spot {
            20
        } else {
            10
        };
        if request.orders.len() > max_orders {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "poloniex batch_place_orders supports at most {max_orders} orders"
                ),
            });
        }

        let mut body = Vec::with_capacity(request.orders.len());
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market(order.symbol.market_type)?;
            if order.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "poloniex batch_place_orders cannot mix market types".to_string(),
                });
            }
            body.push(poloniex_place_order_params(order)?);
        }
        let endpoint = match market_type {
            MarketType::Spot => "/orders/batch",
            MarketType::Perpetual => "/v3/trade/orders",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let body_value =
            serde_json::to_value(&body).map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?;
        let value = self
            .send_signed_post_json("poloniex.batch_place_orders", endpoint, &body_value)
            .await?;
        let items = response_items(data_or_root(&value));
        let mut orders = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            if response_item_is_error(item) {
                continue;
            }
            let request_order = request.orders.get(index).unwrap_or(&request.orders[0]);
            orders.push(order_state_from_place_ack(
                &self.exchange_id,
                request_order,
                item,
            ));
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
        let market_type = batch_cancel_market_type(&request.cancels)?;
        let max_cancels = if market_type == MarketType::Spot {
            20
        } else {
            10
        };
        if request.cancels.len() > max_cancels {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "poloniex batch_cancel_orders supports at most {max_cancels} cancels"
                ),
            });
        }
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market(cancel.symbol.market_type)?;
            if cancel.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "poloniex batch_cancel_orders cannot mix market types".to_string(),
                });
            }
        }

        let body = if market_type == MarketType::Spot {
            spot_batch_cancel_body(&request.cancels)?
        } else {
            futures_batch_cancel_body(&request.cancels)?
        };
        let endpoint = if market_type == MarketType::Spot {
            "/orders/cancelByIds"
        } else {
            "/v3/trade/batchOrders"
        };
        let value = self
            .send_signed_delete_json("poloniex.batch_cancel_orders", endpoint, &body)
            .await?;
        let items = response_items(data_or_root(&value));
        let mut orders = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            if response_item_is_error(item) {
                continue;
            }
            let cancel = request.cancels.get(index).unwrap_or(&request.cancels[0]);
            orders.push(order_state_from_cancel_ack(&self.exchange_id, cancel, item));
        }
        Ok(BatchCancelOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count: orders.len() as u32,
            orders,
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
                message: "poloniex cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        if request
            .market_type
            .is_some_and(|market_type| market_type != symbol.market_type)
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "poloniex cancel_all_orders market_type does not match symbol".to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_poloniex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        let value = match symbol.market_type {
            MarketType::Spot => {
                let body = json!({
                    "symbols": [normalize_poloniex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?],
                    "accountTypes": ["SPOT"],
                });
                self.send_signed_delete_json("poloniex.cancel_all_orders", "/orders", &body)
                    .await?
            }
            MarketType::Perpetual => {
                self.send_signed_delete(
                    "poloniex.cancel_all_orders",
                    "/v3/trade/allOrders",
                    &params,
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let orders = parse_orders(&self.exchange_id, Some(symbol), symbol.market_type, &value)
            .unwrap_or_default();
        let cancelled_count = if orders.is_empty() {
            data_or_root(&value)
                .as_array()
                .map(|items| items.len() as u32)
                .unwrap_or(0)
        } else {
            orders.len() as u32
        };
        Ok(CancelAllOrdersResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            cancelled_count,
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
        let params = poloniex_query_order_params(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => {
                let id = spot_order_path_id_from_query(&request)?;
                format!("/orders/{id}")
            }
            MarketType::Perpetual => "/v3/trade/order/details".to_string(),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let empty_params = HashMap::new();
        let query_params = if request.symbol.market_type == MarketType::Spot {
            &empty_params
        } else {
            &params
        };
        let value = self
            .send_signed_get("poloniex.query_order", &endpoint, query_params)
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
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_poloniex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
            );
        }
        let endpoint = match market_type {
            MarketType::Spot => "/orders",
            MarketType::Perpetual => "/v3/trade/order/opens",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("poloniex.get_open_orders", endpoint, &params)
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "poloniex get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "poloniex.get_recent_fills")?;
        let mut params = HashMap::new();
        params.insert(
            if symbol.market_type == MarketType::Spot {
                "symbols".to_string()
            } else {
                "symbol".to_string()
            },
            normalize_poloniex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert(
                if symbol.market_type == MarketType::Perpetual {
                    "ordId".to_string()
                } else {
                    "orderId".to_string()
                },
                order_id.to_string(),
            );
        }
        if let Some(start_time) = request.start_time {
            let key = if symbol.market_type == MarketType::Perpetual {
                "sTime"
            } else {
                "startTime"
            };
            params.insert(key.to_string(), start_time.timestamp_millis().to_string());
        }
        if let Some(end_time) = request.end_time {
            let key = if symbol.market_type == MarketType::Perpetual {
                "eTime"
            } else {
                "endTime"
            };
            params.insert(key.to_string(), end_time.timestamp_millis().to_string());
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(1000).to_string());
        }
        let endpoint = match symbol.market_type {
            MarketType::Spot => "/trades",
            MarketType::Perpetual => "/v3/trade/order/trades",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("poloniex.get_recent_fills", endpoint, &params)
            .await?;
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

    pub async fn get_position_mode(&self) -> ExchangeApiResult<PoloniexPrivateAck> {
        let value = self
            .send_signed_get(
                "poloniex.get_position_mode",
                "/v3/position/mode",
                &HashMap::new(),
            )
            .await?;
        Ok(poloniex_private_ack(
            &self.exchange_id,
            "poloniex.get_position_mode",
            value,
        ))
    }

    pub async fn switch_position_mode(
        &self,
        target_mode: PoloniexPositionMode,
    ) -> ExchangeApiResult<PoloniexPrivateAck> {
        let body = json!({
            "positionMode": poloniex_position_mode_text(target_mode),
        });
        let value = self
            .send_signed_post_json("poloniex.switch_position_mode", "/v3/position/mode", &body)
            .await?;
        Ok(poloniex_private_ack(
            &self.exchange_id,
            "poloniex.switch_position_mode",
            value,
        ))
    }

    pub async fn get_leverages(
        &self,
        symbol: SymbolScope,
    ) -> ExchangeApiResult<PoloniexPrivateAck> {
        ensure_poloniex_perpetual_symbol(self, &symbol)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_poloniex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
        );
        let value = self
            .send_signed_get("poloniex.get_leverages", "/v3/position/leverages", &params)
            .await?;
        Ok(poloniex_private_ack(
            &self.exchange_id,
            "poloniex.get_leverages",
            value,
        ))
    }

    pub async fn set_leverage(
        &self,
        symbol: SymbolScope,
        leverage: &str,
        margin_mode: PoloniexMarginMode,
        position_side: Option<PositionSide>,
    ) -> ExchangeApiResult<PoloniexPrivateAck> {
        ensure_poloniex_perpetual_symbol(self, &symbol)?;
        let mut body = json!({
            "symbol": normalize_poloniex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
            "lever": non_empty("leverage", leverage)?,
            "mgnMode": poloniex_margin_mode_text(margin_mode),
        });
        if let Some(position_side) = position_side {
            body["posSide"] = Value::String(position_side_text(position_side).to_string());
        }
        let value = self
            .send_signed_post_json("poloniex.set_leverage", "/v3/position/leverage", &body)
            .await?;
        Ok(poloniex_private_ack(
            &self.exchange_id,
            "poloniex.set_leverage",
            value,
        ))
    }

    pub async fn adjust_isolated_margin(
        &self,
        symbol: SymbolScope,
        amount: &str,
        position_side: PositionSide,
        add_margin: bool,
    ) -> ExchangeApiResult<PoloniexPrivateAck> {
        ensure_poloniex_perpetual_symbol(self, &symbol)?;
        let body = json!({
            "symbol": normalize_poloniex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
            "amt": non_empty("amount", amount)?,
            "posSide": position_side_text(position_side),
            "type": if add_margin { "ADD" } else { "REDUCE" },
        });
        let value = self
            .send_signed_post_json(
                "poloniex.adjust_isolated_margin",
                "/v3/position/margin",
                &body,
            )
            .await?;
        Ok(poloniex_private_ack(
            &self.exchange_id,
            "poloniex.adjust_isolated_margin",
            value,
        ))
    }
}

fn poloniex_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    if request.symbol.market_type == MarketType::Spot && request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "poloniex spot order does not support reduce_only".to_string(),
        });
    }
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_poloniex_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    params.insert("side".to_string(), side_text(request.side).to_string());
    params.insert(
        "type".to_string(),
        order_type_text(request.order_type).to_string(),
    );
    if request.order_type == OrderType::Market
        && request.symbol.market_type == MarketType::Spot
        && request.side == OrderSide::Buy
    {
        let quote_quantity =
            request
                .quote_quantity
                .as_deref()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "poloniex spot market buy requires quote_quantity".to_string(),
                })?;
        params.insert("amount".to_string(), quote_quantity.to_string());
    } else {
        let quantity_key = if request.symbol.market_type == MarketType::Perpetual {
            "sz"
        } else {
            "quantity"
        };
        params.insert(quantity_key.to_string(), request.quantity.clone());
    }
    if request.order_type.requires_limit_price() {
        params.insert(
            if request.symbol.market_type == MarketType::Perpetual {
                "px".to_string()
            } else {
                "price".to_string()
            },
            request
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "poloniex limit-like order requires price".to_string(),
                })?,
        );
    } else if let Some(price) = &request.price {
        let price_key = if request.symbol.market_type == MarketType::Perpetual {
            "px"
        } else {
            "price"
        };
        params.insert(price_key.to_string(), price.clone());
    }
    if let Some(tif) = request
        .time_in_force
        .or_else(|| request.post_only.then_some(TimeInForce::GTX))
    {
        params.insert("timeInForce".to_string(), tif_text(tif).to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        let client_key = if request.symbol.market_type == MarketType::Perpetual {
            "clOrdId"
        } else {
            "clientOrderId"
        };
        params.insert(client_key.to_string(), client_order_id.to_string());
    }
    if request.symbol.market_type == MarketType::Perpetual {
        params.insert("mgnMode".to_string(), "CROSS".to_string());
        params.insert(
            "posSide".to_string(),
            position_side_text(request.position_side.unwrap_or(PositionSide::Net)).to_string(),
        );
        if request.reduce_only {
            params.insert("reduceOnly".to_string(), "true".to_string());
        }
    }
    Ok(params)
}

fn poloniex_cancel_order_params(
    request: &CancelOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_poloniex_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        let key = if request.symbol.market_type == MarketType::Perpetual {
            "ordId"
        } else {
            "orderId"
        };
        params.insert(key.to_string(), order_id.to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        let key = if request.symbol.market_type == MarketType::Perpetual {
            "clOrdId"
        } else {
            "clientOrderId"
        };
        params.insert(key.to_string(), client_order_id.to_string());
    }
    if !params.contains_key("orderId")
        && !params.contains_key("clientOrderId")
        && !params.contains_key("ordId")
        && !params.contains_key("clOrdId")
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "poloniex cancel_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(params)
}

fn poloniex_query_order_params(
    request: &QueryOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_poloniex_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        let key = if request.symbol.market_type == MarketType::Perpetual {
            "ordId"
        } else {
            "orderId"
        };
        params.insert(key.to_string(), order_id.to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        let key = if request.symbol.market_type == MarketType::Perpetual {
            "clOrdId"
        } else {
            "clientOrderId"
        };
        params.insert(key.to_string(), client_order_id.to_string());
    }
    if !params.contains_key("orderId")
        && !params.contains_key("clientOrderId")
        && !params.contains_key("ordId")
        && !params.contains_key("clOrdId")
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "poloniex query_order requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(params)
}

fn data_or_root(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn response_items(value: &Value) -> &[Value] {
    value
        .as_array()
        .map(Vec::as_slice)
        .or_else(|| {
            value
                .get("data")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
        })
        .map(|items| items as &[Value])
        .unwrap_or_else(|| std::slice::from_ref(value))
}

fn response_item_is_error(item: &Value) -> bool {
    item.get("code")
        .and_then(|code| code.as_i64().or_else(|| code.as_str()?.parse().ok()))
        .is_some_and(|code| code != 0 && code != 200)
}

fn spot_order_path_id(request: &CancelOrderRequest) -> ExchangeApiResult<String> {
    request
        .exchange_order_id
        .clone()
        .or_else(|| {
            request
                .client_order_id
                .as_ref()
                .map(|id| format!("cid:{id}"))
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "poloniex spot cancel_order requires exchange_order_id or client_order_id"
                .to_string(),
        })
}

fn spot_order_path_id_from_query(request: &QueryOrderRequest) -> ExchangeApiResult<String> {
    request
        .exchange_order_id
        .clone()
        .or_else(|| {
            request
                .client_order_id
                .as_ref()
                .map(|id| format!("cid:{id}"))
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "poloniex spot query_order requires exchange_order_id or client_order_id"
                .to_string(),
        })
}

fn spot_amend_path_id(request: &AmendOrderRequest) -> ExchangeApiResult<String> {
    request
        .exchange_order_id
        .clone()
        .or_else(|| {
            request
                .client_order_id
                .as_ref()
                .map(|id| format!("cid:{id}"))
        })
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "poloniex spot amend_order requires exchange_order_id or client_order_id"
                .to_string(),
        })
}

fn batch_order_market_type(orders: &[PlaceOrderRequest]) -> ExchangeApiResult<MarketType> {
    orders
        .first()
        .map(|order| order.symbol.market_type)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "poloniex batch_place_orders requires at least one order".to_string(),
        })
}

fn batch_cancel_market_type(cancels: &[CancelOrderRequest]) -> ExchangeApiResult<MarketType> {
    cancels
        .first()
        .map(|cancel| cancel.symbol.market_type)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "poloniex batch_cancel_orders requires at least one cancel".to_string(),
        })
}

fn spot_batch_cancel_body(cancels: &[CancelOrderRequest]) -> ExchangeApiResult<Value> {
    let mut order_ids = Vec::new();
    let mut client_order_ids = Vec::new();
    for cancel in cancels {
        if let Some(order_id) = cancel.exchange_order_id.as_deref() {
            order_ids.push(order_id.to_string());
        } else if let Some(client_order_id) = cancel.client_order_id.as_deref() {
            client_order_ids.push(client_order_id.to_string());
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "poloniex batch_cancel_orders item requires exchange_order_id or client_order_id".to_string(),
            });
        }
    }
    Ok(json!({
        "orderIds": order_ids,
        "clientOrderIds": client_order_ids,
    }))
}

fn futures_batch_cancel_body(cancels: &[CancelOrderRequest]) -> ExchangeApiResult<Value> {
    let first_symbol = &cancels[0].symbol;
    let mut order_ids = Vec::new();
    let mut client_order_ids = Vec::new();
    for cancel in cancels {
        if cancel.symbol.exchange_symbol.symbol != first_symbol.exchange_symbol.symbol {
            return Err(ExchangeApiError::InvalidRequest {
                message: "poloniex futures batch_cancel_orders requires one symbol".to_string(),
            });
        }
        if let Some(order_id) = cancel.exchange_order_id.as_deref() {
            order_ids.push(order_id.to_string());
        } else if let Some(client_order_id) = cancel.client_order_id.as_deref() {
            client_order_ids.push(client_order_id.to_string());
        } else {
            return Err(ExchangeApiError::InvalidRequest {
                message: "poloniex batch_cancel_orders item requires exchange_order_id or client_order_id".to_string(),
            });
        }
    }
    let mut body = serde_json::Map::new();
    body.insert(
        "symbol".to_string(),
        Value::String(normalize_poloniex_symbol(
            &first_symbol.exchange_symbol.symbol,
            first_symbol.market_type,
        )?),
    );
    if !order_ids.is_empty() {
        body.insert("ordIds".to_string(), json!(order_ids));
    }
    if !client_order_ids.is_empty() {
        body.insert("clOrdIds".to_string(), json!(client_order_ids));
    }
    Ok(Value::Object(body))
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
            .get("clientOrderId")
            .or_else(|| value.get("clOrdId"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| request.new_client_order_id.clone())
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value
            .get("id")
            .or_else(|| value.get("orderId"))
            .or_else(|| value.get("ordId"))
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
        created_at: Some(Utc::now()),
        updated_at: Utc::now(),
    }
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
            .get("clientOrderId")
            .or_else(|| value.get("clientOrderID"))
            .or_else(|| value.get("clOrdId"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value
            .get("orderId")
            .or_else(|| value.get("id"))
            .or_else(|| value.get("ordId"))
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
            .get("clientOrderId")
            .or_else(|| value.get("clOrdId"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value
            .get("orderId")
            .or_else(|| value.get("ordId"))
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

fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn order_type_text(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "MARKET",
        OrderType::StopMarket => "STOP_MARKET",
        OrderType::StopLimit => "STOP",
        _ => "LIMIT",
    }
}

fn tif_text(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GTC => "GTC",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTX => "LIMIT_MAKER",
    }
}

fn position_side_text(side: PositionSide) -> &'static str {
    match side {
        PositionSide::Long => "LONG",
        PositionSide::Short => "SHORT",
        _ => "BOTH",
    }
}

fn ensure_poloniex_perpetual_symbol(
    adapter: &PoloniexGatewayAdapter,
    symbol: &SymbolScope,
) -> ExchangeApiResult<()> {
    adapter.ensure_exchange(&symbol.exchange)?;
    if symbol.market_type != MarketType::Perpetual {
        return Err(ExchangeApiError::Unsupported {
            operation: "poloniex.perpetual_position_settings",
        });
    }
    adapter.ensure_supported_market(symbol.market_type)
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("poloniex {field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn poloniex_position_mode_text(mode: PoloniexPositionMode) -> &'static str {
    match mode {
        PoloniexPositionMode::OneWay => "ONE_WAY",
        PoloniexPositionMode::Hedge => "HEDGE",
    }
}

fn poloniex_margin_mode_text(mode: PoloniexMarginMode) -> &'static str {
    match mode {
        PoloniexMarginMode::Cross => "CROSS",
        PoloniexMarginMode::Isolated => "ISOLATED",
    }
}

fn poloniex_private_ack(
    exchange_id: &rustcta_types::ExchangeId,
    operation: &str,
    value: Value,
) -> PoloniexPrivateAck {
    PoloniexPrivateAck {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        operation: operation.to_string(),
        data: value.get("data").cloned(),
        raw: value,
    }
}
