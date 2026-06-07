use std::collections::HashMap;

use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderListConditionalLeg, OrderListKind,
    OrderListLegType, OrderListRequest, OrderListResponse, OrderState, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, SymbolScope, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::Value;

use super::parser::{normalize_bingx_symbol, value_as_string};
use super::private_parser::{
    parse_balances, parse_fee_snapshots, parse_fills, parse_order, parse_order_state, parse_orders,
    parse_positions,
};
use super::BingxGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

impl BingxGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bingx.get_balances")?;
        let endpoint = match market_type {
            MarketType::Spot => "/openApi/spot/v1/account/balance",
            MarketType::Perpetual => "/openApi/swap/v2/user/balance",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("bingx.get_balances", endpoint, &HashMap::new())
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
                operation: "bingx.positions_non_perpetual",
            });
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bingx.get_positions")?;
        let mut params = HashMap::new();
        if let Some(symbol) = request.symbols.first() {
            if symbol.exchange_id != self.exchange_id {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!(
                        "bingx adapter cannot serve position request for exchange {}",
                        symbol.exchange_id
                    ),
                });
            }
            params.insert(
                "symbol".to_string(),
                normalize_bingx_symbol(&symbol.symbol, MarketType::Perpetual)?,
            );
        }
        let value = self
            .send_signed_get(
                "bingx.get_positions",
                "/openApi/swap/v2/user/positions",
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
                message: "bingx get_fees requires at least one symbol".to_string(),
            });
        }
        let mut fees = Vec::new();
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market(symbol.market_type)?;
            let endpoint = match symbol.market_type {
                MarketType::Spot => "/openApi/spot/v1/user/commissionRate",
                MarketType::Perpetual => "/openApi/swap/v2/user/commissionRate",
                _ => unreachable!("checked by ensure_supported_market"),
            };
            let mut params = HashMap::new();
            if symbol.market_type == MarketType::Spot {
                params.insert(
                    "symbol".to_string(),
                    normalize_bingx_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
                );
            }
            let value = self
                .send_signed_get("bingx.get_fees", endpoint, &params)
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
        let params = bingx_place_order_params(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/openApi/spot/v1/trade/order",
            MarketType::Perpetual => "/openApi/swap/v2/trade/order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post("bingx.place_order", endpoint, &params)
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
        let params = bingx_cancel_order_params(&request)?;
        let (method, endpoint) = match request.symbol.market_type {
            MarketType::Spot => ("POST", "/openApi/spot/v1/trade/cancel"),
            MarketType::Perpetual => ("DELETE", "/openApi/swap/v2/trade/order"),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = if method == "POST" {
            self.send_signed_post("bingx.cancel_order", endpoint, &params)
                .await?
        } else {
            self.send_signed_delete("bingx.cancel_order", endpoint, &params)
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
        if request.symbol.market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "bingx.amend_order_spot",
            });
        }
        if request.new_client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bingx.amend_order_new_client_order_id",
            });
        }
        let params = bingx_amend_order_params(&request)?;
        let value = self
            .send_signed_post("bingx.amend_order", "/openApi/swap/v1/trade/amend", &params)
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

    pub(super) async fn place_order_list_impl(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        ensure_exchange_api_schema(request.schema_version())?;
        let symbol = request.symbol().clone();
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        if symbol.market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "bingx.order_list_non_spot",
            });
        }
        let params = bingx_oco_order_params(&request)?;
        let value = self
            .send_signed_post(
                "bingx.place_order_list",
                "/openApi/spot/v1/oco/order",
                &params,
            )
            .await?;
        let orders = parse_orders(&self.exchange_id, Some(&symbol), symbol.market_type, &value)
            .unwrap_or_default();
        let order_list_id = order_list_id_from_value(&value);
        Ok(OrderListResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(symbol.exchange.clone(), request.context_request_id()),
            symbol,
            kind: OrderListKind::Oco,
            order_list_id,
            list_client_order_id: request.list_client_order_id(),
            list_status_type: Some("EXEC_STARTED".to_string()),
            list_order_status: Some("EXECUTING".to_string()),
            orders,
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
            ensure_exchange_api_schema(order.schema_version)?;
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market(order.symbol.market_type)?;
            if order.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bingx batch_place_orders requires one market type".to_string(),
                });
            }
        }
        let field = if market_type == MarketType::Spot {
            "data"
        } else {
            "batchOrders"
        };
        let mut params = HashMap::new();
        params.insert(
            field.to_string(),
            serde_json::to_string(
                &request
                    .orders
                    .iter()
                    .map(bingx_batch_order_params)
                    .collect::<ExchangeApiResult<Vec<_>>>()?,
            )
            .map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?,
        );
        let endpoint = match market_type {
            MarketType::Spot => "/openApi/spot/v1/trade/batchOrders",
            MarketType::Perpetual => "/openApi/swap/v2/trade/batchOrders",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_post("bingx.batch_place_orders", endpoint, &params)
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
            ensure_exchange_api_schema(cancel.schema_version)?;
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_supported_market(cancel.symbol.market_type)?;
            if cancel.symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "bingx batch_cancel_orders requires one market type".to_string(),
                });
            }
        }
        let params = bingx_batch_cancel_params(market_type, &request.cancels)?;
        let (method, endpoint) = match market_type {
            MarketType::Spot => ("POST", "/openApi/spot/v1/trade/cancelOrders"),
            MarketType::Perpetual => ("DELETE", "/openApi/swap/v2/trade/batchOrders"),
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = if method == "POST" {
            self.send_signed_post("bingx.batch_cancel_orders", endpoint, &params)
                .await?
        } else {
            self.send_signed_delete("bingx.batch_cancel_orders", endpoint, &params)
                .await?
        };
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
                message: "bingx cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        if request
            .market_type
            .is_some_and(|market_type| market_type != symbol.market_type)
        {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bingx cancel_all_orders market_type does not match symbol".to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bingx_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        let value = match symbol.market_type {
            MarketType::Spot => {
                self.send_signed_post(
                    "bingx.cancel_all_orders",
                    "/openApi/spot/v1/trade/cancelOpenOrders",
                    &params,
                )
                .await?
            }
            MarketType::Perpetual => {
                self.send_signed_delete(
                    "bingx.cancel_all_orders",
                    "/openApi/swap/v2/trade/allOpenOrders",
                    &params,
                )
                .await?
            }
            _ => unreachable!("checked by ensure_supported_market"),
        };
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
        let params = bingx_query_order_params(&request)?;
        let endpoint = match request.symbol.market_type {
            MarketType::Spot => "/openApi/spot/v1/trade/query",
            MarketType::Perpetual => "/openApi/swap/v2/trade/order",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("bingx.query_order", endpoint, &params)
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
                normalize_bingx_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
            );
        }
        let endpoint = match market_type {
            MarketType::Spot => "/openApi/spot/v1/trade/openOrders",
            MarketType::Perpetual => "/openApi/swap/v2/trade/openOrders",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("bingx.get_open_orders", endpoint, &params)
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
                message: "bingx get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "bingx.get_recent_fills")?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bingx_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderId".to_string(), order_id.to_string());
        }
        if let Some(start_time) = request.start_time {
            let key = if symbol.market_type == MarketType::Perpetual {
                "startTs"
            } else {
                "startTime"
            };
            params.insert(key.to_string(), start_time.timestamp_millis().to_string());
        }
        if let Some(end_time) = request.end_time {
            let key = if symbol.market_type == MarketType::Perpetual {
                "endTs"
            } else {
                "endTime"
            };
            params.insert(key.to_string(), end_time.timestamp_millis().to_string());
        }
        if let Some(limit) = request.limit {
            params.insert("limit".to_string(), limit.min(1000).to_string());
        }
        if symbol.market_type == MarketType::Perpetual {
            params.insert("tradingUnit".to_string(), "COIN".to_string());
        }
        let endpoint = match symbol.market_type {
            MarketType::Spot => "/openApi/spot/v1/trade/myTrades",
            MarketType::Perpetual => "/openApi/swap/v2/trade/allFillOrders",
            _ => unreachable!("checked by ensure_supported_market"),
        };
        let value = self
            .send_signed_get("bingx.get_recent_fills", endpoint, &params)
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

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn set_perpetual_leverage(
        &self,
        symbol: SymbolScope,
        side: PositionSide,
        leverage: u32,
    ) -> ExchangeApiResult<Value> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        if symbol.market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "bingx.set_leverage_non_perpetual",
            });
        }
        if leverage == 0 {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bingx leverage must be greater than zero".to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bingx_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        params.insert("side".to_string(), position_side_text(side).to_string());
        params.insert("leverage".to_string(), leverage.to_string());
        self.send_signed_post(
            "bingx.set_perpetual_leverage",
            "/openApi/swap/v2/trade/leverage",
            &params,
        )
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn set_position_mode(&self, dual_side_position: bool) -> ExchangeApiResult<Value> {
        let mut params = HashMap::new();
        params.insert(
            "dualSidePosition".to_string(),
            dual_side_position.to_string(),
        );
        self.send_signed_post(
            "bingx.set_position_mode",
            "/openApi/swap/v1/positionSide/dual",
            &params,
        )
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn query_position_mode(&self) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "bingx.query_position_mode",
            "/openApi/swap/v1/positionSide/dual",
            &HashMap::new(),
        )
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn switch_multi_assets_mode(&self, asset_mode: &str) -> ExchangeApiResult<Value> {
        if !matches!(asset_mode, "singleAssetMode" | "multiAssetsMode") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bingx asset_mode must be singleAssetMode or multiAssetsMode".to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert("assetMode".to_string(), asset_mode.to_string());
        self.send_signed_post(
            "bingx.switch_multi_assets_mode",
            "/openApi/swap/v1/trade/assetMode",
            &params,
        )
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn query_multi_assets_mode(&self) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            "bingx.query_multi_assets_mode",
            "/openApi/swap/v1/trade/assetMode",
            &HashMap::new(),
        )
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn change_perpetual_margin_type(
        &self,
        symbol: SymbolScope,
        margin_type: &str,
    ) -> ExchangeApiResult<Value> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market(symbol.market_type)?;
        if symbol.market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "bingx.change_margin_type_non_perpetual",
            });
        }
        let normalized = margin_type.trim().to_ascii_uppercase();
        if !matches!(
            normalized.as_str(),
            "ISOLATED" | "CROSSED" | "SEPARATE_ISOLATED"
        ) {
            return Err(ExchangeApiError::InvalidRequest {
                message: "bingx margin_type must be ISOLATED, CROSSED, or SEPARATE_ISOLATED"
                    .to_string(),
            });
        }
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_bingx_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        params.insert("marginType".to_string(), normalized);
        self.send_signed_post(
            "bingx.change_perpetual_margin_type",
            "/openApi/swap/v2/trade/marginType",
            &params,
        )
        .await
    }
}

fn bingx_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    if request.symbol.market_type == MarketType::Spot && request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bingx spot order does not support reduce_only".to_string(),
        });
    }
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_bingx_symbol(
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
                    message: "bingx spot market buy requires quote_quantity".to_string(),
                })?;
        params.insert("quoteOrderQty".to_string(), quote_quantity.to_string());
    } else {
        params.insert("quantity".to_string(), request.quantity.clone());
    }
    if request.order_type.requires_limit_price() {
        params.insert(
            "price".to_string(),
            request
                .price
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bingx limit-like order requires price".to_string(),
                })?,
        );
    } else if let Some(price) = &request.price {
        params.insert("price".to_string(), price.clone());
    }
    if let Some(tif) = request
        .time_in_force
        .or_else(|| request.post_only.then_some(TimeInForce::GTX))
    {
        params.insert("timeInForce".to_string(), tif_text(tif).to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("clientOrderId".to_string(), client_order_id.to_string());
    }
    if request.symbol.market_type == MarketType::Perpetual {
        params.insert(
            "positionSide".to_string(),
            position_side_text(request.position_side.unwrap_or(PositionSide::Net)).to_string(),
        );
        if request.reduce_only {
            params.insert("reduceOnly".to_string(), "true".to_string());
        }
    }
    Ok(params)
}

fn bingx_batch_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = bingx_place_order_params(request)?;
    if request.symbol.market_type == MarketType::Spot {
        if let Some(client_order_id) = params.remove("clientOrderId") {
            params.insert("newClientOrderId".to_string(), client_order_id);
        }
    }
    Ok(params)
}

fn bingx_batch_cancel_params(
    market_type: MarketType,
    cancels: &[CancelOrderRequest],
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    let symbol = &cancels[0].symbol;
    params.insert(
        "symbol".to_string(),
        normalize_bingx_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
    );
    let order_ids = cancels
        .iter()
        .filter_map(|cancel| cancel.exchange_order_id.clone())
        .collect::<Vec<_>>();
    let client_order_ids = cancels
        .iter()
        .filter_map(|cancel| cancel.client_order_id.clone())
        .collect::<Vec<_>>();
    match market_type {
        MarketType::Spot => {
            if !order_ids.is_empty() {
                params.insert("orderIds".to_string(), order_ids.join(","));
            }
            if !client_order_ids.is_empty() {
                params.insert("clientOrderIDs".to_string(), client_order_ids.join(","));
            }
            params.insert("process".to_string(), "1".to_string());
        }
        MarketType::Perpetual => {
            if !order_ids.is_empty() {
                params.insert(
                    "orderIdList".to_string(),
                    serde_json::to_string(&order_ids).map_err(|error| {
                        ExchangeApiError::Serialization {
                            message: error.to_string(),
                        }
                    })?,
                );
            }
            if !client_order_ids.is_empty() {
                params.insert(
                    "clientOrderIdList".to_string(),
                    serde_json::to_string(&client_order_ids).map_err(|error| {
                        ExchangeApiError::Serialization {
                            message: error.to_string(),
                        }
                    })?,
                );
            }
        }
        _ => unreachable!("checked by caller"),
    }
    if !params.contains_key("orderIds")
        && !params.contains_key("clientOrderIDs")
        && !params.contains_key("orderIdList")
        && !params.contains_key("clientOrderIdList")
    {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bingx batch_cancel_orders requires exchange_order_id or client_order_id"
                .to_string(),
        });
    }
    Ok(params)
}

fn bingx_amend_order_params(
    request: &AmendOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_bingx_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("clientOrderId".to_string(), client_order_id.to_string());
    }
    if !params.contains_key("orderId") && !params.contains_key("clientOrderId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bingx amend_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    if request.new_quantity.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bingx amend_order requires new_quantity".to_string(),
        });
    }
    params.insert("quantity".to_string(), request.new_quantity.clone());
    Ok(params)
}

fn bingx_oco_order_params(
    request: &OrderListRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let OrderListRequest::Oco {
        symbol,
        list_client_order_id,
        side,
        quantity,
        above,
        below,
        ..
    } = request
    else {
        return Err(ExchangeApiError::Unsupported {
            operation: "bingx.order_list_oto",
        });
    };
    let (limit_leg, stop_leg) = oco_limit_and_stop_legs(above, below)?;
    let limit_price = limit_leg
        .price
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bingx OCO limit leg requires price".to_string(),
        })?;
    let order_price = stop_leg
        .price
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bingx OCO stop-limit leg requires price".to_string(),
        })?;
    let trigger_price = stop_leg
        .stop_price
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "bingx OCO stop-limit leg requires stop_price".to_string(),
        })?;

    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_bingx_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
    );
    params.insert("side".to_string(), side_text(*side).to_string());
    params.insert("quantity".to_string(), quantity.clone());
    params.insert("limitPrice".to_string(), limit_price.to_string());
    params.insert("orderPrice".to_string(), order_price.to_string());
    params.insert("triggerPrice".to_string(), trigger_price.to_string());
    if let Some(client_order_id) = list_client_order_id.as_deref() {
        params.insert("listClientOrderId".to_string(), client_order_id.to_string());
    }
    if let Some(client_order_id) = limit_leg.client_order_id.as_deref() {
        params.insert(
            "aboveClientOrderId".to_string(),
            client_order_id.to_string(),
        );
    }
    if let Some(client_order_id) = stop_leg.client_order_id.as_deref() {
        params.insert(
            "belowClientOrderId".to_string(),
            client_order_id.to_string(),
        );
    }
    Ok(params)
}

fn oco_limit_and_stop_legs<'a>(
    above: &'a OrderListConditionalLeg,
    below: &'a OrderListConditionalLeg,
) -> ExchangeApiResult<(&'a OrderListConditionalLeg, &'a OrderListConditionalLeg)> {
    let above_stop = oco_leg_is_stop_limit(above);
    let below_stop = oco_leg_is_stop_limit(below);
    match (above_stop, below_stop) {
        (false, true) => Ok((above, below)),
        (true, false) => Ok((below, above)),
        _ => Err(ExchangeApiError::InvalidRequest {
            message: "bingx OCO requires one limit leg and one stop-limit leg".to_string(),
        }),
    }
}

fn oco_leg_is_stop_limit(leg: &OrderListConditionalLeg) -> bool {
    matches!(
        leg.order_type,
        OrderListLegType::StopLossLimit | OrderListLegType::TakeProfitLimit
    ) || leg.stop_price.is_some()
}

fn bingx_cancel_order_params(
    request: &CancelOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_bingx_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("clientOrderId".to_string(), client_order_id.to_string());
    }
    if !params.contains_key("orderId") && !params.contains_key("clientOrderId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bingx cancel_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    Ok(params)
}

fn bingx_query_order_params(
    request: &QueryOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_bingx_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        params.insert("orderId".to_string(), order_id.to_string());
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert("clientOrderId".to_string(), client_order_id.to_string());
    }
    if !params.contains_key("orderId") && !params.contains_key("clientOrderId") {
        return Err(ExchangeApiError::InvalidRequest {
            message: "bingx query_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    Ok(params)
}

fn data_or_root(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn order_list_id_from_value(value: &Value) -> Option<String> {
    let data = data_or_root(value);
    data.get("orderListId")
        .and_then(|value| value_as_string(Some(value)))
        .or_else(|| {
            data.as_array()
                .and_then(|orders| orders.first())
                .and_then(|order| value_as_string(order.get("orderListId")))
        })
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
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value
            .get("orderId")
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
        client_order_id: value_as_string(value.get("clientOrderId"))
            .or_else(|| request.client_order_id.clone()),
        exchange_order_id: value_as_string(value.get("orderId"))
            .or_else(|| request.exchange_order_id.clone()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::Net),
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
        TimeInForce::GTX => "PostOnly",
    }
}

fn position_side_text(side: PositionSide) -> &'static str {
    match side {
        PositionSide::Long => "LONG",
        PositionSide::Short => "SHORT",
        _ => "BOTH",
    }
}
