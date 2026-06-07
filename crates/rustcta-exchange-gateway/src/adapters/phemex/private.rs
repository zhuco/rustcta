#![allow(dead_code)]

use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderState, PlaceOrderRequest, PlaceOrderResponse,
    PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, SymbolScope, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::parser::normalize_phemex_symbol;
use super::private_parser::{
    parse_account_balances, parse_fee_snapshots, parse_open_orders, parse_order_state,
    parse_positions, parse_recent_fills,
};
use super::PhemexGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhemexPositionMode {
    OneWay,
    Hedged,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhemexPrivateAck {
    pub schema_version: u16,
    pub exchange: rustcta_types::ExchangeId,
    pub operation: String,
    pub data: Option<Value>,
    pub raw: Value,
}

impl PhemexGatewayAdapter {
    pub(super) async fn get_balances_impl(
        &self,
        request: BalancesRequest,
    ) -> ExchangeApiResult<BalancesResponse> {
        ensure_exchange_api_schema(request.schema_version)?;
        self.ensure_exchange(&request.exchange)?;
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "phemex.get_balances")?;
        let mut params = HashMap::new();
        if let Some(asset) = request.assets.first() {
            params.insert("currency".to_string(), asset.trim().to_ascii_uppercase());
        } else if market_type == MarketType::Perpetual {
            params.insert("currency".to_string(), "USDT".to_string());
        }
        let endpoint = if market_type == MarketType::Spot {
            "/spot/wallets"
        } else {
            "/g-accounts/accountPositions"
        };
        let value = self
            .send_signed_get("phemex.get_balances", endpoint, &params)
            .await?;
        let balances = parse_account_balances(
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
        if let Some(market_type) = request.market_type {
            self.ensure_perpetual(market_type)?;
        }
        let (tenant_id, account_id) =
            self.context_account(&request.context, "phemex.get_positions")?;
        let mut params = HashMap::new();
        params.insert("currency".to_string(), "USDT".to_string());
        if let Some(symbol) = request.symbols.first() {
            params.insert(
                "symbol".to_string(),
                normalize_phemex_symbol(&symbol.symbol, MarketType::Perpetual)?,
            );
        }
        let value = self
            .send_signed_get(
                "phemex.get_positions",
                "/g-accounts/accountPositions",
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
                message: "phemex get_fees requires at least one symbol".to_string(),
            });
        }
        let market_type = request.symbols[0].market_type;
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
            if symbol.market_type != market_type {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "phemex get_fees cannot mix market types".to_string(),
                });
            }
        }
        let mut params = HashMap::new();
        let endpoint = if market_type == MarketType::Spot {
            let quote = request.symbols[0]
                .canonical_symbol
                .as_ref()
                .map(|symbol| symbol.quote_asset())
                .unwrap_or("USDT")
                .to_ascii_uppercase();
            params.insert("quoteCurrency".to_string(), quote);
            "/api-data/spots/fee-rate"
        } else {
            params.insert("settleCurrency".to_string(), "USDT".to_string());
            "/api-data/futures/fee-rate"
        };
        let value = self
            .send_signed_get("phemex.get_fees", endpoint, &params)
            .await?;
        let fees = parse_fee_snapshots(&self.exchange_id, &request.symbols, &value)?;
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
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let params = phemex_place_order_params(&request)?;
        let endpoint = if request.symbol.market_type == MarketType::Spot {
            "/spot/orders/create"
        } else {
            "/g-orders/create"
        };
        let value = self
            .send_signed_put("phemex.place_order", endpoint, &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| order_state_from_place_ack(&self.exchange_id, &request, &value));
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
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&request.symbol.exchange_symbol.symbol, MarketType::Spot)?,
        );
        params.insert("side".to_string(), phemex_side(request.side).to_string());
        params.insert("ordType".to_string(), "Market".to_string());
        params.insert("qtyType".to_string(), "ByQuote".to_string());
        params.insert(
            "quoteQtyRq".to_string(),
            non_empty("quote_quantity", &request.quote_quantity)?,
        );
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert(
                "clOrdID".to_string(),
                non_empty("client_order_id", client_order_id)?,
            );
        }
        let value = self
            .send_signed_put(
                "phemex.place_quote_market_order",
                "/spot/orders/create",
                &params,
            )
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| order_state_from_quote_ack(&self.exchange_id, &request, &value));
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
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert(
                "orderID".to_string(),
                non_empty("exchange_order_id", order_id)?,
            );
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert(
                "clOrdID".to_string(),
                non_empty("client_order_id", client_order_id)?,
            );
        }
        if !params.contains_key("orderID") && !params.contains_key("clOrdID") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "phemex cancel_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        if request.symbol.market_type == MarketType::Perpetual {
            params.insert("posSide".to_string(), "Merged".to_string());
        }
        let endpoint = if request.symbol.market_type == MarketType::Spot {
            "/spot/orders"
        } else {
            "/g-orders/cancel"
        };
        let value = self
            .send_signed_delete("phemex.cancel_order", endpoint, &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| order_state_from_cancel_ack(&self.exchange_id, &request, &value));
        Ok(CancelOrderResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.symbol.exchange, request.context.request_id),
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
        self.ensure_supported_market_type(request.symbol.market_type)?;
        if request.new_client_order_id.is_some() {
            return Err(ExchangeApiError::Unsupported {
                operation: "phemex.amend_order.new_client_order_id",
            });
        }
        let params = phemex_amend_order_params(&request)?;
        let endpoint = if request.symbol.market_type == MarketType::Spot {
            "/spot/orders"
        } else {
            "/g-orders/replace"
        };
        let value = self
            .send_signed_put("phemex.amend_order", endpoint, &params)
            .await?;
        let order = parse_order_state(&self.exchange_id, Some(&request.symbol), &value)
            .unwrap_or_else(|_| order_state_from_amend_ack(&self.exchange_id, &request, &value));
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
            return Err(ExchangeApiError::InvalidRequest {
                message: "phemex batch_place_orders requires at least one order".to_string(),
            });
        }

        let mut orders = Vec::with_capacity(request.orders.len());
        for order in request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market_type(order.symbol.market_type)?;
            let response = self.place_order_impl(order).await?;
            orders.push(response.order);
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
                message: "phemex batch_cancel_orders requires at least one cancel".to_string(),
            });
        }
        let first_symbol = request.cancels[0].symbol.clone();
        self.ensure_exchange(&first_symbol.exchange)?;
        self.ensure_perpetual(first_symbol.market_type)?;

        let mut order_ids = Vec::new();
        let mut client_order_ids = Vec::new();
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_perpetual(cancel.symbol.market_type)?;
            if cancel.symbol != first_symbol {
                return Err(ExchangeApiError::InvalidRequest {
                    message: "phemex batch_cancel_orders requires one symbol".to_string(),
                });
            }
            match (
                cancel.exchange_order_id.as_deref(),
                cancel.client_order_id.as_deref(),
            ) {
                (Some(order_id), None) => {
                    order_ids.push(non_empty("exchange_order_id", order_id)?);
                }
                (None, Some(client_order_id)) => {
                    client_order_ids.push(non_empty("client_order_id", client_order_id)?);
                }
                (Some(_), Some(_)) => {
                    return Err(ExchangeApiError::InvalidRequest {
                        message: "phemex batch_cancel_orders requires one id type per cancel"
                            .to_string(),
                    });
                }
                (None, None) => {
                    return Err(ExchangeApiError::InvalidRequest {
                        message: "phemex batch_cancel_orders requires ids".to_string(),
                    });
                }
            }
        }
        if !order_ids.is_empty() && !client_order_ids.is_empty() {
            return Err(ExchangeApiError::InvalidRequest {
                message: "phemex batch_cancel_orders cannot mix orderID and clOrdID".to_string(),
            });
        }

        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(
                &first_symbol.exchange_symbol.symbol,
                first_symbol.market_type,
            )?,
        );
        if !order_ids.is_empty() {
            params.insert("orderID".to_string(), order_ids.join(","));
        } else {
            params.insert("clOrdID".to_string(), client_order_ids.join(","));
        }
        params.insert("posSide".to_string(), "Merged".to_string());

        let value = self
            .send_signed_delete("phemex.batch_cancel_orders", "/g-orders", &params)
            .await?;
        let orders = parse_open_orders(&self.exchange_id, Some(&first_symbol), &value)
            .unwrap_or_else(|_| {
                request
                    .cancels
                    .iter()
                    .map(|cancel| order_state_from_cancel_ack(&self.exchange_id, cancel, &value))
                    .collect()
            });
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
                message: "phemex cancel_all_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        params.insert("untriggered".to_string(), "false".to_string());
        let endpoint = if symbol.market_type == MarketType::Spot {
            "/spot/orders/all"
        } else {
            "/g-orders/all"
        };
        let value = self
            .send_signed_delete("phemex.cancel_all_orders", endpoint, &params)
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
        self.ensure_supported_market_type(request.symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(
                &request.symbol.exchange_symbol.symbol,
                request.symbol.market_type,
            )?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert(
                "orderID".to_string(),
                non_empty("exchange_order_id", order_id)?,
            );
        }
        if let Some(client_order_id) = request.client_order_id.as_deref() {
            params.insert(
                "clOrdID".to_string(),
                non_empty("client_order_id", client_order_id)?,
            );
        }
        if !params.contains_key("orderID") && !params.contains_key("clOrdID") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "phemex query_order requires exchange_order_id or client_order_id"
                    .to_string(),
            });
        }
        let endpoint = if request.symbol.market_type == MarketType::Spot {
            "/api-data/spots/orders/by-order-id"
        } else {
            "/api-data/g-futures/orders/by-order-id"
        };
        let value = self
            .send_signed_get("phemex.query_order", endpoint, &params)
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
        let market_type = request.market_type.unwrap_or(MarketType::Spot);
        self.ensure_supported_market_type(market_type)?;
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "phemex get_open_orders requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        let endpoint = if symbol.market_type == MarketType::Spot {
            "/spot/orders/activeList"
        } else {
            "/g-orders/activeList"
        };
        let value = self
            .send_signed_get("phemex.get_open_orders", endpoint, &params)
            .await?;
        let orders = parse_open_orders(&self.exchange_id, Some(symbol), &value)?;
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
        let symbol = request
            .symbol
            .as_ref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "phemex get_recent_fills requires symbol".to_string(),
            })?;
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let (tenant_id, account_id) =
            self.context_account(&request.context, "phemex.get_recent_fills")?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        if let Some(order_id) = request.exchange_order_id.as_deref() {
            params.insert("orderID".to_string(), order_id.to_string());
        }
        if let Some(start_time) = request.start_time {
            params.insert(
                "start".to_string(),
                start_time.timestamp_millis().to_string(),
            );
        }
        if let Some(end_time) = request.end_time {
            params.insert("end".to_string(), end_time.timestamp_millis().to_string());
        }
        params.insert(
            "limit".to_string(),
            request.limit.unwrap_or(200).min(200).to_string(),
        );
        let endpoint = if symbol.market_type == MarketType::Spot {
            "/api-data/spots/trades"
        } else {
            "/api-data/g-futures/trades"
        };
        let value = self
            .send_signed_get("phemex.get_recent_fills", endpoint, &params)
            .await?;
        let fills = parse_recent_fills(&self.exchange_id, tenant_id, account_id, symbol, &value)?;
        Ok(RecentFillsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(request.exchange, request.context.request_id),
            fills,
        })
    }

    pub async fn switch_position_mode(
        &self,
        symbol: SymbolScope,
        target_mode: PhemexPositionMode,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
        );
        params.insert(
            "targetPosMode".to_string(),
            match target_mode {
                PhemexPositionMode::OneWay => "OneWay",
                PhemexPositionMode::Hedged => "Hedged",
            }
            .to_string(),
        );
        let value = self
            .send_signed_put(
                "phemex.switch_position_mode",
                "/g-positions/switch-pos-mode-sync",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.switch_position_mode",
            value,
        ))
    }

    pub async fn set_one_way_leverage(
        &self,
        symbol: SymbolScope,
        leverage: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
        );
        params.insert("leverageRr".to_string(), non_empty("leverage", leverage)?);
        let value = self
            .send_signed_put("phemex.set_leverage", "/g-positions/leverage", &params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.set_leverage",
            value,
        ))
    }

    pub async fn set_hedged_leverage(
        &self,
        symbol: SymbolScope,
        long_leverage: &str,
        short_leverage: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
        );
        params.insert(
            "longLeverageRr".to_string(),
            non_empty("long_leverage", long_leverage)?,
        );
        params.insert(
            "shortLeverageRr".to_string(),
            non_empty("short_leverage", short_leverage)?,
        );
        let value = self
            .send_signed_put("phemex.set_leverage", "/g-positions/leverage", &params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.set_leverage",
            value,
        ))
    }

    pub async fn assign_position_balance(
        &self,
        symbol: SymbolScope,
        position_side: PositionSide,
        amount: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
        );
        params.insert(
            "posSide".to_string(),
            phemex_position_side(position_side)?.to_string(),
        );
        params.insert(
            "posBalanceRv".to_string(),
            non_empty("posBalanceRv", amount)?,
        );
        let value = self
            .send_signed_post(
                "phemex.assign_position_balance",
                "/g-positions/assign",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.assign_position_balance",
            value,
        ))
    }

    pub async fn get_perp_risk_units(&self) -> ExchangeApiResult<PhemexPrivateAck> {
        let params = HashMap::new();
        let value = self
            .send_signed_get(
                "phemex.get_perp_risk_units",
                "/g-accounts/riskUnit",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_perp_risk_units",
            value,
        ))
    }

    pub async fn get_perp_funding_fees(
        &self,
        symbol: SymbolScope,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_perpetual(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
        );
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.min(200).to_string());
        }
        if let Some(offset) = offset {
            params.insert("offset".to_string(), offset.to_string());
        }
        let value = self
            .send_signed_get(
                "phemex.get_perp_funding_fees",
                "/api-data/g-futures/funding-fees",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_perp_funding_fees",
            value,
        ))
    }

    pub async fn get_perp_closed_positions(
        &self,
        symbol: Option<SymbolScope>,
        currency: Option<&str>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_perpetual(symbol.market_type)?;
            params.insert(
                "symbol".to_string(),
                normalize_phemex_symbol(&symbol.exchange_symbol.symbol, MarketType::Perpetual)?,
            );
        }
        if let Some(currency) = currency {
            params.insert("currency".to_string(), non_empty("currency", currency)?);
        }
        if !params.contains_key("symbol") && !params.contains_key("currency") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "phemex get_perp_closed_positions requires symbol or currency".to_string(),
            });
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.min(200).to_string());
        }
        if let Some(offset) = offset {
            params.insert("offset".to_string(), offset.to_string());
        }
        let value = self
            .send_signed_get(
                "phemex.get_perp_closed_positions",
                "/api-data/g-futures/closedPosition",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_perp_closed_positions",
            value,
        ))
    }

    pub async fn get_spot_funds_history(
        &self,
        currency: Option<&str>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        if let Some(currency) = currency {
            params.insert("currency".to_string(), non_empty("currency", currency)?);
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.min(200).to_string());
        }
        if let Some(offset) = offset {
            params.insert("offset".to_string(), offset.to_string());
        }
        let value = self
            .send_signed_get(
                "phemex.get_spot_funds_history",
                "/api-data/spots/funds",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_spot_funds_history",
            value,
        ))
    }

    pub async fn get_spot_pnls(
        &self,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        if let Some(start_ms) = start_ms {
            params.insert("start".to_string(), start_ms.to_string());
        }
        if let Some(end_ms) = end_ms {
            params.insert("end".to_string(), end_ms.to_string());
        }
        let value = self
            .send_signed_get("phemex.get_spot_pnls", "/api-data/spots/pnls", &params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_spot_pnls",
            value,
        ))
    }

    pub async fn transfer_between_spot_and_futures(
        &self,
        currency: &str,
        amount_ev: i64,
        move_op: i32,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let body = json!({
            "amountEv": amount_ev,
            "currency": non_empty("currency", currency)?,
            "moveOp": move_op,
        });
        let value = self
            .send_signed_post_json("phemex.transfer_assets", "/assets/transfer", &body)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.transfer_assets",
            value,
        ))
    }

    pub async fn get_transfer_history(
        &self,
        currency: &str,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        offset: Option<u32>,
        limit: Option<u32>,
        side: Option<i32>,
        biz_type: Option<i32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        insert_history_params(&mut params, start_ms, end_ms, offset, limit);
        if let Some(side) = side {
            params.insert("side".to_string(), side.to_string());
        }
        if let Some(biz_type) = biz_type {
            params.insert("bizType".to_string(), biz_type.to_string());
        }
        let value = self
            .send_signed_get("phemex.get_transfer_history", "/assets/transfer", &params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_transfer_history",
            value,
        ))
    }

    pub async fn create_spot_sub_account_transfer(
        &self,
        currency: &str,
        amount_ev: i64,
        request_key: Option<&str>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut body = json!({
            "amountEv": amount_ev,
            "currency": non_empty("currency", currency)?,
        });
        insert_json_string(&mut body, "requestKey", request_key)?;
        let value = self
            .send_signed_post_json(
                "phemex.create_spot_sub_account_transfer",
                "/assets/spots/sub-accounts/transfer",
                &body,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.create_spot_sub_account_transfer",
            value,
        ))
    }

    pub async fn get_spot_sub_account_transfer_history(
        &self,
        currency: &str,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        offset: Option<u32>,
        limit: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        insert_history_params(&mut params, start_ms, end_ms, offset, limit);
        let value = self
            .send_signed_get(
                "phemex.get_spot_sub_account_transfer_history",
                "/assets/spots/sub-accounts/transfer",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_spot_sub_account_transfer_history",
            value,
        ))
    }

    pub async fn create_futures_sub_account_transfer(
        &self,
        currency: &str,
        amount_ev: i64,
        request_key: Option<&str>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut body = json!({
            "amountEv": amount_ev,
            "currency": non_empty("currency", currency)?,
        });
        insert_json_string(&mut body, "requestKey", request_key)?;
        let value = self
            .send_signed_post_json(
                "phemex.create_futures_sub_account_transfer",
                "/assets/futures/sub-accounts/transfer",
                &body,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.create_futures_sub_account_transfer",
            value,
        ))
    }

    pub async fn get_futures_sub_account_transfer_history(
        &self,
        currency: &str,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        offset: Option<u32>,
        limit: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        insert_history_params(&mut params, start_ms, end_ms, offset, limit);
        let value = self
            .send_signed_get(
                "phemex.get_futures_sub_account_transfer_history",
                "/assets/futures/sub-accounts/transfer",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_futures_sub_account_transfer_history",
            value,
        ))
    }

    pub async fn create_universal_transfer(
        &self,
        currency: &str,
        amount_ev: i64,
        biz_type: &str,
        from_user_id: Option<i64>,
        to_user_id: Option<i64>,
        request_key: Option<&str>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut body = json!({
            "amountEv": amount_ev,
            "bizType": non_empty("biz_type", biz_type)?,
            "currency": non_empty("currency", currency)?,
            "fromUserId": from_user_id.unwrap_or(0),
            "toUserId": to_user_id.unwrap_or(0),
        });
        insert_json_string(&mut body, "requestKey", request_key)?;
        let value = self
            .send_signed_post_json(
                "phemex.create_universal_transfer",
                "/assets/universal-transfer",
                &body,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.create_universal_transfer",
            value,
        ))
    }

    pub async fn get_convert_quote(
        &self,
        from_currency: &str,
        to_currency: &str,
        from_amount_ev: i64,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert(
            "fromCurrency".to_string(),
            non_empty("from_currency", from_currency)?,
        );
        params.insert(
            "toCurrency".to_string(),
            non_empty("to_currency", to_currency)?,
        );
        params.insert("fromAmountEv".to_string(), from_amount_ev.to_string());
        let value = self
            .send_signed_get("phemex.get_convert_quote", "/assets/quote", &params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_convert_quote",
            value,
        ))
    }

    pub async fn create_convert(
        &self,
        from_currency: &str,
        to_currency: &str,
        code: &str,
        from_amount_ev: Option<i64>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut body = json!({
            "code": non_empty("code", code)?,
            "fromCurrency": non_empty("from_currency", from_currency)?,
            "toCurrency": non_empty("to_currency", to_currency)?,
        });
        if let Some(from_amount_ev) = from_amount_ev {
            body["fromAmountEv"] = json!(from_amount_ev);
        }
        let value = self
            .send_signed_post_json("phemex.create_convert", "/assets/convert", &body)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.create_convert",
            value,
        ))
    }

    pub async fn get_convert_history(
        &self,
        from_currency: Option<&str>,
        to_currency: Option<&str>,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        offset: Option<u32>,
        limit: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        if let Some(from_currency) = from_currency {
            params.insert(
                "fromCurrency".to_string(),
                non_empty("from_currency", from_currency)?,
            );
        }
        if let Some(to_currency) = to_currency {
            params.insert(
                "toCurrency".to_string(),
                non_empty("to_currency", to_currency)?,
            );
        }
        if let Some(start_ms) = start_ms {
            params.insert("startTime".to_string(), start_ms.to_string());
        }
        if let Some(end_ms) = end_ms {
            params.insert("endTime".to_string(), end_ms.to_string());
        }
        if let Some(offset) = offset {
            params.insert("offset".to_string(), offset.to_string());
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.min(200).to_string());
        }
        let value = self
            .send_signed_get("phemex.get_convert_history", "/assets/convert", &params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_convert_history",
            value,
        ))
    }

    pub async fn get_deposit_chain_config(
        &self,
        currency: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        let value = self
            .send_signed_get(
                "phemex.get_deposit_chain_config",
                "/phemex-deposit/wallets/api/chainCfg",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_deposit_chain_config",
            value,
        ))
    }

    pub async fn get_deposit_address_v2(
        &self,
        currency: &str,
        chain_name: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        params.insert(
            "chainName".to_string(),
            non_empty("chain_name", chain_name)?,
        );
        let value = self
            .send_signed_get(
                "phemex.get_deposit_address_v2",
                "/phemex-deposit/wallets/api/depositAddress",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_deposit_address_v2",
            value,
        ))
    }

    pub async fn get_deposit_history_v2(
        &self,
        currencies: &[String],
        offset: Option<u32>,
        limit: Option<u32>,
        with_count: Option<bool>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        if !currencies.is_empty() {
            params.insert("currency".to_string(), currencies.join(","));
        }
        if let Some(offset) = offset {
            params.insert("offset".to_string(), offset.to_string());
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.min(200).to_string());
        }
        if let Some(with_count) = with_count {
            params.insert("withCount".to_string(), with_count.to_string());
        }
        let value = self
            .send_signed_get(
                "phemex.get_deposit_history_v2",
                "/phemex-deposit/wallets/api/depositHist",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_deposit_history_v2",
            value,
        ))
    }

    pub async fn get_deposit_address(
        &self,
        currency: &str,
        chain_name: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        params.insert(
            "chainName".to_string(),
            non_empty("chain_name", chain_name)?,
        );
        let value = self
            .send_signed_get(
                "phemex.get_deposit_address",
                "/exchange/wallets/v2/depositAddress",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_deposit_address",
            value,
        ))
    }

    pub async fn get_recent_deposits(
        &self,
        currency: &str,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.min(200).to_string());
        }
        if let Some(offset) = offset {
            params.insert("offset".to_string(), offset.to_string());
        }
        let value = self
            .send_signed_get(
                "phemex.get_recent_deposits",
                "/exchange/wallets/depositList",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_recent_deposits",
            value,
        ))
    }

    pub async fn get_recent_withdrawals(
        &self,
        currency: &str,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.min(200).to_string());
        }
        if let Some(offset) = offset {
            params.insert("offset".to_string(), offset.to_string());
        }
        let value = self
            .send_signed_get(
                "phemex.get_recent_withdrawals",
                "/exchange/wallets/withdrawList",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_recent_withdrawals",
            value,
        ))
    }

    pub async fn get_withdraw_history_v2(
        &self,
        currencies: &[String],
        chain_names: &[String],
        offset: Option<u32>,
        limit: Option<u32>,
        with_count: Option<bool>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        if !currencies.is_empty() {
            params.insert("currency".to_string(), currencies.join(","));
        }
        if !chain_names.is_empty() {
            params.insert("chainName".to_string(), chain_names.join(","));
        }
        if let Some(offset) = offset {
            params.insert("offset".to_string(), offset.to_string());
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.min(200).to_string());
        }
        if let Some(with_count) = with_count {
            params.insert("withCount".to_string(), with_count.to_string());
        }
        let value = self
            .send_signed_get(
                "phemex.get_withdraw_history_v2",
                "/phemex-withdraw/wallets/api/withdrawHist",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_withdraw_history_v2",
            value,
        ))
    }

    pub async fn create_withdraw(
        &self,
        currency: &str,
        address: &str,
        amount: &str,
        chain_name: &str,
        address_tag: Option<&str>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        params.insert("address".to_string(), non_empty("address", address)?);
        params.insert("amount".to_string(), non_empty("amount", amount)?);
        params.insert(
            "chainName".to_string(),
            non_empty("chain_name", chain_name)?,
        );
        if let Some(address_tag) = address_tag {
            params.insert(
                "addressTag".to_string(),
                non_empty("address_tag", address_tag)?,
            );
        }
        let value = self
            .send_signed_post(
                "phemex.create_withdraw",
                "/phemex-withdraw/wallets/api/createWithdraw",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.create_withdraw",
            value,
        ))
    }

    pub async fn cancel_withdraw(&self, id: i64) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("id".to_string(), id.to_string());
        let value = self
            .send_signed_post(
                "phemex.cancel_withdraw",
                "/phemex-withdraw/wallets/api/cancelWithdraw",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.cancel_withdraw",
            value,
        ))
    }

    pub async fn get_withdraw_asset_info(
        &self,
        currency: Option<&str>,
        amount: Option<&str>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        if let Some(currency) = currency {
            params.insert("currency".to_string(), non_empty("currency", currency)?);
        }
        if let Some(amount) = amount {
            params.insert("amount".to_string(), non_empty("amount", amount)?);
        }
        let value = self
            .send_signed_get(
                "phemex.get_withdraw_asset_info",
                "/phemex-withdraw/wallets/api/asset/info",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_withdraw_asset_info",
            value,
        ))
    }

    pub async fn get_legacy_contract_account_positions(
        &self,
        currency: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        let value = self
            .send_signed_get(
                "phemex.get_legacy_contract_account_positions",
                "/accounts/accountPositions",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_legacy_contract_account_positions",
            value,
        ))
    }

    pub async fn get_legacy_contract_positions(
        &self,
        currency: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        let value = self
            .send_signed_get(
                "phemex.get_legacy_contract_positions",
                "/accounts/positions",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_legacy_contract_positions",
            value,
        ))
    }

    pub async fn get_perp_positions_by_currency(
        &self,
        currency: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        let value = self
            .send_signed_get(
                "phemex.get_perp_positions_by_currency",
                "/g-accounts/positions",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_perp_positions_by_currency",
            value,
        ))
    }

    pub async fn get_perp_risk_units_v2(&self) -> ExchangeApiResult<PhemexPrivateAck> {
        let value = self
            .send_signed_get(
                "phemex.get_perp_risk_units_v2",
                "/g-accounts/risk-unit",
                &HashMap::new(),
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_perp_risk_units_v2",
            value,
        ))
    }

    pub async fn get_legacy_futures_fee_rate(
        &self,
        settle_currency: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert(
            "settleCurrency".to_string(),
            non_empty("settle_currency", settle_currency)?,
        );
        let value = self
            .send_signed_get(
                "phemex.get_legacy_futures_fee_rate",
                "/api-data/futures/fee-rate",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_legacy_futures_fee_rate",
            value,
        ))
    }

    pub async fn get_legacy_futures_funding_fees(
        &self,
        symbol: SymbolScope,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        let mut params = HashMap::new();
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        let value = self
            .send_signed_get(
                "phemex.get_legacy_futures_funding_fees",
                "/api-data/futures/funding-fees",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_legacy_futures_funding_fees",
            value,
        ))
    }

    pub async fn get_futures_trade_account_details(
        &self,
        currency: &str,
        detail_type: Option<&str>,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        offset: Option<u32>,
        limit: Option<u32>,
        with_count: Option<bool>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        insert_optional_str(&mut params, "type", detail_type)?;
        insert_history_params(&mut params, start_ms, end_ms, offset, limit);
        insert_optional_bool(&mut params, "withCount", with_count);
        let value = self
            .send_signed_get(
                "phemex.get_futures_trade_account_details",
                "/api-data/futures/v2/tradeAccountDetail",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_futures_trade_account_details",
            value,
        ))
    }

    pub async fn get_exchange_order_list(
        &self,
        symbol: SymbolScope,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        offset: Option<u32>,
        limit: Option<u32>,
        order_status: Option<&str>,
        with_count: Option<bool>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = self.history_symbol_params(symbol, start_ms, end_ms, offset, limit)?;
        insert_optional_str(&mut params, "ordStatus", order_status)?;
        insert_optional_bool(&mut params, "withCount", with_count);
        let value = self
            .send_signed_get(
                "phemex.get_exchange_order_list",
                "/exchange/order/list",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_exchange_order_list",
            value,
        ))
    }

    pub async fn get_exchange_trade_list(
        &self,
        symbol: SymbolScope,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        offset: Option<u32>,
        limit: Option<u32>,
        with_count: Option<bool>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = self.history_symbol_params(symbol, start_ms, end_ms, offset, limit)?;
        insert_optional_bool(&mut params, "withCount", with_count);
        let value = self
            .send_signed_get(
                "phemex.get_exchange_trade_list",
                "/exchange/order/trade",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_exchange_trade_list",
            value,
        ))
    }

    pub async fn get_exchange_orders_by_ids(
        &self,
        symbol: SymbolScope,
        order_ids: &[String],
        client_order_ids: &[String],
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        self.insert_symbol_param(&mut params, symbol)?;
        if !order_ids.is_empty() {
            params.insert("orderID".to_string(), order_ids.join(","));
        }
        if !client_order_ids.is_empty() {
            params.insert("clOrdID".to_string(), client_order_ids.join(","));
        }
        if !params.contains_key("orderID") && !params.contains_key("clOrdID") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "phemex get_exchange_orders_by_ids requires ids".to_string(),
            });
        }
        let value = self
            .send_signed_get(
                "phemex.get_exchange_orders_by_ids",
                "/exchange/order",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_exchange_orders_by_ids",
            value,
        ))
    }

    pub async fn get_exchange_order_v2_list(
        &self,
        symbol: Option<SymbolScope>,
        currency: Option<&str>,
        order_status: Option<&str>,
        order_type: Option<&str>,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        offset: Option<u32>,
        limit: Option<u32>,
        with_count: Option<bool>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            self.insert_symbol_param(&mut params, symbol)?;
        }
        insert_optional_str(&mut params, "currency", currency)?;
        insert_optional_str(&mut params, "ordStatus", order_status)?;
        insert_optional_str(&mut params, "ordType", order_type)?;
        insert_history_params(&mut params, start_ms, end_ms, offset, limit);
        insert_optional_bool(&mut params, "withCount", with_count);
        let value = self
            .send_signed_get(
                "phemex.get_exchange_order_v2_list",
                "/exchange/order/v2/orderList",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_exchange_order_v2_list",
            value,
        ))
    }

    pub async fn get_exchange_trade_v2_list(
        &self,
        symbol: Option<SymbolScope>,
        currency: Option<&str>,
        exec_type: Option<&str>,
        offset: Option<u32>,
        limit: Option<u32>,
        with_count: Option<bool>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            self.insert_symbol_param(&mut params, symbol)?;
        }
        insert_optional_str(&mut params, "currency", currency)?;
        insert_optional_str(&mut params, "execType", exec_type)?;
        if let Some(offset) = offset {
            params.insert("offset".to_string(), offset.to_string());
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.min(200).to_string());
        }
        insert_optional_bool(&mut params, "withCount", with_count);
        let value = self
            .send_signed_get(
                "phemex.get_exchange_trade_v2_list",
                "/exchange/order/v2/tradingList",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_exchange_trade_v2_list",
            value,
        ))
    }

    pub async fn get_margin_wallets(
        &self,
        currency: Option<&str>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        insert_optional_str(&mut params, "currency", currency)?;
        let value = self
            .send_signed_get(
                "phemex.get_margin_wallets",
                "/margin-trade/wallets",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_margin_wallets",
            value,
        ))
    }

    pub async fn get_margin_orders(
        &self,
        symbol: SymbolScope,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        self.insert_symbol_param(&mut params, symbol)?;
        let value = self
            .send_signed_get("phemex.get_margin_orders", "/margin-trade/orders", &params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_margin_orders",
            value,
        ))
    }

    pub async fn get_margin_active_order(
        &self,
        symbol: SymbolScope,
        order_id: Option<&str>,
        client_order_id: Option<&str>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        self.insert_symbol_param(&mut params, symbol)?;
        insert_optional_str(&mut params, "orderID", order_id)?;
        insert_optional_str(&mut params, "clOrdID", client_order_id)?;
        if !params.contains_key("orderID") && !params.contains_key("clOrdID") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "phemex get_margin_active_order requires order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .send_signed_get(
                "phemex.get_margin_active_order",
                "/margin-trade/orders/active",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_margin_active_order",
            value,
        ))
    }

    pub async fn create_margin_order(
        &self,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let value = self
            .send_signed_put(
                "phemex.create_margin_order",
                "/margin-trade/orders/create",
                params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.create_margin_order",
            value,
        ))
    }

    pub async fn cancel_margin_order(
        &self,
        symbol: SymbolScope,
        order_id: Option<&str>,
        client_order_id: Option<&str>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        self.insert_symbol_param(&mut params, symbol)?;
        insert_optional_str(&mut params, "orderID", order_id)?;
        insert_optional_str(&mut params, "clOrdID", client_order_id)?;
        if !params.contains_key("orderID") && !params.contains_key("clOrdID") {
            return Err(ExchangeApiError::InvalidRequest {
                message: "phemex cancel_margin_order requires order_id or client_order_id"
                    .to_string(),
            });
        }
        let value = self
            .send_signed_delete(
                "phemex.cancel_margin_order",
                "/margin-trade/orders",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.cancel_margin_order",
            value,
        ))
    }

    pub async fn cancel_all_margin_orders(
        &self,
        symbol: SymbolScope,
        untriggered: Option<bool>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        self.insert_symbol_param(&mut params, symbol)?;
        insert_optional_bool(&mut params, "untriggered", untriggered);
        let value = self
            .send_signed_delete(
                "phemex.cancel_all_margin_orders",
                "/margin-trade/orders/all",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.cancel_all_margin_orders",
            value,
        ))
    }

    pub async fn get_margin_borrow_history(
        &self,
        currencies: &[String],
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        page_num: Option<u32>,
        page_size: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let params = margin_history_params(currencies, start_ms, end_ms, page_num, page_size);
        let value = self
            .send_signed_get(
                "phemex.get_margin_borrow_history",
                "/margin/borrow",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_margin_borrow_history",
            value,
        ))
    }

    pub async fn create_margin_borrow(
        &self,
        currency: &str,
        amount: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        params.insert("amountRv".to_string(), non_empty("amount", amount)?);
        let value = self
            .send_signed_post("phemex.create_margin_borrow", "/margin/borrow", &params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.create_margin_borrow",
            value,
        ))
    }

    pub async fn get_margin_borrow_interest_history(
        &self,
        currencies: &[String],
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        page_num: Option<u32>,
        page_size: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let params = margin_history_params(currencies, start_ms, end_ms, page_num, page_size);
        let value = self
            .send_signed_get(
                "phemex.get_margin_borrow_interest_history",
                "/margin/borrow/interests",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_margin_borrow_interest_history",
            value,
        ))
    }

    pub async fn get_margin_payback_history(
        &self,
        currencies: &[String],
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        page_num: Option<u32>,
        page_size: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let params = margin_history_params(currencies, start_ms, end_ms, page_num, page_size);
        let value = self
            .send_signed_get(
                "phemex.get_margin_payback_history",
                "/margin/payback",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_margin_payback_history",
            value,
        ))
    }

    pub async fn create_margin_payback(
        &self,
        currency: &str,
        amount: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        params.insert("amountRv".to_string(), non_empty("amount", amount)?);
        let value = self
            .send_signed_post("phemex.create_margin_payback", "/margin/payback", &params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.create_margin_payback",
            value,
        ))
    }

    pub async fn get_margin_order_history(
        &self,
        symbol: SymbolScope,
        order_status: Option<&str>,
        order_type: Option<&str>,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        page_num: Option<u32>,
        page_size: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params =
            margin_symbol_history_params(symbol, start_ms, end_ms, page_num, page_size, self)?;
        insert_optional_str(&mut params, "ordStatus", order_status)?;
        insert_optional_str(&mut params, "ordType", order_type)?;
        let value = self
            .send_signed_get("phemex.get_margin_order_history", "/margin/orders", &params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_margin_order_history",
            value,
        ))
    }

    pub async fn get_margin_trade_history(
        &self,
        symbol: SymbolScope,
        exec_type: Option<&str>,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        page_num: Option<u32>,
        page_size: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params =
            margin_symbol_history_params(symbol, start_ms, end_ms, page_num, page_size, self)?;
        insert_optional_str(&mut params, "execType", exec_type)?;
        let value = self
            .send_signed_get(
                "phemex.get_margin_trade_history",
                "/margin/orders/trades",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_margin_trade_history",
            value,
        ))
    }

    pub async fn get_uta_risk_mode(&self) -> ExchangeApiResult<PhemexPrivateAck> {
        let value = self
            .send_signed_get(
                "phemex.get_uta_risk_mode",
                "/uta-api/risk/risk-mode",
                &HashMap::new(),
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_uta_risk_mode",
            value,
        ))
    }

    pub async fn switch_uta_risk_mode(
        &self,
        risk_mode: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("riskMode".to_string(), non_empty("risk_mode", risk_mode)?);
        let value = self
            .send_signed_post(
                "phemex.switch_uta_risk_mode",
                "/uta-account/switch-mode",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.switch_uta_risk_mode",
            value,
        ))
    }

    pub async fn get_uta_risk_units(
        &self,
        currency: &str,
        risk_type: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert("currency".to_string(), non_empty("currency", currency)?);
        params.insert("riskType".to_string(), non_empty("risk_type", risk_type)?);
        let value = self
            .send_signed_get(
                "phemex.get_uta_risk_units",
                "/uta-api/risk/risk-units",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_uta_risk_units",
            value,
        ))
    }

    pub async fn get_uta_assets(
        &self,
        currency: Option<&str>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        insert_optional_str(&mut params, "currency", currency)?;
        let value = self
            .send_signed_get("phemex.get_uta_assets", "/uta-biz/assets", &params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_uta_assets",
            value,
        ))
    }

    pub async fn get_uta_convert_assets(
        &self,
        from_currency: &str,
        to_currency: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let mut params = HashMap::new();
        params.insert(
            "fromCurrency".to_string(),
            non_empty("from_currency", from_currency)?,
        );
        params.insert(
            "toCurrency".to_string(),
            non_empty("to_currency", to_currency)?,
        );
        let value = self
            .send_signed_get(
                "phemex.get_uta_convert_assets",
                "/uta-exchanger/assets/convert",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_uta_convert_assets",
            value,
        ))
    }

    pub async fn get_uta_contract_borrow_history(
        &self,
        currencies: &[String],
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        page_num: Option<u32>,
        page_size: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let params = margin_history_params(currencies, start_ms, end_ms, page_num, page_size);
        let value = self
            .send_signed_get(
                "phemex.get_uta_contract_borrow_history",
                "/uta-funds/contract/borrow",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_uta_contract_borrow_history",
            value,
        ))
    }

    pub async fn get_uta_contract_borrow_interest_history(
        &self,
        currencies: &[String],
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        page_num: Option<u32>,
        page_size: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let params = margin_history_params(currencies, start_ms, end_ms, page_num, page_size);
        let value = self
            .send_signed_get(
                "phemex.get_uta_contract_borrow_interest_history",
                "/uta-funds/contract/borrow/interests",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_uta_contract_borrow_interest_history",
            value,
        ))
    }

    pub async fn get_uta_contract_payback_history(
        &self,
        currencies: &[String],
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        page_num: Option<u32>,
        page_size: Option<u32>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let params = margin_history_params(currencies, start_ms, end_ms, page_num, page_size);
        let value = self
            .send_signed_get(
                "phemex.get_uta_contract_payback_history",
                "/uta-funds/contract/payback",
                &params,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_uta_contract_payback_history",
            value,
        ))
    }

    pub async fn create_uta_contract_payback(
        &self,
        body: &Value,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let value = self
            .send_signed_post_json(
                "phemex.create_uta_contract_payback",
                "/uta-funds/contract/payback",
                body,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.create_uta_contract_payback",
            value,
        ))
    }

    pub async fn get_account_transfer_history(&self) -> ExchangeApiResult<PhemexPrivateAck> {
        let value = self
            .send_signed_get(
                "phemex.get_account_transfer_history",
                "/wallets/account/transfer",
                &HashMap::new(),
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.get_account_transfer_history",
            value,
        ))
    }

    pub async fn create_account_transfer(
        &self,
        body: &Value,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let value = self
            .send_signed_post_json(
                "phemex.create_account_transfer",
                "/wallets/account/transfer",
                body,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.create_account_transfer",
            value,
        ))
    }

    pub async fn create_main_sub_account_transfer(
        &self,
        body: &Value,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        let value = self
            .send_signed_post_json(
                "phemex.create_main_sub_account_transfer",
                "/wallets/account/main-sub-transfer",
                body,
            )
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.create_main_sub_account_transfer",
            value,
        ))
    }

    pub async fn get_signed_raw(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        validate_raw_endpoint(endpoint)?;
        let value = self
            .send_signed_get("phemex.raw_signed_get", endpoint, params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.raw_signed_get",
            value,
        ))
    }

    pub async fn post_signed_raw(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        validate_raw_endpoint(endpoint)?;
        let value = self
            .send_signed_post("phemex.raw_signed_post", endpoint, params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.raw_signed_post",
            value,
        ))
    }

    pub async fn post_signed_json_raw(
        &self,
        endpoint: &str,
        body: &Value,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        validate_raw_endpoint(endpoint)?;
        let value = self
            .send_signed_post_json("phemex.raw_signed_post_json", endpoint, body)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.raw_signed_post_json",
            value,
        ))
    }

    pub async fn put_signed_raw(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        validate_raw_endpoint(endpoint)?;
        let value = self
            .send_signed_put("phemex.raw_signed_put", endpoint, params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.raw_signed_put",
            value,
        ))
    }

    pub async fn delete_signed_raw(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        validate_raw_endpoint(endpoint)?;
        let value = self
            .send_signed_delete("phemex.raw_signed_delete", endpoint, params)
            .await?;
        Ok(phemex_private_ack(
            &self.exchange_id,
            "phemex.raw_signed_delete",
            value,
        ))
    }

    fn insert_symbol_param(
        &self,
        params: &mut HashMap<String, String>,
        symbol: SymbolScope,
    ) -> ExchangeApiResult<()> {
        self.ensure_exchange(&symbol.exchange)?;
        self.ensure_supported_market_type(symbol.market_type)?;
        params.insert(
            "symbol".to_string(),
            normalize_phemex_symbol(&symbol.exchange_symbol.symbol, symbol.market_type)?,
        );
        Ok(())
    }

    fn history_symbol_params(
        &self,
        symbol: SymbolScope,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        offset: Option<u32>,
        limit: Option<u32>,
    ) -> ExchangeApiResult<HashMap<String, String>> {
        let mut params = HashMap::new();
        self.insert_symbol_param(&mut params, symbol)?;
        insert_history_params(&mut params, start_ms, end_ms, offset, limit);
        Ok(params)
    }

    pub async fn set_dead_man_switch(
        &self,
        _timeout_seconds: u64,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        Err(ExchangeApiError::Unsupported {
            operation: "phemex.dead_man_switch.no_official_endpoint",
        })
    }

    pub async fn set_manual_risk_limit(
        &self,
        _symbol: SymbolScope,
        _risk_limit: &str,
    ) -> ExchangeApiResult<PhemexPrivateAck> {
        Err(ExchangeApiError::Unsupported {
            operation: "phemex.manual_risk_limit.deprecated",
        })
    }
}

fn insert_history_params(
    params: &mut HashMap<String, String>,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    offset: Option<u32>,
    limit: Option<u32>,
) {
    if let Some(start_ms) = start_ms {
        params.insert("start".to_string(), start_ms.to_string());
    }
    if let Some(end_ms) = end_ms {
        params.insert("end".to_string(), end_ms.to_string());
    }
    if let Some(offset) = offset {
        params.insert("offset".to_string(), offset.to_string());
    }
    if let Some(limit) = limit {
        params.insert("limit".to_string(), limit.min(200).to_string());
    }
}

fn insert_optional_str(
    params: &mut HashMap<String, String>,
    key: &str,
    value: Option<&str>,
) -> ExchangeApiResult<()> {
    if let Some(value) = value {
        params.insert(key.to_string(), non_empty(key, value)?);
    }
    Ok(())
}

fn insert_optional_bool(params: &mut HashMap<String, String>, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        params.insert(key.to_string(), value.to_string());
    }
}

fn margin_history_params(
    currencies: &[String],
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    page_num: Option<u32>,
    page_size: Option<u32>,
) -> HashMap<String, String> {
    let mut params = HashMap::new();
    if !currencies.is_empty() {
        params.insert("currency".to_string(), currencies.join(","));
    }
    if let Some(start_ms) = start_ms {
        params.insert("start".to_string(), start_ms.to_string());
    }
    if let Some(end_ms) = end_ms {
        params.insert("end".to_string(), end_ms.to_string());
    }
    if let Some(page_num) = page_num {
        params.insert("pageNum".to_string(), page_num.to_string());
    }
    if let Some(page_size) = page_size {
        params.insert("pageSize".to_string(), page_size.min(200).to_string());
    }
    params
}

fn margin_symbol_history_params(
    symbol: SymbolScope,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    page_num: Option<u32>,
    page_size: Option<u32>,
    adapter: &PhemexGatewayAdapter,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    adapter.insert_symbol_param(&mut params, symbol)?;
    if let Some(start_ms) = start_ms {
        params.insert("start".to_string(), start_ms.to_string());
    }
    if let Some(end_ms) = end_ms {
        params.insert("end".to_string(), end_ms.to_string());
    }
    if let Some(page_num) = page_num {
        params.insert("pageNum".to_string(), page_num.to_string());
    }
    if let Some(page_size) = page_size {
        params.insert("pageSize".to_string(), page_size.min(200).to_string());
    }
    Ok(params)
}

fn insert_json_string(body: &mut Value, key: &str, value: Option<&str>) -> ExchangeApiResult<()> {
    if let Some(value) = value {
        body[key] = json!(non_empty(key, value)?);
    }
    Ok(())
}

fn validate_raw_endpoint(endpoint: &str) -> ExchangeApiResult<()> {
    if endpoint.starts_with('/') && !endpoint.contains('?') && !endpoint.contains("..") {
        return Ok(());
    }
    Err(ExchangeApiError::InvalidRequest {
        message: "phemex raw endpoint must be an absolute path without query text".to_string(),
    })
}

fn phemex_amend_order_params(
    request: &AmendOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_phemex_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    if let Some(order_id) = request.exchange_order_id.as_deref() {
        params.insert(
            "orderID".to_string(),
            non_empty("exchange_order_id", order_id)?,
        );
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert(
            "origClOrdID".to_string(),
            non_empty("client_order_id", client_order_id)?,
        );
    }
    if !params.contains_key("orderID") && !params.contains_key("origClOrdID") {
        return Err(ExchangeApiError::InvalidRequest {
            message: "phemex amend_order requires exchange_order_id or client_order_id".to_string(),
        });
    }
    if request.symbol.market_type == MarketType::Spot {
        params.insert(
            "baseQtyEv".to_string(),
            decimal_to_scaled_text("new_quantity", &request.new_quantity, 8)?,
        );
    } else {
        params.insert(
            "orderQtyRq".to_string(),
            non_empty("new_quantity", &request.new_quantity)?,
        );
        params.insert("posSide".to_string(), "Merged".to_string());
    }
    Ok(params)
}

fn phemex_place_order_params(
    request: &PlaceOrderRequest,
) -> ExchangeApiResult<HashMap<String, String>> {
    let mut params = HashMap::new();
    params.insert(
        "symbol".to_string(),
        normalize_phemex_symbol(
            &request.symbol.exchange_symbol.symbol,
            request.symbol.market_type,
        )?,
    );
    params.insert("side".to_string(), phemex_side(request.side).to_string());
    params.insert(
        "ordType".to_string(),
        phemex_order_type(request.order_type).to_string(),
    );
    params.insert(
        "orderQtyRq".to_string(),
        non_empty("quantity", &request.quantity)?,
    );
    if request.symbol.market_type == MarketType::Perpetual {
        params.insert("posSide".to_string(), "Merged".to_string());
        params.insert("reduceOnly".to_string(), request.reduce_only.to_string());
    } else if request.reduce_only {
        return Err(ExchangeApiError::InvalidRequest {
            message: "phemex spot order does not support reduce_only".to_string(),
        });
    }
    if request.order_type.requires_limit_price() {
        let price = request
            .price
            .as_deref()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "phemex limit-style order requires price".to_string(),
            })?;
        params.insert("priceRp".to_string(), non_empty("price", price)?);
    }
    let tif = request
        .time_in_force
        .or_else(|| request.post_only.then_some(TimeInForce::GTX));
    if let Some(time_in_force) = tif {
        params.insert(
            "timeInForce".to_string(),
            phemex_time_in_force(time_in_force).to_string(),
        );
    }
    if let Some(client_order_id) = request.client_order_id.as_deref() {
        params.insert(
            "clOrdID".to_string(),
            non_empty("client_order_id", client_order_id)?,
        );
    }
    Ok(params)
}

fn phemex_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "Buy",
        OrderSide::Sell => "Sell",
    }
}

fn phemex_order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "Market",
        OrderType::StopMarket => "Stop",
        OrderType::StopLimit => "StopLimit",
        _ => "Limit",
    }
}

fn phemex_time_in_force(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::GTC => "GoodTillCancel",
        TimeInForce::IOC => "ImmediateOrCancel",
        TimeInForce::FOK => "FillOrKill",
        TimeInForce::GTX => "PostOnly",
    }
}

fn phemex_position_side(position_side: PositionSide) -> ExchangeApiResult<&'static str> {
    match position_side {
        PositionSide::Long => Ok("Long"),
        PositionSide::Short => Ok("Short"),
        PositionSide::Net | PositionSide::None => Ok("Merged"),
    }
}

fn non_empty(field: &str, value: &str) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("{field} must not be empty"),
        });
    }
    Ok(value.to_string())
}

fn decimal_to_scaled_text(field: &str, value: &str, scale: usize) -> ExchangeApiResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("{field} must not be empty"),
        });
    }
    let (sign, unsigned) = value
        .strip_prefix('-')
        .map(|text| ("-", text))
        .unwrap_or(("", value));
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty() || !whole.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("{field} must be a decimal number"),
        });
    }
    if !fraction.chars().all(|ch| ch.is_ascii_digit()) || fraction.len() > scale {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("{field} supports at most {scale} decimal places"),
        });
    }
    let mut digits = String::with_capacity(whole.len() + scale);
    digits.push_str(whole);
    digits.push_str(fraction);
    digits.push_str(&"0".repeat(scale - fraction.len()));
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Ok("0".to_string());
    }
    Ok(format!("{sign}{digits}"))
}

fn phemex_private_ack(
    exchange_id: &rustcta_types::ExchangeId,
    operation: &str,
    value: Value,
) -> PhemexPrivateAck {
    PhemexPrivateAck {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        operation: operation.to_string(),
        data: value.get("data").cloned(),
        raw: value,
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
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: value
            .get("data")
            .and_then(|data| data.get("orderID").or_else(|| data.get("orderId")))
            .and_then(|value| value.as_str().map(ToOwned::to_owned)),
        side: request.side,
        position_side: Some(PositionSide::None),
        order_type: request.order_type,
        time_in_force: request.time_in_force,
        status: OrderStatus::New,
        quantity: request.quantity.clone(),
        price: request.price.clone(),
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: request.reduce_only,
        post_only: request.post_only,
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
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
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: value
            .get("data")
            .and_then(|data| data.get("orderID").or_else(|| data.get("orderId")))
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
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
        updated_at: chrono::Utc::now(),
    }
}

fn order_state_from_quote_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &QuoteMarketOrderRequest,
    value: &Value,
) -> OrderState {
    OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: request.symbol.market_type,
        canonical_symbol: request.symbol.canonical_symbol.clone(),
        exchange_symbol: request.symbol.exchange_symbol.clone(),
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: value
            .get("data")
            .and_then(|data| data.get("orderID").or_else(|| data.get("orderId")))
            .and_then(|value| value.as_str().map(ToOwned::to_owned)),
        side: request.side,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Market,
        time_in_force: None,
        status: OrderStatus::New,
        quantity: "0".to_string(),
        price: None,
        filled_quantity: "0".to_string(),
        average_fill_price: None,
        reduce_only: false,
        post_only: false,
        created_at: Some(chrono::Utc::now()),
        updated_at: chrono::Utc::now(),
    }
}

fn order_state_from_cancel_ack(
    exchange_id: &rustcta_types::ExchangeId,
    request: &CancelOrderRequest,
    _value: &Value,
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
        updated_at: chrono::Utc::now(),
    }
}
