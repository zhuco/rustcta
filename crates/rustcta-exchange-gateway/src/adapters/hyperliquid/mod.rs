use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse, BatchAtomicity,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchCapability, BatchExecutionMode,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, CapabilitySupport,
    CredentialScope, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, HistoryCapability, OpenOrdersRequest,
    OpenOrdersResponse, OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamCapabilities, PrivateStreamSubscription, PublicStreamSubscription,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, SymbolRulesRequest, SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType, OrderType, TimeInForce};

use super::GatewayAdapter;
use crate::GatewayExchangeStatus;

mod config;
mod parser;
mod private;
mod private_parser;
mod public;
mod signing;
mod streams;
#[cfg(test)]
mod tests;
mod transport;

pub use config::HyperliquidGatewayConfig;
#[allow(unused_imports)]
pub use private::{
    HyperliquidCancelAction, HyperliquidCancelWire, HyperliquidOrderAction, HyperliquidOrderWire,
    HyperliquidScheduleCancelAction,
};
#[allow(unused_imports)]
pub use signing::HyperliquidPrivateCredentials;
use transport::HyperliquidRest;

#[derive(Clone)]
pub struct HyperliquidGatewayAdapter {
    exchange_id: ExchangeId,
    config: HyperliquidGatewayConfig,
    rest: HyperliquidRest,
    next_nonce: Arc<AtomicU64>,
    asset_ids: Arc<RwLock<HashMap<String, u32>>>,
}

impl HyperliquidGatewayAdapter {
    pub fn new(config: HyperliquidGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id =
            ExchangeId::new("hyperliquid").map_err(|error| ExchangeApiError::InvalidRequest {
                message: error.to_string(),
            })?;
        let rest = HyperliquidRest::new(
            exchange_id.clone(),
            config.rest_base_url.clone(),
            config.request_timeout_ms,
        )?;
        Ok(Self {
            exchange_id,
            config,
            rest,
            next_nonce: Arc::new(AtomicU64::new(Utc::now().timestamp_millis() as u64)),
            asset_ids: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "hyperliquid adapter cannot serve request for exchange {exchange}"
                ),
            });
        }
        Ok(())
    }

    fn ensure_perpetual(
        &self,
        market_type: MarketType,
        operation: &'static str,
    ) -> ExchangeApiResult<()> {
        if market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok(())
    }

    fn ensure_optional_perpetual(
        &self,
        market_type: Option<MarketType>,
        operation: &'static str,
    ) -> ExchangeApiResult<()> {
        if market_type.is_some_and(|market_type| market_type != MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok(())
    }

    fn ensure_private_rest(&self, operation: &'static str) -> ExchangeApiResult<()> {
        if !self.config.private_rest_available() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok(())
    }

    fn private_credentials(
        &self,
        operation: &'static str,
    ) -> ExchangeApiResult<HyperliquidPrivateCredentials> {
        self.credentials()
            .ok_or(ExchangeApiError::Unsupported { operation })
    }

    fn credentials(&self) -> Option<HyperliquidPrivateCredentials> {
        HyperliquidPrivateCredentials::from_config(&self.config)
    }

    fn account_address(&self, operation: &'static str) -> ExchangeApiResult<String> {
        self.config
            .normalized_account_address()
            .ok_or(ExchangeApiError::Unsupported { operation })
    }

    fn context_account(
        &self,
        context: &rustcta_exchange_api::RequestContext,
        operation: &'static str,
    ) -> ExchangeApiResult<(
        rustcta_exchange_api::TenantId,
        rustcta_exchange_api::AccountId,
    )> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("{operation} requires context.tenant_id"),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("{operation} requires context.account_id"),
                })?;
        Ok((tenant_id, account_id))
    }

    async fn asset_id_for_symbol(&self, symbol: &str) -> ExchangeApiResult<u32> {
        let coin = parser::normalize_hyperliquid_coin(symbol)?;
        if let Some(asset_id) = self.asset_ids.read().expect("asset_ids").get(&coin) {
            return Ok(*asset_id);
        }
        let value = self.get_hyperliquid_meta().await?;
        let mut map = HashMap::new();
        if let Some(universe) = value.get("universe").and_then(serde_json::Value::as_array) {
            for (index, item) in universe.iter().enumerate() {
                if let Some(name) = item.get("name").and_then(serde_json::Value::as_str) {
                    map.insert(name.to_ascii_uppercase(), index as u32);
                }
            }
        }
        let Some(asset_id) = map.get(&coin).copied() else {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("Hyperliquid asset id not found for {coin}"),
            });
        };
        *self.asset_ids.write().expect("asset_ids") = map;
        Ok(asset_id)
    }
}

#[async_trait]
impl GatewayAdapter for HyperliquidGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some(
                "hyperliquid USDC perpetual REST/WS gateway adapter; DEX wallet signing stays inside gateway"
                    .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for HyperliquidGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private = self.config.private_rest_available();
        let private_streams = self.config.private_streams_available();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private;
        capabilities.supports_public_streams = self.config.enabled_public_streams;
        capabilities.supports_private_streams = private_streams;
        capabilities.private_stream_capabilities = if private_streams {
            Some(streams::hyperliquid_private_stream_capabilities())
        } else {
            Some(PrivateStreamCapabilities::unsupported(
                EXCHANGE_API_SCHEMA_VERSION,
            ))
        };
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private;
        capabilities.supports_positions = private;
        capabilities.supports_fees = false;
        capabilities.supports_place_order = private;
        capabilities.supports_cancel_order = private;
        capabilities.supports_cancel_all_orders = private;
        capabilities.supports_query_order = private;
        capabilities.supports_open_orders = private;
        capabilities.supports_recent_fills = private;
        capabilities.supports_batch_place_order = private;
        capabilities.supports_batch_cancel_order = private;
        capabilities.supports_quote_market_order = false;
        capabilities.supports_amend_order = false;
        capabilities.supports_order_list = false;
        capabilities.supports_client_order_id = true;
        capabilities.supports_reduce_only = true;
        capabilities.supports_post_only = true;
        capabilities.supports_time_in_force =
            vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::GTX];
        capabilities.supports_order_types = vec![
            OrderType::Market,
            OrderType::Limit,
            OrderType::PostOnly,
            OrderType::IOC,
        ];
        capabilities.max_order_book_depth = Some(20);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(20));
        capabilities.max_recent_fill_limit = Some(500);
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.batch_place_orders = if private {
            BatchCapability {
                support: CapabilitySupport::native(),
                mode: BatchExecutionMode::Native,
                atomicity: BatchAtomicity::Partial,
                max_items: None,
                same_symbol_required: false,
                same_market_type_required: true,
                supports_client_order_id: true,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported("Hyperliquid batch place requires API wallet signing key")
        };
        capabilities.capabilities_v2.batch_cancel_orders = if private {
            BatchCapability {
                support: CapabilitySupport::native(),
                mode: BatchExecutionMode::Native,
                atomicity: BatchAtomicity::Partial,
                max_items: None,
                same_symbol_required: false,
                same_market_type_required: true,
                supports_client_order_id: true,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported("Hyperliquid batch cancel requires API wallet signing key")
        };
        capabilities.capabilities_v2.fills_history = HistoryCapability {
            support: if private {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported("Hyperliquid fills require account address")
            },
            supports_since: true,
            supports_until: true,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: Some(500),
            max_window_ms: None,
        };
        capabilities.capabilities_v2.public_streams = CapabilitySupport::native();
        capabilities.capabilities_v2.private_streams = if private_streams {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("Hyperliquid private streams require account address")
        };
        capabilities.capabilities_v2.stream_runtime =
            streams::hyperliquid_stream_runtime_capability(private_streams);
        capabilities.capabilities_v2.credential_scopes = if private {
            vec![CredentialScope::ReadOnly, CredentialScope::Trade]
        } else {
            Vec::new()
        };
        capabilities.apply_v2_to_legacy_flags();
        capabilities
    }

    async fn get_symbol_rules(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        self.get_symbol_rules_impl(request).await
    }

    async fn get_order_book(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        self.get_order_book_impl(request).await
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.get_balances_impl(request).await
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.get_positions_impl(request).await
    }

    async fn get_fees(&self, request: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        let _ = request;
        Err(ExchangeApiError::Unsupported {
            operation: "hyperliquid.fee_schedule_source_boundary_only",
        })
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.place_order_impl(request).await
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.cancel_order_impl(request).await
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.batch_place_orders_impl(request).await
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.batch_cancel_orders_impl(request).await
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.cancel_all_orders_impl(request).await
    }

    async fn query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.query_order_impl(request).await
    }

    async fn get_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        self.get_open_orders_impl(request).await
    }

    async fn get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        self.get_recent_fills_impl(request).await
    }

    async fn place_quote_market_order(
        &self,
        _request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "hyperliquid.quote_sized_market_orders_unsupported",
        })
    }

    async fn amend_order(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "hyperliquid.shared_amend_requires_price_and_asset_specific_order_body",
        })
    }

    async fn place_order_list(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "hyperliquid.oco_oto_order_lists_unsupported",
        })
    }

    async fn subscribe_public_stream(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.subscribe_public_stream_impl(subscription).await
    }

    async fn subscribe_private_stream(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.subscribe_private_stream_impl(subscription).await
    }
}
