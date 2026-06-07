use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    CancelOrderRequest, ExchangeApiError, ExchangeClient, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::ExchangeId;

use crate::{
    ensure_secret_free_serializable, BookSubscriptionAck, CredentialBoundary, GatewayError,
    GatewayExchangeStatus, GatewayIdentity, GatewayMode, GatewayProtocolRequest,
    GatewayProtocolResponse, GatewayRequestPayload, GatewayResponsePayload, GatewayStatus,
    GetCapabilitiesResponse, GetStatusResponse, LocalGateway, PrivateSubscriptionAck,
    SubscribeBooksResponse, SubscribePrivateResponse, GATEWAY_API_VERSION,
    GATEWAY_PROTOCOL_SCHEMA_VERSION,
};

pub mod binance;
pub mod bitget;
pub mod coinex;
pub mod gateio;
pub mod kucoin;
pub mod mexc;
pub mod okx;
pub mod paper;

pub use binance::BinanceGatewayConfig;
pub use bitget::BitgetGatewayConfig;
pub use coinex::CoinExGatewayConfig;
pub use gateio::GateIoGatewayConfig;
pub use kucoin::KuCoinGatewayConfig;
pub use mexc::MexcGatewayConfig;
pub use okx::OkxGatewayConfig;

#[cfg(test)]
mod paper_tests;

#[async_trait]
pub trait GatewayAdapter: ExchangeClient {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus;
}

#[derive(Clone)]
pub struct AdapterBackedGateway {
    identity: GatewayIdentity,
    adapters: Arc<RwLock<HashMap<ExchangeId, Arc<dyn GatewayAdapter>>>>,
}

impl AdapterBackedGateway {
    pub fn new(gateway_id: impl Into<String>) -> Self {
        Self {
            identity: GatewayIdentity {
                gateway_id: gateway_id.into(),
                mode: GatewayMode::Local,
                credential_boundary: CredentialBoundary::GatewayOnly,
                started_at: Utc::now(),
            },
            adapters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn paper_only(gateway_id: impl Into<String>) -> Result<Self, GatewayError> {
        let gateway = Self::new(gateway_id);
        gateway.register_paper_adapter()?;
        Ok(gateway)
    }

    pub fn with_named_adapters(
        gateway_id: impl Into<String>,
        adapters: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, GatewayError> {
        let gateway = Self::new(gateway_id);
        for adapter in adapters {
            gateway.register_named_adapter(adapter.as_ref())?;
        }
        Ok(gateway)
    }

    pub fn register_named_adapter(&self, adapter: &str) -> Result<(), GatewayError> {
        let adapter = adapter.trim().to_ascii_lowercase();
        match adapter.as_str() {
            "" => Ok(()),
            "paper" => self.register_paper_adapter(),
            "binance" => self.register_binance_public_adapter(None),
            "bitget" => self.register_bitget_public_adapter(None),
            "coinex" => self.register_coinex_public_adapter(None),
            "gate" | "gate.io" | "gateio" => self.register_gateio_public_adapter(None),
            "kucoin" => self.register_kucoin_public_adapter(None),
            "mexc" => self.register_mexc_public_adapter(None),
            "okx" => self.register_okx_public_adapter(None),
            other => Err(GatewayError::UnsupportedOperation {
                operation: format!("unknown gateway adapter {other}"),
            }),
        }
    }

    pub fn register_paper_adapter(&self) -> Result<(), GatewayError> {
        self.register_adapter(Arc::new(paper::PaperGatewayAdapter::default_paper()?))
    }

    pub fn register_binance_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = binance::BinanceGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_binance_adapter(config)
    }

    pub fn register_binance_adapter(
        &self,
        config: binance::BinanceGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            binance::BinanceGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_bitget_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = bitget::BitgetGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_bitget_adapter(config)
    }

    pub fn register_bitget_adapter(
        &self,
        config: bitget::BitgetGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            bitget::BitgetGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_coinex_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = coinex::CoinExGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_coinex_adapter(config)
    }

    pub fn register_coinex_adapter(
        &self,
        config: coinex::CoinExGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            coinex::CoinExGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_kucoin_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = kucoin::KuCoinGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_kucoin_adapter(config)
    }

    pub fn register_kucoin_adapter(
        &self,
        config: kucoin::KuCoinGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            kucoin::KuCoinGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_mexc_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = mexc::MexcGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_mexc_adapter(config)
    }

    pub fn register_mexc_adapter(
        &self,
        config: mexc::MexcGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            mexc::MexcGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_gateio_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = gateio::GateIoGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_gateio_adapter(config)
    }

    pub fn register_gateio_adapter(
        &self,
        config: gateio::GateIoGatewayConfig,
    ) -> Result<(), GatewayError> {
        let adapter =
            gateio::GateIoGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_okx_public_adapter(
        &self,
        rest_base_url: Option<String>,
    ) -> Result<(), GatewayError> {
        let mut config = okx::OkxGatewayConfig::default();
        if let Some(rest_base_url) = rest_base_url {
            config.rest_base_url = rest_base_url;
        }
        self.register_okx_adapter(config)
    }

    pub fn register_okx_adapter(&self, config: okx::OkxGatewayConfig) -> Result<(), GatewayError> {
        let adapter = okx::OkxGatewayAdapter::new(config).map_err(exchange_api_error_to_gateway)?;
        self.register_adapter(Arc::new(adapter))
    }

    pub fn register_adapter(&self, adapter: Arc<dyn GatewayAdapter>) -> Result<(), GatewayError> {
        let exchange = adapter.exchange();
        let mut adapters = self
            .adapters
            .write()
            .map_err(|_| GatewayError::Rejected("adapter registry lock poisoned".to_string()))?;
        if adapters.contains_key(&exchange) {
            return Err(GatewayError::Rejected(format!(
                "gateway adapter already registered for {exchange}"
            )));
        }
        adapters.insert(exchange, adapter);
        Ok(())
    }

    pub fn adapter_count(&self) -> Result<usize, GatewayError> {
        Ok(self
            .adapters
            .read()
            .map_err(|_| GatewayError::Rejected("adapter registry lock poisoned".to_string()))?
            .len())
    }

    fn adapter_for(&self, exchange: &ExchangeId) -> Result<Arc<dyn GatewayAdapter>, GatewayError> {
        self.adapters
            .read()
            .map_err(|_| GatewayError::Rejected("adapter registry lock poisoned".to_string()))?
            .get(exchange)
            .cloned()
            .ok_or_else(|| GatewayError::UnsupportedOperation {
                operation: format!("no gateway adapter registered for {exchange}"),
            })
    }
}

#[async_trait]
impl LocalGateway for AdapterBackedGateway {
    async fn status(&self) -> Result<GatewayStatus, GatewayError> {
        let adapters = self
            .adapters
            .read()
            .map_err(|_| GatewayError::Rejected("adapter registry lock poisoned".to_string()))?;
        let exchanges = adapters
            .values()
            .map(|adapter| adapter.gateway_exchange_status())
            .collect();
        Ok(GatewayStatus {
            api_version: GATEWAY_API_VERSION.to_string(),
            identity: self.identity.clone(),
            exchanges,
        })
    }

    async fn handle_typed(
        &self,
        request: GatewayProtocolRequest,
    ) -> Result<GatewayProtocolResponse, GatewayError> {
        request.validate()?;
        ensure_secret_free_serializable(&request, "request")?;

        let request_id = request.request_id.clone();
        let operation = request.operation;
        let payload = match request.payload {
            GatewayRequestPayload::GetStatus(request) => {
                let mut status = self.status().await?;
                if !request.include_exchanges {
                    status.exchanges.clear();
                }
                GatewayResponsePayload::Status(GetStatusResponse {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    status,
                })
            }
            GatewayRequestPayload::GetCapabilities(request) => {
                let capabilities = if request.exchanges.is_empty() {
                    self.adapters
                        .read()
                        .map_err(|_| {
                            GatewayError::Rejected("adapter registry lock poisoned".to_string())
                        })?
                        .values()
                        .map(|adapter| adapter.capabilities())
                        .collect()
                } else {
                    let mut capabilities = Vec::with_capacity(request.exchanges.len());
                    for exchange in request.exchanges {
                        capabilities.push(self.adapter_for(&exchange)?.capabilities());
                    }
                    capabilities
                };
                GatewayResponsePayload::Capabilities(GetCapabilitiesResponse {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    capabilities,
                })
            }
            GatewayRequestPayload::GetBalances(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::Balances(
                    adapter
                        .get_balances(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::GetPositions(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::Positions(
                    adapter
                        .get_positions(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::GetOrderBook(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::OrderBook(
                    adapter
                        .get_order_book(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::GetSymbolRules(request) => {
                let exchange = request
                    .symbols
                    .first()
                    .map(|symbol| symbol.exchange.clone())
                    .ok_or_else(|| {
                        GatewayError::Rejected(
                            "get_symbol_rules requires at least one symbol scope".to_string(),
                        )
                    })?;
                let adapter = self.adapter_for(&exchange)?;
                GatewayResponsePayload::SymbolRules(
                    adapter
                        .get_symbol_rules(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::GetFees(request) => {
                let exchange = request
                    .symbols
                    .first()
                    .map(|symbol| symbol.exchange.clone())
                    .ok_or_else(|| {
                        GatewayError::Rejected(
                            "get_fees requires at least one symbol scope".to_string(),
                        )
                    })?;
                let adapter = self.adapter_for(&exchange)?;
                GatewayResponsePayload::Fees(
                    adapter
                        .get_fees(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::PlaceOrder(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::PlaceOrder(
                    adapter
                        .place_order(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::CancelOrder(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::CancelOrder(
                    adapter
                        .cancel_order(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::BatchPlaceOrders(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::BatchPlaceOrders(
                    adapter
                        .batch_place_orders(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::BatchCancelOrders(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::BatchCancelOrders(
                    adapter
                        .batch_cancel_orders(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::CancelAllOrders(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::CancelAllOrders(
                    adapter
                        .cancel_all_orders(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::QueryOrder(request) => {
                let adapter = self.adapter_for(&request.symbol.exchange)?;
                GatewayResponsePayload::QueryOrder(
                    adapter
                        .query_order(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::GetOpenOrders(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::OpenOrders(
                    adapter
                        .get_open_orders(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::GetRecentFills(request) => {
                let adapter = self.adapter_for(&request.exchange)?;
                GatewayResponsePayload::RecentFills(
                    adapter
                        .get_recent_fills(request)
                        .await
                        .map_err(exchange_api_error_to_gateway)?,
                )
            }
            GatewayRequestPayload::SubscribeBooks(request) => {
                if request.subscriptions.is_empty() {
                    return Err(GatewayError::Rejected(
                        "subscribe_books requires at least one subscription".to_string(),
                    ));
                }

                let mut subscriptions = Vec::with_capacity(request.subscriptions.len());
                for subscription in request.subscriptions {
                    let adapter = self.adapter_for(&subscription.symbol.exchange)?;
                    let subscription_id = adapter
                        .subscribe_public_stream(subscription.clone())
                        .await
                        .map_err(exchange_api_error_to_gateway)?;
                    subscriptions.push(BookSubscriptionAck {
                        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                        subscription_id,
                        exchange: subscription.symbol.exchange,
                        market_type: subscription.symbol.market_type,
                        canonical_symbol: subscription.symbol.canonical_symbol,
                        exchange_symbol: subscription.symbol.exchange_symbol,
                        kind: subscription.kind,
                        subscribed_at: Utc::now(),
                    });
                }

                GatewayResponsePayload::BooksSubscribed(SubscribeBooksResponse {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    subscriptions,
                })
            }
            GatewayRequestPayload::SubscribePrivate(request) => {
                let mut subscriptions = Vec::with_capacity(request.subscriptions.len());
                for subscription in request.subscriptions {
                    let adapter = self.adapter_for(&subscription.exchange)?;
                    let subscription_id = adapter
                        .subscribe_private_stream(subscription.clone())
                        .await
                        .map_err(exchange_api_error_to_gateway)?;
                    subscriptions.push(PrivateSubscriptionAck {
                        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                        subscription_id,
                        exchange: subscription.exchange,
                        market_type: subscription.market_type,
                        account_id: subscription.account_id,
                        kind: subscription.kind,
                        subscribed_at: Utc::now(),
                    });
                }

                GatewayResponsePayload::PrivateSubscribed(SubscribePrivateResponse {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    subscriptions,
                })
            }
        };

        let response = GatewayProtocolResponse::accepted(request_id, operation, payload);
        ensure_secret_free_serializable(&response, "response")?;
        Ok(response)
    }
}

fn exchange_api_error_to_gateway(error: ExchangeApiError) -> GatewayError {
    match error {
        ExchangeApiError::Unsupported { operation } => GatewayError::UnsupportedOperation {
            operation: operation.to_string(),
        },
        ExchangeApiError::InvalidRequest { message }
        | ExchangeApiError::Serialization { message }
        | ExchangeApiError::Transport { message } => GatewayError::Rejected(message),
        ExchangeApiError::Exchange(error) => GatewayError::Rejected(error.message),
    }
}

pub(crate) fn missing_order_identity(request: &CancelOrderRequest) -> bool {
    request.client_order_id.is_none() && request.exchange_order_id.is_none()
}

pub(crate) fn response_metadata(
    exchange: ExchangeId,
    request_id: Option<String>,
) -> rustcta_exchange_api::ResponseMetadata {
    rustcta_exchange_api::ResponseMetadata {
        request_id,
        ..rustcta_exchange_api::ResponseMetadata::new(exchange, Utc::now())
    }
}

pub(crate) fn ensure_exchange_api_schema(schema_version: u16) -> Result<(), ExchangeApiError> {
    if schema_version != EXCHANGE_API_SCHEMA_VERSION {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!(
                "unsupported exchange schema_version {}, expected {}",
                schema_version, EXCHANGE_API_SCHEMA_VERSION
            ),
        });
    }
    Ok(())
}
