use rustcta_exchange_api::ExchangeApiResult;

mod config;
mod parser;
mod private;
mod public;
mod signing;
#[cfg(test)]
mod tests;
mod transport;

pub use config::BybiteuGatewayConfig;

pub type BybiteuGatewayAdapter = super::bybit::BybitGatewayAdapter;

pub fn new_adapter(config: BybiteuGatewayConfig) -> ExchangeApiResult<BybiteuGatewayAdapter> {
    BybiteuGatewayAdapter::new(config.into_bybit_config())
}

impl BybiteuGatewayConfig {
    pub fn into_bybit_config(self) -> super::bybit::BybitGatewayConfig {
        super::bybit::BybitGatewayConfig {
            exchange_id: "bybiteu".to_string(),
            rest_base_url: self.rest_base_url,
            public_ws_url: self.public_ws_url,
            private_ws_url: self.private_ws_url,
            api_key: self.api_key,
            api_secret: self.api_secret,
            recv_window_ms: self.recv_window_ms,
            request_timeout_ms: self.request_timeout_ms,
            enabled_private_rest: self.enabled_private_rest,
            enabled: self.enabled,
            status_message: "bybiteu Bybit EU V5 profile; private REST reuses Bybit V5 runtime when EU credentials and explicit enable flag are configured".to_string(),
            unsupported_market_type_operation: "bybiteu.unsupported_market_type",
        }
    }
}
