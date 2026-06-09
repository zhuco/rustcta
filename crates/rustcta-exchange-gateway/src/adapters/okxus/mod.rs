use rustcta_exchange_api::ExchangeApiResult;

mod config;
mod parser;
mod private;
mod public;
mod signing;
mod streams;
#[cfg(test)]
mod tests;
mod transport;

pub use config::OkxusGatewayConfig;

pub type OkxusGatewayAdapter = super::okx::OkxGatewayAdapter;

pub fn new_adapter(config: OkxusGatewayConfig) -> ExchangeApiResult<OkxusGatewayAdapter> {
    OkxusGatewayAdapter::new(config.into_okx_config())
}

impl OkxusGatewayConfig {
    pub fn into_okx_config(self) -> super::okx::OkxGatewayConfig {
        super::okx::OkxGatewayConfig {
            exchange_id: "okxus".to_string(),
            rest_base_url: self.rest_base_url,
            public_ws_url: self.public_ws_url,
            request_timeout_ms: self.request_timeout_ms,
            enabled: self.enabled,
            api_key: self.api_key,
            api_secret: self.api_secret,
            passphrase: self.passphrase,
            enabled_private_rest: self.enabled_private_rest,
            enabled_public_streams: true,
            status_message:
                "okxus OKX US Spot profile; private REST reuses OKX runtime when US credentials and explicit enable flag are configured"
                    .to_string(),
            unsupported_market_type_operation: "okxus.non_spot_market_type",
        }
    }
}
