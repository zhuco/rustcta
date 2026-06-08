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
            request_timeout_ms: self.request_timeout_ms,
            enabled: self.enabled,
            api_key: None,
            api_secret: None,
            passphrase: None,
            enabled_private_rest: false,
            status_message:
                "okxus OKX US spot public REST profile; private trading unsupported pending US credential/product audit"
                    .to_string(),
            unsupported_market_type_operation: "okxus.non_spot_market_type",
        }
    }
}
