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

pub use config::MyOkxGatewayConfig;

pub type MyOkxGatewayAdapter = super::okx::OkxGatewayAdapter;

pub fn new_adapter(config: MyOkxGatewayConfig) -> ExchangeApiResult<MyOkxGatewayAdapter> {
    MyOkxGatewayAdapter::new(config.into_okx_config())
}
