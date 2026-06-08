#![allow(dead_code)]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub fn unsupported_node_signing_boundary() -> ExchangeApiResult<()> {
    Err(ExchangeApiError::Unsupported {
        operation: "dydx.node_signing_requires_wallet_subaccount_and_cosmos_tx_authenticator",
    })
}

#[cfg(test)]
mod tests {
    use super::unsupported_node_signing_boundary;

    #[test]
    fn dydx_node_signing_boundary_should_be_explicit() {
        let error = unsupported_node_signing_boundary().expect_err("unsupported");
        assert!(error
            .to_string()
            .contains("dydx.node_signing_requires_wallet_subaccount"));
    }
}
