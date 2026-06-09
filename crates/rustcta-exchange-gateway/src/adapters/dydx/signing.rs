#![allow(dead_code)]

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub const NODE_WRITE_PROJECT_UNIMPLEMENTED: &str =
    "dydx.node_write_requires_wallet_signer_account_sequence_subaccount_guard_dry_run_and_indexer_reconciliation";
pub const AMEND_PROJECT_UNIMPLEMENTED: &str =
    "dydx.amend_requires_wallet_signer_account_sequence_subaccount_guard_dry_run_and_indexer_reconciliation";
pub const BATCH_PLACE_PROJECT_UNIMPLEMENTED: &str =
    "dydx.batch_place_requires_batch_tx_builder_wallet_signer_sequence_partial_parser_dry_run_and_indexer_reconciliation";
pub const BATCH_CANCEL_PROJECT_UNIMPLEMENTED: &str =
    "dydx.batch_cancel_requires_batch_tx_builder_wallet_signer_sequence_partial_parser_dry_run_and_indexer_reconciliation";

pub fn unsupported_node_signing_boundary() -> ExchangeApiResult<()> {
    Err(ExchangeApiError::Unsupported {
        operation: "dydx.node_signing_requires_wallet_subaccount_and_cosmos_tx_authenticator",
    })
}

pub fn node_write_project_unimplemented(operation: &'static str) -> ExchangeApiError {
    ExchangeApiError::Unsupported { operation }
}

#[cfg(test)]
mod tests {
    use super::{node_write_project_unimplemented, unsupported_node_signing_boundary};

    #[test]
    fn dydx_node_signing_boundary_should_be_explicit() {
        let error = unsupported_node_signing_boundary().expect_err("unsupported");
        assert!(error
            .to_string()
            .contains("dydx.node_signing_requires_wallet_subaccount"));
    }

    #[test]
    fn dydx_node_write_runtime_boundary_should_be_project_unimplemented() {
        let error = node_write_project_unimplemented(super::NODE_WRITE_PROJECT_UNIMPLEMENTED);
        assert!(error.to_string().contains("account_sequence"));
        assert!(error.to_string().contains("indexer_reconciliation"));
    }
}
