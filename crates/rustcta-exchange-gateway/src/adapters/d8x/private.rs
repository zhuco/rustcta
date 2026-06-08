use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub(super) fn unsupported<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

pub(super) const BALANCES_UNSUPPORTED: &str = "d8x.balances_require_wallet_contract_audit";
pub(super) const POSITIONS_UNSUPPORTED: &str = "d8x.positions_require_wallet_contract_audit";
pub(super) const FEES_UNSUPPORTED: &str = "d8x.fees_not_mapped_to_gateway_account";
pub(super) const PLACE_ORDER_UNSUPPORTED: &str =
    "d8x.place_order_requires_evm_signer_and_contract_audit";
pub(super) const CANCEL_ORDER_UNSUPPORTED: &str =
    "d8x.cancel_order_requires_evm_signer_and_contract_audit";
pub(super) const AMEND_ORDER_UNSUPPORTED: &str = "d8x.amend_order_unsupported";
pub(super) const ORDER_LIST_UNSUPPORTED: &str = "d8x.conditional_orders_require_sdk_contract_audit";
pub(super) const BATCH_PLACE_UNSUPPORTED: &str =
    "d8x.batch_place_orders_not_mapped_to_gateway_contract_semantics";
pub(super) const BATCH_CANCEL_UNSUPPORTED: &str =
    "d8x.batch_cancel_orders_not_mapped_to_gateway_contract_semantics";
pub(super) const CANCEL_ALL_UNSUPPORTED: &str = "d8x.cancel_all_orders_requires_contract_audit";
pub(super) const QUERY_ORDER_UNSUPPORTED: &str = "d8x.query_order_requires_contract_audit";
pub(super) const OPEN_ORDERS_UNSUPPORTED: &str = "d8x.open_orders_require_sdk_contract_audit";
pub(super) const RECENT_FILLS_UNSUPPORTED: &str = "d8x.recent_fills_require_indexer_contract_audit";
