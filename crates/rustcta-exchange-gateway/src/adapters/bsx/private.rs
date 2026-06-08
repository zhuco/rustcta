use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub(super) fn unsupported<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

pub(super) const BALANCES_UNSUPPORTED: &str = "bsx.balances_require_account_scope_audit";
pub(super) const POSITIONS_UNSUPPORTED: &str = "bsx.positions_require_account_scope_audit";
pub(super) const FEES_UNSUPPORTED: &str = "bsx.fees_not_mapped_to_gateway_account";
pub(super) const PLACE_ORDER_UNSUPPORTED: &str =
    "bsx.place_order_requires_eip712_wallet_signature_audit";
pub(super) const CANCEL_ORDER_UNSUPPORTED: &str =
    "bsx.cancel_order_requires_cancel_v2_semantics_audit";
pub(super) const AMEND_ORDER_UNSUPPORTED: &str = "bsx.amend_order_unsupported";
pub(super) const ORDER_LIST_UNSUPPORTED: &str = "bsx.conditional_order_list_not_mapped_to_gateway";
pub(super) const BATCH_PLACE_UNSUPPORTED: &str =
    "bsx.batch_place_orders_requires_eip712_wallet_signature_audit";
pub(super) const BATCH_CANCEL_UNSUPPORTED: &str =
    "bsx.batch_cancel_orders_requires_cancel_v2_semantics_audit";
pub(super) const CANCEL_ALL_UNSUPPORTED: &str =
    "bsx.cancel_all_orders_requires_cancel_v2_semantics_audit";
pub(super) const QUERY_ORDER_UNSUPPORTED: &str = "bsx.query_order_requires_account_scope_audit";
pub(super) const OPEN_ORDERS_UNSUPPORTED: &str = "bsx.open_orders_require_account_scope_audit";
pub(super) const RECENT_FILLS_UNSUPPORTED: &str = "bsx.recent_fills_require_account_scope_audit";
