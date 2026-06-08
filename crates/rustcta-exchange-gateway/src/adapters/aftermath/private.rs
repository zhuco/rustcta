use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub(super) fn sui_transaction_unsupported<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

pub(super) const BALANCES_UNSUPPORTED: &str = "aftermath.balances_require_sui_account_cap";
pub(super) const POSITIONS_UNSUPPORTED: &str = "aftermath.positions_require_sui_account_cap";
pub(super) const FEES_UNSUPPORTED: &str = "aftermath.fees_not_mapped_to_gateway_account";
pub(super) const PLACE_ORDER_UNSUPPORTED: &str =
    "aftermath.place_order_requires_sui_transaction_signing";
pub(super) const CANCEL_ORDER_UNSUPPORTED: &str =
    "aftermath.cancel_order_requires_sui_transaction_signing";
pub(super) const AMEND_ORDER_UNSUPPORTED: &str = "aftermath.amend_order_unsupported";
pub(super) const ORDER_LIST_UNSUPPORTED: &str = "aftermath.order_list_unsupported";
pub(super) const BATCH_PLACE_UNSUPPORTED: &str =
    "aftermath.batch_place_orders_requires_sui_transaction_signing";
pub(super) const BATCH_CANCEL_UNSUPPORTED: &str =
    "aftermath.batch_cancel_orders_requires_sui_transaction_signing";
pub(super) const CANCEL_ALL_UNSUPPORTED: &str = "aftermath.cancel_all_orders_not_native";
pub(super) const QUERY_ORDER_UNSUPPORTED: &str = "aftermath.query_order_requires_sui_account_scope";
pub(super) const OPEN_ORDERS_UNSUPPORTED: &str = "aftermath.open_orders_require_sui_account_scope";
pub(super) const RECENT_FILLS_UNSUPPORTED: &str =
    "aftermath.recent_fills_require_sui_account_scope";
