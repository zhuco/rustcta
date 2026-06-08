use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub(super) fn unsupported<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

pub(super) const ORDER_BOOK_UNSUPPORTED: &str =
    "modetrade.order_book_requires_orderly_account_signed_request";
pub(super) const BALANCES_UNSUPPORTED: &str = "modetrade.balances_require_orderly_account_audit";
pub(super) const POSITIONS_UNSUPPORTED: &str = "modetrade.positions_require_orderly_account_audit";
pub(super) const FEES_UNSUPPORTED: &str = "modetrade.fees_not_mapped_to_gateway_account";
pub(super) const PLACE_ORDER_UNSUPPORTED: &str =
    "modetrade.place_order_requires_orderly_ed25519_account_audit";
pub(super) const CANCEL_ORDER_UNSUPPORTED: &str =
    "modetrade.cancel_order_requires_orderly_ed25519_account_audit";
pub(super) const AMEND_ORDER_UNSUPPORTED: &str = "modetrade.amend_order_unsupported";
pub(super) const ORDER_LIST_UNSUPPORTED: &str = "modetrade.order_list_unsupported";
pub(super) const BATCH_PLACE_UNSUPPORTED: &str =
    "modetrade.batch_place_orders_unverified_orderly_semantics";
pub(super) const BATCH_CANCEL_UNSUPPORTED: &str =
    "modetrade.batch_cancel_orders_unverified_orderly_semantics";
pub(super) const CANCEL_ALL_UNSUPPORTED: &str =
    "modetrade.cancel_all_orders_unverified_orderly_semantics";
pub(super) const QUERY_ORDER_UNSUPPORTED: &str =
    "modetrade.query_order_requires_orderly_account_audit";
pub(super) const OPEN_ORDERS_UNSUPPORTED: &str =
    "modetrade.open_orders_require_orderly_account_audit";
pub(super) const RECENT_FILLS_UNSUPPORTED: &str =
    "modetrade.recent_fills_require_orderly_account_audit";
