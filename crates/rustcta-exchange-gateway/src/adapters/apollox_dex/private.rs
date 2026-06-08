use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub(super) fn unsupported<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

pub(super) const BALANCES_UNSUPPORTED: &str =
    "apollox_dex.private_rest_runtime_disabled_balances_request_spec_only";
pub(super) const POSITIONS_UNSUPPORTED: &str =
    "apollox_dex.private_rest_runtime_disabled_positions_request_spec_only";
pub(super) const FEES_UNSUPPORTED: &str =
    "apollox_dex.fee_readback_requires_private_rest_runtime_audit";
pub(super) const PLACE_ORDER_UNSUPPORTED: &str =
    "apollox_dex.place_order_request_spec_only_no_live_writes";
pub(super) const CANCEL_ORDER_UNSUPPORTED: &str =
    "apollox_dex.cancel_order_request_spec_only_no_live_writes";
pub(super) const AMEND_ORDER_UNSUPPORTED: &str = "apollox_dex.amend_order_unsupported";
pub(super) const ORDER_LIST_UNSUPPORTED: &str = "apollox_dex.order_list_unsupported";
pub(super) const BATCH_PLACE_UNSUPPORTED: &str = "apollox_dex.batch_place_unsupported";
pub(super) const BATCH_CANCEL_UNSUPPORTED: &str = "apollox_dex.batch_cancel_unsupported";
pub(super) const CANCEL_ALL_UNSUPPORTED: &str =
    "apollox_dex.cancel_all_request_spec_only_no_live_writes";
pub(super) const QUERY_ORDER_UNSUPPORTED: &str =
    "apollox_dex.query_order_private_rest_runtime_disabled";
pub(super) const OPEN_ORDERS_UNSUPPORTED: &str =
    "apollox_dex.open_orders_private_rest_runtime_disabled";
pub(super) const RECENT_FILLS_UNSUPPORTED: &str =
    "apollox_dex.recent_fills_private_rest_runtime_disabled";
