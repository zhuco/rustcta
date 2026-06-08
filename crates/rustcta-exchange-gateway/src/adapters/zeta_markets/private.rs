use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub(super) fn unsupported<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

pub(super) const BALANCES_UNSUPPORTED: &str =
    "zeta_markets.balances_require_solana_margin_account_sdk_audit";
pub(super) const POSITIONS_UNSUPPORTED: &str =
    "zeta_markets.positions_require_solana_margin_account_sdk_audit";
pub(super) const FEES_UNSUPPORTED: &str = "zeta_markets.fees_unavailable_after_venue_shutdown";
pub(super) const PLACE_ORDER_UNSUPPORTED: &str =
    "zeta_markets.place_order_unsupported_venue_ceased_operations";
pub(super) const CANCEL_ORDER_UNSUPPORTED: &str =
    "zeta_markets.cancel_order_unsupported_venue_ceased_operations";
pub(super) const AMEND_ORDER_UNSUPPORTED: &str = "zeta_markets.amend_order_unsupported";
pub(super) const ORDER_LIST_UNSUPPORTED: &str = "zeta_markets.order_list_unsupported";
pub(super) const BATCH_PLACE_UNSUPPORTED: &str =
    "zeta_markets.batch_place_orders_unsupported_venue_ceased_operations";
pub(super) const BATCH_CANCEL_UNSUPPORTED: &str =
    "zeta_markets.batch_cancel_orders_unsupported_venue_ceased_operations";
pub(super) const CANCEL_ALL_UNSUPPORTED: &str =
    "zeta_markets.cancel_all_orders_unsupported_venue_ceased_operations";
pub(super) const QUERY_ORDER_UNSUPPORTED: &str =
    "zeta_markets.query_order_requires_solana_margin_account_sdk_audit";
pub(super) const OPEN_ORDERS_UNSUPPORTED: &str =
    "zeta_markets.open_orders_require_solana_margin_account_sdk_audit";
pub(super) const RECENT_FILLS_UNSUPPORTED: &str =
    "zeta_markets.recent_fills_public_data_only_margin_account_api_not_runtime_mapped";
