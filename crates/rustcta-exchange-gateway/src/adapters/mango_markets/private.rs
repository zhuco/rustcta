use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub(super) fn unsupported<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

pub(super) const SYMBOL_RULES_UNSUPPORTED: &str =
    "mango_markets.symbol_rules_require_sdk_or_rpc_scan";
pub(super) const ORDER_BOOK_UNSUPPORTED: &str =
    "mango_markets.order_book_requires_onchain_book_and_event_queue_audit";
pub(super) const BALANCES_UNSUPPORTED: &str =
    "mango_markets.balances_require_mango_account_health_audit";
pub(super) const POSITIONS_UNSUPPORTED: &str =
    "mango_markets.positions_require_mango_account_perp_audit";
pub(super) const FEES_UNSUPPORTED: &str = "mango_markets.fees_require_group_risk_parameter_audit";
pub(super) const PLACE_ORDER_UNSUPPORTED: &str =
    "mango_markets.place_order_requires_solana_wallet_transaction_signing_audit";
pub(super) const CANCEL_ORDER_UNSUPPORTED: &str =
    "mango_markets.cancel_order_requires_solana_wallet_transaction_signing_audit";
pub(super) const AMEND_ORDER_UNSUPPORTED: &str =
    "mango_markets.amend_order_is_cancel_replace_transaction_semantics";
pub(super) const ORDER_LIST_UNSUPPORTED: &str =
    "mango_markets.order_list_unsupported_onchain_transaction_semantics";
pub(super) const BATCH_PLACE_UNSUPPORTED: &str =
    "mango_markets.batch_place_orders_require_atomic_transaction_audit";
pub(super) const BATCH_CANCEL_UNSUPPORTED: &str =
    "mango_markets.batch_cancel_orders_require_atomic_transaction_audit";
pub(super) const CANCEL_ALL_UNSUPPORTED: &str =
    "mango_markets.cancel_all_requires_open_orders_scan_and_transaction_audit";
pub(super) const QUERY_ORDER_UNSUPPORTED: &str =
    "mango_markets.query_order_requires_openbook_event_queue_audit";
pub(super) const OPEN_ORDERS_UNSUPPORTED: &str =
    "mango_markets.open_orders_require_mango_account_and_book_scan";
pub(super) const RECENT_FILLS_UNSUPPORTED: &str =
    "mango_markets.recent_fills_require_event_queue_and_indexer_audit";
