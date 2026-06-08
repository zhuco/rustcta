use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};

pub(super) fn waves_unsupported<T>(operation: &'static str) -> ExchangeApiResult<T> {
    Err(ExchangeApiError::Unsupported { operation })
}

pub(super) const BALANCES_UNSUPPORTED: &str =
    "wavesexchange.balances_require_waves_address_and_node_balance_mapping";
pub(super) const POSITIONS_UNSUPPORTED: &str = "wavesexchange.positions_not_applicable_to_spot";
pub(super) const FEES_UNSUPPORTED: &str =
    "wavesexchange.matcher_fee_rates_are_asset_specific_not_gateway_account_fee_rates";
pub(super) const PLACE_ORDER_UNSUPPORTED: &str =
    "wavesexchange.place_order_requires_waves_order_curve25519_signing";
pub(super) const CANCEL_ORDER_UNSUPPORTED: &str =
    "wavesexchange.cancel_order_requires_waves_public_key_signature";
pub(super) const AMEND_ORDER_UNSUPPORTED: &str = "wavesexchange.amend_order_unsupported";
pub(super) const ORDER_LIST_UNSUPPORTED: &str = "wavesexchange.order_list_unsupported";
pub(super) const BATCH_PLACE_UNSUPPORTED: &str = "wavesexchange.batch_place_orders_not_native";
pub(super) const BATCH_CANCEL_UNSUPPORTED: &str = "wavesexchange.batch_cancel_orders_not_native";
pub(super) const CANCEL_ALL_UNSUPPORTED: &str = "wavesexchange.cancel_all_orders_not_native";
pub(super) const QUERY_ORDER_UNSUPPORTED: &str =
    "wavesexchange.query_order_requires_pair_and_order_id_mapping";
pub(super) const OPEN_ORDERS_UNSUPPORTED: &str =
    "wavesexchange.open_orders_require_waves_public_key_signature";
pub(super) const RECENT_FILLS_UNSUPPORTED: &str =
    "wavesexchange.recent_fills_require_waves_public_key_signature";
