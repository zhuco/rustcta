use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::GatewayError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayOperation {
    GetStatus,
    GetCapabilities,
    GetBalances,
    GetPositions,
    GetSymbolRules,
    GetOrderBook,
    GetFees,
    GetFundingRates,
    PlaceOrder,
    PlaceQuoteMarketOrder,
    CancelOrder,
    AmendOrder,
    PlaceOrderList,
    BatchPlaceOrders,
    BatchCancelOrders,
    CancelAllOrders,
    QueryOrder,
    GetOpenOrders,
    GetRecentFills,
    GetSymbolAccountConfig,
    SetLeverage,
    SetMarginMode,
    SetPositionMode,
    ClosePosition,
    SetCountdownCancelAll,
    SubscribeBooks,
    SubscribePrivate,
}

impl GatewayOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GetStatus => "get_status",
            Self::GetCapabilities => "get_capabilities",
            Self::GetBalances => "get_balances",
            Self::GetPositions => "get_positions",
            Self::GetSymbolRules => "get_symbol_rules",
            Self::GetOrderBook => "get_order_book",
            Self::GetFees => "get_fees",
            Self::GetFundingRates => "get_funding_rates",
            Self::PlaceOrder => "place_order",
            Self::PlaceQuoteMarketOrder => "place_quote_market_order",
            Self::CancelOrder => "cancel_order",
            Self::AmendOrder => "amend_order",
            Self::PlaceOrderList => "place_order_list",
            Self::BatchPlaceOrders => "batch_place_orders",
            Self::BatchCancelOrders => "batch_cancel_orders",
            Self::CancelAllOrders => "cancel_all_orders",
            Self::QueryOrder => "query_order",
            Self::GetOpenOrders => "get_open_orders",
            Self::GetRecentFills => "get_recent_fills",
            Self::GetSymbolAccountConfig => "get_symbol_account_config",
            Self::SetLeverage => "set_leverage",
            Self::SetMarginMode => "set_margin_mode",
            Self::SetPositionMode => "set_position_mode",
            Self::ClosePosition => "close_position",
            Self::SetCountdownCancelAll => "set_countdown_cancel_all",
            Self::SubscribeBooks => "subscribe_books",
            Self::SubscribePrivate => "subscribe_private",
        }
    }
}

impl FromStr for GatewayOperation {
    type Err = GatewayError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().replace('-', "_").to_ascii_lowercase();
        match normalized.as_str() {
            "get_status" | "status" => Ok(Self::GetStatus),
            "get_capabilities" | "capabilities" => Ok(Self::GetCapabilities),
            "get_balances" | "balances" => Ok(Self::GetBalances),
            "get_positions" | "positions" => Ok(Self::GetPositions),
            "get_symbol_rules" | "symbol_rules" => Ok(Self::GetSymbolRules),
            "get_order_book" | "order_book" => Ok(Self::GetOrderBook),
            "get_fees" | "fees" => Ok(Self::GetFees),
            "get_funding_rates" | "funding_rates" | "funding" => Ok(Self::GetFundingRates),
            "place_order" => Ok(Self::PlaceOrder),
            "place_quote_market_order" | "quote_market_order" => Ok(Self::PlaceQuoteMarketOrder),
            "cancel_order" => Ok(Self::CancelOrder),
            "amend_order" | "modify_order" => Ok(Self::AmendOrder),
            "place_order_list" | "order_list" => Ok(Self::PlaceOrderList),
            "batch_place_orders" | "batch_place_order" | "batch_place" => {
                Ok(Self::BatchPlaceOrders)
            }
            "batch_cancel_orders" | "batch_cancel_order" | "batch_cancel" => {
                Ok(Self::BatchCancelOrders)
            }
            "cancel_all_orders" | "cancel_all" => Ok(Self::CancelAllOrders),
            "query_order" | "get_order" | "order" => Ok(Self::QueryOrder),
            "get_open_orders" | "open_orders" => Ok(Self::GetOpenOrders),
            "get_recent_fills" | "recent_fills" | "fills" => Ok(Self::GetRecentFills),
            "get_symbol_account_config" | "symbol_account_config" => {
                Ok(Self::GetSymbolAccountConfig)
            }
            "set_leverage" | "leverage" => Ok(Self::SetLeverage),
            "set_margin_mode" | "margin_mode" => Ok(Self::SetMarginMode),
            "set_position_mode" | "position_mode" => Ok(Self::SetPositionMode),
            "close_position" => Ok(Self::ClosePosition),
            "set_countdown_cancel_all" | "countdown_cancel_all" => Ok(Self::SetCountdownCancelAll),
            "subscribe_books" => Ok(Self::SubscribeBooks),
            "subscribe_private" | "private_stream" | "subscribe_private_stream" => {
                Ok(Self::SubscribePrivate)
            }
            _ => Err(GatewayError::UnsupportedOperation {
                operation: value.to_string(),
            }),
        }
    }
}
