use serde::{Deserialize, Deserializer};
use serde_json::Value;
use serde_with::{serde_as, DisplayFromStr};
use std::str::FromStr;

fn deserialize_string_to_i32<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    i32::from_str(&s).map_err(serde::de::Error::custom)
}

fn deserialize_string_to_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    i64::from_str(&s).map_err(serde::de::Error::custom)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfo {
    pub fee_tier: i32,
    pub can_trade: bool,
    pub can_deposit: bool,
    pub can_withdraw: bool,
    pub update_time: i64,
    pub total_initial_margin: String,
    pub total_maint_margin: String,
    pub total_wallet_balance: String,
    pub total_unrealized_profit: String,
    pub total_margin_balance: String,
    pub total_position_initial_margin: String,
    pub total_open_order_initial_margin: String,
    pub total_cross_wallet_balance: String,
    pub total_cross_unpnl: String,
    pub available_balance: String,
    pub max_withdraw_amount: String,
    pub assets: Vec<Asset>,
    pub positions: Vec<Position>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Asset {
    pub asset: String,
    pub wallet_balance: String,
    pub unrealized_profit: String,
    pub margin_balance: String,
    pub maint_margin: String,
    pub initial_margin: String,
    pub position_initial_margin: String,
    pub open_order_initial_margin: String,
    pub cross_wallet_balance: String,
    pub cross_un_pnl: String,
    pub available_balance: String,
    pub max_withdraw_amount: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub symbol: String,
    pub initial_margin: String,
    pub maint_margin: String,
    pub unrealized_profit: String,
    pub position_initial_margin: String,
    pub open_order_initial_margin: String,
    pub leverage: String,
    pub isolated: bool,
    pub entry_price: String,
    pub max_notional: String,
    pub position_side: String,
    pub position_amt: String,
}

#[serde_as]
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    pub order_id: i64,
    pub symbol: String,
    pub status: String,
    pub client_order_id: String,
    #[serde_as(as = "DisplayFromStr")]
    pub price: f64,
    #[serde_as(as = "DisplayFromStr")]
    pub avg_price: f64,
    #[serde_as(as = "DisplayFromStr")]
    pub orig_qty: f64,
    #[serde_as(as = "DisplayFromStr")]
    pub executed_qty: f64,
    #[serde_as(as = "DisplayFromStr")]
    pub cum_quote: f64,
    pub time_in_force: String,
    #[serde(rename = "type")]
    pub order_type: String,
    pub side: String,
    #[serde_as(as = "DisplayFromStr")]
    pub stop_price: f64,
    pub time: Option<i64>,
    pub update_time: i64,
    pub working_type: String,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub activate_price: Option<f64>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub price_rate: Option<f64>,
    pub orig_type: String,
    pub position_side: String,
    pub close_position: bool,
    pub price_protect: bool,
    pub reduce_only: bool,
}

#[derive(Debug, Deserialize)]
pub struct Kline {
    pub open_time: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub close_time: i64,
    pub quote_asset_volume: String,
    pub number_of_trades: i64,
    pub taker_buy_base_asset_volume: String,
    pub taker_buy_quote_asset_volume: String,
    pub ignore: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeInfo {
    pub timezone: String,
    pub server_time: i64,
    pub rate_limits: Vec<RateLimit>,
    pub exchange_filters: Vec<Value>,
    pub symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateLimit {
    pub rate_limit_type: String,
    pub interval: String,
    pub interval_num: i64,
    pub limit: i64,
}

#[serde_as]
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SymbolInfo {
    pub symbol: String,
    pub pair: String,
    pub contract_type: String,
    pub delivery_date: i64,
    pub onboard_date: i64,
    pub status: String,
    pub maint_margin_percent: String,
    pub required_margin_percent: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub margin_asset: String,
    pub price_precision: i32,
    pub quantity_precision: i32,
    pub base_asset_precision: i32,
    pub quote_precision: i32,
    pub underlying_type: String,
    pub underlying_sub_type: Vec<String>,
    pub settle_plan: Option<i64>,
    pub trigger_protect: String,
    pub filters: Vec<Filter>,
    pub order_types: Vec<String>,
    pub time_in_force: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "filterType", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Filter {
    #[serde(rename_all = "camelCase")]
    PriceFilter {
        min_price: String,
        max_price: String,
        tick_size: String,
    },
    #[serde(rename_all = "camelCase")]
    LotSize {
        min_qty: String,
        max_qty: String,
        step_size: String,
    },
    #[serde(rename_all = "camelCase")]
    MarketLotSize {
        min_qty: String,
        max_qty: String,
        step_size: String,
    },
    MaxNumOrders {
        limit: i64,
    },
    MaxNumAlgoOrders {
        limit: i64,
    },
    MinNotional {
        notional: String,
    },
    #[serde(rename_all = "camelCase")]
    PercentPrice {
        multiplier_up: String,
        multiplier_down: String,
        #[serde(deserialize_with = "deserialize_string_to_i32")]
        multiplier_decimal: i32,
    },
    #[serde(rename_all = "camelCase")]
    PositionRiskControl {
        position_control_side: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "e")]
pub enum WsEvent {
    #[serde(rename = "ORDER_TRADE_UPDATE")]
    OrderTradeUpdate {
        #[serde(rename = "E")]
        event_time: i64,
        #[serde(rename = "T")]
        transaction_time: i64,
        #[serde(rename = "o")]
        order: OrderUpdate,
    },
    #[serde(rename = "ACCOUNT_UPDATE")]
    AccountUpdate {
        #[serde(rename = "E")]
        event_time: i64,
        #[serde(rename = "T")]
        transaction_time: i64,
        #[serde(rename = "a")]
        account_update: AccountUpdateData,
    },
    #[serde(rename = "kline")]
    Kline {
        #[serde(rename = "E")]
        event_time: i64,
        #[serde(rename = "s")]
        symbol: String,
        #[serde(rename = "k")]
        kline: WsKline,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountUpdateData {
    #[serde(rename = "m")]
    pub event_reason_type: String,
    #[serde(rename = "B")]
    pub balances: Vec<WsBalance>,
    #[serde(rename = "P")]
    pub positions: Vec<WsPosition>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsBalance {
    #[serde(rename = "a")]
    pub asset: String,
    #[serde(rename = "wb")]
    pub wallet_balance: String,
    #[serde(rename = "cw")]
    pub cross_wallet_balance: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsPosition {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "pa")]
    pub position_amount: String,
    #[serde(rename = "ep")]
    pub entry_price: String,
    #[serde(rename = "cr")]
    pub pre_fee_unrealized_pnl: String,
    #[serde(rename = "up")]
    pub unrealized_pnl: String,
    #[serde(rename = "mt")]
    pub margin_type: String,
    #[serde(rename = "iw")]
    pub isolated_wallet: String,
    #[serde(rename = "ps")]
    pub position_side: String,
}

#[serde_as]
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderUpdate {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub client_order_id: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "o")]
    pub order_type: String,
    #[serde(rename = "f")]
    pub time_in_force: String,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "q")]
    pub original_quantity: f64,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "p")]
    pub original_price: f64,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "ap")]
    pub average_price: f64,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "sp")]
    pub stop_price: f64,
    #[serde(rename = "x")]
    pub execution_type: String,
    #[serde(rename = "X")]
    pub order_status: String,
    #[serde(rename = "i")]
    pub order_id: i64,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "l")]
    pub order_last_filled_quantity: f64,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "z")]
    pub order_filled_accumulated_quantity: f64,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "L")]
    pub last_filled_price: f64,
    #[serde(rename = "N")]
    pub commission_asset: String,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "n")]
    pub commission_amount: f64,
    #[serde(rename = "T")]
    pub order_trade_time: i64,
    #[serde(rename = "t")]
    pub trade_id: i64,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "b")]
    pub bids_notional: f64,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "a")]
    pub asks_notional: f64,
    #[serde(rename = "m")]
    pub is_maker: bool,
    #[serde(rename = "R")]
    pub reduce_only: bool,
    #[serde(rename = "wt")]
    pub working_type: String,
    #[serde(rename = "ot")]
    pub original_order_type: String,
    #[serde(rename = "ps")]
    pub position_side: String,
    #[serde(rename = "cp")]
    pub close_all: bool,
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "AP")]
    pub activation_price: Option<f64>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "cr")]
    pub callback_rate: Option<f64>,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "rp")]
    pub realized_profit: f64,
    #[serde(rename = "pP")]
    pub price_protect: Option<bool>,
    #[serde(rename = "si")]
    pub stop_price_working_type: Option<i32>,
    #[serde(rename = "ss")]
    pub stop_price_working_type_2: Option<i32>,
    #[serde(rename = "V")]
    pub self_trade_prevention_mode: Option<String>,
    #[serde(rename = "pm")]
    pub price_match: Option<String>,
    #[serde(rename = "gtd")]
    pub good_till_date: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsKline {
    #[serde(rename = "t")]
    pub start_time: i64,
    #[serde(rename = "T")]
    pub close_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "i")]
    pub interval: String,
    #[serde(rename = "f")]
    pub first_trade_id: i64,
    #[serde(rename = "L")]
    pub last_trade_id: i64,
    #[serde(rename = "o")]
    pub open_price: String,
    #[serde(rename = "c")]
    pub close_price: String,
    #[serde(rename = "h")]
    pub high_price: String,
    #[serde(rename = "l")]
    pub low_price: String,
    #[serde(rename = "v")]
    pub base_asset_volume: String,
    #[serde(rename = "n")]
    pub number_of_trades: i64,
    #[serde(rename = "x")]
    pub is_closed: bool,
    #[serde(rename = "q")]
    pub quote_asset_volume: String,
    #[serde(rename = "V")]
    pub taker_buy_base_asset_volume: String,
    #[serde(rename = "Q")]
    pub taker_buy_quote_asset_volume: String,
    #[serde(rename = "B")]
    pub ignore: String,
}

/// 资金费率信息
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FundingRate {
    pub symbol: String,
    #[serde(deserialize_with = "deserialize_string_to_f64")]
    pub mark_price: f64,
    #[serde(deserialize_with = "deserialize_string_to_f64")]
    pub index_price: f64,
    #[serde(deserialize_with = "deserialize_string_to_f64")]
    pub estimated_settle_price: f64,
    #[serde(deserialize_with = "deserialize_string_to_f64")]
    pub last_funding_rate: f64,
    #[serde(deserialize_with = "deserialize_string_to_f64")]
    pub interest_rate: f64,
    pub next_funding_time: i64,
    pub time: i64,
}

fn deserialize_string_to_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    s.parse::<f64>().map_err(serde::de::Error::custom)
}
