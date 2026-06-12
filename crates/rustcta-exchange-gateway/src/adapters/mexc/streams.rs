#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    BalancesResponse, ExchangeApiError, ExchangeApiResult, ExchangeStreamEvent, OrderState,
    PositionsResponse, PrivateOrderStreamEventKind, PrivateStreamCapabilities, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition, ExchangeSymbol,
    Fill, FillStatus, LiquidityRole, MarketType, OrderBookLevel, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, SchemaVersion, TimeInForce,
};
use serde_json::{json, Value};

use super::parser::{decimal_text_to_f64, normalize_mexc_symbol_for_market};
use super::signing::sign_raw_query;
use super::MexcGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::orderbook_state::{OrderBookDelta, OrderBookDeltaLevel};

const MEXC_SPOT_PUBLIC_WS_URL: &str = "wss://wbs-api.mexc.com/ws";
const MEXC_CONTRACT_PUBLIC_WS_URL: &str = "wss://contract.mexc.com/edge";

#[derive(Debug, Clone, PartialEq)]
pub struct MexcContractPrivateWsLoginSpec {
    pub url: String,
    pub login_payload: Value,
    pub default_subscribe: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MexcContractPrivateWsFilterSpec {
    pub url: String,
    pub filters: Vec<String>,
    pub filter_payload: Value,
    pub reset_payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MexcWsSubscriptionSpec {
    pub url: String,
    pub channel: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MexcBookTicker {
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub symbol: String,
    pub bid_price: f64,
    pub bid_quantity: f64,
    pub ask_price: f64,
    pub ask_quantity: f64,
    pub sequence: Option<u64>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
}

impl MexcGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_market_type(subscription.symbol.market_type)?;
        let spec = mexc_public_subscription_spec(&subscription)?;
        Ok(format!("mexc:{}:{}", spec.url, spec.channel))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if subscription
            .market_type
            .is_some_and(|market_type| market_type != MarketType::Perpetual)
        {
            return Err(ExchangeApiError::Unsupported {
                operation: "mexc.contract_private_stream.market_type",
            });
        }
        let (api_key, api_secret, _) = self.private_credentials("mexc.subscribe_private_stream")?;
        let login = mexc_contract_private_login_spec(
            api_key,
            api_secret,
            Utc::now().timestamp_millis(),
            false,
        )?;
        let filter = mexc_contract_private_filter_spec(&subscription)?;
        Ok(format!(
            "mexc:{}:{}:{}",
            login.url,
            login.login_payload["method"].as_str().unwrap_or("login"),
            filter.filters.join(",")
        ))
    }
}

pub fn mexc_contract_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: true,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
            PrivateOrderStreamEventKind::Reject,
            PrivateOrderStreamEventKind::Expired,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}

pub fn mexc_public_subscription_spec(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<MexcWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    match subscription.symbol.market_type {
        MarketType::Spot => mexc_spot_public_subscription_spec(subscription),
        MarketType::Perpetual => mexc_contract_public_subscription_spec(subscription),
        _ => Err(ExchangeApiError::Unsupported {
            operation: "mexc.public_stream.market_type",
        }),
    }
}

pub fn mexc_ping_payload() -> Value {
    json!({ "method": "ping" })
}

pub fn mexc_contract_private_login_spec(
    api_key: &str,
    api_secret: &str,
    req_time_ms: i64,
    default_subscribe: bool,
) -> ExchangeApiResult<MexcContractPrivateWsLoginSpec> {
    let req_time = req_time_ms.to_string();
    let signature = sign_raw_query(api_secret, &format!("{api_key}{req_time}"))?;
    let mut login_payload = json!({
        "method": "login",
        "param": {
            "apiKey": api_key,
            "reqTime": req_time,
            "signature": signature,
        },
    });
    if !default_subscribe {
        login_payload["subscribe"] = json!(false);
    }
    Ok(MexcContractPrivateWsLoginSpec {
        url: MEXC_CONTRACT_PUBLIC_WS_URL.to_string(),
        login_payload,
        default_subscribe,
    })
}

pub fn mexc_contract_private_filter_spec(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<MexcContractPrivateWsFilterSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription
        .market_type
        .is_some_and(|market_type| market_type != MarketType::Perpetual)
    {
        return Err(ExchangeApiError::Unsupported {
            operation: "mexc.contract_private_stream.market_type",
        });
    }
    let filters = match subscription.kind {
        PrivateStreamKind::Orders => vec!["order"],
        PrivateStreamKind::Fills => vec!["order.deal"],
        PrivateStreamKind::Balances => vec!["asset"],
        PrivateStreamKind::Positions => vec!["position"],
        PrivateStreamKind::Account => vec!["order", "order.deal", "position", "asset"],
    };
    let filter_values: Vec<Value> = filters
        .iter()
        .map(|filter| json!({ "filter": filter }))
        .collect();
    Ok(MexcContractPrivateWsFilterSpec {
        url: MEXC_CONTRACT_PUBLIC_WS_URL.to_string(),
        filters: filters.into_iter().map(str::to_string).collect(),
        filter_payload: json!({
            "method": "personal.filter",
            "param": {
                "filters": filter_values,
            },
        }),
        reset_payload: json!({
            "method": "personal.filter",
            "param": {
                "filters": [],
            },
        }),
    })
}

pub fn parse_mexc_private_stream_events(
    exchange_id: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let channel = value
        .get("channel")
        .or_else(|| value.get("method"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if channel == "rs.login"
        || channel == "pong"
        || value
            .get("data")
            .and_then(Value::as_str)
            .is_some_and(|data| data.eq_ignore_ascii_case("success"))
    {
        return Ok(vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange_id.clone(),
            received_at: Utc::now(),
        }]);
    }
    if channel == "rs.error" {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("mexc private stream error: {value}"),
        });
    }

    let data = value.get("data").unwrap_or(value);
    let is_order_deal_channel = channel.contains("personal.order.deal");
    let is_order_channel = channel.contains("personal.order");
    let is_asset_channel = channel.contains("personal.asset");
    let is_position_channel = channel.contains("personal.position");
    if is_order_deal_channel {
        return parse_mexc_private_fill_event(exchange_id, subscription, symbol_hint, data);
    }
    if is_order_channel
        || (!is_asset_channel
            && !is_position_channel
            && matches!(
                subscription.kind,
                PrivateStreamKind::Orders | PrivateStreamKind::Fills | PrivateStreamKind::Account
            ))
    {
        return parse_mexc_private_order_event(exchange_id, subscription, symbol_hint, data);
    }
    if is_asset_channel
        || (!is_position_channel
            && matches!(
                subscription.kind,
                PrivateStreamKind::Balances | PrivateStreamKind::Account
            ))
    {
        return parse_mexc_private_asset_event(exchange_id, subscription, data);
    }
    if is_position_channel
        || matches!(
            subscription.kind,
            PrivateStreamKind::Positions | PrivateStreamKind::Account
        )
    {
        return parse_mexc_private_position_event(exchange_id, subscription, symbol_hint, data);
    }
    Ok(Vec::new())
}

pub fn parse_mexc_spot_aggre_depth_delta(
    exchange_id: &ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookDelta> {
    let payload = value
        .get("publicAggreDepths")
        .or_else(|| value.get("publicaggredepths"))
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc ws depth parser requires canonical_symbol".to_string(),
            })?;
    let mut delta = OrderBookDelta::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        Utc::now(),
    );
    delta.bids = parse_delta_levels(
        payload
            .get("bids")
            .or_else(|| payload.get("b"))
            .or_else(|| payload.get("bid")),
    )?;
    delta.asks = parse_delta_levels(
        payload
            .get("asks")
            .or_else(|| payload.get("a"))
            .or_else(|| payload.get("ask")),
    )?;
    delta.first_sequence = first_u64(payload, &["fromVersion", "fromversion", "from_version"]);
    delta.last_sequence = first_u64(
        payload,
        &["toVersion", "toversion", "to_version", "version"],
    );
    delta.exchange_timestamp = first_i64(value, &["sendTime", "sendtime", "timestamp", "ts"])
        .or_else(|| first_i64(payload, &["sendTime", "sendtime", "timestamp", "ts"]))
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(delta)
}

pub fn parse_mexc_spot_limit_depth_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let payload = value
        .get("publicLimitDepths")
        .or_else(|| value.get("publiclimitdepths"))
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc ws limit-depth parser requires canonical_symbol".to_string(),
            })?;
    let bids = parse_snapshot_levels(
        payload
            .get("bids")
            .or_else(|| payload.get("b"))
            .or_else(|| payload.get("bid")),
    )?;
    let asks = parse_snapshot_levels(
        payload
            .get("asks")
            .or_else(|| payload.get("a"))
            .or_else(|| payload.get("ask")),
    )?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = first_u64(payload, &["version", "toVersion", "toversion"]);
    snapshot.exchange_timestamp = first_i64(value, &["sendTime", "sendtime", "timestamp", "ts"])
        .or_else(|| first_i64(payload, &["sendTime", "sendtime", "timestamp", "ts"]))
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn parse_mexc_contract_depth_delta(
    exchange_id: &ExchangeId,
    symbol: &rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookDelta> {
    let payload = value.get("data").unwrap_or(value);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc contract depth parser requires canonical_symbol".to_string(),
            })?;
    let version = first_u64(payload, &["version"]);
    let mut delta = OrderBookDelta::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        Utc::now(),
    );
    delta.bids = parse_delta_levels(payload.get("bids").or_else(|| payload.get("b")))?;
    delta.asks = parse_delta_levels(payload.get("asks").or_else(|| payload.get("a")))?;
    delta.first_sequence = version;
    delta.last_sequence = version;
    delta.exchange_timestamp = first_i64(value, &["ts", "timestamp"])
        .or_else(|| first_i64(payload, &["ts", "timestamp"]))
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(delta)
}

pub fn parse_mexc_contract_depth_snapshot(
    exchange_id: &ExchangeId,
    symbol: rustcta_exchange_api::SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookSnapshot> {
    let payload = value.get("data").unwrap_or(value);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc contract depth snapshot parser requires canonical_symbol"
                    .to_string(),
            })?;
    let bids = parse_snapshot_levels(payload.get("bids").or_else(|| payload.get("b")))?;
    let asks = parse_snapshot_levels(payload.get("asks").or_else(|| payload.get("a")))?;
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id.clone(),
        symbol.market_type,
        canonical_symbol,
        bids,
        asks,
        Utc::now(),
    )
    .map_err(validation_error)?;
    snapshot.exchange_symbol = Some(symbol.exchange_symbol);
    snapshot.sequence = first_u64(payload, &["version"]);
    snapshot.exchange_timestamp = first_i64(value, &["ts", "timestamp"])
        .or_else(|| first_i64(payload, &["ts", "timestamp"]))
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(snapshot)
}

pub fn parse_mexc_spot_book_ticker(
    exchange_id: &ExchangeId,
    market_type: MarketType,
    value: &Value,
) -> ExchangeApiResult<MexcBookTicker> {
    let payload = value
        .get("publicBookTicker")
        .or_else(|| value.get("publicbookticker"))
        .or_else(|| value.get("data"))
        .unwrap_or(value);
    let symbol = first_string(value, &["symbol", "s"])
        .or_else(|| first_string(payload, &["symbol", "s"]))
        .unwrap_or_default();
    Ok(MexcBookTicker {
        exchange_id: exchange_id.clone(),
        market_type,
        symbol,
        bid_price: required_f64(payload, &["bidPrice", "bidprice", "bid_price", "b"])?,
        bid_quantity: required_f64(payload, &["bidQuantity", "bidquantity", "bid_qty", "B"])?,
        ask_price: required_f64(payload, &["askPrice", "askprice", "ask_price", "a"])?,
        ask_quantity: required_f64(payload, &["askQuantity", "askquantity", "ask_qty", "A"])?,
        sequence: first_u64(payload, &["version", "toVersion", "toversion"]),
        exchange_timestamp: first_i64(value, &["sendTime", "sendtime", "timestamp", "ts"])
            .or_else(|| first_i64(payload, &["sendTime", "sendtime", "timestamp", "ts"]))
            .and_then(DateTime::<Utc>::from_timestamp_millis),
    })
}

fn mexc_spot_public_subscription_spec(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<MexcWsSubscriptionSpec> {
    let symbol = normalize_mexc_symbol_for_market(
        &subscription.symbol.exchange_symbol.symbol,
        MarketType::Spot,
    )?;
    let channel = match &subscription.kind {
        PublicStreamKind::Trades => {
            format!("spot@public.aggre.deals.v3.api.pb@100ms@{symbol}")
        }
        PublicStreamKind::Ticker => {
            format!("spot@public.aggre.bookTicker.v3.api.pb@10ms@{symbol}")
        }
        PublicStreamKind::OrderBookDelta => {
            format!("spot@public.aggre.depth.v3.api.pb@10ms@{symbol}")
        }
        PublicStreamKind::OrderBookSnapshot => {
            format!("spot@public.limit.depth.v3.api.pb@{symbol}@20")
        }
        PublicStreamKind::Candles { interval } => {
            format!("spot@public.kline.v3.api.pb@{symbol}@{interval}")
        }
    };
    Ok(json_subscription(
        MEXC_SPOT_PUBLIC_WS_URL.to_string(),
        channel,
    ))
}

fn mexc_contract_public_subscription_spec(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<MexcWsSubscriptionSpec> {
    let symbol = normalize_mexc_symbol_for_market(
        &subscription.symbol.exchange_symbol.symbol,
        MarketType::Perpetual,
    )?;
    let (method, param) = match &subscription.kind {
        PublicStreamKind::OrderBookDelta => {
            ("sub.depth", json!({ "symbol": symbol, "compress": true }))
        }
        PublicStreamKind::OrderBookSnapshot => {
            ("sub.depth.full", json!({ "symbol": symbol, "limit": 20 }))
        }
        PublicStreamKind::Ticker => ("sub.ticker", json!({ "symbol": symbol })),
        PublicStreamKind::Trades => ("sub.deal", json!({ "symbol": symbol })),
        PublicStreamKind::Candles { interval } => (
            "sub.kline",
            json!({ "symbol": symbol, "interval": interval }),
        ),
    };
    Ok(MexcWsSubscriptionSpec {
        url: MEXC_CONTRACT_PUBLIC_WS_URL.to_string(),
        channel: method.to_string(),
        subscribe_payload: json!({ "method": method, "param": param }),
        unsubscribe_payload: json!({ "method": method.replacen("sub.", "unsub.", 1), "param": param }),
    })
}

fn json_subscription(url: String, channel: String) -> MexcWsSubscriptionSpec {
    MexcWsSubscriptionSpec {
        url,
        channel: channel.clone(),
        subscribe_payload: json!({
            "method": "SUBSCRIPTION",
            "params": [channel],
        }),
        unsubscribe_payload: json!({
            "method": "UNSUBSCRIPTION",
            "params": [channel],
        }),
    }
}

fn parse_mexc_private_order_event(
    exchange_id: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let order = mexc_private_order_state(exchange_id, symbol_hint, data)?;
    let mut events = vec![ExchangeStreamEvent::OrderUpdate(order)];
    if matches!(
        subscription.kind,
        PrivateStreamKind::Fills | PrivateStreamKind::Orders | PrivateStreamKind::Account
    ) {
        if let Some(fill) = mexc_private_order_fill(exchange_id, subscription, symbol_hint, data)? {
            events.push(ExchangeStreamEvent::Fill(fill));
        }
    }
    Ok(events)
}

fn parse_mexc_private_fill_event(
    exchange_id: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    if !matches!(
        subscription.kind,
        PrivateStreamKind::Fills | PrivateStreamKind::Orders | PrivateStreamKind::Account
    ) {
        return Ok(Vec::new());
    }
    Ok(
        mexc_private_order_fill(exchange_id, subscription, symbol_hint, data)?
            .map(ExchangeStreamEvent::Fill)
            .into_iter()
            .collect(),
    )
}

fn mexc_private_order_state(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = mexc_private_symbol_scope(exchange_id, symbol_hint, data)?;
    let side_code = first_i64(data, &["side"]);
    let order_type_code = first_i64(data, &["orderType", "type"]);
    let state = first_i64(data, &["state"]);
    let quantity = first_string_or_number(data, &["vol", "quantity"]).unwrap_or_default();
    let filled_quantity = first_string_or_number(
        data,
        &["dealVol", "deal_vol", "filledQty", "filled_quantity"],
    )
    .unwrap_or_else(|| {
        if state == Some(3) {
            quantity.clone()
        } else {
            "0".to_string()
        }
    });
    let price = first_string_or_number(data, &["price"]).filter(|value| !is_zero_decimal(value));
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: first_string_or_number(data, &["externalOid", "clientOrderId"]),
        exchange_order_id: first_string_or_number(data, &["orderId", "id"]),
        side: side_code
            .map(mexc_contract_side_to_order_side)
            .unwrap_or(OrderSide::Buy),
        position_side: side_code.map(mexc_contract_side_to_position_side),
        order_type: order_type_code
            .map(mexc_contract_type_to_order_type)
            .unwrap_or(OrderType::Limit),
        time_in_force: order_type_code.and_then(mexc_contract_type_to_tif),
        status: state
            .map(|state| mexc_contract_status(state, &quantity, &filled_quantity))
            .unwrap_or(OrderStatus::Unknown),
        quantity,
        price,
        filled_quantity,
        average_fill_price: first_string_or_number(data, &["dealAvgPrice", "avgPrice"])
            .filter(|value| !is_zero_decimal(value)),
        reduce_only: side_code.is_some_and(|side| matches!(side, 2 | 4)),
        post_only: order_type_code == Some(2),
        created_at: first_timestamp(data, &["createTime", "create_time", "ts"]),
        updated_at: first_timestamp(data, &["updateTime", "update_time", "ts"])
            .unwrap_or_else(Utc::now),
    })
}

fn mexc_private_order_fill(
    exchange_id: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<Option<Fill>> {
    let quantity = first_f64(data, &["dealVol", "vol", "quantity"]).unwrap_or(0.0);
    let price = first_f64(data, &["dealAvgPrice", "dealPrice", "price"]).unwrap_or(0.0);
    if quantity <= 0.0 || price <= 0.0 {
        return Ok(None);
    }
    let symbol = mexc_private_symbol_scope(exchange_id, symbol_hint, data)?;
    let tenant_id =
        subscription
            .context
            .tenant_id
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc private stream fill requires tenant_id".to_string(),
            })?;
    let side_code = first_i64(data, &["side"]);
    Ok(Some(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id: subscription.account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: symbol.canonical_symbol.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "mexc private stream fill requires canonical_symbol".to_string(),
            }
        })?,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: first_string_or_number(data, &["orderId", "id"]),
        client_order_id: first_string_or_number(data, &["externalOid", "clientOrderId"]),
        fill_id: first_string_or_number(data, &["tradeId", "dealId", "id", "version"]),
        side: side_code
            .map(mexc_contract_side_to_order_side)
            .unwrap_or(OrderSide::Buy),
        position_side: side_code
            .map(mexc_contract_side_to_position_side)
            .unwrap_or(PositionSide::Net),
        status: FillStatus::Confirmed,
        liquidity_role: mexc_private_liquidity_role(data),
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: first_string_or_number(data, &["feeCurrency", "feeCcy"]),
        fee_amount: first_f64(data, &["takerFee", "makerFee", "fee"]),
        fee_rate: None,
        realized_pnl: first_f64(data, &["profit", "realised", "realizedPnl"]),
        filled_at: first_timestamp(data, &["timestamp", "updateTime", "createTime", "ts"])
            .unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    }))
}

fn parse_mexc_private_asset_event(
    exchange_id: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    data: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let tenant_id =
        subscription
            .context
            .tenant_id
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc private stream balance requires tenant_id".to_string(),
            })?;
    let asset = first_string(data, &["currency", "asset", "coin"])
        .unwrap_or_else(|| "USDT".to_string())
        .to_ascii_uppercase();
    let available = first_f64(
        data,
        &["availableBalance", "available_balance", "available"],
    )
    .unwrap_or(0.0);
    let locked = first_f64(data, &["frozenBalance", "frozen_balance", "frozen"])
        .or_else(|| first_f64(data, &["positionMargin", "position_margin"]))
        .unwrap_or(0.0);
    let total = first_f64(data, &["equity", "balance", "cashBalance", "cash_balance"])
        .unwrap_or(available + locked);
    let balance = AssetBalance::new(asset, total, available, locked).map_err(validation_error)?;
    Ok(vec![ExchangeStreamEvent::BalanceSnapshot(
        BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                exchange_id.clone(),
                subscription.context.request_id.clone(),
            ),
            balances: vec![ExchangeBalance {
                schema_version: SchemaVersion::current(),
                tenant_id,
                account_id: subscription.account_id.clone(),
                exchange_id: exchange_id.clone(),
                market_type: subscription.market_type.unwrap_or(MarketType::Perpetual),
                balances: vec![balance],
                observed_at: Utc::now(),
            }],
        },
    )])
}

fn parse_mexc_private_position_event(
    exchange_id: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let quantity = first_f64(data, &["holdVol", "positionVol", "quantity"]).unwrap_or(0.0);
    if quantity == 0.0 {
        return Ok(Vec::new());
    }
    let symbol = mexc_private_symbol_scope(exchange_id, symbol_hint, data)?;
    let tenant_id =
        subscription
            .context
            .tenant_id
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc private stream position requires tenant_id".to_string(),
            })?;
    let position = ExchangePosition {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id: subscription.account_id.clone(),
        exchange_id: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: symbol.canonical_symbol.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "mexc private stream position requires canonical_symbol".to_string(),
            }
        })?,
        exchange_symbol: Some(symbol.exchange_symbol),
        side: mexc_contract_position_side(data),
        quantity,
        entry_price: first_f64(data, &["holdAvgPrice", "openAvgPrice", "entryPrice"]),
        mark_price: first_f64(data, &["markPrice", "fairPrice"]),
        liquidation_price: first_f64(data, &["liquidatePrice", "liquidationPrice"])
            .filter(|value| *value > 0.0),
        unrealized_pnl: first_f64(data, &["unrealised", "unrealizedPnl", "unrealisedPnl"]),
        leverage: first_f64(data, &["leverage"]).filter(|value| *value > 0.0),
        observed_at: Utc::now(),
    };
    position.validate().map_err(validation_error)?;
    Ok(vec![ExchangeStreamEvent::PositionSnapshot(
        PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(
                exchange_id.clone(),
                subscription.context.request_id.clone(),
            ),
            positions: vec![position],
        },
    )])
}

fn parse_delta_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookDeltaLevel>> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mexc ws depth message missing levels".to_string(),
        })?
        .iter()
        .map(|level| {
            let (price, quantity) = level_price_quantity(level)?;
            Ok(OrderBookDeltaLevel::new(price, quantity))
        })
        .collect()
}

fn parse_snapshot_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookLevel>> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "mexc ws depth message missing levels".to_string(),
        })?
        .iter()
        .map(|level| {
            let (price, quantity) = level_price_quantity(level)?;
            OrderBookLevel::new(price, quantity).map_err(validation_error)
        })
        .collect()
}

fn level_price_quantity(value: &Value) -> ExchangeApiResult<(f64, f64)> {
    if let Some(array) = value.as_array() {
        let price =
            value_to_f64(array.first()).ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "mexc ws level missing price".to_string(),
            })?;
        let quantity = value_to_f64(array.get(2).or_else(|| array.get(1))).ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "mexc ws level missing quantity".to_string(),
            }
        })?;
        return Ok((price, quantity));
    }
    let price = required_f64(value, &["price", "p"])?;
    let quantity = required_f64(value, &["quantity", "q", "qty", "v"])?;
    Ok((price, quantity))
}

fn required_f64(value: &Value, keys: &[&str]) -> ExchangeApiResult<f64> {
    value_to_f64(keys.iter().find_map(|key| value.get(*key))).ok_or_else(|| {
        ExchangeApiError::InvalidRequest {
            message: format!("mexc ws message missing numeric field {keys:?}"),
        }
    })
}

fn value_to_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn first_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
    })
}

fn first_i64(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
    })
}

fn first_string(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str).map(str::to_string))
}

fn first_string_or_number(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| value_to_string(value.get(*key)))
}

fn value_to_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

fn first_f64(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|key| value_to_f64(value.get(*key)))
}

fn first_timestamp(value: &Value, keys: &[&str]) -> Option<DateTime<Utc>> {
    first_i64(value, keys).and_then(DateTime::<Utc>::from_timestamp_millis)
}

fn mexc_private_symbol_scope(
    exchange_id: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let raw_symbol = first_string(data, &["symbol", "contract"]).ok_or_else(|| {
        ExchangeApiError::InvalidRequest {
            message: "mexc private stream event missing symbol".to_string(),
        }
    })?;
    let (base, quote) =
        split_contract_symbol(&raw_symbol).ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("mexc private stream symbol missing base/quote: {raw_symbol}"),
        })?;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            raw_symbol,
        )
        .map_err(validation_error)?,
    })
}

fn split_contract_symbol(symbol: &str) -> Option<(String, String)> {
    let mut parts = symbol.split('_');
    let base = parts.next()?.to_ascii_uppercase();
    let quote = parts.next()?.to_ascii_uppercase();
    (!base.is_empty() && !quote.is_empty()).then_some((base, quote))
}

fn mexc_contract_side_to_order_side(side: i64) -> OrderSide {
    match side {
        3 | 4 => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn mexc_contract_side_to_position_side(side: i64) -> PositionSide {
    match side {
        1 | 4 => PositionSide::Long,
        2 | 3 => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn mexc_contract_type_to_order_type(order_type: i64) -> OrderType {
    match order_type {
        2 => OrderType::PostOnly,
        3 => OrderType::IOC,
        4 => OrderType::FOK,
        5 => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn mexc_contract_type_to_tif(order_type: i64) -> Option<TimeInForce> {
    match order_type {
        2 => Some(TimeInForce::GTX),
        3 => Some(TimeInForce::IOC),
        4 => Some(TimeInForce::FOK),
        1 => Some(TimeInForce::GTC),
        _ => None,
    }
}

fn mexc_contract_status(state: i64, quantity: &str, filled_quantity: &str) -> OrderStatus {
    match state {
        1 => OrderStatus::New,
        2 => {
            let total = decimal_text_to_f64(quantity).unwrap_or(0.0);
            let filled = decimal_text_to_f64(filled_quantity).unwrap_or(0.0);
            if filled > 0.0 && (total <= 0.0 || filled < total) {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Open
            }
        }
        3 => OrderStatus::Filled,
        4 => OrderStatus::Cancelled,
        5 => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn mexc_contract_position_side(data: &Value) -> PositionSide {
    match first_string_or_number(data, &["positionType", "position_type", "side"])
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "long" => PositionSide::Long,
        "2" | "short" => PositionSide::Short,
        _ => PositionSide::Net,
    }
}

fn mexc_private_liquidity_role(data: &Value) -> LiquidityRole {
    if let Some(is_taker) = data.get("isTaker").and_then(Value::as_bool) {
        return if is_taker {
            LiquidityRole::Taker
        } else {
            LiquidityRole::Maker
        };
    }
    if first_f64(data, &["makerFee"]).unwrap_or(0.0) != 0.0 {
        return LiquidityRole::Maker;
    }
    if first_f64(data, &["takerFee"]).unwrap_or(0.0) != 0.0 {
        return LiquidityRole::Taker;
    }
    match first_string(data, &["role", "liquidity"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "maker" | "m" => LiquidityRole::Maker,
        "taker" | "t" => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

fn is_zero_decimal(value: &str) -> bool {
    value.parse::<f64>().unwrap_or(0.0) == 0.0
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
