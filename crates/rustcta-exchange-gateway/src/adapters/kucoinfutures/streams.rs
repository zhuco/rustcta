#![cfg_attr(not(test), allow(dead_code))]

use chrono::{DateTime, Duration, Utc};
use rustcta_exchange_api::{
    AuthRenewalKind, AuthRenewalPolicy, BalancesResponse, ExchangeApiError, ExchangeApiResult,
    ExchangeStreamEvent, OrderState, PositionsResponse, PrivateOrderStreamEventKind,
    PrivateStreamCapabilities, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, SymbolScope, TenantId, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangePosition,
    ExchangeSymbol, Fill, FillStatus, LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType,
    PositionSide, SchemaVersion,
};
use serde_json::{json, Value};

use super::parser::{normalize_kucoinfutures_symbol, string_or_number, validation_error};
use super::KuCoinFuturesGatewayAdapter;
use crate::adapters::{ensure_exchange_api_schema, response_metadata};
use crate::orderbook_state::{OrderBookDelta, OrderBookDeltaLevel};
use crate::streams::StreamRuntimeState;

const DEFAULT_BULLET_TOKEN_TTL_MS: i64 = 24 * 60 * 60 * 1_000;
const DEFAULT_BULLET_RENEW_BEFORE_MS: i64 = 60 * 60 * 1_000;

#[derive(Debug, Clone, PartialEq)]
pub struct KuCoinWsSubscriptionSpec {
    pub url: String,
    pub topic: String,
    pub subscribe_payload: Value,
    pub unsubscribe_payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KuCoinBulletTokenLease {
    pub exchange: ExchangeId,
    pub token: String,
    pub endpoint: String,
    pub connect_id: String,
    pub ping_interval_ms: i64,
    pub ping_timeout_ms: i64,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub renewal_policy: AuthRenewalPolicy,
}

impl KuCoinBulletTokenLease {
    pub fn websocket_url(&self) -> String {
        format!(
            "{}?token={}&connectId={}",
            self.endpoint.trim_end_matches('/'),
            self.token,
            self.connect_id
        )
    }

    pub fn should_renew(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at - Duration::milliseconds(self.renewal_policy.renew_before_expiry_ms)
    }
}

#[derive(Debug, Clone)]
pub struct KuCoinWsSession {
    pub spec: KuCoinWsSubscriptionSpec,
    pub state: StreamRuntimeState,
}

#[derive(Debug, Clone)]
pub struct KuCoinPrivateWsResubscribePlan {
    pub specs: Vec<KuCoinWsSubscriptionSpec>,
    pub rest_resync_operations: Vec<&'static str>,
}

pub fn kucoinfutures_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
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

impl KuCoinFuturesGatewayAdapter {
    pub(super) async fn subscribe_public_stream_impl(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let spec = kucoinfutures_public_subscription_spec(&subscription, "1", None)?;
        Ok(format!("kucoinfutures:{}:{}", spec.url, spec.topic))
    }

    pub(super) async fn subscribe_private_stream_impl(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        ensure_exchange_api_schema(subscription.schema_version)?;
        self.ensure_exchange(&subscription.exchange)?;
        if !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported {
                operation: "kucoinfutures.subscribe_private_stream.credentials",
            });
        }
        let market_type = subscription.market_type.unwrap_or(MarketType::Perpetual);
        self.ensure_perpetual(market_type)?;
        let topic = kucoinfutures_private_topic(&subscription.kind)?;
        Ok(format!(
            "kucoinfutures:bullet-private:{topic}:{}",
            subscription.account_id
        ))
    }

    pub async fn request_private_bullet_token(
        &self,
        connect_id: &str,
    ) -> ExchangeApiResult<KuCoinBulletTokenLease> {
        let value = self
            .send_signed_post(
                "kucoinfutures.private_stream.bullet_token",
                "/api/v1/bullet-private",
                &std::collections::HashMap::new(),
                &json!({}),
            )
            .await?;
        kucoinfutures_bullet_token_lease(&self.exchange_id, connect_id, &value, Utc::now())
    }
}

pub fn kucoinfutures_public_subscription_spec(
    subscription: &PublicStreamSubscription,
    request_id: &str,
    token_lease: Option<&KuCoinBulletTokenLease>,
) -> ExchangeApiResult<KuCoinWsSubscriptionSpec> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.symbol.market_type != MarketType::Perpetual {
        return Err(ExchangeApiError::Unsupported {
            operation: "kucoinfutures.public_stream.market_type",
        });
    }
    let symbol = normalize_kucoinfutures_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    let topic = match &subscription.kind {
        PublicStreamKind::Trades => format!("/contractMarket/execution:{symbol}"),
        PublicStreamKind::Ticker => format!("/contractMarket/ticker:{symbol}"),
        PublicStreamKind::OrderBookDelta => format!("/contractMarket/level2:{symbol}"),
        PublicStreamKind::OrderBookSnapshot => format!("/contractMarket/level2Depth50:{symbol}"),
        PublicStreamKind::Candles { interval } => {
            format!("/contractMarket/limitCandle:{symbol}_{interval}")
        }
    };
    let url = token_lease
        .map(KuCoinBulletTokenLease::websocket_url)
        .unwrap_or_else(|| "wss://ws-api-futures.kucoin.com/endpoint".to_string());
    Ok(subscription_spec(url, topic, request_id, false))
}

pub fn kucoinfutures_private_subscription_spec(
    subscription: &PrivateStreamSubscription,
    request_id: &str,
    token_lease: &KuCoinBulletTokenLease,
) -> ExchangeApiResult<KuCoinWsSubscriptionSpec> {
    kucoinfutures_private_subscription_specs(subscription, request_id, token_lease)?
        .into_iter()
        .next()
        .ok_or_else(|| ExchangeApiError::Unsupported {
            operation: "kucoinfutures.private_stream.empty_topics",
        })
}

pub fn kucoinfutures_private_subscription_specs(
    subscription: &PrivateStreamSubscription,
    request_id: &str,
    token_lease: &KuCoinBulletTokenLease,
) -> ExchangeApiResult<Vec<KuCoinWsSubscriptionSpec>> {
    ensure_exchange_api_schema(subscription.schema_version)?;
    if subscription.market_type.unwrap_or(MarketType::Perpetual) != MarketType::Perpetual {
        return Err(ExchangeApiError::Unsupported {
            operation: "kucoinfutures.private_stream.market_type",
        });
    }
    Ok(kucoinfutures_private_topics(&subscription.kind)
        .into_iter()
        .enumerate()
        .map(|(index, topic)| {
            subscription_spec(
                token_lease.websocket_url(),
                topic.to_string(),
                &format!("{request_id}-{index}"),
                true,
            )
        })
        .collect())
}

pub fn kucoinfutures_private_resubscribe_plan(
    subscription: &PrivateStreamSubscription,
    request_id: &str,
    token_lease: &KuCoinBulletTokenLease,
) -> ExchangeApiResult<KuCoinPrivateWsResubscribePlan> {
    Ok(KuCoinPrivateWsResubscribePlan {
        specs: kucoinfutures_private_subscription_specs(subscription, request_id, token_lease)?,
        rest_resync_operations: kucoinfutures_private_resync_operations(&subscription.kind),
    })
}

pub fn kucoinfutures_pong_response(value: &Value) -> Option<Value> {
    let kind = value.get("type").and_then(Value::as_str)?;
    if !kind.eq_ignore_ascii_case("ping") {
        return None;
    }
    Some(json!({
        "id": value.get("id").and_then(Value::as_str).unwrap_or("pong"),
        "type": "pong"
    }))
}

pub fn parse_kucoinfutures_level2_delta(
    exchange: &ExchangeId,
    symbol: &SymbolScope,
    value: &Value,
) -> ExchangeApiResult<OrderBookDelta> {
    let data = value.get("data").unwrap_or(value);
    let canonical_symbol =
        symbol
            .canonical_symbol
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoinfutures level2 delta parser requires canonical_symbol".to_string(),
            })?;
    let first_sequence = data
        .get("sequenceStart")
        .or_else(|| data.get("sequence"))
        .and_then(value_as_u64);
    let last_sequence = data
        .get("sequenceEnd")
        .or_else(|| data.get("sequence"))
        .and_then(value_as_u64);
    if first_sequence.is_none() && last_sequence.is_none() {
        return Err(ExchangeApiError::InvalidRequest {
            message: format!("kucoinfutures level2 delta missing sequence: {value}"),
        });
    }
    let mut delta = OrderBookDelta::new(
        exchange.clone(),
        symbol.market_type,
        canonical_symbol,
        Utc::now(),
    )
    .with_sequences(first_sequence, last_sequence)
    .with_previous_sequence(data.get("previousSequence").and_then(value_as_u64));
    parse_kucoinfutures_delta_changes(data, &mut delta)?;
    delta.exchange_timestamp = data
        .get("time")
        .or_else(|| value.get("ts"))
        .and_then(value_as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis);
    Ok(delta)
}

pub fn parse_kucoinfutures_private_stream_events(
    exchange: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    value: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    if value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| matches!(kind, "welcome" | "pong"))
    {
        return Ok(vec![ExchangeStreamEvent::Heartbeat {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange: exchange.clone(),
            received_at: Utc::now(),
        }]);
    }

    let topic = value
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = value.get("data").unwrap_or(value);
    if matches!(
        subscription.kind,
        PrivateStreamKind::Orders | PrivateStreamKind::Fills
    ) || topic.contains("/contractMarket/tradeOrders")
    {
        return parse_kucoinfutures_trade_order_event(exchange, subscription, symbol_hint, data);
    }
    if matches!(
        subscription.kind,
        PrivateStreamKind::Balances | PrivateStreamKind::Account
    ) || topic.contains("/contractAccount/wallet")
    {
        return parse_kucoinfutures_wallet_event(exchange, subscription, data);
    }
    if matches!(subscription.kind, PrivateStreamKind::Positions)
        || topic.contains("/contract/position")
    {
        return parse_kucoinfutures_position_event(exchange, subscription, symbol_hint, data);
    }
    Ok(Vec::new())
}

fn parse_kucoinfutures_trade_order_event(
    exchange: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let order = kucoinfutures_ws_order_state(exchange, symbol_hint, data)?;
    let mut events = vec![ExchangeStreamEvent::OrderUpdate(order)];
    if is_match_event(data) {
        if let Some(fill) = kucoinfutures_ws_fill(exchange, subscription, symbol_hint, data)? {
            events.push(ExchangeStreamEvent::Fill(fill));
        }
    }
    Ok(events)
}

fn kucoinfutures_ws_order_state(
    exchange: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<OrderState> {
    let symbol = kucoinfutures_ws_symbol_scope(exchange, symbol_hint, data)?;
    let event_type = data
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let quantity = first_string_or_number(data, &["size", "orderSize", "matchSize"])
        .unwrap_or_else(|| "0".to_string());
    let filled_quantity = first_string_or_number(data, &["dealSize", "filledSize", "matchSize"])
        .unwrap_or_else(|| {
            if matches!(event_type.as_str(), "filled" | "done") {
                quantity.clone()
            } else {
                "0".to_string()
            }
        });
    let price = first_string_or_number(data, &["price", "orderPrice", "matchPrice"])
        .filter(|value| value.parse::<f64>().unwrap_or(0.0) > 0.0);
    Ok(OrderState {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange: exchange.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: symbol.canonical_symbol.clone(),
        exchange_symbol: symbol.exchange_symbol,
        client_order_id: first_string_or_number(data, &["clientOid", "clientOrderId"]),
        exchange_order_id: first_string_or_number(data, &["orderId", "id"]),
        side: kucoinfutures_ws_side(data.get("side").and_then(Value::as_str)),
        position_side: Some(PositionSide::None),
        order_type: kucoinfutures_ws_order_type(
            data.get("orderType")
                .or_else(|| data.get("order_type"))
                .and_then(Value::as_str),
        ),
        time_in_force: kucoinfutures_ws_time_in_force(
            data.get("timeInForce")
                .or_else(|| data.get("time_in_force"))
                .and_then(Value::as_str),
        ),
        status: kucoinfutures_ws_order_status(&event_type),
        quantity,
        price,
        filled_quantity,
        average_fill_price: first_string_or_number(data, &["matchPrice", "averagePrice"]),
        reduce_only: data
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        post_only: data
            .get("postOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: first_ws_timestamp(data, &["orderTime", "createdAt", "ts"]),
        updated_at: first_ws_timestamp(data, &["ts", "orderTime", "updatedAt"])
            .unwrap_or_else(Utc::now),
    })
}

fn kucoinfutures_ws_fill(
    exchange: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<Option<Fill>> {
    let quantity = first_f64(data, &["matchSize", "size"]).unwrap_or(0.0);
    let price = first_f64(data, &["matchPrice", "price"]).unwrap_or(0.0);
    if quantity <= 0.0 || price <= 0.0 {
        return Ok(None);
    }
    let symbol = kucoinfutures_ws_symbol_scope(exchange, symbol_hint, data)?;
    let (tenant_id, account_id) = kucoinfutures_ws_account(subscription)?;
    Ok(Some(Fill {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: symbol.canonical_symbol.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "kucoinfutures fill event requires canonical symbol".to_string(),
            }
        })?,
        exchange_symbol: Some(symbol.exchange_symbol),
        order_id: first_string_or_number(data, &["orderId", "id"]),
        client_order_id: first_string_or_number(data, &["clientOid", "clientOrderId"]),
        fill_id: first_string_or_number(data, &["tradeId", "matchId"]),
        side: kucoinfutures_ws_side(data.get("side").and_then(Value::as_str)),
        position_side: PositionSide::None,
        status: FillStatus::Confirmed,
        liquidity_role: kucoinfutures_ws_liquidity(data.get("liquidity").and_then(Value::as_str)),
        price,
        quantity,
        quote_quantity: Some(price * quantity),
        fee_asset: first_string_or_number(data, &["feeCurrency"]),
        fee_amount: first_f64(data, &["fee"]),
        fee_rate: None,
        realized_pnl: first_f64(data, &["realisedPnl", "realizedPnl"]),
        filled_at: first_ws_timestamp(data, &["ts", "tradeTime"]).unwrap_or_else(Utc::now),
        received_at: Utc::now(),
    }))
}

fn parse_kucoinfutures_wallet_event(
    exchange: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    data: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let (tenant_id, account_id) = kucoinfutures_ws_account(subscription)?;
    let asset = data
        .get("currency")
        .and_then(Value::as_str)
        .unwrap_or("USDT")
        .to_ascii_uppercase();
    let available = first_f64(data, &["availableBalance", "available"]).unwrap_or(0.0);
    let locked = first_f64(data, &["holdBalance", "hold"]).unwrap_or(0.0);
    let total = first_f64(data, &["walletBalance", "accountEquity", "balance"])
        .unwrap_or(available + locked);
    let balance = AssetBalance::new(asset, total, available, locked).map_err(validation_error)?;
    Ok(vec![ExchangeStreamEvent::BalanceSnapshot(
        BalancesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange.clone(), subscription.context.request_id.clone()),
            balances: vec![ExchangeBalance {
                schema_version: SchemaVersion::current(),
                tenant_id,
                account_id,
                exchange_id: exchange.clone(),
                market_type: subscription.market_type.unwrap_or(MarketType::Perpetual),
                balances: vec![balance],
                observed_at: Utc::now(),
            }],
        },
    )])
}

fn parse_kucoinfutures_position_event(
    exchange: &ExchangeId,
    subscription: &PrivateStreamSubscription,
    symbol_hint: Option<&SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<Vec<ExchangeStreamEvent>> {
    let quantity = first_f64(data, &["currentQty", "quantity", "size"]).unwrap_or(0.0);
    if quantity == 0.0 {
        return Ok(Vec::new());
    }
    let symbol = kucoinfutures_ws_symbol_scope(exchange, symbol_hint, data)?;
    let (tenant_id, account_id) = kucoinfutures_ws_account(subscription)?;
    let position = ExchangePosition {
        schema_version: SchemaVersion::current(),
        tenant_id,
        account_id,
        exchange_id: exchange.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: symbol.canonical_symbol.clone().ok_or_else(|| {
            ExchangeApiError::InvalidRequest {
                message: "kucoinfutures position event requires canonical symbol".to_string(),
            }
        })?,
        exchange_symbol: Some(symbol.exchange_symbol),
        side: if quantity > 0.0 {
            PositionSide::Long
        } else {
            PositionSide::Short
        },
        quantity: quantity.abs(),
        entry_price: first_f64(data, &["avgEntryPrice", "entryPrice"]),
        mark_price: first_f64(data, &["markPrice"]),
        liquidation_price: first_f64(data, &["liquidationPrice"]),
        unrealized_pnl: first_f64(data, &["unrealisedPnl", "unrealizedPnl"]),
        leverage: first_f64(data, &["realLeverage", "leverage"]),
        observed_at: Utc::now(),
    };
    position.validate().map_err(validation_error)?;
    Ok(vec![ExchangeStreamEvent::PositionSnapshot(
        PositionsResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: response_metadata(exchange.clone(), subscription.context.request_id.clone()),
            positions: vec![position],
        },
    )])
}

fn parse_kucoinfutures_delta_changes(
    data: &Value,
    delta: &mut OrderBookDelta,
) -> ExchangeApiResult<()> {
    if let Some(changes) = data.get("changes") {
        delta.bids = parse_delta_levels(
            changes
                .get("bids")
                .or_else(|| changes.get("buy"))
                .or_else(|| changes.get("bid")),
        )?;
        delta.asks = parse_delta_levels(
            changes
                .get("asks")
                .or_else(|| changes.get("sell"))
                .or_else(|| changes.get("ask")),
        )?;
        return Ok(());
    }
    if let Some(change) = data.get("change").and_then(Value::as_str) {
        let parts = change.split(',').map(str::trim).collect::<Vec<_>>();
        if parts.len() < 3 {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("kucoinfutures invalid level2 change: {change}"),
            });
        }
        let level = OrderBookDeltaLevel::new(parse_f64(parts[0])?, parse_f64(parts[1])?);
        match parts[2].to_ascii_lowercase().as_str() {
            "buy" | "bid" => delta.bids.push(level),
            "sell" | "ask" => delta.asks.push(level),
            side => {
                return Err(ExchangeApiError::InvalidRequest {
                    message: format!("kucoinfutures unsupported level2 side {side}"),
                })
            }
        }
    }
    Ok(())
}

fn parse_delta_levels(value: Option<&Value>) -> ExchangeApiResult<Vec<OrderBookDeltaLevel>> {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    levels
        .iter()
        .map(|level| {
            let values = level
                .as_array()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("kucoinfutures invalid level2 level: {level}"),
                })?;
            let price = values.first().and_then(value_as_f64).ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: format!("kucoinfutures invalid level2 price: {level}"),
                }
            })?;
            let quantity = values.get(1).and_then(value_as_f64).ok_or_else(|| {
                ExchangeApiError::InvalidRequest {
                    message: format!("kucoinfutures invalid level2 quantity: {level}"),
                }
            })?;
            Ok(OrderBookDeltaLevel::new(price, quantity))
        })
        .collect()
}

pub fn kucoinfutures_bullet_token_lease(
    exchange: &ExchangeId,
    connect_id: &str,
    value: &Value,
    issued_at: DateTime<Utc>,
) -> ExchangeApiResult<KuCoinBulletTokenLease> {
    let token = required_str(value, "token")?.to_string();
    let server = value
        .get("instanceServers")
        .and_then(Value::as_array)
        .and_then(|servers| servers.first())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "kucoinfutures bullet response missing instanceServers[0]".to_string(),
        })?;
    let endpoint = required_str(server, "endpoint")?.to_string();
    let ping_interval_ms = value_i64(server.get("pingInterval")).unwrap_or(20_000);
    let ping_timeout_ms = value_i64(server.get("pingTimeout")).unwrap_or(10_000);
    Ok(KuCoinBulletTokenLease {
        exchange: exchange.clone(),
        token,
        endpoint,
        connect_id: connect_id.trim().to_string(),
        ping_interval_ms,
        ping_timeout_ms,
        issued_at,
        expires_at: issued_at + Duration::milliseconds(DEFAULT_BULLET_TOKEN_TTL_MS),
        renewal_policy: AuthRenewalPolicy {
            kind: AuthRenewalKind::TokenRefresh,
            renew_before_expiry_ms: DEFAULT_BULLET_RENEW_BEFORE_MS,
            renewal_interval_ms: Some(20 * 60 * 60 * 1_000),
            reconnect_on_renewal_failure: true,
            resubscribe_after_renewal: true,
        },
    })
}

pub fn kucoinfutures_public_ws_session(
    exchange: ExchangeId,
    subscription: PublicStreamSubscription,
    request_id: &str,
    token_lease: Option<&KuCoinBulletTokenLease>,
) -> ExchangeApiResult<KuCoinWsSession> {
    let spec = kucoinfutures_public_subscription_spec(&subscription, request_id, token_lease)?;
    Ok(KuCoinWsSession {
        spec,
        state: StreamRuntimeState::new(exchange, MarketType::Perpetual),
    })
}

pub fn kucoinfutures_private_ws_session(
    exchange: ExchangeId,
    subscription: PrivateStreamSubscription,
    request_id: &str,
    token_lease: &KuCoinBulletTokenLease,
) -> ExchangeApiResult<KuCoinWsSession> {
    let spec = kucoinfutures_private_subscription_spec(&subscription, request_id, token_lease)?;
    Ok(KuCoinWsSession {
        spec,
        state: StreamRuntimeState::new(exchange, MarketType::Perpetual),
    })
}

fn subscription_spec(
    url: String,
    topic: String,
    request_id: &str,
    private_channel: bool,
) -> KuCoinWsSubscriptionSpec {
    KuCoinWsSubscriptionSpec {
        url,
        topic: topic.clone(),
        subscribe_payload: json!({
            "id": request_id,
            "type": "subscribe",
            "topic": topic,
            "privateChannel": private_channel,
            "response": true
        }),
        unsubscribe_payload: json!({
            "id": request_id,
            "type": "unsubscribe",
            "topic": topic,
            "privateChannel": private_channel,
            "response": true
        }),
    }
}

fn kucoinfutures_private_topic(kind: &PrivateStreamKind) -> ExchangeApiResult<&'static str> {
    kucoinfutures_private_topics(kind)
        .into_iter()
        .next()
        .ok_or(ExchangeApiError::Unsupported {
            operation: "kucoinfutures.private_stream.kind",
        })
}

fn kucoinfutures_private_topics(kind: &PrivateStreamKind) -> Vec<&'static str> {
    match kind {
        PrivateStreamKind::Orders | PrivateStreamKind::Fills => {
            vec!["/contractMarket/tradeOrders"]
        }
        PrivateStreamKind::Balances => vec!["/contractAccount/wallet"],
        PrivateStreamKind::Positions => vec!["/contract/position"],
        PrivateStreamKind::Account => vec![
            "/contractMarket/tradeOrders",
            "/contractAccount/wallet",
            "/contract/position",
        ],
    }
}

fn kucoinfutures_private_resync_operations(kind: &PrivateStreamKind) -> Vec<&'static str> {
    match kind {
        PrivateStreamKind::Orders => vec!["get_open_orders", "query_order"],
        PrivateStreamKind::Fills => vec!["get_recent_fills", "query_order"],
        PrivateStreamKind::Balances => vec!["get_balances"],
        PrivateStreamKind::Positions => vec!["get_positions"],
        PrivateStreamKind::Account => vec![
            "get_balances",
            "get_positions",
            "get_open_orders",
            "get_recent_fills",
            "query_order",
        ],
    }
}

fn kucoinfutures_ws_account(
    subscription: &PrivateStreamSubscription,
) -> ExchangeApiResult<(TenantId, AccountId)> {
    let tenant_id =
        subscription
            .context
            .tenant_id
            .clone()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "kucoinfutures private stream event requires context.tenant_id"
                    .to_string(),
            })?;
    Ok((tenant_id, subscription.account_id.clone()))
}

fn kucoinfutures_ws_symbol_scope(
    exchange: &ExchangeId,
    symbol_hint: Option<&SymbolScope>,
    data: &Value,
) -> ExchangeApiResult<SymbolScope> {
    if let Some(symbol) = symbol_hint {
        return Ok(symbol.clone());
    }
    let symbol = required_str(data, "symbol")?;
    let normalized = normalize_kucoinfutures_symbol(symbol)?;
    let (base, quote) = split_kucoinfutures_linear_symbol(&normalized);
    Ok(SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).map_err(validation_error)?),
        exchange_symbol: ExchangeSymbol::new(exchange.clone(), MarketType::Perpetual, normalized)
            .map_err(validation_error)?,
    })
}

fn split_kucoinfutures_linear_symbol(symbol: &str) -> (String, String) {
    let trimmed = symbol.trim_end_matches('M');
    for quote in ["USDT", "USDC", "USD"] {
        if let Some(base) = trimmed.strip_suffix(quote) {
            if !base.is_empty() {
                return (base.to_string(), quote.to_string());
            }
        }
    }
    (trimmed.to_string(), "USDT".to_string())
}

fn is_match_event(data: &Value) -> bool {
    data.get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind.eq_ignore_ascii_case("match"))
}

fn kucoinfutures_ws_order_status(event_type: &str) -> OrderStatus {
    match event_type {
        "open" => OrderStatus::New,
        "match" => OrderStatus::PartiallyFilled,
        "filled" | "done" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

fn kucoinfutures_ws_side(value: Option<&str>) -> OrderSide {
    if value.unwrap_or_default().eq_ignore_ascii_case("sell") {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
}

fn kucoinfutures_ws_order_type(value: Option<&str>) -> OrderType {
    match value.unwrap_or("limit").to_ascii_lowercase().as_str() {
        "market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

fn kucoinfutures_ws_time_in_force(value: Option<&str>) -> Option<TimeInForce> {
    match value?.to_ascii_lowercase().as_str() {
        "ioc" => Some(TimeInForce::IOC),
        "fok" => Some(TimeInForce::FOK),
        "post_only" | "postonly" | "gtx" => Some(TimeInForce::GTX),
        "gtc" => Some(TimeInForce::GTC),
        _ => None,
    }
}

fn kucoinfutures_ws_liquidity(value: Option<&str>) -> LiquidityRole {
    if value
        .unwrap_or_default()
        .to_ascii_lowercase()
        .starts_with('m')
    {
        LiquidityRole::Maker
    } else {
        LiquidityRole::Taker
    }
}

fn first_string_or_number(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| string_or_number(value.get(*field)))
}

fn first_f64(value: &Value, fields: &[&str]) -> Option<f64> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_f64))
}

fn first_ws_timestamp(value: &Value, fields: &[&str]) -> Option<DateTime<Utc>> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(value_as_i64))
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

fn required_str<'a>(value: &'a Value, field: &str) -> ExchangeApiResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: format!("kucoinfutures bullet response missing {field}"),
        })
}

fn value_i64(value: Option<&Value>) -> Option<i64> {
    value.and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn parse_f64(value: &str) -> ExchangeApiResult<f64> {
    value
        .parse()
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("kucoinfutures invalid decimal {value}: {error}"),
        })
}
