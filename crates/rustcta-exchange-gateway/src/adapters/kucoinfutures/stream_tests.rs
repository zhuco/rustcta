use chrono::{Duration, TimeZone, Utc};
use rustcta_exchange_api::{
    AuthRenewalKind, CapabilitySupport, ExchangeClient, ExchangeStreamEvent, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    StreamHeartbeatDirection, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, LiquidityRole, MarketType, OrderStatus, PositionSide};
use serde_json::json;

use crate::orderbook_state::{OrderBookApplyOutcome, OrderBookState, OrderBookStateConfig};

use super::parser::parse_orderbook_snapshot;
use super::streams::{
    kucoinfutures_bullet_token_lease, kucoinfutures_pong_response,
    kucoinfutures_private_resubscribe_plan, kucoinfutures_private_subscription_spec,
    kucoinfutures_public_subscription_spec, parse_kucoinfutures_level2_delta,
    parse_kucoinfutures_private_stream_events,
};
use super::test_support::{context, exchange_id, symbol_scope};
use super::{GatewayAdapter, KuCoinFuturesGatewayAdapter, KuCoinFuturesGatewayConfig};

#[test]
fn kucoinfutures_public_stream_spec_should_normalize_symbol_and_ping_pong() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("kucoin-public-ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let spec = kucoinfutures_public_subscription_spec(&subscription, "req-1", None).expect("spec");

    assert_eq!(spec.url, "wss://ws-api-futures.kucoin.com/endpoint");
    assert_eq!(spec.topic, "/contractMarket/level2:XBTUSDTM");
    assert_eq!(spec.subscribe_payload["type"], "subscribe");
    assert_eq!(spec.subscribe_payload["privateChannel"], false);
    assert_eq!(
        kucoinfutures_pong_response(&json!({"type": "ping", "id": "42"})).expect("pong")["id"],
        "42"
    );
}

#[test]
fn kucoinfutures_private_stream_spec_should_use_bullet_token_lease_and_renewal() {
    let issued_at = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    let lease = kucoinfutures_bullet_token_lease(
        &exchange_id(),
        "connect-1",
        &json!({
            "token": "bullet-token",
            "instanceServers": [{
                "endpoint": "wss://ws-api-futures.kucoin.com/endpoint",
                "pingInterval": 18000,
                "pingTimeout": 10000
            }]
        }),
        issued_at,
    )
    .expect("lease");
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("kucoin-private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };

    let spec =
        kucoinfutures_private_subscription_spec(&subscription, "req-2", &lease).expect("spec");

    assert_eq!(lease.ping_interval_ms, 18_000);
    assert!(lease
        .websocket_url()
        .contains("token=bullet-token&connectId=connect-1"));
    assert!(!lease.should_renew(issued_at + Duration::hours(1)));
    assert!(lease.should_renew(issued_at + Duration::hours(23)));
    assert_eq!(spec.topic, "/contractMarket/tradeOrders");
    assert_eq!(spec.subscribe_payload["privateChannel"], true);
}

#[test]
fn kucoinfutures_private_resubscribe_plan_should_cover_account_topics_and_rest_resync() {
    let issued_at = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
    let lease = kucoinfutures_bullet_token_lease(
        &exchange_id(),
        "connect-resubscribe",
        &json!({
            "token": "bullet-token",
            "instanceServers": [{
                "endpoint": "wss://ws-api-futures.kucoin.com/endpoint",
                "pingInterval": 18000,
                "pingTimeout": 10000
            }]
        }),
        issued_at,
    )
    .expect("lease");
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("kucoin-private-account"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Account,
    };

    let plan =
        kucoinfutures_private_resubscribe_plan(&subscription, "req-account", &lease).expect("plan");

    let topics: Vec<_> = plan.specs.iter().map(|spec| spec.topic.as_str()).collect();
    assert_eq!(
        topics,
        vec![
            "/contractMarket/tradeOrders",
            "/contractAccount/wallet",
            "/contract/position"
        ]
    );
    assert!(plan.specs.iter().all(|spec| {
        spec.url
            .contains("token=bullet-token&connectId=connect-resubscribe")
            && spec.subscribe_payload["privateChannel"] == true
    }));
    assert_eq!(
        plan.rest_resync_operations,
        vec![
            "get_balances",
            "get_positions",
            "get_open_orders",
            "get_recent_fills",
            "query_order"
        ]
    );
}

#[test]
fn kucoinfutures_level2_delta_should_apply_contiguous_sequence_and_resync_on_gap() {
    let exchange = exchange_id();
    let symbol = symbol_scope();
    let snapshot = parse_orderbook_snapshot(
        &exchange,
        symbol.clone(),
        &json!({
            "data": {
                "sequence": 100_u64,
                "bids": [["65000", "2"]],
                "asks": [["65001", "3"]]
            }
        }),
    )
    .expect("snapshot");
    let mut state = OrderBookState::from_snapshot(snapshot)
        .expect("state")
        .with_config(OrderBookStateConfig::strict_delta());

    let contiguous = parse_kucoinfutures_level2_delta(
        &exchange,
        &symbol,
        &json!({
            "type": "message",
            "topic": "/contractMarket/level2:XBTUSDTM",
            "data": {
                "sequenceStart": 101_u64,
                "sequenceEnd": 101_u64,
                "changes": {
                    "bids": [["65000", "2.5"]],
                    "asks": [["65001", "0"]]
                },
                "time": 1700000000000_i64
            }
        }),
    )
    .expect("delta");
    assert!(matches!(
        state.apply_delta(contiguous, None).expect("apply"),
        OrderBookApplyOutcome::DeltaApplied {
            sequence: Some(101)
        }
    ));

    let gap = parse_kucoinfutures_level2_delta(
        &exchange,
        &symbol,
        &json!({
            "type": "message",
            "topic": "/contractMarket/level2:XBTUSDTM",
            "data": {
                "sequenceStart": 104_u64,
                "sequenceEnd": 104_u64,
                "change": "65000,3,buy"
            }
        }),
    )
    .expect("gap");
    let outcome = state.apply_delta(gap, None).expect("apply gap");
    let OrderBookApplyOutcome::ResyncRequired(request) = outcome else {
        panic!("expected resync required");
    };
    assert_eq!(request.expected_next_sequence, Some(102));
    assert_eq!(request.received_first_sequence, Some(104));
}

#[test]
fn kucoinfutures_private_stream_parser_should_normalize_order_fill_wallet_and_position_events() {
    let exchange = exchange_id();
    let orders = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("kucoin-private-orders"),
        exchange: exchange.clone(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };
    let symbol = symbol_scope();

    let open = parse_kucoinfutures_private_stream_events(
        &exchange,
        &orders,
        Some(&symbol),
        &json!({
            "type": "message",
            "topic": "/contractMarket/tradeOrders",
            "data": {
                "type": "open",
                "symbol": "XBTUSDTM",
                "orderId": "order-1",
                "clientOid": "client-1",
                "side": "buy",
                "orderType": "limit",
                "size": "10",
                "price": "65000",
                "orderTime": 1700000000000_i64
            }
        }),
    )
    .expect("open event");
    let ExchangeStreamEvent::OrderUpdate(order) = &open[0] else {
        panic!("expected order update");
    };
    assert_eq!(order.status, OrderStatus::New);
    assert_eq!(order.exchange_order_id.as_deref(), Some("order-1"));

    let match_events = parse_kucoinfutures_private_stream_events(
        &exchange,
        &orders,
        Some(&symbol),
        &json!({
            "type": "message",
            "topic": "/contractMarket/tradeOrders",
            "data": {
                "type": "match",
                "symbol": "XBTUSDTM",
                "orderId": "order-1",
                "clientOid": "client-1",
                "tradeId": "trade-1",
                "side": "buy",
                "orderType": "limit",
                "size": "10",
                "matchSize": "3",
                "matchPrice": "65001",
                "liquidity": "taker",
                "fee": "0.01",
                "feeCurrency": "USDT",
                "ts": 1700000001000_i64
            }
        }),
    )
    .expect("match event");
    assert_eq!(match_events.len(), 2);
    let ExchangeStreamEvent::OrderUpdate(order) = &match_events[0] else {
        panic!("expected order update");
    };
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.filled_quantity, "3");
    let ExchangeStreamEvent::Fill(fill) = &match_events[1] else {
        panic!("expected fill");
    };
    assert_eq!(fill.fill_id.as_deref(), Some("trade-1"));
    assert_eq!(fill.quantity, 3.0);
    assert_eq!(fill.liquidity_role, LiquidityRole::Taker);

    for (event_type, expected) in [
        ("filled", OrderStatus::Filled),
        ("canceled", OrderStatus::Cancelled),
        ("rejected", OrderStatus::Rejected),
    ] {
        let events = parse_kucoinfutures_private_stream_events(
            &exchange,
            &orders,
            Some(&symbol),
            &json!({
                "topic": "/contractMarket/tradeOrders",
                "data": {
                    "type": event_type,
                    "symbol": "XBTUSDTM",
                    "orderId": "order-1",
                    "clientOid": "client-1",
                    "side": "buy",
                    "orderType": "limit",
                    "size": "10",
                    "price": "65000"
                }
            }),
        )
        .expect("terminal event");
        let ExchangeStreamEvent::OrderUpdate(order) = &events[0] else {
            panic!("expected order update");
        };
        assert_eq!(order.status, expected);
    }

    let balances = parse_kucoinfutures_private_stream_events(
        &exchange,
        &PrivateStreamSubscription {
            kind: PrivateStreamKind::Balances,
            ..orders.clone()
        },
        None,
        &json!({
            "topic": "/contractAccount/wallet",
            "data": {
                "currency": "USDT",
                "availableBalance": "90",
                "holdBalance": "10",
                "walletBalance": "100"
            }
        }),
    )
    .expect("wallet event");
    let ExchangeStreamEvent::BalanceSnapshot(snapshot) = &balances[0] else {
        panic!("expected balance snapshot");
    };
    assert_eq!(snapshot.balances[0].balances[0].asset, "USDT");
    assert_eq!(snapshot.balances[0].balances[0].locked, 10.0);

    let positions = parse_kucoinfutures_private_stream_events(
        &exchange,
        &PrivateStreamSubscription {
            kind: PrivateStreamKind::Positions,
            ..orders
        },
        Some(&symbol),
        &json!({
            "topic": "/contract/position:XBTUSDTM",
            "data": {
                "symbol": "XBTUSDTM",
                "currentQty": "-4",
                "avgEntryPrice": "65000",
                "markPrice": "65100",
                "unrealisedPnl": "-4.0",
                "realLeverage": "3"
            }
        }),
    )
    .expect("position event");
    let ExchangeStreamEvent::PositionSnapshot(snapshot) = &positions[0] else {
        panic!("expected position snapshot");
    };
    assert_eq!(snapshot.positions[0].side, PositionSide::Short);
    assert_eq!(snapshot.positions[0].quantity, 4.0);
}

#[tokio::test]
async fn kucoinfutures_adapter_should_ack_ws_specs_and_expose_v2_policy() {
    let adapter = KuCoinFuturesGatewayAdapter::new(KuCoinFuturesGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        api_passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..KuCoinFuturesGatewayConfig::default()
    })
    .expect("adapter");

    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("kucoin-public-id"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::Ticker,
        })
        .await
        .expect("public stream id");
    assert!(public_id
        .contains("wss://ws-api-futures.kucoin.com/endpoint:/contractMarket/ticker:XBTUSDTM"));

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("kucoin-private-id"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Balances,
        })
        .await
        .expect("private stream id");
    assert_eq!(
        private_id,
        "kucoinfutures:bullet-private:/contractAccount/wallet:account"
    );

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_streams);
    assert!(capabilities.supports_private_streams);
    assert!(capabilities.supports_funding_rates);
    assert!(capabilities.capabilities_v2.funding_rates.is_supported());
    assert!(matches!(
        capabilities.capabilities_v2.private_streams,
        CapabilitySupport::Native
    ));
    let account_control = GatewayAdapter::account_control_capabilities(&adapter);
    assert!(!account_control.supports_leverage);
    assert!(!account_control.supports_position_mode_change);
    assert!(account_control.supports_close_position);
    let runtime = &capabilities.capabilities_v2.stream_runtime;
    assert!(runtime.public.is_supported());
    assert!(runtime.private.is_supported());
    assert!(runtime.supports_public_subscribe);
    assert!(runtime.supports_private_subscribe);
    assert!(runtime.supports_public_unsubscribe);
    assert!(runtime.supports_private_unsubscribe);
    assert!(runtime.heartbeat.required);
    assert_eq!(
        runtime.heartbeat.direction,
        StreamHeartbeatDirection::ServerPing
    );
    assert_eq!(runtime.heartbeat.interval_ms, Some(20_000));
    assert!(runtime.reconnect.supported);
    assert!(runtime.reconnect.requires_resubscribe);
    assert!(runtime.resync.order_book);
    assert!(runtime.resync.orders);
    assert!(runtime.resync.positions);
    assert!(runtime.resync.balances);
    assert!(runtime.auth.required);
    assert!(runtime.auth.uses_listen_key);
    assert_eq!(runtime.auth.renewal_ms, Some(20 * 60 * 60 * 1_000));
    assert!(runtime.auth.requires_relogin_on_reconnect);
    assert!(capabilities.capabilities_v2.fills_history.supports_cursor);
    assert!(capabilities
        .private_stream_capabilities
        .as_ref()
        .is_some_and(|capabilities| capabilities.supports_orders
            && capabilities.supports_fills
            && capabilities.supports_balances
            && capabilities.supports_positions));
    let lease = kucoinfutures_bullet_token_lease(
        &exchange_id(),
        "connect-2",
        &json!({
            "token": "bullet-token",
            "instanceServers": [{"endpoint": "wss://ws-api-futures.kucoin.com/endpoint"}]
        }),
        Utc::now(),
    )
    .expect("lease");
    assert_eq!(lease.renewal_policy.kind, AuthRenewalKind::TokenRefresh);
}
