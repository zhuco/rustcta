use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderSide, TenantId};
use serde_json::json;

use super::streams::{
    parse_phemex_private_stream_message, parse_phemex_public_stream_message,
    phemex_check_sequence_continuity, phemex_order_book_ws_policy, phemex_orderbook_params,
    phemex_private_subscribe_payload, phemex_public_subscribe_payload,
    phemex_risk_account_subscribe_payload, phemex_stream_reconnect_policy, phemex_ws_auth_payload,
    PhemexPrivateStreamMessage, PhemexPublicStreamMessage, PhemexSequenceContinuity,
    PhemexWsSessionEvent,
};
use super::test_support::{context, exchange_id, perp_symbol_scope, spot_symbol_scope};
use super::{PhemexGatewayAdapter, PhemexGatewayConfig};

#[test]
fn phemex_public_stream_payloads_should_match_official_methods() {
    let spot_book = public_subscription(spot_symbol_scope(), PublicStreamKind::OrderBookDelta);
    let spot_payload = phemex_public_subscribe_payload(&spot_book, 1).expect("spot payload");
    assert_eq!(spot_payload["method"], "orderbook.subscribe");
    assert_eq!(spot_payload["params"], json!(["sBTCUSDT", false, 30]));

    let perp_candle = public_subscription(
        perp_symbol_scope(),
        PublicStreamKind::Candles {
            interval: "1m".to_string(),
        },
    );
    let perp_payload = phemex_public_subscribe_payload(&perp_candle, 2).expect("perp payload");
    assert_eq!(perp_payload["method"], "kline_p.subscribe");
    assert_eq!(perp_payload["params"], json!(["BTCUSDT", 60]));

    let perp_ticker = public_subscription(perp_symbol_scope(), PublicStreamKind::Ticker);
    let ticker_payload = phemex_public_subscribe_payload(&perp_ticker, 3).expect("ticker payload");
    assert_eq!(ticker_payload["method"], "perp_market24h_pack_p.subscribe");
    assert_eq!(ticker_payload["params"], json!([]));
}

#[test]
fn phemex_public_orderbook_policy_should_cover_depth_interval_and_sequence() {
    let policy = phemex_order_book_ws_policy();
    assert_eq!(policy.spot_method, "orderbook.subscribe");
    assert_eq!(policy.perpetual_method, "orderbook_p.subscribe");
    assert_eq!(policy.supported_depths, &[0, 1, 5, 10, 30]);
    assert_eq!(policy.default_depth, 30);
    assert_eq!(policy.fast_interval_ms, 20);
    assert_eq!(policy.aggregated_interval_ms, 120);
    assert_eq!(policy.full_depth_interval_ms, 100);
    assert_eq!(policy.snapshot_self_check_ms, 60_000);
    assert_eq!(policy.sequence_field, "sequence");
    assert_eq!(policy.checksum, None);
    assert!(policy.resync.contains("60s snapshot self-check"));

    assert_eq!(
        phemex_orderbook_params("BTCUSDT", false, 5).expect("params"),
        json!(["BTCUSDT", false, 5])
    );
    assert_eq!(
        phemex_orderbook_params("BTCUSDT", false, 0).expect("full depth params"),
        json!(["BTCUSDT", false, 0])
    );
    assert!(phemex_orderbook_params("BTCUSDT", false, 25).is_err());

    assert_eq!(
        phemex_check_sequence_continuity(None, 10),
        PhemexSequenceContinuity::First
    );
    assert_eq!(
        phemex_check_sequence_continuity(Some(10), 11),
        PhemexSequenceContinuity::Continuous
    );
    assert_eq!(
        phemex_check_sequence_continuity(Some(11), 11),
        PhemexSequenceContinuity::DuplicateOrStale
    );
    let gap = phemex_check_sequence_continuity(Some(11), 13);
    assert_eq!(
        gap,
        PhemexSequenceContinuity::Gap {
            expected: 12,
            actual: 13
        }
    );
    assert!(gap.requires_resync());
}

#[test]
fn phemex_private_stream_payloads_and_auth_should_match_official_methods() {
    let auth = phemex_ws_auth_payload("key", "secret", 1_700_000_000, 1);
    assert_eq!(auth["method"], "user.auth");
    assert_eq!(auth["params"][0], "API");
    assert_eq!(auth["params"][1], "key");
    assert!(auth["params"][2]
        .as_str()
        .is_some_and(|value| !value.is_empty()));

    let spot = private_subscription(Some(MarketType::Spot), PrivateStreamKind::Orders);
    let spot_payload =
        phemex_private_subscribe_payload(&spot, MarketType::Spot, 2).expect("spot private");
    assert_eq!(spot_payload["method"], "wo.subscribe");

    let perp = private_subscription(Some(MarketType::Perpetual), PrivateStreamKind::Positions);
    let perp_payload =
        phemex_private_subscribe_payload(&perp, MarketType::Perpetual, 3).expect("perp private");
    assert_eq!(perp_payload["method"], "aop_p.subscribe");

    let ras_payload = phemex_risk_account_subscribe_payload(4);
    assert_eq!(ras_payload["method"], "ras_p.subscribe");
    assert_eq!(ras_payload["params"], json!({}));
}

#[test]
fn phemex_public_stream_parser_should_parse_orderbook_trade_and_ticker() {
    let exchange = exchange_id();
    let book = parse_phemex_public_stream_message(
        &exchange,
        MarketType::Perpetual,
        perp_symbol_scope(),
        &json!({
            "depth": 30,
            "orderbook_p": {
                "asks": [["20702.9", "0.718"]],
                "bids": [["20700.5", "1.622"]]
            },
            "sequence": 77668172_u64,
            "symbol": "BTCUSDT",
            "timestamp": 1666854171201355264_u64,
            "type": "incremental"
        }),
    )
    .expect("book");
    match book {
        PhemexPublicStreamMessage::OrderBook(snapshot) => {
            assert_eq!(snapshot.sequence, Some(77668172));
            assert_eq!(snapshot.bids[0].price, 20700.5);
        }
        other => panic!("unexpected message: {other:?}"),
    }

    let trades = parse_phemex_public_stream_message(
        &exchange,
        MarketType::Perpetual,
        perp_symbol_scope(),
        &json!({
            "symbol": "BTCUSDT",
            "trades_p": [[1666854171201355264_u64, "Buy", "20701.3", "0.01"]]
        }),
    )
    .expect("trades");
    match trades {
        PhemexPublicStreamMessage::Trades(trades) => {
            assert_eq!(trades[0].side, OrderSide::Buy);
            assert_eq!(trades[0].quantity, "0.01");
        }
        other => panic!("unexpected message: {other:?}"),
    }

    let ticker = parse_phemex_public_stream_message(
        &exchange,
        MarketType::Perpetual,
        perp_symbol_scope(),
        &json!({
            "perp_market24h_pack_p": {
                "fields": [
                    "symbol",
                    "closeRp",
                    "markPriceRp",
                    "indexPriceRp",
                    "fundingRateRr",
                    "openInterestRv"
                ],
                "data": [[
                    "BTCUSDT",
                    "20701.3",
                    "20701.1",
                    "20700.9",
                    "0.0001",
                    "12.3"
                ]]
            }
        }),
    )
    .expect("ticker");
    match ticker {
        PhemexPublicStreamMessage::Ticker(ticker) => {
            assert_eq!(ticker.last_price, "20701.3");
            assert_eq!(ticker.funding_rate.as_deref(), Some("0.0001"));
        }
        other => panic!("unexpected message: {other:?}"),
    }
}

#[test]
fn phemex_public_orderbook_fixtures_should_parse_snapshot_incremental_and_delete_level() {
    let exchange = exchange_id();
    let snapshot = parse_phemex_public_stream_message(
        &exchange,
        MarketType::Perpetual,
        perp_symbol_scope(),
        &serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/phemex/ws_public_orderbook_snapshot.json"
        ))
        .expect("snapshot fixture"),
    )
    .expect("snapshot");
    match snapshot {
        PhemexPublicStreamMessage::OrderBook(book) => {
            assert_eq!(book.sequence, Some(77668172));
            assert_eq!(book.best_bid().expect("bid").price, 20700.5);
        }
        other => panic!("unexpected snapshot message: {other:?}"),
    }

    let incremental = parse_phemex_public_stream_message(
        &exchange,
        MarketType::Perpetual,
        perp_symbol_scope(),
        &serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/phemex/ws_public_orderbook_incremental.json"
        ))
        .expect("incremental fixture"),
    )
    .expect("incremental");
    match incremental {
        PhemexPublicStreamMessage::OrderBook(book) => {
            assert_eq!(book.sequence, Some(77668173));
            assert!(book.asks.is_empty());
            assert_eq!(book.best_bid().expect("bid").price, 20700.6);
        }
        other => panic!("unexpected incremental message: {other:?}"),
    }
}

#[test]
fn phemex_private_stream_parser_should_parse_spot_wallet_order_and_fill() {
    let exchange = exchange_id();
    let message = parse_phemex_private_stream_message(
        &exchange,
        tenant_id(),
        account_id(),
        MarketType::Spot,
        Some(spot_symbol_scope()),
        &json!({
            "sequence": 349,
            "timestamp": 1587549121318737700_u64,
            "type": "snapshot",
            "wallets": [{
                "currency": "USDT",
                "balanceEv": 351802500000_i64,
                "lockedTradingBalanceEv": 100000000_i64,
                "lockedWithdrawEv": 0_i64
            }],
            "orders": {
                "open": [{
                    "action": "New",
                    "baseQtyEv": 100000000_i64,
                    "clOrdID": "client-1",
                    "cumBaseQtyEv": 0_i64,
                    "ordStatus": "New",
                    "ordType": "Limit",
                    "orderID": "order-1",
                    "priceEp": 666500000000_i64,
                    "side": "Sell",
                    "symbol": "sBTCUSDT",
                    "timeInForce": "GoodTillCancel",
                    "transactTimeNs": 1587547657442753000_u64
                }],
                "closed": [],
                "fills": [{
                    "clOrdID": "client-1",
                    "execBaseQtyEv": 10000_i64,
                    "execFeeEv": 12_i64,
                    "execID": "fill-1",
                    "execPriceEp": 669000000000_i64,
                    "execQuoteQtyEv": 66900000_i64,
                    "feeCurrency": "BTC",
                    "orderID": "order-1",
                    "side": "Sell",
                    "symbol": "sBTCUSDT",
                    "transactTimeNs": 1587463924964876800_u64
                }]
            }
        }),
    )
    .expect("private stream");

    let PhemexPrivateStreamMessage::Events(events) = message else {
        panic!("expected events");
    };
    assert_eq!(events.len(), 3);
    match &events[0] {
        ExchangeStreamEvent::BalanceSnapshot(snapshot) => {
            assert_eq!(snapshot.balances[0].balances[0].asset, "USDT");
            assert_eq!(snapshot.balances[0].balances[0].total, 3518.025);
            assert_eq!(snapshot.balances[0].balances[0].available, 3517.025);
        }
        other => panic!("unexpected event: {other:?}"),
    }
    match &events[1] {
        ExchangeStreamEvent::OrderUpdate(order) => {
            assert_eq!(order.exchange_order_id.as_deref(), Some("order-1"));
            assert_eq!(order.quantity, "1");
            assert_eq!(order.price.as_deref(), Some("6665"));
        }
        other => panic!("unexpected event: {other:?}"),
    }
    match &events[2] {
        ExchangeStreamEvent::Fill(fill) => {
            assert_eq!(fill.fill_id.as_deref(), Some("fill-1"));
            assert_eq!(fill.price, 6690.0);
            assert_eq!(fill.quantity, 0.0001);
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn phemex_private_stream_parser_should_parse_perp_aop_and_ras() {
    let exchange = exchange_id();
    let message = parse_phemex_private_stream_message(
        &exchange,
        tenant_id(),
        account_id(),
        MarketType::Perpetual,
        Some(perp_symbol_scope()),
        &json!({
            "accounts_p": [{
                "accountBalanceRv": "512.097102123158",
                "currency": "USDT",
                "totalUsedBalanceRv": "0.0074407494"
            }],
            "orders_p": [{
                "action": "New",
                "clOrdID": "perp-client",
                "cumQty": "0",
                "ordStatus": "New",
                "ordType": "Limit",
                "orderID": "perp-order",
                "orderQtyRq": "0.01",
                "posSide": "Merged",
                "priceRp": "20700.5",
                "side": "Buy",
                "symbol": "BTCUSDT",
                "timeInForce": "GoodTillCancel"
            }],
            "positions_p": [{
                "symbol": "BTCUSDT",
                "posSide": "Merged",
                "sizeRq": "0.01",
                "avgEntryPriceRp": "20700.5",
                "markPriceRp": "20701.1",
                "leverageRr": "5"
            }]
        }),
    )
    .expect("aop");

    let PhemexPrivateStreamMessage::Events(events) = message else {
        panic!("expected events");
    };
    assert!(events
        .iter()
        .any(|event| matches!(event, ExchangeStreamEvent::BalanceSnapshot(_))));
    assert!(events
        .iter()
        .any(|event| matches!(event, ExchangeStreamEvent::PositionSnapshot(_))));
    assert!(events.iter().any(|event| {
        matches!(event, ExchangeStreamEvent::OrderUpdate(order) if order.exchange_order_id.as_deref() == Some("perp-order"))
    }));

    let risk = parse_phemex_private_stream_message(
        &exchange,
        tenant_id(),
        account_id(),
        MarketType::Perpetual,
        None,
        &json!({
            "risk_units": [{"symbol": "BTCUSDT", "totalPosCostRv": "10"}],
            "risk_wallets": [{"currency": "USDT", "balanceRv": "100"}]
        }),
    )
    .expect("risk");
    match risk {
        PhemexPrivateStreamMessage::RiskAccount(risk) => {
            assert_eq!(risk.risk_units.len(), 1);
            assert_eq!(risk.risk_wallets.len(), 1);
        }
        other => panic!("unexpected message: {other:?}"),
    }
}

#[tokio::test]
async fn phemex_adapter_should_ack_public_and_private_stream_subscriptions() {
    let adapter = PhemexGatewayAdapter::new(PhemexGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..PhemexGatewayConfig::default()
    })
    .expect("adapter");

    let public_id = adapter
        .subscribe_public_stream(public_subscription(
            spot_symbol_scope(),
            PublicStreamKind::Trades,
        ))
        .await
        .expect("public subscription");
    assert!(public_id.contains("trade.subscribe"));

    let private_id = adapter
        .subscribe_private_stream(private_subscription(
            Some(MarketType::Perpetual),
            PrivateStreamKind::Account,
        ))
        .await
        .expect("private subscription");
    assert!(private_id.contains("aop_p.subscribe"));
}

#[test]
fn phemex_public_ws_session_should_emit_initial_ping_and_stream_events() {
    let adapter = PhemexGatewayAdapter::default_public().expect("adapter");
    let mut session = adapter
        .public_ws_session(public_subscription(
            perp_symbol_scope(),
            PublicStreamKind::OrderBookDelta,
        ))
        .expect("public ws session");

    let initial = session.initial_requests();
    assert_eq!(initial.len(), 1);
    assert_eq!(initial[0]["method"], "orderbook_p.subscribe");

    let now = chrono::Utc::now();
    let ping = session.heartbeat_request(now);
    assert_eq!(ping["method"], "server.ping");
    assert!(session.state().last_ping_at.is_some());

    let pong = session
        .handle_text_message(r#"{"id":1,"result":"pong"}"#)
        .expect("pong");
    assert!(session.state().last_pong_at.is_some());
    assert!(pong.iter().any(|event| {
        matches!(
            event,
            PhemexWsSessionEvent::Stream(events)
                if events.iter().any(|event| matches!(event, ExchangeStreamEvent::Heartbeat { .. }))
        )
    }));

    let events = session
        .handle_text_message(
            &json!({
                "depth": 30,
                "orderbook_p": {
                    "asks": [["20702.9", "0.718"]],
                    "bids": [["20700.5", "1.622"]]
                },
                "sequence": 77668172_u64,
                "symbol": "BTCUSDT",
                "timestamp": 1666854171201355264_u64
            })
            .to_string(),
        )
        .expect("book");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            PhemexWsSessionEvent::Stream(stream_events)
                if stream_events.iter().any(|event| matches!(event, ExchangeStreamEvent::OrderBookSnapshot(_)))
        )
    }));

    assert_eq!(
        session.supervisor_action(now, &phemex_stream_reconnect_policy()),
        crate::streams::StreamSupervisorAction::None
    );
}

#[test]
fn phemex_private_ws_session_should_auth_subscribe_ping_and_forward_events() {
    let adapter = PhemexGatewayAdapter::default_public().expect("adapter");
    let mut session = adapter
        .private_ws_session(
            private_subscription(Some(MarketType::Perpetual), PrivateStreamKind::Account),
            "key",
            "secret",
            1_700_000_000,
        )
        .expect("private ws session")
        .with_symbol_hint(perp_symbol_scope());

    let initial = session.initial_requests();
    assert_eq!(initial.len(), 2);
    assert_eq!(initial[0]["method"], "user.auth");
    assert_eq!(initial[1]["method"], "aop_p.subscribe");

    let ping = session.heartbeat_request(chrono::Utc::now());
    assert_eq!(ping["method"], "server.ping");

    let events = session
        .handle_text_message(
            &json!({
                "accounts_p": [{
                    "accountBalanceRv": "512.097102123158",
                    "currency": "USDT",
                    "totalUsedBalanceRv": "0.0074407494"
                }],
                "orders_p": [{
                    "action": "New",
                    "clOrdID": "perp-client",
                    "cumQty": "0",
                    "ordStatus": "New",
                    "ordType": "Limit",
                    "orderID": "perp-order",
                    "orderQtyRq": "0.01",
                    "posSide": "Merged",
                    "priceRp": "20700.5",
                    "side": "Buy",
                    "symbol": "BTCUSDT",
                    "timeInForce": "GoodTillCancel"
                }]
            })
            .to_string(),
        )
        .expect("private events");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            PhemexWsSessionEvent::Stream(stream_events)
                if stream_events.iter().any(|event| matches!(event, ExchangeStreamEvent::OrderUpdate(_)))
        )
    }));

    let pong = session
        .handle_text_message(r#"{"id":2,"result":"pong"}"#)
        .expect("private pong");
    assert!(pong.iter().any(|event| {
        matches!(
            event,
            PhemexWsSessionEvent::Stream(stream_events)
                if stream_events.iter().any(|event| matches!(event, ExchangeStreamEvent::Heartbeat { .. }))
        )
    }));
}

#[test]
fn phemex_task14_private_stream_fixture_should_parse_order_update() {
    let exchange = exchange_id();
    let value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/phemex/ws_order.json"
    ))
    .expect("private ws order fixture");
    let message = parse_phemex_private_stream_message(
        &exchange,
        tenant_id(),
        account_id(),
        MarketType::Perpetual,
        Some(perp_symbol_scope()),
        &value,
    )
    .expect("private stream message");

    match message {
        PhemexPrivateStreamMessage::Events(events) => {
            assert!(events.iter().any(|event| {
                matches!(
                    event,
                    ExchangeStreamEvent::OrderUpdate(order)
                        if order.exchange_order_id.as_deref() == Some("exchange-1")
                            && order.client_order_id.as_deref() == Some("client-1")
                )
            }));
        }
        other => panic!("expected stream events, got {other:?}"),
    }
}

fn public_subscription(
    symbol: rustcta_exchange_api::SymbolScope,
    kind: PublicStreamKind,
) -> PublicStreamSubscription {
    PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("stream"),
        symbol,
        kind,
    }
}

fn private_subscription(
    market_type: Option<MarketType>,
    kind: PrivateStreamKind,
) -> PrivateStreamSubscription {
    PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-stream"),
        exchange: exchange_id(),
        market_type,
        account_id: rustcta_types::AccountId::new("account").expect("account"),
        kind,
    }
}

fn tenant_id() -> TenantId {
    TenantId::new("tenant").expect("tenant")
}

fn account_id() -> AccountId {
    AccountId::new("account").expect("account")
}
