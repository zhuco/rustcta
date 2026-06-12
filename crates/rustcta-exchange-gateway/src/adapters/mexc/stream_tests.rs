use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, OrderBookStrictness, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{LiquidityRole, MarketType, OrderStatus, PositionSide};

use super::streams::{
    mexc_contract_private_filter_spec, mexc_contract_private_login_spec, mexc_ping_payload,
    mexc_public_subscription_spec, parse_mexc_contract_depth_delta,
    parse_mexc_contract_depth_snapshot, parse_mexc_private_stream_events,
    parse_mexc_spot_aggre_depth_delta, parse_mexc_spot_book_ticker,
    parse_mexc_spot_limit_depth_snapshot,
};
use super::test_support::{context, exchange_id, perpetual_symbol_scope, symbol_scope};
use super::{MexcGatewayAdapter, MexcGatewayConfig};
use crate::orderbook_state::{OrderBookApplyOutcome, OrderBookState, OrderBookStateConfig};

#[test]
fn mexc_spot_public_ws_spec_should_cover_10ms_depth_and_book_ticker() {
    let delta = mexc_public_subscription_spec(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mexc-depth"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("delta spec");
    assert_eq!(delta.url, "wss://wbs-api.mexc.com/ws");
    assert_eq!(
        delta.channel,
        "spot@public.aggre.depth.v3.api.pb@10ms@BTCUSDT"
    );
    assert_eq!(delta.subscribe_payload["method"], "SUBSCRIPTION");
    assert_eq!(delta.unsubscribe_payload["method"], "UNSUBSCRIPTION");

    let partial = mexc_public_subscription_spec(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mexc-partial-depth"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    })
    .expect("partial depth spec");
    assert_eq!(
        partial.channel,
        "spot@public.limit.depth.v3.api.pb@BTCUSDT@20"
    );

    let ticker = mexc_public_subscription_spec(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mexc-book-ticker"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::Ticker,
    })
    .expect("ticker spec");
    assert_eq!(
        ticker.channel,
        "spot@public.aggre.bookTicker.v3.api.pb@10ms@BTCUSDT"
    );
    assert_eq!(mexc_ping_payload()["method"], "ping");
}

#[test]
fn mexc_contract_public_ws_spec_should_cover_depth_profiles() {
    let delta = mexc_public_subscription_spec(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mexc-contract-depth"),
        symbol: perpetual_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("contract delta spec");
    assert_eq!(delta.url, "wss://contract.mexc.com/edge");
    assert_eq!(delta.channel, "sub.depth");
    assert_eq!(delta.subscribe_payload["param"]["symbol"], "BTC_USDT");
    assert_eq!(delta.subscribe_payload["param"]["compress"], true);

    let snapshot = mexc_public_subscription_spec(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mexc-contract-depth-full"),
        symbol: perpetual_symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    })
    .expect("contract full-depth spec");
    assert_eq!(snapshot.channel, "sub.depth.full");
    assert_eq!(snapshot.subscribe_payload["param"]["limit"], 20);
}

#[test]
fn mexc_contract_private_ws_specs_should_build_login_and_filters() {
    let login =
        mexc_contract_private_login_spec("mx-test-key", "test-secret", 1_611_038_237_237, false)
            .expect("login spec");
    assert_eq!(login.url, "wss://contract.mexc.com/edge");
    assert_eq!(login.login_payload["method"], "login");
    assert_eq!(login.login_payload["subscribe"], false);
    assert_eq!(login.login_payload["param"]["apiKey"], "mx-test-key");
    assert_eq!(login.login_payload["param"]["reqTime"], "1611038237237");
    assert_eq!(
        login.login_payload["param"]["signature"],
        "09f1f80d2e37c8be308d32b2b5cb09906a63a3967928f75d3db30c38029fe406"
    );

    let account = mexc_contract_private_filter_spec(&PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mexc-private-account-filter"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: context("mexc-private-account-filter")
            .account_id
            .expect("account id in test context"),
        kind: PrivateStreamKind::Account,
    })
    .expect("account filter");
    assert_eq!(
        account.filters,
        vec!["order", "order.deal", "position", "asset"]
    );
    assert_eq!(account.filter_payload["method"], "personal.filter");
    assert_eq!(
        account.filter_payload["param"]["filters"][1]["filter"],
        "order.deal"
    );
    assert_eq!(
        account.reset_payload["param"]["filters"],
        serde_json::json!([])
    );

    let fills = mexc_contract_private_filter_spec(&PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mexc-private-fill-filter"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: context("mexc-private-fill-filter")
            .account_id
            .expect("account id in test context"),
        kind: PrivateStreamKind::Fills,
    })
    .expect("fill filter");
    assert_eq!(fills.filters, vec!["order.deal"]);
}

#[test]
fn mexc_spot_ws_parser_fixtures_should_extract_sequence_and_bbo() {
    let delta_fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mexc/ws/spot_aggre_depth_10ms.json"
    ))
    .expect("delta fixture");
    let delta = parse_mexc_spot_aggre_depth_delta(&exchange_id(), &symbol_scope(), &delta_fixture)
        .expect("delta");
    assert_eq!(delta.first_sequence, Some(101));
    assert_eq!(delta.last_sequence, Some(102));
    assert_eq!(delta.bids[0].price, 65000.0);
    assert_eq!(delta.asks[0].quantity, 0.8);

    let partial_fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mexc/ws/spot_limit_depth_20.json"
    ))
    .expect("partial fixture");
    let snapshot =
        parse_mexc_spot_limit_depth_snapshot(&exchange_id(), symbol_scope(), &partial_fixture)
            .expect("snapshot");
    assert_eq!(snapshot.sequence, Some(102));
    assert_eq!(snapshot.bids.len(), 2);
    assert_eq!(snapshot.asks[0].price, 65010.0);

    let ticker_fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mexc/ws/spot_book_ticker_10ms.json"
    ))
    .expect("ticker fixture");
    let ticker = parse_mexc_spot_book_ticker(&exchange_id(), MarketType::Spot, &ticker_fixture)
        .expect("ticker");
    assert_eq!(ticker.symbol, "BTCUSDT");
    assert_eq!(ticker.sequence, Some(102));
    assert_eq!(ticker.bid_price, 65000.0);
    assert_eq!(ticker.ask_quantity, 0.8);
}

#[test]
fn mexc_contract_depth_parser_should_use_version_sequence_and_detect_gaps() {
    let snapshot_fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mexc/ws/contract_depth_full.json"
    ))
    .expect("contract snapshot fixture");
    let snapshot = parse_mexc_contract_depth_snapshot(
        &exchange_id(),
        perpetual_symbol_scope(),
        &snapshot_fixture,
    )
    .expect("snapshot");
    assert_eq!(snapshot.sequence, Some(100));
    assert_eq!(snapshot.bids[0].price, 65000.0);
    assert_eq!(snapshot.bids[0].quantity, 1.0);

    let mut state = OrderBookState::from_snapshot(snapshot)
        .expect("state")
        .with_config(OrderBookStateConfig::strict_delta());
    let delta_fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/mexc/ws/contract_depth_delta.json"
    ))
    .expect("contract delta fixture");
    let contiguous =
        parse_mexc_contract_depth_delta(&exchange_id(), &perpetual_symbol_scope(), &delta_fixture)
            .expect("delta");
    assert_eq!(contiguous.first_sequence, Some(101));
    assert_eq!(contiguous.bids[0].quantity, 1.5);
    assert!(matches!(
        state.apply_delta(contiguous, None).expect("apply"),
        OrderBookApplyOutcome::DeltaApplied {
            sequence: Some(101)
        }
    ));

    let gap = parse_mexc_contract_depth_delta(
        &exchange_id(),
        &perpetual_symbol_scope(),
        &serde_json::json!({
            "channel": "push.depth",
            "symbol": "BTC_USDT",
            "data": {
                "asks": [],
                "bids": [[65000.0, 3, 2.0]],
                "version": 103_u64
            }
        }),
    )
    .expect("gap delta");
    let outcome = state.apply_delta(gap, None).expect("apply gap");
    let OrderBookApplyOutcome::ResyncRequired(request) = outcome else {
        panic!("expected resync");
    };
    assert_eq!(request.expected_next_sequence, Some(102));
    assert_eq!(request.received_first_sequence, Some(103));
}

#[test]
fn mexc_private_stream_parser_should_normalize_order_fill_balance_and_position_events() {
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("mexc-private"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: context("mexc-private")
            .account_id
            .expect("account id in test context"),
        kind: PrivateStreamKind::Account,
    };

    let heartbeat = parse_mexc_private_stream_events(
        &exchange_id(),
        &subscription,
        None,
        &serde_json::json!({
            "channel": "rs.login",
            "data": "success",
            "ts": 1610005069000_i64
        }),
    )
    .expect("heartbeat");
    assert!(matches!(
        heartbeat.as_slice(),
        [ExchangeStreamEvent::Heartbeat { .. }]
    ));

    for (state, expected) in [
        (1, OrderStatus::New),
        (2, OrderStatus::PartiallyFilled),
        (3, OrderStatus::Filled),
        (4, OrderStatus::Cancelled),
        (5, OrderStatus::Rejected),
    ] {
        let events = parse_mexc_private_stream_events(
            &exchange_id(),
            &subscription,
            Some(&perpetual_symbol_scope()),
            &serde_json::json!({
                "channel": "push.personal.order",
                "data": {
                    "createTime": 1610005069976_i64,
                    "dealAvgPrice": 65010.5,
                    "dealVol": 1,
                    "externalOid": "client-1",
                    "feeCurrency": "USDT",
                    "leverage": 15,
                    "makerFee": 0,
                    "orderId": "102067003631907840",
                    "orderType": 5,
                    "price": 65000.0,
                    "profit": -0.25,
                    "side": 4,
                    "state": state,
                    "symbol": "BTC_USDT",
                    "takerFee": 0.42,
                    "updateTime": 1610005069983_i64,
                    "version": 2,
                    "vol": 2
                },
                "ts": 1610005069989_i64
            }),
        )
        .expect("order event");
        let ExchangeStreamEvent::OrderUpdate(order) = &events[0] else {
            panic!("expected order update");
        };
        assert_eq!(order.status, expected);
        assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
        assert_eq!(
            order.exchange_order_id.as_deref(),
            Some("102067003631907840")
        );
        assert_eq!(order.position_side, Some(PositionSide::Long));
        assert!(order.reduce_only);

        if state == 2 {
            assert!(events.iter().any(|event| {
                matches!(
                    event,
                    ExchangeStreamEvent::Fill(fill)
                        if fill.quantity == 1.0
                            && fill.price == 65010.5
                            && fill.realized_pnl == Some(-0.25)
                )
            }));
        }
    }

    let balance = parse_mexc_private_stream_events(
        &exchange_id(),
        &subscription,
        None,
        &serde_json::json!({
            "channel": "push.personal.asset",
            "data": {
                "availableBalance": 100.5,
                "currency": "USDT",
                "frozenBalance": 1.5,
                "positionMargin": 2.0
            },
            "ts": 1610005070083_i64
        }),
    )
    .expect("asset");
    let [ExchangeStreamEvent::BalanceSnapshot(snapshot)] = balance.as_slice() else {
        panic!("expected balance snapshot");
    };
    assert_eq!(snapshot.balances[0].balances[0].asset, "USDT");
    assert_eq!(snapshot.balances[0].balances[0].available, 100.5);
    assert_eq!(snapshot.balances[0].balances[0].locked, 1.5);

    let position = parse_mexc_private_stream_events(
        &exchange_id(),
        &subscription,
        None,
        &serde_json::json!({
            "channel": "push.personal.position",
            "data": {
                "holdAvgPrice": 65000.0,
                "holdVol": 3,
                "leverage": 15,
                "liquidatePrice": 58000.0,
                "openAvgPrice": 65000.0,
                "positionId": 1397818,
                "positionType": 1,
                "realised": -0.5,
                "state": 1,
                "symbol": "BTC_USDT"
            },
            "ts": 1610005070157_i64
        }),
    )
    .expect("position");
    let [ExchangeStreamEvent::PositionSnapshot(snapshot)] = position.as_slice() else {
        panic!("expected position snapshot");
    };
    assert_eq!(snapshot.positions[0].side, PositionSide::Long);
    assert_eq!(snapshot.positions[0].quantity, 3.0);
    assert_eq!(snapshot.positions[0].entry_price, Some(65000.0));

    let fill = parse_mexc_private_stream_events(
        &exchange_id(),
        &subscription,
        None,
        &serde_json::json!({
            "channel": "push.personal.order.deal",
            "data": {
                "id": 987654321,
                "symbol": "BTC_USDT",
                "side": 1,
                "vol": 10,
                "price": 45000.5,
                "feeCurrency": "USDT",
                "fee": 0.225,
                "timestamp": 1760942212000_i64,
                "profit": 1.25,
                "isTaker": true,
                "category": 1,
                "orderId": 123456789,
                "externalOid": "ext_001"
            },
            "ts": 1760942212000_i64
        }),
    )
    .expect("fill");
    let [ExchangeStreamEvent::Fill(fill)] = fill.as_slice() else {
        panic!("expected fill only");
    };
    assert_eq!(fill.fill_id.as_deref(), Some("987654321"));
    assert_eq!(fill.order_id.as_deref(), Some("123456789"));
    assert_eq!(fill.client_order_id.as_deref(), Some("ext_001"));
    assert_eq!(fill.quantity, 10.0);
    assert_eq!(fill.price, 45000.5);
    assert_eq!(fill.fee_amount, Some(0.225));
    assert_eq!(fill.liquidity_role, LiquidityRole::Taker);
}

#[tokio::test]
async fn mexc_adapter_should_ack_public_ws_and_expose_native_capability() {
    let adapter = MexcGatewayAdapter::new(MexcGatewayConfig::default()).expect("adapter");
    let id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("mexc-public-id"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .await
        .expect("public stream id");
    assert!(id.contains("spot@public.aggre.depth.v3.api.pb@10ms@BTCUSDT"));

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_streams);
    assert_eq!(
        capabilities.order_book.strictness,
        OrderBookStrictness::SnapshotOnly
    );
    assert!(!capabilities.order_book.supports_sequence);
    assert_eq!(capabilities.max_order_book_depth, Some(50));
}
