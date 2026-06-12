use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    AccountId, AmendOrderRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelOrderRequest, CapabilitySupport, ClosePositionRequest, ExchangeClient,
    ExchangeStreamEvent, PlaceOrderRequest, PositionMode, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, RequestContext,
    StreamHeartbeatDirection, SymbolScope, TenantId, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, LiquidityRole, MarketType, OrderSide, OrderStatus,
    OrderType, PositionSide,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::orderbook_state::{OrderBookApplyOutcome, OrderBookState, OrderBookStateConfig};

use super::parser::parse_aster_funding_snapshots;
use super::parser::parse_orderbook_snapshot;
use super::private::parse_aster_position_mode;
use super::private_parser::parse_positions;
use super::signing::{sign_message, signing_address_from_private_key};
use super::streams::{
    aster_partial_depth_stream_name, aster_public_stream_name, aster_public_subscribe_payload,
    parse_aster_diff_depth_delta, parse_aster_private_stream_events,
};
use super::{AsterGatewayAdapter, AsterGatewayConfig, GatewayAdapter};

#[test]
fn aster_v3_signing_should_emit_recoverable_signature() {
    let private_key = "0000000000000000000000000000000000000000000000000000000000000001";
    let message =
        "nonce=1748310859508867&signer=0x7e5f4552091a69125d5dfcb7b8c2659029395bdf&symbol=ASTERUSDT";
    let signature = sign_message(private_key, message).expect("signature");
    assert_eq!(signature.len(), 132);
    assert_eq!(
        signing_address_from_private_key(private_key).expect("address"),
        "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
    );
}

#[test]
fn aster_ws_payload_should_use_lowercase_futures_stream_names() {
    let subscription = public_subscription(PublicStreamKind::OrderBookDelta);
    assert_eq!(
        aster_public_stream_name(&subscription).expect("stream"),
        "btcusdt@depth@100ms"
    );
    assert_eq!(
        aster_public_subscribe_payload(&subscription).expect("payload")["params"][0],
        "btcusdt@depth@100ms"
    );
    let ticker = public_subscription(PublicStreamKind::Ticker);
    assert_eq!(
        aster_public_stream_name(&ticker).expect("ticker"),
        "btcusdt@bookTicker"
    );
    let depth20 = public_subscription(PublicStreamKind::OrderBookSnapshot);
    assert_eq!(
        aster_public_stream_name(&depth20).expect("depth20"),
        "btcusdt@depth20@100ms"
    );
    assert_eq!(
        aster_partial_depth_stream_name("BTCUSDT", 5).expect("depth5"),
        "btcusdt@depth5@100ms"
    );
    assert_eq!(
        aster_partial_depth_stream_name("BTCUSDT", 10).expect("depth10"),
        "btcusdt@depth10@100ms"
    );
    assert_eq!(
        aster_partial_depth_stream_name("BTCUSDT", 20).expect("depth20"),
        "btcusdt@depth20@100ms"
    );
    assert!(aster_partial_depth_stream_name("BTCUSDT", 50).is_err());
}

#[tokio::test]
async fn aster_listen_key_lifecycle_should_create_renew_and_delete_signed_stream_key() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"listenKey": "listen-key-1"}),
        json!({}),
        json!({}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    let stream_id = adapter
        .subscribe_private_stream(private_subscription(PrivateStreamKind::Account))
        .await
        .expect("private stream");
    assert_eq!(
        stream_id,
        "aster:wss://fstream.asterdex.com/ws:listen-key-1:ACCOUNT_UPDATE"
    );

    adapter
        .renew_listen_key("listen-key-1")
        .await
        .expect("renew listen key");
    adapter
        .delete_listen_key("listen-key-1")
        .await
        .expect("delete listen key");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/fapi/v3/listenKey");
    assert!(requests[0].form.contains_key("signature"));

    assert_eq!(requests[1].method, "PUT");
    assert_eq!(requests[1].path, "/fapi/v3/listenKey");
    assert_eq!(
        requests[1].form.get("listenKey").map(String::as_str),
        Some("listen-key-1")
    );
    assert!(requests[1].form.contains_key("signature"));

    assert_eq!(requests[2].method, "DELETE");
    assert_eq!(requests[2].path, "/fapi/v3/listenKey");
    assert_eq!(
        requests[2].form.get("listenKey").map(String::as_str),
        Some("listen-key-1")
    );
    assert!(requests[2].form.contains_key("signature"));
}

#[test]
fn aster_position_parser_should_split_usdt_symbols_before_usd_suffix() {
    let exchange = ExchangeId::new("aster").expect("exchange");
    let positions = parse_positions(
        &exchange,
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &[],
        &json!([{
            "symbol": "BTCUSDT",
            "positionAmt": "1.5",
            "positionSide": "BOTH",
            "entryPrice": "100.0",
            "markPrice": "101.0",
            "unRealizedProfit": "1.0",
            "leverage": "5"
        }]),
    )
    .expect("positions");
    assert_eq!(positions[0].canonical_symbol.as_str(), "BTC/USDT");
}

#[test]
fn aster_diff_depth_delta_should_apply_contiguous_update_and_resync_on_pu_gap() {
    let exchange = ExchangeId::new("aster").expect("exchange");
    let symbol = btc_symbol_scope(&exchange);
    let snapshot = parse_orderbook_snapshot(
        &exchange,
        symbol.clone(),
        &json!({
            "lastUpdateId": 100_u64,
            "bids": [["65000", "1"]],
            "asks": [["65001", "2"]]
        }),
    )
    .expect("snapshot");
    let mut state = OrderBookState::from_snapshot(snapshot)
        .expect("state")
        .with_config(OrderBookStateConfig::strict_delta());

    let contiguous = parse_aster_diff_depth_delta(
        &exchange,
        &symbol,
        &json!({
            "e": "depthUpdate",
            "E": 1700000000000_i64,
            "s": "BTCUSDT",
            "U": 101_u64,
            "u": 101_u64,
            "pu": 100_u64,
            "b": [["65000", "1.5"]],
            "a": [["65001", "0"]]
        }),
    )
    .expect("delta");
    assert!(matches!(
        state.apply_delta(contiguous, None).expect("apply"),
        OrderBookApplyOutcome::DeltaApplied {
            sequence: Some(101)
        }
    ));

    let gap = parse_aster_diff_depth_delta(
        &exchange,
        &symbol,
        &json!({
            "e": "depthUpdate",
            "s": "BTCUSDT",
            "U": 104_u64,
            "u": 104_u64,
            "pu": 103_u64,
            "b": [["65000", "2"]],
            "a": []
        }),
    )
    .expect("gap delta");
    let outcome = state.apply_delta(gap, None).expect("apply gap");
    let OrderBookApplyOutcome::ResyncRequired(request) = outcome else {
        panic!("expected resync required");
    };
    assert_eq!(request.expected_next_sequence, Some(102));
    assert_eq!(request.received_first_sequence, Some(104));
}

#[test]
fn aster_private_ws_parser_should_normalize_order_fill_balance_and_position_events() {
    let exchange = ExchangeId::new("aster").expect("exchange");
    let subscription = private_subscription(PrivateStreamKind::Account);
    let symbol = btc_symbol_scope(&exchange);

    let order_events = parse_aster_private_stream_events(
        &exchange,
        &subscription,
        Some(&symbol),
        &json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 1717171200001_i64,
            "o": {
                "s": "BTCUSDT",
                "c": "cli-1",
                "S": "BUY",
                "o": "LIMIT",
                "f": "GTC",
                "q": "0.02",
                "p": "65000",
                "x": "TRADE",
                "X": "PARTIALLY_FILLED",
                "i": 2001,
                "l": "0.01",
                "z": "0.01",
                "L": "65000",
                "n": "0.01",
                "N": "USDT",
                "t": 3001,
                "m": false,
                "T": 1717171200000_i64,
                "ps": "LONG"
            }
        }),
    )
    .expect("order events");

    assert_eq!(order_events.len(), 2);
    let ExchangeStreamEvent::OrderUpdate(order) = &order_events[0] else {
        panic!("expected order update");
    };
    assert_eq!(order.exchange_order_id.as_deref(), Some("2001"));
    assert_eq!(order.client_order_id.as_deref(), Some("cli-1"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    let ExchangeStreamEvent::Fill(fill) = &order_events[1] else {
        panic!("expected fill");
    };
    assert_eq!(fill.order_id.as_deref(), Some("2001"));
    assert_eq!(fill.fill_id.as_deref(), Some("3001"));
    assert_eq!(fill.quantity, 0.01);
    assert_eq!(fill.liquidity_role, LiquidityRole::Taker);
    assert_eq!(fill.position_side, PositionSide::Long);

    let account_events = parse_aster_private_stream_events(
        &exchange,
        &subscription,
        Some(&symbol),
        &json!({
            "e": "ACCOUNT_UPDATE",
            "E": 1717171201000_i64,
            "a": {
                "m": "ORDER",
                "B": [{"a": "USDT", "wb": "100.5", "cw": "90.5"}],
                "P": [{
                    "s": "BTCUSDT",
                    "pa": "0.02",
                    "ep": "65000",
                    "mp": "65100",
                    "up": "2.0",
                    "ps": "LONG"
                }]
            }
        }),
    )
    .expect("account events");

    assert_eq!(account_events.len(), 2);
    let ExchangeStreamEvent::BalanceSnapshot(balances) = &account_events[0] else {
        panic!("expected balances");
    };
    assert_eq!(balances.balances[0].balances[0].asset, "USDT");
    assert_eq!(balances.balances[0].balances[0].total, 100.5);
    let ExchangeStreamEvent::PositionSnapshot(positions) = &account_events[1] else {
        panic!("expected positions");
    };
    assert_eq!(positions.positions[0].canonical_symbol.base_asset(), "BTC");
    assert_eq!(positions.positions[0].quantity, 0.02);
    assert_eq!(positions.positions[0].side, PositionSide::Long);
}

#[test]
fn aster_account_control_capabilities_should_track_private_rest_credentials() {
    let public = AsterGatewayAdapter::default_public().expect("adapter");
    let public_capabilities = GatewayAdapter::account_control_capabilities(&public);
    assert!(!public_capabilities.supports_symbol_account_config);
    assert!(!public_capabilities.supports_leverage);
    assert!(!public_capabilities.supports_position_mode_change);
    assert!(!public_capabilities.supports_close_position);

    let private = AsterGatewayAdapter::new(AsterGatewayConfig {
        enabled_private_rest: true,
        user_address: Some("0x1111111111111111111111111111111111111111".to_string()),
        signer_address: Some("0x2222222222222222222222222222222222222222".to_string()),
        signer_private_key: Some(
            "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        ),
        ..AsterGatewayConfig::default()
    })
    .expect("adapter");
    let private_capabilities = GatewayAdapter::account_control_capabilities(&private);
    assert!(private_capabilities.supports_symbol_account_config);
    assert!(private_capabilities.supports_leverage);
    assert!(private_capabilities.supports_position_mode_change);
    assert!(private_capabilities.supports_close_position);
}

#[tokio::test]
async fn aster_close_position_should_submit_reduce_only_ioc_order() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "symbol": "BTCUSDT",
        "orderId": 3001,
        "clientOrderId": "CLOSE_LONG",
        "side": "SELL",
        "positionSide": "LONG",
        "type": "LIMIT",
        "origQty": "0.02",
        "price": "65000",
        "status": "NEW",
        "reduceOnly": true,
        "updateTime": 1700000000000_i64
    })])
    .await;
    let adapter = private_adapter(base_url);
    let exchange = ExchangeId::new("aster").expect("exchange");

    let response = GatewayAdapter::close_position(
        &adapter,
        ClosePositionRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("close-long"),
            symbol: btc_symbol_scope(&exchange),
            position_side: PositionSide::Long,
            quantity: "0.02".to_string(),
            price: Some("65000".to_string()),
            order_type: OrderType::IOC,
            time_in_force: TimeInForce::IOC,
            client_order_id: "CLOSE_LONG".to_string(),
            max_slippage_pct: None,
        },
    )
    .await
    .expect("close position");

    assert!(response.accepted);
    assert_eq!(response.exchange_order_id.as_deref(), Some("3001"));
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/fapi/v3/order");
    assert_eq!(
        requests[0].form.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(
        requests[0].form.get("side").map(String::as_str),
        Some("SELL")
    );
    assert_eq!(
        requests[0].form.get("type").map(String::as_str),
        Some("LIMIT")
    );
    assert_eq!(
        requests[0].form.get("timeInForce").map(String::as_str),
        Some("IOC")
    );
    assert_eq!(
        requests[0].form.get("reduceOnly").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        requests[0].form.get("positionSide").map(String::as_str),
        Some("LONG")
    );
    assert_eq!(
        requests[0].form.get("newClientOrderId").map(String::as_str),
        Some("CLOSE_LONG")
    );
    assert!(requests[0].form.contains_key("signature"));
}

#[test]
fn aster_private_stream_runtime_capability_should_declare_listen_key_resync_contract() {
    let private = AsterGatewayAdapter::new(AsterGatewayConfig {
        enabled_private_rest: true,
        enabled_private_streams: true,
        user_address: Some("0x1111111111111111111111111111111111111111".to_string()),
        signer_address: Some("0x2222222222222222222222222222222222222222".to_string()),
        signer_private_key: Some(
            "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        ),
        ..AsterGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = private.capabilities();
    assert!(capabilities.supports_private_streams);
    assert!(matches!(
        capabilities.capabilities_v2.private_streams,
        CapabilitySupport::Native
    ));
    let runtime = &capabilities.capabilities_v2.stream_runtime;
    assert!(runtime.public.is_supported());
    assert!(runtime.private.is_supported());
    assert!(runtime.supports_public_subscribe);
    assert!(runtime.supports_private_subscribe);
    assert!(runtime.heartbeat.required);
    assert_eq!(
        runtime.heartbeat.direction,
        StreamHeartbeatDirection::ClientPing
    );
    assert!(runtime.reconnect.supported);
    assert!(runtime.reconnect.requires_resubscribe);
    assert!(runtime.resync.order_book);
    assert!(runtime.resync.orders);
    assert!(runtime.resync.positions);
    assert!(runtime.resync.balances);
    assert!(runtime.auth.required);
    assert!(runtime.auth.uses_listen_key);
    assert_eq!(runtime.auth.renewal_ms, Some(30 * 60 * 1_000));
    assert!(runtime.auth.requires_relogin_on_reconnect);
}

#[test]
fn aster_private_stream_runtime_capability_should_track_private_stream_flag() {
    let adapter = AsterGatewayAdapter::new(AsterGatewayConfig {
        enabled_private_rest: true,
        enabled_private_streams: false,
        user_address: Some("0x1111111111111111111111111111111111111111".to_string()),
        signer_address: Some("0x2222222222222222222222222222222222222222".to_string()),
        signer_private_key: Some(
            "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        ),
        ..AsterGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_private_streams);
    assert!(matches!(
        capabilities.capabilities_v2.private_streams,
        CapabilitySupport::Unsupported { .. }
    ));
    let runtime = &capabilities.capabilities_v2.stream_runtime;
    assert!(!runtime.private.is_supported());
    assert!(!runtime.supports_private_subscribe);
    assert!(!runtime.auth.required);
    assert!(!runtime.resync.orders);
    assert!(!runtime.resync.positions);
    assert!(!runtime.resync.balances);
}

#[test]
fn aster_position_mode_parser_should_decode_dual_side_flag() {
    assert_eq!(
        parse_aster_position_mode(&json!({"dualSidePosition": true})).expect("hedge"),
        PositionMode::Hedge
    );
    assert_eq!(
        parse_aster_position_mode(&json!({"dualSidePosition": "false"})).expect("one-way"),
        PositionMode::OneWay
    );
}

#[test]
fn aster_funding_parser_should_merge_premium_and_history_snapshot() {
    let exchange = ExchangeId::new("aster").expect("exchange");
    let symbol = btc_symbol_scope(&exchange);
    let premium = fixture("premium_index_single.json");
    let history = fixture("funding_rate_history.json");
    let history_by_symbol = HashMap::from([("BTCUSDT".to_string(), history)]);

    let snapshots = parse_aster_funding_snapshots(
        &exchange,
        std::slice::from_ref(&symbol),
        &premium,
        &history_by_symbol,
    )
    .expect("funding");

    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].symbol, symbol);
    assert_eq!(snapshots[0].funding_rate, "0.00010000");
    assert_eq!(snapshots[0].predicted_funding_rate, None);
    assert_eq!(snapshots[0].mark_price.as_deref(), Some("65000.12500000"));
    assert_eq!(snapshots[0].index_price.as_deref(), Some("64999.90000000"));
    assert_eq!(snapshots[0].open_interest.as_deref(), Some("12345.67"));
    assert_eq!(snapshots[0].turnover_24h.as_deref(), Some("9876543.21"));
    assert_eq!(snapshots[0].volume_24h.as_deref(), Some("151.25"));
    assert_eq!(
        snapshots[0]
            .funding_time
            .expect("funding time")
            .timestamp_millis(),
        1717171200000
    );
    assert_eq!(
        snapshots[0]
            .next_funding_time
            .expect("next funding")
            .timestamp_millis(),
        1717200000000
    );
    assert_eq!(snapshots[0].updated_at.timestamp_millis(), 1717171200123);
}

#[test]
fn aster_funding_parser_should_support_all_symbols_without_history() {
    let exchange = ExchangeId::new("aster").expect("exchange");
    let premium = fixture("premium_index_all.json");
    let history_by_symbol = HashMap::new();

    let snapshots = parse_aster_funding_snapshots(&exchange, &[], &premium, &history_by_symbol)
        .expect("funding");

    assert_eq!(snapshots.len(), 2);
    assert_eq!(
        snapshots[0]
            .symbol
            .canonical_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "BTC/USDT"
    );
    assert_eq!(
        snapshots[1]
            .symbol
            .canonical_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "ETH/USDT"
    );
    assert_eq!(snapshots[1].funding_rate, "-0.00005000");
    assert!(snapshots
        .iter()
        .all(|snapshot| snapshot.funding_time.is_none()));
    assert!(snapshots
        .iter()
        .all(|snapshot| snapshot.next_funding_time.is_some()));
}

#[tokio::test]
async fn aster_advanced_orders_should_send_amend_and_native_batch_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "symbol": "BTCUSDT",
            "orderId": 1001,
            "clientOrderId": "AMEND1",
            "side": "BUY",
            "type": "LIMIT",
            "origQty": "0.02",
            "price": "65000",
            "status": "NEW",
            "updateTime": 1700000000000_i64
        }),
        json!([
            {
                "symbol": "BTCUSDT",
                "orderId": 2001,
                "clientOrderId": "BATCH1",
                "side": "BUY",
                "type": "LIMIT",
                "origQty": "0.01",
                "price": "65000",
                "status": "NEW",
                "updateTime": 1700000000001_i64
            },
            {"code": -2010, "msg": "Account has insufficient balance"}
        ]),
        json!([
            {
                "symbol": "BTCUSDT",
                "orderId": 2001,
                "clientOrderId": "BATCH1",
                "side": "BUY",
                "type": "LIMIT",
                "origQty": "0.01",
                "price": "65000",
                "status": "CANCELED",
                "updateTime": 1700000000002_i64
            },
            {"code": -2011, "msg": "Unknown order sent"}
        ]),
    ])
    .await;
    let adapter = private_adapter(base_url);
    let symbol = btc_symbol_scope(&ExchangeId::new("aster").expect("exchange"));

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("aster-amend"),
            symbol: symbol.clone(),
            client_order_id: Some("AMEND1".to_string()),
            exchange_order_id: Some("1001".to_string()),
            new_client_order_id: None,
            new_quantity: "0.02".to_string(),
        })
        .await
        .expect("amend");
    assert_eq!(amended.order.exchange_order_id.as_deref(), Some("1001"));

    let first_order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-1"),
        symbol: symbol.clone(),
        client_order_id: Some("BATCH1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let second_order = PlaceOrderRequest {
        client_order_id: Some("BATCH2".to_string()),
        price: Some("65010".to_string()),
        ..first_order.clone()
    };
    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: ExchangeId::new("aster").expect("exchange"),
            orders: vec![first_order, second_order],
        })
        .await
        .expect("batch place");
    assert_eq!(placed.orders.len(), 1);
    let place_report = placed.report.expect("place report");
    assert_eq!(place_report.total_items, 2);
    assert_eq!(place_report.succeeded_count(), 1);
    assert_eq!(place_report.failed_count(), 1);

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: ExchangeId::new("aster").expect("exchange"),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-1"),
                    symbol: symbol.clone(),
                    client_order_id: None,
                    exchange_order_id: Some("2001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-2"),
                    symbol: symbol.clone(),
                    client_order_id: None,
                    exchange_order_id: Some("2002".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 1);
    let cancel_report = cancelled.report.expect("cancel report");
    assert_eq!(cancel_report.total_items, 2);
    assert_eq!(cancel_report.succeeded_count(), 1);
    assert_eq!(cancel_report.failed_count(), 1);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "PUT");
    assert_eq!(requests[0].path, "/fapi/v3/order");
    assert_eq!(
        requests[0].form.get("quantity").map(String::as_str),
        Some("0.02")
    );
    assert_eq!(requests[1].method, "POST");
    assert_eq!(requests[1].path, "/fapi/v3/batchOrders");
    let batch_orders = serde_json::from_str::<serde_json::Value>(
        requests[1]
            .form
            .get("batchOrders")
            .expect("batchOrders form field"),
    )
    .expect("batchOrders json");
    assert_eq!(batch_orders[0]["newClientOrderId"], "BATCH1");
    assert_eq!(requests[2].method, "DELETE");
    assert_eq!(requests[2].path, "/fapi/v3/batchOrders");
    assert_eq!(
        requests[2].form.get("orderIdList").map(String::as_str),
        Some("[\"2001\",\"2002\"]")
    );
}

fn public_subscription(kind: PublicStreamKind) -> PublicStreamSubscription {
    let exchange = ExchangeId::new("aster").expect("exchange");
    PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: RequestContext::new(chrono::Utc::now()),
        symbol: rustcta_exchange_api::SymbolScope {
            exchange: exchange.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
            exchange_symbol: ExchangeSymbol::new(exchange, MarketType::Perpetual, "BTCUSDT")
                .expect("symbol"),
        },
        kind,
    }
}

fn private_subscription(kind: PrivateStreamKind) -> PrivateStreamSubscription {
    PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("aster-private-ws"),
        exchange: ExchangeId::new("aster").expect("exchange"),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind,
    }
}

fn btc_symbol_scope(exchange: &ExchangeId) -> SymbolScope {
    SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange.clone(), MarketType::Perpetual, "BTCUSDT")
            .expect("symbol"),
    }
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("account").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn private_adapter(rest_base_url: String) -> AsterGatewayAdapter {
    AsterGatewayAdapter::new(AsterGatewayConfig {
        rest_base_url,
        enabled_private_rest: true,
        user_address: Some("0x1111111111111111111111111111111111111111".to_string()),
        signer_address: Some("0x2222222222222222222222222222222222222222".to_string()),
        signer_private_key: Some(
            "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        ),
        ..AsterGatewayConfig::default()
    })
    .expect("adapter")
}

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    form: HashMap<String, String>,
}

async fn spawn_rest_server(
    responses: Vec<serde_json::Value>,
) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_requests = Arc::clone(&seen);
    let responses = Arc::new(Mutex::new(responses.into_iter()));

    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut buffer = vec![0_u8; 16384];
            let bytes_read = stream.read(&mut buffer).await.unwrap();
            let request_text = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
            seen_requests
                .lock()
                .unwrap()
                .push(parse_seen_request(&request_text));
            let body = responses
                .lock()
                .unwrap()
                .next()
                .unwrap_or_else(|| json!({}));
            let body_text = body.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body_text.len(),
                body_text
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    (format!("http://{address}"), seen)
}

fn parse_seen_request(request_text: &str) -> SeenRequest {
    let request_line = request_text.lines().next().unwrap_or_default();
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default().to_string();
    let target = request_parts.next().unwrap_or_default();
    let path = target.split_once('?').map_or(target, |(path, _)| path);
    let form = request_text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.trim())
        .unwrap_or_default()
        .split('&')
        .filter(|part| !part.is_empty())
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            Some((
                key.to_string(),
                urlencoding::decode(value).ok()?.into_owned(),
            ))
        })
        .collect();
    SeenRequest {
        method,
        path: path.to_string(),
        form,
    }
}

fn fixture(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/aster/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("fixture");
    serde_json::from_str(&text).expect("fixture json")
}
