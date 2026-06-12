use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    AmendOrderRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelOrderRequest,
    CapabilitySupport, ClosePositionRequest, CountdownCancelAllRequest, ExchangeClient,
    ExchangeStreamEvent, FeesRequest, FundingRatesRequest, PlaceOrderRequest, PositionMode,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    RequestContext, SetLeverageRequest, SetPositionModeRequest, StreamHeartbeatDirection,
    SymbolScope, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, PositionSide, TenantId,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::orderbook_state::{OrderBookApplyOutcome, OrderBookState, OrderBookStateConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};
use crate::signing_spec::SigningVector;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private_parser::parse_fee_snapshots;
use super::signing::{sign_rest_payload, sign_ws_auth};
use super::streams::{
    bybit_heartbeat_payload, bybit_private_auth_payload, bybit_private_resubscribe_plan,
    bybit_private_subscribe_payload, bybit_private_subscribe_payloads,
    bybit_public_subscribe_payload, bybit_public_ws_url, classify_ws_control,
    parse_bybit_orderbook_delta, parse_bybit_private_stream_events,
    parse_bybit_public_stream_events,
};
use super::{BybitGatewayAdapter, BybitGatewayConfig, GatewayAdapter};

#[test]
fn bybit_signing_should_match_fixture_vector() {
    for path in [
        "place_order_limit.json",
        "amend_order_qty.json",
        "batch_place_orders.json",
        "batch_cancel_orders.json",
    ] {
        let vector = load_signing_vector(path);
        vector.verify().expect("fixture signature");
    }

    let payload = "{\"category\":\"linear\",\"orderLinkId\":\"cli-place\",\"orderType\":\"Limit\",\"positionIdx\":1,\"price\":\"25000\",\"qty\":\"0.01\",\"side\":\"Buy\",\"symbol\":\"BTCUSDT\"}";
    let signature = sign_rest_payload("test-secret", "1700000000000", "test-key", "5000", payload)
        .expect("signature");
    assert_eq!(
        signature,
        "61ca803aa771613200b534a9e9e2ab4b46c7b7870baa675f3f83991500a9d783"
    );
}

#[test]
fn bybit_parser_should_parse_instruments_and_orderbook() {
    let exchange = exchange_id();
    let rules = parse_symbol_rules(
        &exchange,
        MarketType::Perpetual,
        &json!({
            "result": {
                "list": [{
                    "symbol": "BTCUSDT",
                    "status": "Trading",
                    "baseCoin": "BTC",
                    "quoteCoin": "USDT",
                    "priceFilter": {"tickSize": "0.10", "minPrice": "0.10"},
                    "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"}
                }]
            }
        }),
    )
    .expect("rules");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTCUSDT");
    assert!(rules[0].supports_reduce_only);

    let book = parse_orderbook_snapshot(
        &exchange,
        symbol_scope("BTCUSDT"),
        &json!({
            "result": {
                "b": [["25000", "1.2"]],
                "a": [["25000.5", "0.8"]],
                "u": 100,
                "ts": 1700000000000_i64
            }
        }),
    )
    .expect("book");
    assert_eq!(book.sequence, Some(100));
    assert_eq!(book.bids[0].quantity, 1.2);
}

#[test]
fn bybit_fee_parser_should_parse_account_fee_rate_rows() {
    let fees = parse_fee_snapshots(
        &exchange_id(),
        &[symbol_scope("BTCUSDT")],
        &json!({
            "retCode": 0,
            "result": {
                "list": [{
                    "symbol": "BTCUSDT",
                    "makerFeeRate": "0.0001",
                    "takerFeeRate": "0.0006"
                }]
            }
        }),
    )
    .expect("fees");

    assert_eq!(fees.len(), 1);
    assert_eq!(fees[0].maker_rate, "0.0001");
    assert_eq!(fees[0].taker_rate, "0.0006");
    assert_eq!(fees[0].source.as_deref(), Some("bybit.v5.account_fee_rate"));
}

#[test]
fn bybit_capabilities_should_declare_public_funding_rates() {
    let adapter = BybitGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_funding_rates);
    assert!(capabilities.capabilities_v2.funding_rates.is_supported());
}

#[test]
fn bybit_capabilities_should_declare_private_stream_runtime_when_credentials_are_enabled() {
    let adapter = BybitGatewayAdapter::new(BybitGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BybitGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_private_streams);
    assert!(capabilities
        .private_stream_capabilities
        .as_ref()
        .is_some_and(|capabilities| capabilities.supports_orders
            && capabilities.supports_fills
            && capabilities.supports_balances
            && capabilities.supports_positions));
    assert!(matches!(
        capabilities.capabilities_v2.private_streams,
        CapabilitySupport::Native
    ));
    assert!(capabilities.order_book.supports_sequence);
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
    assert!(runtime.auth.requires_relogin_on_reconnect);
}

#[test]
fn bybit_account_control_capabilities_should_track_private_rest() {
    let public = BybitGatewayAdapter::default_public().expect("adapter");
    let public_capabilities = GatewayAdapter::account_control_capabilities(&public);
    assert!(!public_capabilities.supports_leverage);
    assert!(!public_capabilities.supports_position_mode_change);
    assert!(!public_capabilities.supports_close_position);
    assert!(!public_capabilities.supports_countdown_cancel_all);

    let private = BybitGatewayAdapter::new(BybitGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BybitGatewayConfig::default()
    })
    .expect("adapter");
    let private_capabilities = GatewayAdapter::account_control_capabilities(&private);
    assert!(private_capabilities.supports_leverage);
    assert!(private_capabilities.supports_position_mode_change);
    assert!(private_capabilities.supports_close_position);
    assert!(private_capabilities.supports_countdown_cancel_all);
}

#[test]
fn bybit_ws_payloads_should_match_v5_shapes() {
    let public = bybit_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol_scope("BTCUSDT"),
        kind: PublicStreamKind::OrderBookSnapshot,
    })
    .expect("public payload");
    assert_eq!(public["op"], "subscribe");
    assert_eq!(public["args"][0], "orderbook.1.BTCUSDT");
    assert!(public.get("category").is_none());

    let public_depth = bybit_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-depth-ws"),
        symbol: symbol_scope("BTCUSDT"),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("public depth payload");
    assert_eq!(public_depth["args"][0], "orderbook.50.BTCUSDT");
    assert_eq!(
        bybit_public_ws_url(
            "wss://stream.bybit.com/v5/public/linear?max_active_time=1m",
            MarketType::Spot,
        ),
        "wss://stream.bybit.com/v5/public/spot?max_active_time=1m"
    );

    let private = bybit_private_subscribe_payload(&PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").unwrap(),
        kind: PrivateStreamKind::Orders,
    })
    .expect("private payload");
    assert_eq!(private["args"][0], "order");

    let auth =
        bybit_private_auth_payload("test-key", "test-secret", 1700000060000).expect("auth payload");
    assert_eq!(auth["op"], "auth");
    assert_eq!(
        auth["args"][2],
        sign_ws_auth("test-secret", 1700000060000).expect("ws sign")
    );
    assert_eq!(bybit_heartbeat_payload()["op"], "ping");
    assert_eq!(classify_ws_control(&json!({"op": "pong"})).unwrap(), "pong");
}

#[test]
fn bybit_private_ws_account_plan_should_cover_all_topics_and_rest_resync() {
    let account = private_subscription(PrivateStreamKind::Account);
    let payloads = bybit_private_subscribe_payloads(&account).expect("payloads");
    let topics: Vec<_> = payloads
        .iter()
        .map(|payload| payload["args"][0].as_str().expect("topic"))
        .collect();
    assert_eq!(topics, vec!["order", "execution", "position", "wallet"]);

    let plan =
        bybit_private_resubscribe_plan("test-key", "test-secret", 1_700_000_060_000, &account)
            .expect("plan");
    assert_eq!(plan.auth_payload["op"], "auth");
    assert_eq!(
        plan.topics,
        vec!["order", "execution", "position", "wallet"]
    );
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
fn bybit_public_ws_parser_should_emit_orderbook_events() {
    let events = parse_bybit_public_stream_events(
        &exchange_id(),
        symbol_scope("BTCUSDT"),
        &json!({
            "topic": "orderbook.50.BTCUSDT",
            "type": "delta",
            "ts": 1687940967466_i64,
            "data": {
                "s": "BTCUSDT",
                "b": [
                    ["30247.20", "30.028"],
                    ["30240.00", "0"]
                ],
                "a": [
                    ["30248.70", "0"],
                    ["30249.30", "0.892"]
                ],
                "u": 177400507_u64,
                "seq": 66544703342_u64
            },
            "cts": 1687940967464_i64
        }),
    )
    .expect("events");

    let Some(ExchangeStreamEvent::OrderBookSnapshot(response)) = events.first() else {
        panic!("expected order book snapshot event");
    };
    assert_eq!(response.order_book.sequence, Some(66544703342));
    assert_eq!(response.order_book.bids.len(), 1);
    assert_eq!(response.order_book.bids[0].price, 30247.20);
    assert_eq!(response.order_book.asks.len(), 1);
    assert_eq!(response.order_book.asks[0].quantity, 0.892);

    let heartbeat = parse_bybit_public_stream_events(
        &exchange_id(),
        symbol_scope("BTCUSDT"),
        &json!({"success": true, "ret_msg": "pong", "op": "ping"}),
    )
    .expect("heartbeat");
    assert!(matches!(
        heartbeat.first(),
        Some(ExchangeStreamEvent::Heartbeat { .. })
    ));

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybit/ws_public_orderbook.json"
    ))
    .expect("fixture");
    let fixture_events =
        parse_bybit_public_stream_events(&exchange_id(), symbol_scope("BTCUSDT"), &fixture)
            .expect("fixture events");
    assert!(matches!(
        fixture_events.first(),
        Some(ExchangeStreamEvent::OrderBookSnapshot(_))
    ));
}

#[test]
fn bybit_public_ws_orderbook_delta_should_apply_contiguous_update_and_resync_on_gap() {
    let exchange = exchange_id();
    let symbol = symbol_scope("BTCUSDT");
    let snapshot = parse_orderbook_snapshot(
        &exchange,
        symbol.clone(),
        &json!({
            "result": {
                "s": "BTCUSDT",
                "b": [["30247.20", "30.028"]],
                "a": [["30249.30", "0.892"]],
                "u": 177400507_u64,
                "seq": 66544703342_u64
            }
        }),
    )
    .expect("snapshot");
    let mut state = OrderBookState::from_snapshot(snapshot)
        .expect("state")
        .with_config(OrderBookStateConfig::strict_delta());

    let contiguous = parse_bybit_orderbook_delta(
        &exchange,
        &symbol,
        &json!({
            "topic": "orderbook.50.BTCUSDT",
            "type": "delta",
            "data": {
                "s": "BTCUSDT",
                "b": [["30247.20", "31.000"]],
                "a": [["30249.30", "0"]],
                "u": 177400508_u64,
                "seq": 66544703343_u64
            },
            "cts": 1687940967464_i64
        }),
    )
    .expect("delta");
    assert!(matches!(
        state.apply_delta(contiguous, None).expect("apply"),
        OrderBookApplyOutcome::DeltaApplied {
            sequence: Some(66544703343)
        }
    ));

    let gap = parse_bybit_orderbook_delta(
        &exchange,
        &symbol,
        &json!({
            "topic": "orderbook.50.BTCUSDT",
            "type": "delta",
            "data": {
                "s": "BTCUSDT",
                "b": [["30247.20", "32.000"]],
                "a": [],
                "u": 177400511_u64,
                "seq": 66544703346_u64
            }
        }),
    )
    .expect("gap delta");
    let outcome = state.apply_delta(gap, None).expect("apply gap");
    let OrderBookApplyOutcome::ResyncRequired(request) = outcome else {
        panic!("expected resync required");
    };
    assert_eq!(request.expected_next_sequence, Some(66544703344));
    assert_eq!(request.received_first_sequence, Some(66544703346));
}

#[test]
fn bybit_private_ws_parser_should_normalize_order_execution_position_and_wallet_events() {
    let exchange = exchange_id();
    let symbol = symbol_scope("BTCUSDT");
    let subscription = private_subscription(PrivateStreamKind::Orders);
    let order_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bybit/ws_private_order.json"
    ))
    .expect("order fixture");

    let order_events =
        parse_bybit_private_stream_events(&exchange, &subscription, Some(&symbol), &order_fixture)
            .expect("order events");
    let Some(ExchangeStreamEvent::OrderUpdate(order)) = order_events.first() else {
        panic!("expected order update");
    };
    assert_eq!(order.exchange_order_id.as_deref(), Some("order-1"));
    assert_eq!(order.client_order_id.as_deref(), Some("cli-place"));
    assert_eq!(order.status, OrderStatus::New);
    assert_eq!(order.position_side, Some(PositionSide::Long));

    let fill_events = parse_bybit_private_stream_events(
        &exchange,
        &private_subscription(PrivateStreamKind::Fills),
        None,
        &json!({
            "topic": "execution",
            "data": [{
                "symbol": "BTCUSDT",
                "orderId": "order-1",
                "orderLinkId": "cli-place",
                "execId": "fill-1",
                "side": "Buy",
                "execPrice": "25000",
                "execQty": "0.01",
                "execValue": "250",
                "execFee": "0.1",
                "feeCurrency": "USDT",
                "isMaker": false,
                "execTime": "1700000000000"
            }]
        }),
    )
    .expect("fill events");
    let Some(ExchangeStreamEvent::Fill(fill)) = fill_events.first() else {
        panic!("expected fill");
    };
    assert_eq!(fill.fill_id.as_deref(), Some("fill-1"));
    assert_eq!(fill.canonical_symbol.as_str(), "BTC/USDT");
    assert_eq!(fill.price, 25000.0);
    assert_eq!(fill.quantity, 0.01);

    let position_events = parse_bybit_private_stream_events(
        &exchange,
        &private_subscription(PrivateStreamKind::Positions),
        Some(&symbol),
        &json!({
            "topic": "position",
            "data": [{
                "symbol": "BTCUSDT",
                "side": "Buy",
                "size": "0.01",
                "avgPrice": "25000",
                "markPrice": "25010",
                "liqPrice": "0",
                "unrealisedPnl": "1",
                "leverage": "5"
            }]
        }),
    )
    .expect("position events");
    let Some(ExchangeStreamEvent::PositionSnapshot(positions)) = position_events.first() else {
        panic!("expected position snapshot");
    };
    assert_eq!(positions.positions.len(), 1);
    assert_eq!(positions.positions[0].side, PositionSide::Long);
    assert_eq!(positions.positions[0].quantity, 0.01);

    let balance_events = parse_bybit_private_stream_events(
        &exchange,
        &private_subscription(PrivateStreamKind::Balances),
        Some(&symbol),
        &json!({
            "topic": "wallet",
            "data": [{
                "accountType": "UNIFIED",
                "coin": [{
                    "coin": "USDT",
                    "walletBalance": "100",
                    "availableToWithdraw": "90"
                }]
            }]
        }),
    )
    .expect("balance events");
    let Some(ExchangeStreamEvent::BalanceSnapshot(balances)) = balance_events.first() else {
        panic!("expected balance snapshot");
    };
    assert_eq!(balances.balances.len(), 1);
    assert_eq!(balances.balances[0].balances[0].asset, "USDT");
    assert_eq!(balances.balances[0].balances[0].available, 90.0);

    let categorized_fill_events = parse_bybit_private_stream_events(
        &exchange,
        &private_subscription(PrivateStreamKind::Fills),
        None,
        &json!({
            "topic": "execution.linear",
            "data": [{
                "symbol": "BTCUSDT",
                "orderId": "order-2",
                "orderLinkId": "cli-place-2",
                "execId": "fill-2",
                "side": "Sell",
                "execPrice": "25001",
                "execQty": "0.02",
                "execValue": "500.02",
                "execFee": "0.2",
                "feeCurrency": "USDT",
                "isMaker": true,
                "execTime": "1700000001000"
            }]
        }),
    )
    .expect("categorized fill events");
    let Some(ExchangeStreamEvent::Fill(fill)) = categorized_fill_events.first() else {
        panic!("expected categorized fill");
    };
    assert_eq!(fill.fill_id.as_deref(), Some("fill-2"));

    let auth_ack = parse_bybit_private_stream_events(
        &exchange,
        &subscription,
        Some(&symbol),
        &json!({"success": true, "op": "auth", "conn_id": "abc"}),
    )
    .expect("auth ack");
    assert!(auth_ack.is_empty());

    let heartbeat = parse_bybit_private_stream_events(
        &exchange,
        &subscription,
        Some(&symbol),
        &json!({"success": true, "ret_msg": "pong", "op": "ping"}),
    )
    .expect("private heartbeat");
    assert!(matches!(
        heartbeat.first(),
        Some(ExchangeStreamEvent::Heartbeat { .. })
    ));
}

#[tokio::test]
async fn bybit_get_fees_should_send_signed_fee_rate_request() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": [{
                "symbol": "BTCUSDT",
                "takerFeeRate": "0.0006",
                "makerFeeRate": "0.0001"
            }]
        },
        "retExtInfo": {},
        "time": 1676360412576_i64
    })])
    .await;

    let adapter = BybitGatewayAdapter::new(BybitGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..BybitGatewayConfig::default()
    })
    .expect("adapter");
    let response = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope("BTCUSDT")],
        })
        .await
        .expect("fees");

    assert_eq!(response.fees[0].maker_rate, "0.0001");
    assert_eq!(response.fees[0].taker_rate, "0.0006");
    let requests = seen.lock().unwrap();
    assert!(requests[0].starts_with("GET /v5/account/fee-rate?category=linear&symbol=BTCUSDT "));
    let request_lower = requests[0].to_ascii_lowercase();
    assert!(request_lower.contains("x-bapi-api-key: test-key"));
    assert!(!requests[0].contains("test-secret"));
}

#[tokio::test]
async fn bybit_get_funding_rates_should_merge_public_ticker_and_history_snapshot() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [{
                    "symbol": "BTCUSDT",
                    "markPrice": "65000.5",
                    "indexPrice": "64999.9",
                    "openInterest": "12345",
                    "turnover24h": "1000000",
                    "volume24h": "100",
                    "fundingRate": "0.0001",
                    "nextFundingTime": "1700028800000"
                }]
            }
        }),
        json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [{
                    "symbol": "BTCUSDT",
                    "fundingRate": "0.0001",
                    "fundingRateTimestamp": "1700000000000"
                }]
            }
        }),
    ])
    .await;
    let adapter = BybitGatewayAdapter::new(BybitGatewayConfig {
        rest_base_url: base_url,
        ..BybitGatewayConfig::default()
    })
    .expect("adapter");

    let response = ExchangeClient::get_funding_rates(
        &adapter,
        FundingRatesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("funding"),
            symbols: vec![symbol_scope("BTCUSDT")],
        },
    )
    .await
    .expect("funding");

    assert_eq!(response.rates.len(), 1);
    assert_eq!(response.rates[0].funding_rate, "0.0001");
    assert!(response.rates[0].funding_time.is_some());
    assert_eq!(
        response.rates[0]
            .next_funding_time
            .expect("next funding")
            .timestamp_millis(),
        1700028800000
    );
    assert_eq!(response.rates[0].mark_price.as_deref(), Some("65000.5"));
    assert_eq!(response.rates[0].index_price.as_deref(), Some("64999.9"));
    assert_eq!(response.rates[0].open_interest.as_deref(), Some("12345"));
    assert_eq!(response.rates[0].turnover_24h.as_deref(), Some("1000000"));
    assert_eq!(response.rates[0].volume_24h.as_deref(), Some("100"));
    assert_eq!(
        response.rates[0].source.as_deref(),
        Some("bybit.v5.market.tickers")
    );
    let request = actual_http_request(&seen.lock().unwrap()[0]);
    assert_eq!(request.path, "/v5/market/tickers");
    assert_eq!(
        request.query.get("category").map(String::as_str),
        Some("linear")
    );
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    let history_request = actual_http_request(&seen.lock().unwrap()[1]);
    assert_eq!(history_request.path, "/v5/market/funding/history");
    assert_eq!(
        history_request.query.get("limit").map(String::as_str),
        Some("1")
    );
}

#[tokio::test]
async fn bybit_advanced_orders_should_send_signed_v5_runtime_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {"orderId": "2001", "orderLinkId": "cli-amend"},
            "retExtInfo": {},
            "time": 1700000000000_i64
        }),
        json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {"category": "linear", "symbol": "BTCUSDT", "orderId": "3001", "orderLinkId": "cli-batch-1"},
                    {"category": "linear", "symbol": "BTCUSDT", "orderId": "", "orderLinkId": "cli-batch-2"}
                ]
            },
            "retExtInfo": {
                "list": [
                    {"code": 0, "msg": "OK"},
                    {"code": 110007, "msg": "Insufficient available balance"}
                ]
            },
            "time": 1700000000001_i64
        }),
        json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {"category": "linear", "symbol": "BTCUSDT", "orderId": "3001", "orderLinkId": "cli-batch-1"},
                    {"category": "linear", "symbol": "BTCUSDT", "orderId": "3002", "orderLinkId": "cli-batch-2"}
                ]
            },
            "retExtInfo": {
                "list": [
                    {"code": 0, "msg": "OK"},
                    {"code": 110001, "msg": "Order does not exist"}
                ]
            },
            "time": 1700000000002_i64
        }),
    ])
    .await;

    let adapter = BybitGatewayAdapter::new(BybitGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..BybitGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_amend_order);
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_order_list);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol_scope("BTCUSDT"),
            client_order_id: Some("cli-amend".to_string()),
            exchange_order_id: Some("2001".to_string()),
            new_client_order_id: None,
            new_quantity: "0.02".to_string(),
        })
        .await
        .expect("amend");
    assert_eq!(amended.order.exchange_order_id.as_deref(), Some("2001"));
    assert_eq!(amended.order.client_order_id.as_deref(), Some("cli-amend"));
    assert_eq!(amended.order.quantity, "0.02");

    let first_order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-1"),
        symbol: symbol_scope("BTCUSDT"),
        client_order_id: Some("cli-batch-1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("25000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let second_order = PlaceOrderRequest {
        client_order_id: Some("cli-batch-2".to_string()),
        price: Some("25100".to_string()),
        ..first_order.clone()
    };
    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![first_order, second_order],
        })
        .await
        .expect("batch place");
    assert_eq!(placed.orders.len(), 1);
    assert_eq!(placed.orders[0].exchange_order_id.as_deref(), Some("3001"));
    let place_report = placed.report.expect("place report");
    assert_eq!(place_report.total_items, 2);
    assert_eq!(place_report.succeeded_count(), 1);
    assert_eq!(place_report.failed_count(), 1);
    assert!(place_report.requires_reconciliation());

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-1"),
                    symbol: symbol_scope("BTCUSDT"),
                    client_order_id: Some("cli-batch-1".to_string()),
                    exchange_order_id: Some("3001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-2"),
                    symbol: symbol_scope("BTCUSDT"),
                    client_order_id: Some("cli-batch-2".to_string()),
                    exchange_order_id: Some("3002".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 1);
    assert_eq!(cancelled.orders[0].status, OrderStatus::Cancelled);
    let cancel_report = cancelled.report.expect("cancel report");
    assert_eq!(cancel_report.total_items, 2);
    assert_eq!(cancel_report.succeeded_count(), 1);
    assert_eq!(cancel_report.failed_count(), 1);
    assert!(cancel_report.requires_reconciliation());

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert_signed_bybit_request(&requests[0], "POST", "/v5/order/amend");
    load_request_spec("amend_order.json")
        .assert_matches(&actual_http_request(&requests[0]))
        .expect("amend request spec");
    let body = request_body_json(&requests[0]);
    assert_eq!(body["category"], "linear");
    assert_eq!(body["symbol"], "BTCUSDT");
    assert_eq!(body["orderId"], "2001");
    assert_eq!(body["orderLinkId"], "cli-amend");
    assert_eq!(body["qty"], "0.02");

    assert_signed_bybit_request(&requests[1], "POST", "/v5/order/create-batch");
    load_request_spec("batch_place_orders.json")
        .assert_matches(&actual_http_request(&requests[1]))
        .expect("batch place request spec");
    let body = request_body_json(&requests[1]);
    assert_eq!(body["category"], "linear");
    assert_eq!(body["request"][0]["symbol"], "BTCUSDT");
    assert_eq!(body["request"][0]["side"], "Buy");
    assert_eq!(body["request"][0]["orderType"], "Limit");
    assert_eq!(body["request"][0]["qty"], "0.01");
    assert_eq!(body["request"][0]["price"], "25000");
    assert_eq!(body["request"][0]["orderLinkId"], "cli-batch-1");

    assert_signed_bybit_request(&requests[2], "POST", "/v5/order/cancel-batch");
    load_request_spec("batch_cancel_orders.json")
        .assert_matches(&actual_http_request(&requests[2]))
        .expect("batch cancel request spec");
    let body = request_body_json(&requests[2]);
    assert_eq!(body["category"], "linear");
    assert_eq!(body["request"][0]["symbol"], "BTCUSDT");
    assert_eq!(body["request"][0]["orderId"], "3001");
    assert_eq!(body["request"][0]["orderLinkId"], "cli-batch-1");
}

#[tokio::test]
async fn bybit_set_leverage_should_send_signed_v5_position_request() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "retCode": 0,
        "retMsg": "OK",
        "result": {},
        "retExtInfo": {},
        "time": 1700000000000_i64
    })])
    .await;

    let adapter = BybitGatewayAdapter::new(BybitGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..BybitGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .set_leverage(SetLeverageRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("set-leverage"),
            symbol: symbol_scope("BTCUSDT"),
            leverage: 3,
        })
        .await
        .expect("set leverage");

    assert!(response.accepted);
    assert_eq!(response.leverage, 3);
    assert_eq!(response.symbol.exchange_symbol.symbol, "BTCUSDT");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_signed_bybit_request(&requests[0], "POST", "/v5/position/set-leverage");
    load_request_spec("set_leverage.json")
        .assert_matches(&actual_http_request(&requests[0]))
        .expect("set leverage request spec");
    let body = request_body_json(&requests[0]);
    assert_eq!(body["category"], "linear");
    assert_eq!(body["symbol"], "BTCUSDT");
    assert_eq!(body["buyLeverage"], "3");
    assert_eq!(body["sellLeverage"], "3");
}

#[tokio::test]
async fn bybit_set_position_mode_should_send_signed_v5_switch_mode_request() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "retCode": 0,
        "retMsg": "OK",
        "result": {},
        "retExtInfo": {},
        "time": 1700000000000_i64
    })])
    .await;

    let adapter = BybitGatewayAdapter::new(BybitGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..BybitGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .set_position_mode(SetPositionModeRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("set-position-mode"),
            exchange: exchange_id(),
            mode: PositionMode::Hedge,
        })
        .await
        .expect("set position mode");

    assert!(response.accepted);
    assert_eq!(response.mode, PositionMode::Hedge);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_signed_bybit_request(&requests[0], "POST", "/v5/position/switch-mode");
    let body = request_body_json(&requests[0]);
    assert_eq!(body["category"], "linear");
    assert_eq!(body["mode"], 3);
}

#[tokio::test]
async fn bybit_countdown_cancel_all_should_send_signed_v5_dcp_request() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "retCode": 0,
        "retMsg": "success",
        "result": {},
        "retExtInfo": {},
        "time": 1700000000000_i64
    })])
    .await;

    let adapter = BybitGatewayAdapter::new(BybitGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..BybitGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = GatewayAdapter::account_control_capabilities(&adapter);
    assert!(capabilities.supports_countdown_cancel_all);

    let response = GatewayAdapter::set_countdown_cancel_all(
        &adapter,
        CountdownCancelAllRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("set-dcp"),
            exchange: exchange_id(),
            symbol: Some(symbol_scope("BTCUSDT")),
            timeout_secs: 40,
        },
    )
    .await
    .expect("set dcp");

    assert!(response.accepted);
    assert_eq!(response.timeout_secs, 40);
    assert!(response.trigger_time.is_none());

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_signed_bybit_request(&requests[0], "POST", "/v5/order/disconnected-cancel-all");
    load_request_spec("set_countdown_cancel_all.json")
        .assert_matches(&actual_http_request(&requests[0]))
        .expect("set countdown cancel all request spec");
    let body = request_body_json(&requests[0]);
    assert_eq!(body["product"], "DERIVATIVES");
    assert_eq!(body["timeWindow"], 40);
}

#[tokio::test]
async fn bybit_close_position_should_submit_reduce_only_ioc_order() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "symbol": "BTCUSDT",
            "orderId": "4001",
            "orderLinkId": "CLOSE_SHORT",
            "side": "Buy",
            "orderType": "Limit",
            "orderStatus": "New",
            "qty": "0.01",
            "price": "65000",
            "reduceOnly": true
        },
        "retExtInfo": {},
        "time": 1700000000000_i64
    })])
    .await;

    let adapter = BybitGatewayAdapter::new(BybitGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..BybitGatewayConfig::default()
    })
    .expect("adapter");

    let response = GatewayAdapter::close_position(
        &adapter,
        ClosePositionRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("close-short"),
            symbol: symbol_scope("BTCUSDT"),
            position_side: PositionSide::Short,
            quantity: "0.01".to_string(),
            price: Some("65000".to_string()),
            order_type: OrderType::IOC,
            time_in_force: TimeInForce::IOC,
            client_order_id: "CLOSE_SHORT".to_string(),
            max_slippage_pct: None,
        },
    )
    .await
    .expect("close position");

    assert!(response.accepted);
    assert_eq!(response.exchange_order_id.as_deref(), Some("4001"));
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_signed_bybit_request(&requests[0], "POST", "/v5/order/create");
    let body = request_body_json(&requests[0]);
    assert_eq!(body["category"], "linear");
    assert_eq!(body["symbol"], "BTCUSDT");
    assert_eq!(body["side"], "Buy");
    assert_eq!(body["positionIdx"], 2);
    assert_eq!(body["reduceOnly"], true);
    assert_eq!(body["orderType"], "Limit");
    assert_eq!(body["timeInForce"], "IOC");
    assert_eq!(body["orderLinkId"], "CLOSE_SHORT");
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bybit").unwrap()
}

fn symbol_scope(symbol: &str) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, symbol).unwrap(),
    }
}

fn private_subscription(kind: PrivateStreamKind) -> PrivateStreamSubscription {
    PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws-parser"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").unwrap(),
        kind,
    }
}

async fn spawn_rest_server(responses: Vec<serde_json::Value>) -> (String, Arc<Mutex<Vec<String>>>) {
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
            seen_requests.lock().unwrap().push(request_text);
            let body = responses
                .lock()
                .unwrap()
                .next()
                .unwrap_or_else(|| json!({ "retCode": 0, "result": { "list": [] } }));
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

fn assert_signed_bybit_request(request: &str, method: &str, path: &str) {
    assert!(request.starts_with(&format!("{method} {path} ")));
    let request_lower = request.to_ascii_lowercase();
    assert!(request_lower.contains("x-bapi-api-key: test-key"));
    assert!(request_lower.contains("x-bapi-sign: "));
    assert!(request_lower.contains("x-bapi-sign-type: 2"));
    assert!(request_lower.contains("x-bapi-recv-window: 5000"));
    assert!(request_lower.contains("content-type: application/json"));
    assert!(!request.contains("test-secret"));
}

fn request_body_json(request: &str) -> serde_json::Value {
    let body = request
        .split("\r\n\r\n")
        .nth(1)
        .expect("request body")
        .trim();
    serde_json::from_str(body).expect("request body json")
}

fn actual_http_request(request: &str) -> ActualHttpRequest {
    let (head, body) = request
        .split_once("\r\n\r\n")
        .map_or((request, ""), |(head, body)| (head, body));
    let mut lines = head.lines();
    let request_line = lines.next().expect("request line");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().expect("method");
    let path_with_query = parts.next().expect("path");
    let (path, query) = path_with_query
        .split_once('?')
        .map_or((path_with_query, ""), |(path, query)| (path, query));
    let headers = lines
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.trim().to_string(), value.trim().to_string()))
        })
        .collect::<BTreeMap<_, _>>();
    let query = query
        .split('&')
        .filter(|part| !part.is_empty())
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            Some((key.to_string(), value.to_string()))
        })
        .collect::<BTreeMap<_, _>>();
    let body = body
        .trim()
        .is_empty()
        .then_some(None)
        .unwrap_or_else(|| Some(serde_json::from_str(body.trim()).expect("json body")));
    ActualHttpRequest::new(method, path)
        .with_query(query)
        .with_headers(headers)
        .with_body(body)
}

fn load_request_spec(path: &str) -> RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/bybit/request_specs/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("request spec fixture");
    serde_json::from_str(&text).expect("request spec fixture")
}

fn load_signing_vector(path: &str) -> SigningVector {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/bybit/signing_vectors/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("signing vector fixture");
    serde_json::from_str(&text).expect("signing vector fixture")
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").unwrap()),
        account_id: Some(AccountId::new("account").unwrap()),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}
