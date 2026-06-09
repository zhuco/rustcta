use rustcta_exchange_api::ExchangeStreamEvent;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    AuthRenewalKind, BalancesRequest, BatchAtomicity, BatchExecutionMode, BatchPlaceOrdersRequest,
    CancelOrderRequest, CapabilitySupport, ExchangeApiError, ExchangeClient, OpenOrdersRequest,
    PlaceOrderRequest, PositionsRequest, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest,
    RequestContext, SymbolScope, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::request_spec::{ActualHttpRequest, RequestSpec};
use crate::signing_spec::SigningVector;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private::{coindcx_cancel_order_body, coindcx_place_order_body};
use super::private_parser::{parse_balances, parse_fills, parse_order, parse_positions};
use super::signing::coindcx_signature_for_payload;
use super::{CoinDcxGatewayAdapter, CoinDcxGatewayConfig};

fn fixture(path: &str) -> Value {
    let full_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/exchanges/coindcx")
        .join(path);
    let text = std::fs::read_to_string(&full_path)
        .unwrap_or_else(|error| panic!("read fixture {}: {error}", full_path.display()));
    serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("parse fixture {}: {error}", full_path.display()))
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("coindcx").expect("exchange")
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

fn spot_symbol() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTCUSDT")
            .expect("symbol"),
    }
}

fn perp_symbol() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTCUSDT")
            .expect("symbol"),
    }
}

fn private_adapter() -> CoinDcxGatewayAdapter {
    CoinDcxGatewayAdapter::new(CoinDcxGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        public_rest_base_url: "https://public.coindcx.com".to_string(),
        ..CoinDcxGatewayConfig::default()
    })
    .expect("adapter")
}

fn private_adapter_with_base(base_url: String) -> CoinDcxGatewayAdapter {
    CoinDcxGatewayAdapter::new(CoinDcxGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        spot_rest_base_url: base_url.clone(),
        futures_rest_base_url: base_url,
        public_rest_base_url: "https://public.coindcx.com".to_string(),
        ..CoinDcxGatewayConfig::default()
    })
    .expect("adapter")
}

#[test]
fn parser_should_parse_spot_symbol_rules_and_depth() {
    let rules = parse_symbol_rules(
        &exchange_id(),
        &[],
        &fixture("parser/symbol_rules_success.json"),
        Some(MarketType::Spot),
    )
    .expect("rules");

    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.01"));

    let book = parse_orderbook_snapshot(
        &exchange_id(),
        spot_symbol(),
        &fixture("parser/orderbook_success.json"),
    )
    .expect("book");

    assert_eq!(book.bids[0].price, 64000.0);
    assert_eq!(book.asks[0].price, 64100.0);
    assert!(book.exchange_timestamp.is_some());
}

#[test]
fn parser_should_parse_order_ack() {
    let order = parse_order(
        &exchange_id(),
        Some(&spot_symbol()),
        MarketType::Spot,
        &fixture("parser/order_ack_success.json"),
    )
    .expect("order")
    .expect("some order");

    assert_eq!(
        order.exchange_order_id.as_deref(),
        Some("fixture-order-001")
    );
    assert_eq!(order.client_order_id.as_deref(), Some("fixture-client-001"));
    assert_eq!(order.quantity, "0.01");
}

#[test]
fn parser_fixtures_should_cover_empty_error_and_missing_fields() {
    let empty_rules = parse_symbol_rules(
        &exchange_id(),
        &[],
        &fixture("parser/symbol_rules_empty.json"),
        Some(MarketType::Spot),
    )
    .expect("empty rules");
    assert!(empty_rules.is_empty());

    let missing_symbol = parse_symbol_rules(
        &exchange_id(),
        &[],
        &fixture("parser/symbol_rules_missing_symbol.json"),
        Some(MarketType::Spot),
    )
    .expect_err("missing symbol");
    assert!(matches!(missing_symbol, ExchangeApiError::Exchange(_)));

    let missing_bids = parse_orderbook_snapshot(
        &exchange_id(),
        spot_symbol(),
        &fixture("parser/orderbook_missing_bids.json"),
    )
    .expect_err("missing bids");
    assert!(matches!(missing_bids, ExchangeApiError::Exchange(_)));

    let empty_order = parse_order(
        &exchange_id(),
        Some(&spot_symbol()),
        MarketType::Spot,
        &fixture("parser/order_ack_empty.json"),
    )
    .expect("empty order");
    assert!(empty_order.is_none());

    let missing_order_symbol = parse_order(
        &exchange_id(),
        None,
        MarketType::Spot,
        &fixture("parser/order_ack_missing_symbol.json"),
    )
    .expect_err("missing order symbol");
    assert!(matches!(
        missing_order_symbol,
        ExchangeApiError::Exchange(_)
    ));

    let balances = parse_balances(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        MarketType::Spot,
        &[],
        &fixture("parser/balances_success.json"),
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].asset, "USDT");
    assert_eq!(balances[0].balances[0].total, 102.0);

    let positions = parse_positions(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &[],
        &fixture("parser/positions_success.json"),
    )
    .expect("positions");
    assert_eq!(positions[0].quantity, 2.0);

    let fills = parse_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        Some(&spot_symbol()),
        MarketType::Spot,
        &fixture("parser/fills_success.json"),
    )
    .expect("fills");
    assert_eq!(fills[0].fill_id.as_deref(), Some("fixture-trade-001"));

    let error_fixture = fixture("parser/error_response.json");
    assert_eq!(error_fixture["status"], "error");
    assert_eq!(error_fixture["code"], "INVALID_SIGNATURE");
}

#[test]
fn request_spec_fixtures_should_match_adapter_request_bodies() {
    let spot_spec = fixture("request_spec/place_order_spot_limit.json");
    let spot_request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place-spot"),
        symbol: spot_symbol(),
        client_order_id: Some("fixture-client-001".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::IOC),
        quantity: "0.01".to_string(),
        price: Some("64000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    assert_eq!(
        coindcx_place_order_body(&spot_request).expect("spot body"),
        spot_spec["expected_body"]
    );

    let perp_spec = fixture("request_spec/place_order_perp_limit.json");
    let perp_request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place-perp"),
        symbol: perp_symbol(),
        client_order_id: None,
        side: OrderSide::Sell,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "1".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    assert_eq!(
        coindcx_place_order_body(&perp_request).expect("perp body"),
        perp_spec["expected_body"]
    );

    let cancel_spec = fixture("request_spec/cancel_order_spot.json");
    let cancel = CancelOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("cancel"),
        symbol: spot_symbol(),
        client_order_id: None,
        exchange_order_id: Some("fixture-order-001".to_string()),
    };
    assert_eq!(
        coindcx_cancel_order_body(&cancel).expect("cancel body"),
        cancel_spec["expected_body"]
    );

    let batch_place = fixture("request_spec/batch_place_spot.json");
    assert_eq!(batch_place["native_batch"], true);
    assert_eq!(batch_place["atomicity"], "partial");
    assert_eq!(batch_place["max_items"], 10);

    let batch_cancel = fixture("request_spec/batch_cancel_spot.json");
    assert_eq!(batch_cancel["expected_body"]["ids"][0], "fixture-order-001");
    assert_eq!(batch_cancel["max_items"], 50);
}

#[test]
fn signing_vector_should_match_fixture() {
    let vector = fixture("signing/hmac_compact_json.json");
    let secret = vector["secret"].as_str().expect("secret");
    let payload = vector["payload"].as_str().expect("payload");
    let expected = vector["expected_signature"].as_str().expect("signature");
    let signature = coindcx_signature_for_payload(secret, payload).expect("signature");
    assert_eq!(signature, expected);

    let standard: SigningVector = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coindcx/signing_vectors/hmac_compact_json.json"
    ))
    .expect("standard signing vector");
    standard.verify().expect("standard signing vector verifies");
}

#[tokio::test]
async fn private_rest_request_specs_should_match_signed_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("parser/balances_success.json"),
        fixture("parser/positions_success.json"),
        fixture("parser/order_ack_success.json"),
        fixture("parser/order_ack_success.json"),
        fixture("parser/order_ack_success.json"),
        fixture("parser/order_ack_success.json"),
        fixture("parser/fills_success.json"),
    ])
    .await;
    let adapter = private_adapter_with_base(base_url);

    adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect("balances");
    adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![perp_symbol().exchange_symbol],
        })
        .await
        .expect("positions");
    adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: spot_symbol(),
            client_order_id: Some("fixture-client-001".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::IOC),
            quantity: "0.01".to_string(),
            price: Some("64000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");
    adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: spot_symbol(),
            client_order_id: None,
            exchange_order_id: Some("fixture-order-001".to_string()),
        })
        .await
        .expect("cancel");
    adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: spot_symbol(),
            client_order_id: None,
            exchange_order_id: Some("fixture-order-001".to_string()),
        })
        .await
        .expect("query");
    adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol()),
            page: None,
        })
        .await
        .expect("open");
    adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(1000),
            page: None,
        })
        .await
        .expect("fills");

    let requests = seen.lock().unwrap().clone();
    assert_request_matches_spec(&requests[0], "coindcx.get_balances");
    assert_request_matches_spec(&requests[1], "coindcx.get_positions");
    assert_request_matches_spec(&requests[2], "coindcx.place_order");
    assert_request_matches_spec(&requests[3], "coindcx.cancel_order");
    assert_request_matches_spec(&requests[4], "coindcx.query_order");
    assert_request_matches_spec(&requests[5], "coindcx.get_open_orders");
    assert_request_matches_spec(&requests[6], "coindcx.get_recent_fills");
}

#[test]
fn standard_request_specs_should_cover_declared_private_rest_operations() {
    let operations = load_private_request_specs()
        .into_iter()
        .map(|spec| spec.operation)
        .collect::<std::collections::BTreeSet<_>>();
    for operation in [
        "coindcx.get_balances",
        "coindcx.get_positions",
        "coindcx.place_order",
        "coindcx.place_futures_order",
        "coindcx.cancel_order",
        "coindcx.batch_place_orders",
        "coindcx.batch_cancel_orders",
        "coindcx.query_order",
        "coindcx.get_open_orders",
        "coindcx.get_recent_fills",
    ] {
        assert!(operations.contains(operation), "missing {operation}");
    }
}

#[test]
fn capabilities_v2_should_declare_task_18_operational_policies() {
    let adapter = private_adapter();
    let capabilities = adapter.capabilities();
    let v2 = capabilities.capabilities_v2;

    assert!(matches!(
        v2.public_streams,
        CapabilitySupport::WsOnly { .. }
    ));
    assert!(matches!(
        v2.private_streams,
        CapabilitySupport::WsOnly { .. }
    ));
    assert_eq!(v2.stream_runtime.heartbeat.required, true);
    assert_eq!(v2.stream_runtime.heartbeat.interval_ms, Some(25_000));
    assert_eq!(
        v2.stream_runtime.auth_renewal_policy.kind,
        AuthRenewalKind::ReLogin
    );
    assert!(v2.stream_runtime.resync.order_book);
    assert!(v2.stream_runtime.resync.orders);
    assert_eq!(v2.batch_place_orders.mode, BatchExecutionMode::Native);
    assert_eq!(v2.batch_place_orders.atomicity, BatchAtomicity::Partial);
    assert_eq!(v2.batch_place_orders.max_items, Some(10));
    assert_eq!(v2.batch_cancel_orders.max_items, Some(50));
    assert!(v2.fills_history.supports_limit);
    assert_eq!(v2.fills_history.max_limit, Some(1000));
}

#[tokio::test]
async fn futures_batch_place_should_be_explicitly_unsupported() {
    let adapter = private_adapter();
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch"),
        exchange: exchange_id(),
        orders: vec![PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order"),
            symbol: perp_symbol(),
            client_order_id: None,
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "1".to_string(),
            price: Some("64000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        }],
    };

    let error = adapter
        .batch_place_orders(request)
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "coindcx.futures_batch_place_orders"
        }
    ));
}

#[test]
fn websocket_sessions_should_emit_socketio_join_and_heartbeat_payloads() {
    let adapter = private_adapter();
    let public_session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: spot_symbol(),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .expect("public session");
    let initial = public_session.initial_requests();
    assert_eq!(initial[0]["event"], "join");
    assert_eq!(initial[0]["channelName"], "BTCUSDT@orderbook@50");

    let private_session = adapter
        .private_ws_session(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .expect("private session");
    let initial = private_session.initial_requests();
    assert_eq!(initial[0]["event"], "join");
    assert_eq!(initial[0]["channelName"], "coindcx");
    assert!(initial[0]["authSignature"].as_str().is_some());
}

#[test]
fn public_orderbook_ws_policy_should_document_depth_update_boundaries() {
    let policy = super::streams::coindcx_public_orderbook_ws_policy();
    let policy_json = policy.as_json();

    assert_eq!(policy.transport, "socket_io");
    assert_eq!(policy.subscribe_event, "join");
    assert_eq!(policy.update_event, "depth-update");
    assert_eq!(
        policy.official_spot_example_channel,
        "B-BTC_USDT@orderbook@20"
    );
    assert_eq!(policy.official_spot_example_depth, 20);
    assert_eq!(policy.project_depth, 50);
    assert!(policy.fixed_interval_ms.is_none());
    assert!(policy.sequence_fields.is_empty());
    assert!(policy.checksum.is_none());
    assert_eq!(policy.rest_snapshot_operation, "get_order_book");
    assert!(policy
        .resync_strategy
        .contains("fetch REST snapshot and resubscribe"));
    assert_eq!(
        policy_json["depth"]["project_spot_channel_template"],
        "{symbol}@orderbook@50"
    );
    assert_eq!(
        policy_json["depth"]["project_futures_channel_template"],
        "{instrument}@orderbook@50-futures"
    );

    let official_spot_join =
        super::streams::coindcx_public_orderbook_join_payload(MarketType::Spot, "B-BTC_USDT", 20)
            .expect("official spot join");
    assert_eq!(
        official_spot_join,
        fixture("ws/public_orderbook_spot_join_20.json")
    );

    let project_spot_join =
        super::streams::coindcx_public_orderbook_join_payload(MarketType::Spot, "BTCUSDT", 50)
            .expect("project spot join");
    assert_eq!(
        project_spot_join,
        fixture("ws/public_orderbook_spot_join_50.json")
    );

    let project_futures_join =
        super::streams::coindcx_public_orderbook_join_payload(MarketType::Perpetual, "BTCUSDT", 50)
            .expect("project futures join");
    assert_eq!(
        project_futures_join,
        fixture("ws/public_orderbook_futures_join_50.json")
    );

    let unsupported =
        super::streams::coindcx_public_orderbook_join_payload(MarketType::Perpetual, "BTCUSDT", 20)
            .expect_err("futures order book depth is project-bounded to 50");
    assert!(matches!(
        unsupported,
        ExchangeApiError::Unsupported {
            operation: "coindcx.public_futures_orderbook_depth"
        }
    ));
}

#[test]
fn public_websocket_should_parse_socketio_depth_update_without_sequence_or_checksum() {
    let adapter = private_adapter();
    let mut session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-depth-update"),
            symbol: spot_symbol(),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .expect("public session");
    let frame = fixture("ws/public_depth_update_socketio.json");

    let events = session
        .handle_text_message(frame["frame"].as_str().expect("socketio frame"))
        .expect("depth update");
    let stream_events = events
        .iter()
        .find_map(|event| match event {
            super::streams::CoinDcxWsSessionEvent::Stream(events) => Some(events),
            _ => None,
        })
        .expect("stream events");

    assert!(stream_events.iter().any(|event| matches!(
        event,
        ExchangeStreamEvent::OrderBookSnapshot(book)
            if book.order_book.bids[0].price == 64_000.0
                && book.order_book.asks[0].price == 64_100.0
                && book.order_book.sequence.is_none()
    )));
    assert!(frame["sequence"].is_null());
    assert!(frame["checksum"].is_null());
}

#[test]
fn public_websocket_sessions_should_cover_trade_ticker_and_candle_channels() {
    let adapter = private_adapter();
    let cases = [
        (PublicStreamKind::Trades, "BTCUSDT@trades"),
        (PublicStreamKind::Ticker, "BTCUSDT@prices"),
        (
            PublicStreamKind::Candles {
                interval: "1m".to_string(),
            },
            "BTCUSDT_1m",
        ),
    ];

    for (kind, channel) in cases {
        let session = adapter
            .public_ws_session(PublicStreamSubscription {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("public-ws-channel"),
                symbol: spot_symbol(),
                kind,
            })
            .expect("public session");
        let initial = session.initial_requests();
        assert_eq!(initial[0]["event"], "join");
        assert_eq!(initial[0]["channelName"], channel);
    }

    let futures_session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws-futures-channel"),
            symbol: perp_symbol(),
            kind: PublicStreamKind::Trades,
        })
        .expect("futures public session");
    assert_eq!(
        futures_session.initial_requests()[0]["channelName"],
        "BTCUSDT-FUTURES@trades-futures"
    );
}

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Value,
}

async fn spawn_rest_server(responses: Vec<Value>) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
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
                .unwrap_or_else(|| serde_json::json!({}));
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
    let path = target
        .split_once('?')
        .map(|(path, _)| path)
        .unwrap_or(target);
    let headers = request_text
        .lines()
        .skip(1)
        .take_while(|line| !line.trim().is_empty())
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    let body_text = request_text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.trim())
        .unwrap_or_default();
    let body = if body_text.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(body_text).unwrap_or(Value::Null)
    };
    SeenRequest {
        method,
        path: path.to_string(),
        headers,
        body,
    }
}

fn load_private_request_specs() -> Vec<RequestSpec> {
    serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coindcx/request_specs/private_rest.json"
    ))
    .expect("private request specs")
}

fn assert_request_matches_spec(request: &SeenRequest, operation: &str) {
    let specs = load_private_request_specs();
    let spec = specs
        .iter()
        .find(|spec| spec.operation == operation)
        .unwrap_or_else(|| panic!("missing CoinDCX request spec {operation}"));
    spec.assert_matches(&actual_request(request))
        .unwrap_or_else(|error| panic!("{error}"));
}

fn actual_request(request: &SeenRequest) -> ActualHttpRequest {
    ActualHttpRequest::new(request.method.clone(), request.path.clone())
        .with_headers(
            request
                .headers
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        )
        .with_body(Some(request.body.clone()).filter(|value| !value.is_null()))
}

#[test]
fn private_websocket_should_parse_trade_fill_and_balance_events() {
    let adapter = private_adapter();
    let mut session = adapter
        .private_ws_session(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws-events"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Account,
        })
        .expect("private session");

    let trade_events = session
        .handle_text_message(
            r#"42["trade-update",{"id":"trade-1","order_id":"order-1","market":"BTCUSDT","side":"buy","price":"64000","quantity":"0.01","fee_amount":"0.1","fee_currency":"USDT","timestamp":1725000000000}]"#,
        )
        .expect("trade events");
    let stream_events = trade_events
        .iter()
        .find_map(|event| match event {
            super::streams::CoinDcxWsSessionEvent::Stream(events) => Some(events),
            _ => None,
        })
        .expect("stream events");
    assert!(stream_events.iter().any(|event| matches!(
        event,
        ExchangeStreamEvent::Fill(fill)
            if fill.fill_id.as_deref() == Some("trade-1") && fill.quantity == 0.01
    )));

    let order_frame = fixture("parser/ws_order_update.json");
    let order_events = session
        .handle_text_message(order_frame["frame"].as_str().expect("frame"))
        .expect("order events");
    let stream_events = order_events
        .iter()
        .find_map(|event| match event {
            super::streams::CoinDcxWsSessionEvent::Stream(events) => Some(events),
            _ => None,
        })
        .expect("order stream events");
    assert!(stream_events.iter().any(|event| matches!(
        event,
        ExchangeStreamEvent::OrderUpdate(order)
            if order.exchange_order_id.as_deref() == Some("fixture-order-001")
                && order.status == rustcta_types::OrderStatus::Filled
    )));

    let balance_events = session
        .handle_text_message(
            r#"42["balance-update",{"currency":"USDT","balance":"100","locked_balance":"2","total_balance":"102"}]"#,
        )
        .expect("balance events");
    let stream_events = balance_events
        .iter()
        .find_map(|event| match event {
            super::streams::CoinDcxWsSessionEvent::Stream(events) => Some(events),
            _ => None,
        })
        .expect("stream events");
    assert!(stream_events.iter().any(|event| matches!(
        event,
        ExchangeStreamEvent::BalanceSnapshot(snapshot)
            if snapshot.balances[0].balances[0].asset == "USDT"
                && snapshot.balances[0].balances[0].total == 102.0
    )));
}
