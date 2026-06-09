use base64::Engine;
use chrono::Utc;
use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    OrderBookRequest, PlaceOrderRequest, PositionsRequest, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, QueryOrderRequest,
    QuoteMarketOrderRequest, RecentFillsRequest, RequestContext, SymbolScope, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private_parser::{
    parse_balances, parse_batch_orders, parse_fee_snapshots, parse_fills, parse_order,
    parse_orders, parse_positions,
};
use super::signing::{backpack_signature, canonical_batch_payload};
use super::streams::{
    backpack_depth_stream, backpack_depth_subscribe_payload, backpack_ping_payload,
    backpack_private_stream, backpack_public_order_book_ws_policy, backpack_public_stream,
    backpack_public_subscribe_payload, parse_backpack_public_stream_message,
    parse_backpack_stream_event, BackpackPublicStreamMessage,
};
use super::test_support::{
    assert_request_matches_spec, context, fixed_time, fixture, perp_symbol_scope, private_config,
    request_spec, signing_vector, spawn_rest_server, spot_symbol_scope, ws_fixture,
};
use super::{BackpackGatewayAdapter, BackpackGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("backpack").unwrap()
}

fn symbol(market_type: MarketType, text: &str) -> SymbolScope {
    let (base, quote) = text.split_once('_').unwrap_or((text, "USDC"));
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, text).unwrap(),
    }
}

#[test]
fn backpack_parser_should_parse_spot_and_perp_markets() {
    let value = fixture("markets");
    let rules = parse_symbol_rules(&exchange_id(), &[], &value).unwrap();

    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].symbol.market_type, MarketType::Spot);
    assert_eq!(rules[1].symbol.market_type, MarketType::Perpetual);
    assert!(rules[1].supports_reduce_only);
}

#[test]
fn backpack_parser_should_parse_depth_snapshot() {
    let snapshot = parse_orderbook_snapshot(
        &exchange_id(),
        symbol(MarketType::Spot, "SOL_USDC"),
        &fixture("orderbook"),
    )
    .unwrap();

    assert_eq!(snapshot.best_bid().unwrap().price, 10.0);
    assert_eq!(snapshot.best_ask().unwrap().price, 10.1);
    assert_eq!(snapshot.sequence, Some(42));
}

#[test]
fn backpack_parser_fixture_tests_should_cover_success_empty_error_and_missing_fields() {
    let tenant = rustcta_exchange_api::TenantId::new("tenant").unwrap();
    let account = rustcta_exchange_api::AccountId::new("account").unwrap();
    let perp = symbol(MarketType::Perpetual, "SOL_USDC_PERP");

    let balances = parse_balances(
        &exchange_id(),
        tenant.clone(),
        account.clone(),
        MarketType::Spot,
        &[],
        &fixture("balances"),
    )
    .unwrap();
    assert_eq!(balances[0].balances.len(), 2);

    let positions = parse_positions(
        &exchange_id(),
        tenant.clone(),
        account.clone(),
        &[],
        &fixture("positions"),
    )
    .unwrap();
    assert_eq!(positions[0].quantity, 2.0);

    let fees = parse_fee_snapshots(std::slice::from_ref(&perp), &fixture("account")).unwrap();
    assert_eq!(fees[0].maker_rate, "0.00020000");

    let order = parse_order(
        &exchange_id(),
        Some(&perp),
        MarketType::Perpetual,
        &fixture("order_ack"),
    )
    .unwrap()
    .unwrap();
    assert_eq!(order.exchange_order_id.as_deref(), Some("42"));

    let batch = parse_batch_orders(
        &exchange_id(),
        &[perp.clone(), perp.clone()],
        &fixture("batch_order_ack"),
    )
    .unwrap();
    assert_eq!(batch.len(), 2);

    let open_orders = parse_orders(
        &exchange_id(),
        Some(&perp),
        MarketType::Perpetual,
        &fixture("open_orders"),
    )
    .unwrap();
    assert_eq!(open_orders.len(), 1);

    let fills = parse_fills(
        &exchange_id(),
        tenant,
        account,
        Some(&perp),
        MarketType::Perpetual,
        &fixture("fills"),
    )
    .unwrap();
    assert_eq!(fills[0].fill_id.as_deref(), Some("9001"));

    assert!(parse_symbol_rules(&exchange_id(), &[], &json!([]))
        .unwrap()
        .is_empty());
    assert!(parse_orders(
        &exchange_id(),
        Some(&perp),
        MarketType::Perpetual,
        &json!([])
    )
    .unwrap()
    .is_empty());
    assert!(parse_fills(
        &exchange_id(),
        rustcta_exchange_api::TenantId::new("tenant").unwrap(),
        rustcta_exchange_api::AccountId::new("account").unwrap(),
        Some(&perp),
        MarketType::Perpetual,
        &json!([])
    )
    .unwrap()
    .is_empty());

    let missing = parse_symbol_rules(&exchange_id(), &[], &json!([{"marketType":"SPOT"}]))
        .expect_err("missing symbol should fail");
    assert!(matches!(missing, ExchangeApiError::Exchange(_)));

    let error = parse_orderbook_snapshot(&exchange_id(), perp, &fixture("error"))
        .expect_err("error payload should not parse as order book");
    assert!(matches!(error, ExchangeApiError::Exchange(_)));
}

#[test]
fn backpack_stream_helpers_should_build_public_and_private_channels() {
    let public = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: RequestContext::new(Utc::now()),
        symbol: symbol(MarketType::Perpetual, "SOL_USDC_PERP"),
        kind: PublicStreamKind::OrderBookDelta,
    };
    assert_eq!(backpack_public_stream(&public), "depth.SOL_USDC_PERP");
    assert_eq!(
        backpack_public_subscribe_payload(&public),
        json!({"method": "SUBSCRIBE", "params": ["depth.SOL_USDC_PERP"]})
    );
    assert_eq!(
        backpack_depth_stream("SOL_USDC_PERP", Some(200)).unwrap(),
        "depth.200ms.SOL_USDC_PERP"
    );
    assert_eq!(
        backpack_depth_subscribe_payload(&public.symbol, Some(600)).unwrap(),
        json!({"method": "SUBSCRIBE", "params": ["depth.600ms.SOL_USDC_PERP"]})
    );
    assert!(backpack_depth_stream("SOL_USDC_PERP", Some(100)).is_err());
    assert_eq!(backpack_ping_payload(), json!({"method": "PING"}));

    let private = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: RequestContext::new(Utc::now()),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: rustcta_exchange_api::AccountId::new("acct").unwrap(),
        kind: PrivateStreamKind::Orders,
    };
    assert_eq!(
        backpack_private_stream(&private).unwrap(),
        "account.orderUpdate"
    );
}

#[test]
fn backpack_public_order_book_ws_policy_should_describe_depth_replay_and_resync() {
    let policy = backpack_public_order_book_ws_policy();

    assert_eq!(policy.book_ticker_channel_template, "bookTicker.{symbol}");
    assert_eq!(policy.realtime_depth_channel_template, "depth.{symbol}");
    assert_eq!(
        policy.aggregate_depth_channel_template,
        "depth.{interval_ms}ms.{symbol}"
    );
    assert_eq!(policy.aggregate_intervals_ms, &[200, 600, 1000]);
    assert_eq!(policy.rest_snapshot_endpoint, "GET /api/v1/depth");
    assert_eq!(
        policy.rest_snapshot_limits,
        &[5, 10, 20, 50, 100, 500, 1000]
    );
    assert_eq!(policy.snapshot_sequence_field, "lastUpdateId");
    assert_eq!(policy.depth_first_update_field, "U");
    assert_eq!(policy.depth_final_update_field, "u");
    assert_eq!(policy.book_ticker_sequence_field, "u");
    assert_eq!(policy.checksum, None);
    assert!(policy.resync.contains("lastUpdateId"));
    assert!(policy.resync.contains("previous u + 1"));
}

#[tokio::test]
async fn backpack_stream_runtime_capability_should_describe_public_private_policies() {
    let secret = base64::engine::general_purpose::STANDARD.encode([1_u8; 32]);
    let adapter = BackpackGatewayAdapter::new(BackpackGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some(secret),
        recv_window_ms: 5_000,
        ..BackpackGatewayConfig::default()
    })
    .unwrap();
    let public = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: RequestContext::new(Utc::now()),
            symbol: symbol(MarketType::Perpetual, "SOL_USDC_PERP"),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .await
        .unwrap();
    assert!(public.contains("wss://ws.backpack.exchange"));
    assert!(public.contains("depth.SOL_USDC_PERP"));

    let private = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: RequestContext::new(Utc::now()),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: rustcta_exchange_api::AccountId::new("acct").unwrap(),
            kind: PrivateStreamKind::Positions,
        })
        .await
        .unwrap();
    assert!(private.contains("wss://ws.backpack.exchange"));
    assert!(private.contains("account.positionUpdate"));
    assert_eq!(backpack_ping_payload(), json!({"method": "PING"}));
}

#[tokio::test]
async fn backpack_capabilities_should_reflect_private_credentials() {
    let secret = base64::engine::general_purpose::STANDARD.encode([1_u8; 32]);
    let adapter = BackpackGatewayAdapter::new(BackpackGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some(secret),
        ..BackpackGatewayConfig::default()
    })
    .unwrap();

    let capabilities = adapter.capabilities();

    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_public_streams);
    assert!(capabilities.supports_private_streams);
    assert!(
        capabilities
            .private_stream_capabilities
            .unwrap()
            .supports_orders
    );
}

#[test]
fn backpack_private_streams_should_match_documented_channels() {
    let orders = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: RequestContext::new(Utc::now()),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: rustcta_exchange_api::AccountId::new("acct").unwrap(),
        kind: PrivateStreamKind::Orders,
    };
    let positions = PrivateStreamSubscription {
        kind: PrivateStreamKind::Positions,
        ..orders.clone()
    };
    let fills = PrivateStreamSubscription {
        kind: PrivateStreamKind::Fills,
        ..orders.clone()
    };
    let balances = PrivateStreamSubscription {
        kind: PrivateStreamKind::Balances,
        ..orders.clone()
    };

    assert_eq!(
        backpack_private_stream(&orders).unwrap(),
        "account.orderUpdate"
    );
    assert_eq!(
        backpack_private_stream(&positions).unwrap(),
        "account.positionUpdate"
    );
    assert!(backpack_private_stream(&fills).is_err());
    assert!(backpack_private_stream(&balances).is_err());
}

#[test]
fn backpack_private_subscribe_payload_should_sign_subscribe_instruction() {
    let secret = base64::engine::general_purpose::STANDARD.encode([1_u8; 32]);
    let adapter = BackpackGatewayAdapter::new(BackpackGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some(secret),
        recv_window_ms: 5_000,
        ..BackpackGatewayConfig::default()
    })
    .unwrap();
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: RequestContext::new(Utc::now()),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: rustcta_exchange_api::AccountId::new("acct").unwrap(),
        kind: PrivateStreamKind::Positions,
    };

    let payload = adapter
        .private_subscribe_payload(&subscription, 1_714_000_000_000)
        .unwrap();

    assert_eq!(payload["method"], "SUBSCRIBE");
    assert_eq!(payload["params"], json!(["account.positionUpdate"]));
    assert_eq!(payload["signature"][0], "key");
    assert_eq!(payload["signature"][2], "1714000000000");
    assert_eq!(payload["signature"][3], "5000");
    assert!(payload["signature"][1].as_str().unwrap().len() > 40);
}

#[test]
fn backpack_signing_vectors_should_match_fixtures() {
    let vector = signing_vector("order_cancel_ed25519");
    let signature = backpack_signature(
        vector["secret_base64"].as_str().unwrap(),
        vector["payload"].as_str().unwrap(),
    )
    .unwrap();
    assert_eq!(signature, vector["signature_base64"].as_str().unwrap());

    let batch = signing_vector("batch_order_execute_payload");
    let orders = batch["orders"]
        .as_array()
        .unwrap()
        .iter()
        .map(|order| {
            order
                .as_object()
                .unwrap()
                .iter()
                .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_string()))
                .collect()
        })
        .collect::<Vec<std::collections::HashMap<String, String>>>();
    assert_eq!(
        canonical_batch_payload(
            batch["instruction"].as_str().unwrap(),
            &orders,
            batch["timestamp_ms"].as_i64().unwrap(),
            batch["window_ms"].as_u64().unwrap()
        ),
        batch["payload"].as_str().unwrap()
    );
}

#[test]
fn backpack_stream_parser_should_parse_private_order_and_position_events() {
    let symbol = symbol(MarketType::Perpetual, "SOL_USDC_PERP");
    let orders = parse_backpack_stream_event(
        &exchange_id(),
        Some(&symbol),
        r#"{"e":"account.orderUpdate","data":{"symbol":"SOL_USDC_PERP","id":"42","clientId":7,"side":"Bid","orderType":"Limit","quantity":"1","executedQuantity":"0.4","price":"100","status":"PartiallyFilled"}}"#,
    )
    .unwrap();
    assert_eq!(orders.len(), 1);

    let positions = parse_backpack_stream_event(
        &exchange_id(),
        None,
        r#"{"e":"account.positionUpdate","data":{"symbol":"SOL_USDC_PERP","netQuantity":"2","entryPrice":"90","markPrice":"100","pnlUnrealized":"20"}}"#,
    )
    .unwrap();
    assert_eq!(positions.len(), 1);
}

#[test]
fn backpack_public_stream_parser_should_parse_trade_ticker_and_candle_events() {
    let symbol = symbol(MarketType::Perpetual, "SOL_USDC_PERP");
    let trade = parse_backpack_public_stream_message(
        &exchange_id(),
        Some(&symbol),
        &json!({
            "stream": "trade.SOL_USDC_PERP",
            "data": {
                "id": "100",
                "price": "123.4",
                "quantity": "0.5",
                "side": "Ask",
                "timestamp": 1_704_000_000_000i64
            }
        }),
    )
    .unwrap();
    match trade {
        BackpackPublicStreamMessage::Trade(trade) => {
            assert_eq!(trade.price, "123.4");
            assert_eq!(trade.quantity, "0.5");
            assert_eq!(trade.trade_id.as_deref(), Some("100"));
            assert_eq!(trade.side, rustcta_types::OrderSide::Sell);
        }
        other => panic!("unexpected trade event: {other:?}"),
    }

    let ticker = parse_backpack_public_stream_message(
        &exchange_id(),
        Some(&symbol),
        &json!({
            "stream": "ticker.SOL_USDC_PERP",
            "data": {
                "lastPrice": "124.0",
                "markPrice": "123.9",
                "indexPrice": "123.8",
                "high": "130",
                "low": "120",
                "volume": "456",
                "quoteVolume": "56000",
                "timestamp": 1_704_000_010_000i64
            }
        }),
    )
    .unwrap();
    match ticker {
        BackpackPublicStreamMessage::Ticker(ticker) => {
            assert_eq!(ticker.last_price.as_deref(), Some("124.0"));
            assert_eq!(ticker.mark_price.as_deref(), Some("123.9"));
            assert_eq!(ticker.quote_volume.as_deref(), Some("56000"));
        }
        other => panic!("unexpected ticker event: {other:?}"),
    }

    let candle = parse_backpack_public_stream_message(
        &exchange_id(),
        Some(&symbol),
        &json!({
            "stream": "kline.1m.SOL_USDC_PERP",
            "data": {
                "start": 1_704_000_000_000i64,
                "end": 1_704_000_060_000i64,
                "open": "120",
                "high": "125",
                "low": "119",
                "close": "124",
                "volume": "10"
            }
        }),
    )
    .unwrap();
    match candle {
        BackpackPublicStreamMessage::Candle(candle) => {
            assert_eq!(candle.interval.as_deref(), Some("1m"));
            assert_eq!(candle.open, "120");
            assert_eq!(candle.close, "124");
            assert_eq!(candle.volume, "10");
        }
        other => panic!("unexpected candle event: {other:?}"),
    }
}

#[test]
fn backpack_public_stream_parser_should_normalize_depth_and_book_ticker_events() {
    let spot = symbol(MarketType::Spot, "SOL_USDC");
    let depth = parse_backpack_stream_event(
        &exchange_id(),
        Some(&spot),
        &ws_fixture("public_depth_realtime").to_string(),
    )
    .unwrap();
    assert_eq!(depth.len(), 1);
    match &depth[0] {
        rustcta_exchange_api::ExchangeStreamEvent::OrderBookSnapshot(response) => {
            assert_eq!(response.order_book.sequence, Some(77));
            assert_eq!(response.order_book.best_bid().unwrap().price, 10.0);
            assert_eq!(response.order_book.best_ask().unwrap().price, 10.2);
            assert_eq!(
                response
                    .order_book
                    .exchange_timestamp
                    .unwrap()
                    .timestamp_millis(),
                1_704_000_000_000
            );
        }
        other => panic!("unexpected depth event: {other:?}"),
    }

    let perp = symbol(MarketType::Perpetual, "SOL_USDC_PERP");
    let aggregate_depth = parse_backpack_public_stream_message(
        &exchange_id(),
        Some(&perp),
        &ws_fixture("public_depth_200ms"),
    )
    .unwrap();
    match aggregate_depth {
        BackpackPublicStreamMessage::OrderBook(order_book) => {
            assert_eq!(order_book.sequence, Some(202));
            assert_eq!(order_book.best_bid().unwrap().price, 99.9);
            assert_eq!(order_book.best_ask().unwrap().quantity, 0.8);
        }
        other => panic!("unexpected aggregate depth event: {other:?}"),
    }

    let book_ticker = parse_backpack_public_stream_message(
        &exchange_id(),
        Some(&spot),
        &ws_fixture("public_book_ticker"),
    )
    .unwrap();
    match book_ticker {
        BackpackPublicStreamMessage::OrderBook(order_book) => {
            assert_eq!(order_book.sequence, Some(78));
            assert_eq!(order_book.best_bid().unwrap().quantity, 3.0);
            assert_eq!(order_book.best_ask().unwrap().quantity, 4.0);
        }
        other => panic!("unexpected bookTicker event: {other:?}"),
    }
}

#[tokio::test]
async fn backpack_request_specs_should_cover_private_read_operations() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("balances"),
        fixture("positions"),
        fixture("account"),
        fixture("order_ack"),
        fixture("open_orders"),
        fixture("fills"),
    ])
    .await;
    let adapter = BackpackGatewayAdapter::new(private_config(base_url)).unwrap();
    let (start, end) = fixed_time();

    adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .unwrap();
    adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![perp_symbol_scope().exchange_symbol],
        })
        .await
        .unwrap();
    adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .unwrap();
    adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: perp_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("42".to_string()),
        })
        .await
        .unwrap();
    adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
            page: None,
        })
        .await
        .unwrap();
    adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("42".to_string()),
            from_trade_id: None,
            start_time: Some(start),
            end_time: Some(end),
            limit: Some(50),
            page: None,
        })
        .await
        .unwrap();

    let requests = seen.lock().unwrap().clone();
    assert_request_matches_spec(&requests[0], &request_spec("get_balances"));
    assert_request_matches_spec(&requests[1], &request_spec("get_positions"));
    assert_request_matches_spec(&requests[2], &request_spec("get_fees"));
    assert_request_matches_spec(&requests[3], &request_spec("query_order"));
    assert_request_matches_spec(&requests[4], &request_spec("get_open_orders"));
    assert_request_matches_spec(&requests[5], &request_spec("get_recent_fills"));
}

#[tokio::test]
async fn backpack_request_specs_should_cover_private_write_operations() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("order_ack"),
        fixture("order_ack"),
        fixture("cancel_ack"),
        fixture("batch_order_ack"),
        fixture("cancel_ack"),
        fixture("cancel_ack"),
        fixture("cancel_all_ack"),
    ])
    .await;
    let adapter = BackpackGatewayAdapter::new(private_config(base_url)).unwrap();

    adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("7".to_string()),
            side: rustcta_types::OrderSide::Buy,
            position_side: Some(rustcta_types::PositionSide::Net),
            order_type: rustcta_types::OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "1".to_string(),
            price: Some("100".to_string()),
            quote_quantity: None,
            reduce_only: true,
            post_only: false,
        })
        .await
        .unwrap();
    adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("9".to_string()),
            side: rustcta_types::OrderSide::Buy,
            quote_quantity: "25".to_string(),
        })
        .await
        .unwrap();
    adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: perp_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("42".to_string()),
        })
        .await
        .unwrap();
    adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("101".to_string()),
                    side: rustcta_types::OrderSide::Buy,
                    position_side: Some(rustcta_types::PositionSide::Net),
                    order_type: rustcta_types::OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "1".to_string(),
                    price: Some("100".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("102".to_string()),
                    side: rustcta_types::OrderSide::Sell,
                    position_side: Some(rustcta_types::PositionSide::Net),
                    order_type: rustcta_types::OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "1".to_string(),
                    price: Some("101".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
            ],
        })
        .await
        .unwrap();
    adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("42".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("43".to_string()),
                },
            ],
        })
        .await
        .unwrap();
    adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
        })
        .await
        .unwrap();

    let requests = seen.lock().unwrap().clone();
    assert_request_matches_spec(&requests[0], &request_spec("place_order"));
    assert_request_matches_spec(&requests[1], &request_spec("place_quote_market_order"));
    assert_request_matches_spec(&requests[2], &request_spec("cancel_order"));
    assert_request_matches_spec(&requests[3], &request_spec("batch_place_orders"));
    assert_request_matches_spec(&requests[4], &request_spec("batch_cancel_orders"));
    assert_request_matches_spec(&requests[6], &request_spec("cancel_all_orders"));
    assert_request_matches_spec(
        &requests[5],
        &json!({
            "method": "DELETE",
            "path": "/api/v1/order",
            "query": {},
            "body": {"symbol": "SOL_USDC_PERP", "orderId": "43"},
            "signed": true
        }),
    );
}

#[tokio::test]
async fn backpack_public_request_spec_should_cover_orderbook_limit() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("orderbook")]).await;
    let adapter = BackpackGatewayAdapter::new(BackpackGatewayConfig {
        rest_base_url: base_url,
        ..BackpackGatewayConfig::default()
    })
    .unwrap();
    adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: spot_symbol_scope(),
            depth: Some(1001),
        })
        .await
        .unwrap();
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/api/v1/depth");
    assert_eq!(
        requests[0].query.get("symbol").map(String::as_str),
        Some("SOL_USDC")
    );
    assert_eq!(
        requests[0].query.get("limit").map(String::as_str),
        Some("1000")
    );
}
