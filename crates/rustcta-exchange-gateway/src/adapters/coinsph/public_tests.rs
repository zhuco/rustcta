use rustcta_exchange_api::OrderBookStrictness;
use rustcta_exchange_api::{
    CapabilitySupport, ExchangeClient, OrderBookRequest, PublicStreamKind,
    PublicStreamSubscription, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::{
    coinsph_depth_replay_decision, coinsph_diff_depth_stream, coinsph_partial_depth_stream,
    coinsph_public_order_book_ws_policy, coinsph_public_subscription_spec,
    parse_coinsph_public_stream_message, CoinsPhDepthReplayDecision, CoinsPhPublicStreamMessage,
};
use super::test_support::{context, spawn_rest_server, symbol_scope, usdt_symbol_scope};
use super::{CoinsPhGatewayAdapter, CoinsPhGatewayConfig};

fn fixture(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/coinsph/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("fixture");
    serde_json::from_str(&text).expect("fixture json")
}

#[tokio::test]
async fn coinsph_adapter_should_declare_php_spot_boundaries() {
    let adapter = CoinsPhGatewayAdapter::new(CoinsPhGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..CoinsPhGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(matches!(
        capabilities.capabilities_v2.public_rest,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        capabilities.capabilities_v2.private_rest,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        capabilities.capabilities_v2.public_streams,
        CapabilitySupport::Native
    ));
    assert_eq!(
        capabilities.order_book.strictness,
        OrderBookStrictness::StrictDelta
    );
    assert!(!capabilities.supports_quote_market_order);
    assert!(!capabilities.supports_cancel_all_orders);
    assert!(!capabilities.supports_amend_order);
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_order_book"
            && endpoint.path.as_deref() == Some("/openapi/quote/v1/depth")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(
            |endpoint| endpoint.operation == "public_orderbook_diff_depth"
                && endpoint.path.as_deref() == Some("{symbol}@depth@100ms|1000ms")
        ));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "fiat_transfer"
            && matches!(endpoint.support, CapabilitySupport::Unsupported { .. })));
}

#[tokio::test]
async fn coinsph_public_ws_subscription_helpers_should_cover_orderbook_streams() {
    let symbol = symbol_scope("BTCPHP");
    let ticker = coinsph_public_subscription_spec(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ticker"),
        symbol: symbol.clone(),
        kind: PublicStreamKind::Ticker,
    })
    .expect("ticker stream");
    assert_eq!(ticker.url, super::streams::COINSPH_PUBLIC_WS_URL);
    assert_eq!(ticker.stream, "btcphp@bookTicker");
    assert_eq!(ticker.subscribe_payload["method"], "SUBSCRIBE");
    assert_eq!(ticker.subscribe_payload["params"][0], "btcphp@bookTicker");
    assert_eq!(ticker.unsubscribe_payload["method"], "UNSUBSCRIBE");

    let partial = coinsph_public_subscription_spec(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("partial-depth"),
        symbol: symbol.clone(),
        kind: PublicStreamKind::OrderBookSnapshot,
    })
    .expect("partial stream");
    assert_eq!(partial.stream, "btcphp@depth20@100ms");
    assert_eq!(
        coinsph_partial_depth_stream(&symbol, 200, 100).expect("200 stream"),
        "btcphp@depth200@1000ms"
    );

    let delta = coinsph_public_subscription_spec(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("diff-depth"),
        symbol: symbol.clone(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("delta stream");
    assert_eq!(delta.stream, "btcphp@depth@100ms");
    assert_eq!(
        coinsph_diff_depth_stream(&symbol, 1000).expect("1000 stream"),
        "btcphp@depth@1000ms"
    );

    let adapter = CoinsPhGatewayAdapter::default_public().expect("adapter");
    let handle = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("subscribe"),
            symbol,
            kind: PublicStreamKind::OrderBookDelta,
        })
        .await
        .expect("public stream handle");
    assert!(handle.ends_with(":btcphp@depth@100ms"));
}

#[test]
fn coinsph_public_ws_policy_should_describe_snapshot_replay() {
    let policy = coinsph_public_order_book_ws_policy();
    assert_eq!(policy.book_ticker_stream_template, "{symbol}@bookTicker");
    assert_eq!(policy.partial_depth_levels, &[5, 10, 20, 200]);
    assert_eq!(policy.diff_depth_intervals_ms, &[100, 1000]);
    assert_eq!(policy.snapshot_sequence_field, "lastUpdateId");
    assert_eq!(policy.first_update_field, "U");
    assert_eq!(policy.final_update_field, "u");
    assert_eq!(policy.checksum, None);
    assert_eq!(
        coinsph_depth_replay_decision(100, None, 80, 100),
        CoinsPhDepthReplayDecision::Stale
    );
    assert_eq!(
        coinsph_depth_replay_decision(100, None, 99, 101),
        CoinsPhDepthReplayDecision::FirstApplicable
    );
    assert_eq!(
        coinsph_depth_replay_decision(100, Some(101), 102, 104),
        CoinsPhDepthReplayDecision::Contiguous
    );
    assert_eq!(
        coinsph_depth_replay_decision(100, Some(101), 103, 104),
        CoinsPhDepthReplayDecision::Gap
    );
}

#[test]
fn coinsph_public_ws_fixtures_should_parse_book_ticker_partial_and_diff_depth() {
    let exchange = super::test_support::exchange_id();
    let symbol = symbol_scope("BTCPHP");

    let ticker = parse_coinsph_public_stream_message(
        &exchange,
        symbol.clone(),
        &fixture("ws/book_ticker.json"),
    )
    .expect("book ticker");
    match ticker {
        CoinsPhPublicStreamMessage::BookTicker(snapshot) => {
            assert_eq!(snapshot.sequence, Some(400900217));
            assert_eq!(snapshot.best_bid().expect("bid").price, 3_500_000.0);
            assert_eq!(snapshot.best_ask().expect("ask").quantity, 0.04);
            assert!(snapshot.exchange_timestamp.is_some());
        }
        other => panic!("unexpected ticker message: {other:?}"),
    }

    let partial = parse_coinsph_public_stream_message(
        &exchange,
        symbol.clone(),
        &fixture("ws/partial_depth.json"),
    )
    .expect("partial depth");
    match partial {
        CoinsPhPublicStreamMessage::PartialDepth(snapshot) => {
            assert_eq!(snapshot.sequence, Some(400900220));
            assert_eq!(snapshot.bids.len(), 2);
            assert_eq!(snapshot.asks[0].price, 3_500_500.0);
        }
        other => panic!("unexpected partial message: {other:?}"),
    }

    let diff =
        parse_coinsph_public_stream_message(&exchange, symbol, &fixture("ws/diff_depth.json"))
            .expect("diff depth");
    match diff {
        CoinsPhPublicStreamMessage::DiffDepth(delta) => {
            assert_eq!(delta.first_sequence, Some(400900221));
            assert_eq!(delta.last_sequence, Some(400900223));
            assert_eq!(delta.bids[0].quantity, 0.15);
            assert_eq!(delta.asks[0].quantity, 0.0);
            assert!(delta.exchange_timestamp.is_some());
        }
        other => panic!("unexpected diff message: {other:?}"),
    }
}

#[test]
fn coinsph_parser_fixtures_should_cover_php_market_and_errors() {
    let exchange = super::test_support::exchange_id();
    let rules =
        parse_symbol_rules(&exchange, &fixture("parser/symbol_rules_success.json")).expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "PHP");

    let empty =
        parse_symbol_rules(&exchange, &fixture("parser/symbol_rules_empty.json")).expect("empty");
    assert!(empty.is_empty());
    assert!(parse_symbol_rules(
        &exchange,
        &fixture("parser/symbol_rules_missing_field.json")
    )
    .is_err());
    assert!(parse_orderbook_snapshot(
        &exchange,
        symbol_scope("BTCPHP"),
        &fixture("parser/orderbook_error.json")
    )
    .is_err());
}

#[tokio::test]
async fn coinsph_adapter_should_load_php_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "symbols": [{
            "symbol": "BTCPHP",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "PHP",
            "baseAssetPrecision": 8,
            "quoteAssetPrecision": 2,
            "orderTypes": ["LIMIT", "LIMIT_MAKER", "MARKET"],
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "1.00", "maxPrice": "10000000.00", "tickSize": "1.00"},
                {"filterType": "LOT_SIZE", "minQty": "0.00001000", "maxQty": "10.00000000", "stepSize": "0.00001000"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "100.00"}
            ]
        }, {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "filters": []
        }]
    })])
    .await;
    let adapter = CoinsPhGatewayAdapter::new(CoinsPhGatewayConfig {
        rest_base_url: base_url,
        ..CoinsPhGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope("BTC/PHP")],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].symbol.exchange_symbol.symbol, "BTCPHP");
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("100.00"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/openapi/v1/exchangeInfo");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCPHP")
    );
}

#[tokio::test]
async fn coinsph_adapter_should_reject_non_php_spot_symbols() {
    let adapter = CoinsPhGatewayAdapter::default_public().expect("adapter");
    let error = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![usdt_symbol_scope("BTCUSDT")],
        })
        .await
        .expect_err("non-PHP market unsupported");
    assert!(matches!(
        error,
        rustcta_exchange_api::ExchangeApiError::Unsupported { .. }
    ));
}

#[tokio::test]
async fn coinsph_adapter_should_load_depth_from_quote_api_namespace() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "lastUpdateId": 1027024,
        "bids": [["3500000.00", "0.10000000"]],
        "asks": [["3501000.00", "0.05000000"]]
    })])
    .await;
    let adapter = CoinsPhGatewayAdapter::new(CoinsPhGatewayConfig {
        rest_base_url: base_url,
        ..CoinsPhGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope("BTC-PHP"),
            depth: Some(25),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(1027024));
    assert_eq!(response.order_book.bids[0].price, 3_500_000.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/openapi/quote/v1/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCPHP")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("50"));
}
