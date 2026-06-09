use rustcta_exchange_api::{
    BatchAtomicity, BatchExecutionMode, CapabilitySupport, ExchangeClient, OrderBookRequest,
    PublicStreamKind, PublicStreamSubscription, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::{
    binance_partial_depth_stream_name, binance_public_subscribe_payload,
    parse_binance_public_depth_message,
};
use super::test_support::{context, perpetual_symbol_scope, spawn_rest_server, symbol_scope};
use super::{BinanceGatewayAdapter, BinanceGatewayConfig};

fn fixture(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/binance/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("fixture");
    serde_json::from_str(&text).expect("fixture json")
}

fn public_subscription(kind: PublicStreamKind) -> PublicStreamSubscription {
    PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol_scope("BTC/USDT"),
        kind,
    }
}

#[tokio::test]
async fn binance_adapter_should_declare_task9_toolchain_capabilities() {
    let adapter = BinanceGatewayAdapter::new(BinanceGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BinanceGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(capabilities.market_types.contains(&MarketType::Spot));
    assert!(capabilities.market_types.contains(&MarketType::Perpetual));
    assert!(capabilities.supports_positions);
    assert!(capabilities.supports_reduce_only);
    assert!(capabilities
        .supports_time_in_force
        .contains(&rustcta_exchange_api::TimeInForce::IOC));
    assert!(capabilities
        .supports_time_in_force
        .contains(&rustcta_exchange_api::TimeInForce::FOK));
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
        CapabilitySupport::RestFallback { .. }
    ));
    assert!(matches!(
        capabilities.capabilities_v2.private_streams,
        CapabilitySupport::Unsupported { .. }
    ));
    assert!(capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.max_items,
        Some(5)
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.max_items,
        Some(10)
    );
    assert_eq!(
        capabilities.capabilities_v2.fills_history.max_limit,
        Some(1000)
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "place_order"
            && endpoint.path.as_deref() == Some("/api/v3/order")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "place_order"
            && endpoint.path.as_deref() == Some("/fapi/v1/order")
            && endpoint.market_types.contains(&MarketType::Perpetual)));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_place_orders"
            && endpoint.path.as_deref() == Some("/fapi/v1/batchOrders")
            && endpoint.market_types.contains(&MarketType::Perpetual)));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_cancel_orders"
            && endpoint.path.as_deref() == Some("/fapi/v1/batchOrders")
            && endpoint.market_types.contains(&MarketType::Perpetual)));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_positions"
            && endpoint.path.as_deref() == Some("/fapi/v2/positionRisk")
            && endpoint.market_types.contains(&MarketType::Perpetual)));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_recent_fills"
            && endpoint.path.as_deref() == Some("/api/v3/myTrades")));
}

#[tokio::test]
async fn binance_public_ws_should_build_depth_and_book_ticker_subscriptions() {
    let adapter = BinanceGatewayAdapter::new(BinanceGatewayConfig::default()).expect("adapter");
    let snapshot = public_subscription(PublicStreamKind::OrderBookSnapshot);
    let payload = binance_public_subscribe_payload(&snapshot).expect("payload");

    assert_eq!(payload["method"], "SUBSCRIBE");
    assert_eq!(payload["params"][0], "btcusdt@depth20@100ms");
    assert_eq!(
        binance_partial_depth_stream_name("BTC/USDT", 5, 1000).unwrap(),
        "btcusdt@depth5"
    );
    assert_eq!(
        binance_partial_depth_stream_name("BTC/USDT", 10, 100).unwrap(),
        "btcusdt@depth10@100ms"
    );

    let session: serde_json::Value = serde_json::from_str(
        &adapter
            .subscribe_public_stream(public_subscription(PublicStreamKind::Ticker))
            .await
            .expect("subscribe"),
    )
    .expect("session json");
    assert_eq!(session["url"], "wss://stream.binance.com:9443/ws");
    assert_eq!(session["payload"]["params"][0], "btcusdt@bookTicker");
    assert_eq!(session["heartbeat"]["ping_interval_ms"], 20_000);
}

#[test]
fn binance_public_ws_fixtures_should_parse_snapshot_delta_and_book_ticker() {
    let exchange = super::test_support::exchange_id();
    let snapshot = parse_binance_public_depth_message(
        &exchange,
        symbol_scope("BTCUSDT"),
        &fixture("ws/public_depth_snapshot.json"),
    )
    .expect("snapshot");
    assert_eq!(snapshot.sequence, Some(160));
    assert_eq!(snapshot.bids[0].quantity, 10.0);

    let delta = parse_binance_public_depth_message(
        &exchange,
        symbol_scope("BTCUSDT"),
        &fixture("ws/public_depth_delta.json"),
    )
    .expect("delta");
    assert_eq!(delta.sequence, Some(160));
    assert!(delta.exchange_timestamp.is_some());

    let ticker = parse_binance_public_depth_message(
        &exchange,
        symbol_scope("BTCUSDT"),
        &fixture("ws/public_book_ticker.json"),
    )
    .expect("book ticker");
    assert_eq!(ticker.sequence, Some(400900217));
    assert_eq!(ticker.bids[0].price, 25.3519);
}

#[test]
fn binance_parser_fixtures_should_cover_success_empty_and_error_shapes() {
    let exchange = super::test_support::exchange_id();
    let rules = parse_symbol_rules(
        &exchange,
        MarketType::Spot,
        &fixture("parser/symbol_rules_success.json"),
    )
    .expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");

    let empty = parse_symbol_rules(
        &exchange,
        MarketType::Spot,
        &fixture("parser/symbol_rules_empty.json"),
    )
    .expect("empty");
    assert!(empty.is_empty());
    assert!(parse_symbol_rules(
        &exchange,
        MarketType::Spot,
        &fixture("parser/symbol_rules_missing_field.json")
    )
    .is_err());
    assert!(parse_orderbook_snapshot(
        &exchange,
        symbol_scope("BTCUSDT"),
        &fixture("parser/orderbook_error.json")
    )
    .is_err());
}

#[tokio::test]
async fn binance_adapter_should_load_perpetual_depth_from_futures_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "lastUpdateId": 10636757896518_u64,
        "E": 1779821773594_i64,
        "T": 1779821773586_i64,
        "bids": [["65000.10000000", "0.20000000"]],
        "asks": [["65000.20000000", "0.30000000"]]
    })])
    .await;
    let adapter = BinanceGatewayAdapter::new(BinanceGatewayConfig {
        futures_rest_base_url: base_url,
        ..BinanceGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-book"),
            symbol: perpetual_symbol_scope("BTC-USDT"),
            depth: Some(500),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.market_type, MarketType::Perpetual);
    assert_eq!(response.order_book.sequence, Some(10636757896518));
    assert_eq!(response.order_book.bids[0].price, 65000.1);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/fapi/v1/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("500"));
}

#[tokio::test]
async fn binance_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "symbols": [{
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "baseAssetPrecision": 8,
            "quoteAssetPrecision": 8,
            "orderTypes": ["LIMIT", "LIMIT_MAKER", "MARKET"],
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0.01000000", "maxPrice": "1000000.00000000", "tickSize": "0.01000000"},
                {"filterType": "LOT_SIZE", "minQty": "0.00001000", "maxQty": "9000.00000000", "stepSize": "0.00001000"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "5.00000000"}
            ]
        }]
    })])
    .await;
    let adapter = BinanceGatewayAdapter::new(BinanceGatewayConfig {
        rest_base_url: base_url,
        ..BinanceGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope("BTC/USDT")],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(
        response.rules[0].price_increment.as_deref(),
        Some("0.01000000")
    );
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.00001000")
    );
    assert_eq!(
        response.rules[0].min_notional.as_deref(),
        Some("5.00000000")
    );
    assert_eq!(response.rules[0].price_precision, Some(2));
    assert_eq!(response.rules[0].quantity_precision, Some(5));
    assert_eq!(seen.lock().unwrap()[0].path, "/api/v3/exchangeInfo");
}

#[tokio::test]
async fn binance_adapter_should_ignore_unrequested_non_ascii_symbol_rules() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 8,
                "orderTypes": ["LIMIT", "MARKET"],
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.00001000"}
                ]
            },
            {
                "symbol": "BADUSDT",
                "status": "TRADING",
                "baseAsset": "坏币",
                "quoteAsset": "USDT",
                "filters": []
            }
        ]
    })])
    .await;
    let adapter = BinanceGatewayAdapter::new(BinanceGatewayConfig {
        rest_base_url: base_url,
        ..BinanceGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules-non-ascii"),
            symbols: vec![symbol_scope("BTC/USDT")],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].symbol.exchange_symbol.symbol, "BTCUSDT");
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v3/exchangeInfo");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
}

#[tokio::test]
async fn binance_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "lastUpdateId": 1027024,
        "bids": [["4.00000000", "431.00000000"]],
        "asks": [["4.00000200", "12.00000000"]]
    })])
    .await;
    let adapter = BinanceGatewayAdapter::new(BinanceGatewayConfig {
        rest_base_url: base_url,
        ..BinanceGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope("BTC-USDT"),
            depth: Some(11),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(1027024));
    assert_eq!(response.order_book.bids[0].price, 4.0);
    assert_eq!(response.order_book.asks[0].quantity, 12.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v3/depth");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("20"));
}
