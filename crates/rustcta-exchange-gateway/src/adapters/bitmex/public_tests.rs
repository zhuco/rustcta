use rustcta_exchange_api::{
    BatchAtomicity, BatchExecutionMode, CapabilitySupport, ExchangeClient, OrderBookRequest,
    SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::test_support::{context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope};
use super::{BitmexGatewayAdapter, BitmexGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[tokio::test]
async fn bitmex_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!([
        {
            "symbol": "XBT_USDT",
            "typ": "IFXXXP",
            "state": "Open",
            "underlying": "XBT",
            "quoteCurrency": "USDT",
            "tickSize": 0.1,
            "lotSize": 100,
            "maxOrderQty": 100000000
        },
        {
            "symbol": "XBTUSD",
            "typ": "FFWCSX",
            "state": "Open",
            "underlying": "XBT",
            "quoteCurrency": "USD",
            "tickSize": 0.5,
            "lotSize": 1,
            "maxOrderQty": 10000000,
            "isInverse": true
        },
        {
            "symbol": "EURUSD",
            "typ": "FFSCSX",
            "state": "Open",
            "underlying": "EUR",
            "quoteCurrency": "USD",
            "tickSize": 0.00001,
            "lotSize": 1
        }
    ])])
    .await;
    let adapter = BitmexGatewayAdapter::new(BitmexGatewayConfig {
        rest_base_url: base_url,
        ..BitmexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].quote_asset, "USDT");
    assert_eq!(response.rules[0].symbol.exchange_symbol.symbol, "XBT_USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.1"));
    assert_eq!(response.rules[0].quantity_increment.as_deref(), Some("100"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/api/v1/instrument/active");
}

#[tokio::test]
async fn bitmex_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!([
        {
            "symbol": "XBTUSD",
            "id": 8799280000_u64,
            "side": "Sell",
            "size": 500,
            "price": 100.5,
            "timestamp": "2026-06-07T00:00:00.000Z"
        },
        {
            "symbol": "XBTUSD",
            "id": 8799290000_u64,
            "side": "Buy",
            "size": 700,
            "price": 99.5,
            "timestamp": "2026-06-07T00:00:00.000Z"
        },
        {
            "symbol": "XBTUSD",
            "id": 8799300000_u64,
            "side": "Buy",
            "size": 900,
            "price": 99.0
        }
    ])])
    .await;
    let adapter = BitmexGatewayAdapter::new(BitmexGatewayConfig {
        rest_base_url: base_url,
        ..BitmexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: perp_symbol_scope(),
            depth: Some(101),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 99.5);
    assert_eq!(response.order_book.bids[1].quantity, 900.0);
    assert_eq!(response.order_book.asks[0].price, 100.5);
    assert!(response.order_book.exchange_timestamp.is_some());
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v1/orderBook/L2");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("XBTUSD")
    );
    assert_eq!(request.query.get("depth").map(String::as_str), Some("100"));
}

#[test]
fn bitmex_adapter_should_register_by_name() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["bitmex"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn bitmex_capabilities_v2_should_declare_task14_boundaries() {
    let public_adapter =
        BitmexGatewayAdapter::new(BitmexGatewayConfig::default()).expect("adapter");
    let public_capabilities = public_adapter.capabilities();
    assert_eq!(
        public_capabilities.market_types,
        vec![MarketType::Spot, MarketType::Perpetual]
    );
    assert!(matches!(
        public_capabilities.capabilities_v2.private_rest,
        CapabilitySupport::Unsupported { .. }
    ));
    assert!(matches!(
        public_capabilities.capabilities_v2.private_streams,
        CapabilitySupport::RestFallback { .. }
    ));
    assert_eq!(
        public_capabilities.order_book.strictness,
        rustcta_exchange_api::OrderBookStrictness::SnapshotOnly
    );

    let private_adapter = BitmexGatewayAdapter::new(super::test_support::private_config(
        "http://127.0.0.1:1".to_string(),
    ))
    .expect("private adapter");
    let capabilities = private_adapter.capabilities();
    let v2 = capabilities.capabilities_v2;
    assert!(matches!(v2.public_rest, CapabilitySupport::Native));
    assert!(matches!(v2.private_rest, CapabilitySupport::Native));
    assert!(matches!(
        v2.stream_runtime.public,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        v2.stream_runtime.private,
        CapabilitySupport::Native
    ));
    assert!(v2.stream_runtime.resync.order_book);
    assert!(v2.stream_runtime.auth.requires_relogin_on_reconnect);
    assert_eq!(v2.stream_runtime.heartbeat_policy.ping_interval_ms, 30_000);
    assert_eq!(
        v2.stream_runtime.auth_renewal_policy.renewal_interval_ms,
        Some(45_000)
    );
    assert_eq!(v2.batch_place_orders.mode, BatchExecutionMode::Native);
    assert_eq!(v2.batch_place_orders.atomicity, BatchAtomicity::Partial);
    assert_eq!(v2.batch_place_orders.max_items, Some(20));
    assert!(v2.batch_place_orders.supports_partial_failure);
    assert_eq!(v2.batch_cancel_orders.mode, BatchExecutionMode::Native);
    assert_eq!(v2.fills_history.max_limit, Some(500));
    assert!(v2.fills_history.supports_since);
    assert!(v2.fills_history.supports_until);
    assert_eq!(v2.order_history.max_limit, Some(100));
    assert!(v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "funding_open_interest"
            && matches!(endpoint.support, CapabilitySupport::Unsupported { .. })));
}

#[test]
fn bitmex_endpoint_mapping_should_cover_task14_required_sections() {
    let mapping = include_str!("endpoint_mapping.yaml");
    for required in [
        "exchange: bitmex",
        "metadata:",
        "dated futures and options remain unsupported",
        "Contract spec fields:",
        "Snapshot-only",
        "pagination:",
        "operation: place_order",
        "operation: batch_place_orders",
        "operation: cancel_all_orders",
        "operation: recent_fills",
        "operation: funding_open_interest",
        "support: unsupported",
        "native_batch: true",
        "atomicity: partial",
        "live_dry_run_gates:",
        "kill_switch:",
        "disabled_symbol:",
        "max_notional:",
        "auth_renewal:",
        "mode: relogin",
        "mode: rest_reconciliation",
    ] {
        assert!(
            mapping.contains(required),
            "endpoint_mapping.yaml missing {required}"
        );
    }
}
