use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};

use super::parser::{
    parse_grvt_book_frame_meta, parse_grvt_orderbook_snapshot, parse_grvt_symbol_rules,
};
use super::signing::grvt_session_auth_headers;
use super::streams::{
    grvt_ping_payload, grvt_public_subscribe_payload, grvt_stream_reconnect_policy_ms,
};
use super::test_support::{grvt_context, grvt_exchange_id, grvt_symbol};
use super::{GrvtGatewayAdapter, GrvtGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn named_registration_should_accept_grvt() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["grvt"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn capabilities_should_keep_grvt_behind_maturity_gate() {
    let adapter = GrvtGatewayAdapter::new(GrvtGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Perpetual, MarketType::Option]
    );
    assert!(!capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/unsupported_boundary.json"
    ))
    .expect("fixture");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["scan_only"], false);
}

#[tokio::test]
async fn grvt_bulk_orders_should_be_explicitly_session_spec_only() {
    let adapter = GrvtGatewayAdapter::new(GrvtGatewayConfig::default()).expect("adapter");
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: grvt_context("batch"),
        exchange: grvt_exchange_id(),
        orders: vec![PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: grvt_context("order"),
            symbol: grvt_symbol(MarketType::Perpetual),
            client_order_id: Some("client-1".to_string()),
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
            operation: "grvt.bulk_orders_session_spec_only"
        }
    ));
}

#[test]
fn grvt_websocket_helpers_should_match_json_rpc_subscription_shape() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: grvt_context("public-ws"),
        symbol: grvt_symbol(MarketType::Perpetual),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = grvt_public_subscribe_payload(&subscription);
    assert_eq!(payload["method"], "subscribe");
    assert_eq!(payload["stream"], "v1.book.s");
    assert_eq!(payload["feed"][0], "BTC_USDT_Perp@500-1-10");
    assert_eq!(grvt_ping_payload()["method"], "ping");
    assert_eq!(grvt_stream_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[tokio::test]
async fn grvt_public_stream_runtime_should_remain_unsupported_even_if_enabled() {
    let adapter = GrvtGatewayAdapter::new(GrvtGatewayConfig {
        enabled_public_streams: true,
        ..GrvtGatewayConfig::default()
    })
    .expect("adapter");
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: grvt_context("public-ws-runtime"),
        symbol: grvt_symbol(MarketType::Perpetual),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let error = adapter
        .subscribe_public_stream(subscription)
        .await
        .expect_err("runtime disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "grvt.public_stream_session_spec_only"
        }
    ));
}

#[test]
fn grvt_fixtures_should_record_sequence_and_auth_boundaries() {
    let book: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/ws/book_delta.json"
    ))
    .expect("book fixture");
    let meta = parse_grvt_book_frame_meta(&book);
    assert_eq!(meta.stream.as_deref(), Some("v1.book.s"));
    assert_eq!(meta.sequence_number, Some(1));
    assert!(!meta.snapshot);

    let signing: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/signing_vectors/session_cookie_boundary.json"
    ))
    .expect("signing fixture");
    assert_eq!(signing["supported"], false);
    assert_eq!(
        signing["expected_error"],
        "grvt.private_session_unavailable"
    );
    assert!(grvt_session_auth_headers(None, Some("account")).is_err());
}

#[test]
fn grvt_public_rest_fixtures_should_parse_g1_market_data() {
    let instruments: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/instruments.json"
    ))
    .expect("instruments fixture");
    let rules = parse_grvt_symbol_rules(&grvt_exchange_id(), &instruments).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].symbol.market_type, MarketType::Perpetual);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTC_USDT_Perp");
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "USDT");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.1"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.001"));
    assert!(rules[0].supports_reduce_only);
    assert_eq!(rules[1].symbol.market_type, MarketType::Option);

    let book: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/orderbook.json"
    ))
    .expect("book fixture");
    let snapshot = parse_grvt_orderbook_snapshot(
        &grvt_exchange_id(),
        grvt_symbol(MarketType::Perpetual),
        &book,
    )
    .expect("snapshot");
    assert_eq!(snapshot.sequence, Some(42));
    assert_eq!(snapshot.bids[0].price, 64000.0);
    assert_eq!(snapshot.bids[0].quantity, 1.25);
    assert_eq!(snapshot.asks[0].price, 64001.0);
}

#[test]
fn grvt_private_request_and_stream_specs_should_keep_trade_disabled() {
    let specs = [
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/grvt/request_specs/positions_session_spec_only.json"
        ))
        .expect("positions spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/grvt/request_specs/create_order_unsupported.json"
        ))
        .expect("create order spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/grvt/request_specs/bulk_orders_unsupported.json"
        ))
        .expect("bulk orders spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/grvt/request_specs/open_orders_session_spec_only.json"
        ))
        .expect("open orders spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/grvt/request_specs/query_order_session_spec_only.json"
        ))
        .expect("query order spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/grvt/request_specs/recent_fills_session_spec_only.json"
        ))
        .expect("recent fills spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/grvt/request_specs/cancel_order_unsupported.json"
        ))
        .expect("cancel order spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/grvt/request_specs/cancel_all_orders_unsupported.json"
        ))
        .expect("cancel all spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/grvt/request_specs/amend_order_unsupported.json"
        ))
        .expect("amend order spec"),
    ];
    for spec in specs {
        assert_eq!(spec["request_spec_required"], true);
        assert_eq!(spec["trade_enabled"], false);
    }

    let private_stream: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/ws/private_stream_boundary.json"
    ))
    .expect("private stream boundary");
    assert_eq!(private_stream["trade_enabled"], false);
    assert_eq!(private_stream["requires_rest_reconciliation"], true);
    assert_eq!(
        private_stream["expected_error"],
        "grvt.private_stream_session_spec_only"
    );
}
