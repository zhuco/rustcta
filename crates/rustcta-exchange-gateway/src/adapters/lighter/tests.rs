use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};

use super::parser::{
    parse_lighter_order_book_frame_meta, parse_lighter_orderbook_snapshot,
    parse_lighter_symbol_rules,
};
use super::signing::lighter_bearer_auth;
use super::streams::{
    lighter_keepalive_payload, lighter_public_subscribe_payload,
    lighter_public_unsubscribe_payload, lighter_stream_reconnect_policy_ms,
};
use super::test_support::{lighter_context, lighter_exchange_id, lighter_symbol};
use super::{LighterGatewayAdapter, LighterGatewayConfig};
use crate::adapters::AdapterBackedGateway;

#[test]
fn named_registration_should_accept_lighter() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["lighter"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn capabilities_should_keep_lighter_runtime_disabled() {
    let adapter = LighterGatewayAdapter::new(LighterGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(!capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_batch_place_order);
    assert_eq!(capabilities.max_order_book_depth, None);
    assert_eq!(capabilities.order_book.max_depth, None);
    assert!(!capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/unsupported_boundary.json"
    ))
    .expect("fixture");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["checksum"], "unsupported");
}

#[tokio::test]
async fn lighter_batch_orders_should_require_signed_tx_vectors() {
    let adapter = LighterGatewayAdapter::new(LighterGatewayConfig::default()).expect("adapter");
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: lighter_context("batch"),
        exchange: lighter_exchange_id(),
        orders: vec![PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: lighter_context("order"),
            symbol: lighter_symbol(MarketType::Perpetual),
            client_order_id: Some("1234".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "1".to_string(),
            price: Some("3000".to_string()),
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
            operation: "lighter.send_tx_batch_signing_unverified"
        }
    ));
}

#[test]
fn lighter_websocket_helpers_should_build_channel_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: lighter_context("public-ws"),
        symbol: lighter_symbol(MarketType::Perpetual),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = lighter_public_subscribe_payload(&subscription);
    assert_eq!(payload["type"], "subscribe");
    assert_eq!(payload["channel"], "order_book/0");
    let unsubscribe = lighter_public_unsubscribe_payload(&subscription);
    assert_eq!(unsubscribe["type"], "unsubscribe");
    assert_eq!(unsubscribe["channel"], "order_book/0");
    assert_eq!(lighter_keepalive_payload()["type"], "ping");
    assert_eq!(
        lighter_stream_reconnect_policy_ms(),
        (60_000, 120_000, 180_000)
    );
}

#[tokio::test]
async fn lighter_public_stream_runtime_should_remain_unsupported_even_if_enabled() {
    let adapter = LighterGatewayAdapter::new(LighterGatewayConfig {
        enabled_public_streams: true,
        ..LighterGatewayConfig::default()
    })
    .expect("adapter");
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: lighter_context("public-ws-runtime"),
        symbol: lighter_symbol(MarketType::Perpetual),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let error = adapter
        .subscribe_public_stream(subscription)
        .await
        .expect_err("runtime disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "lighter.public_stream_session_spec_only"
        }
    ));
}

#[test]
fn lighter_order_book_fixture_should_use_nonce_not_offset_for_continuity() {
    let book: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/ws/order_book_update.json"
    ))
    .expect("book fixture");
    let meta = parse_lighter_order_book_frame_meta(&book);
    assert_eq!(meta.channel.as_deref(), Some("order_book:0"));
    assert_eq!(meta.offset, Some(1_558_300));
    assert_eq!(meta.begin_nonce, Some(9_182_389_998));
    assert_eq!(meta.nonce, Some(9_182_390_020));
    assert_eq!(meta.checksum, None);
    assert!(meta.is_continuous_after(9_182_389_998));
    assert!(meta.requires_resubscribe_after(9_182_389_997));

    let signing: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/signing_vectors/signed_tx_boundary.json"
    ))
    .expect("signing fixture");
    assert_eq!(signing["supported"], false);
    assert_eq!(signing["expected_error"], "lighter.signed_tx_unavailable");
    assert!(lighter_bearer_auth(None, Some("1")).is_err());
}

#[test]
fn lighter_order_book_resync_fixtures_should_cover_snapshot_gap_and_missing_nonce() {
    let snapshot: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/ws/order_book_snapshot.json"
    ))
    .expect("snapshot fixture");
    let snapshot_meta = parse_lighter_order_book_frame_meta(&snapshot);
    assert_eq!(snapshot_meta.begin_nonce, Some(9_182_389_998));
    assert_eq!(snapshot_meta.nonce, Some(9_182_389_998));
    assert!(snapshot_meta.is_continuous_after(9_182_389_998));
    assert_eq!(snapshot_meta.checksum, None);

    let gap: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/ws/order_book_gap.json"
    ))
    .expect("gap fixture");
    let gap_meta = parse_lighter_order_book_frame_meta(&gap);
    assert_eq!(gap_meta.begin_nonce, Some(9_182_390_025));
    assert!(gap_meta.requires_resubscribe_after(9_182_390_020));

    let missing: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/missing_required_fields.json"
    ))
    .expect("missing fixture");
    let missing_meta = parse_lighter_order_book_frame_meta(&missing);
    assert_eq!(missing_meta.begin_nonce, None);
    assert!(missing_meta.requires_resubscribe_after(9_182_390_020));
}

#[test]
fn lighter_private_request_specs_should_keep_trade_disabled() {
    let specs = [
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/lighter/request_specs/send_tx_unsupported.json"
        ))
        .expect("send tx spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/lighter/request_specs/send_tx_batch_unsupported.json"
        ))
        .expect("send tx batch spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/lighter/request_specs/account_active_orders_readonly.json"
        ))
        .expect("open orders spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/lighter/request_specs/account_positions_readonly.json"
        ))
        .expect("positions spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/lighter/request_specs/trades_readonly.json"
        ))
        .expect("trades spec"),
    ];
    for spec in specs {
        assert_eq!(spec["trade_enabled"], false);
        assert_eq!(spec["request_spec_required"], true);
    }
}

#[test]
fn lighter_public_rest_fixtures_should_parse_g1_market_data() {
    let markets: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/order_books.json"
    ))
    .expect("markets fixture");
    let rules = parse_lighter_symbol_rules(&lighter_exchange_id(), &markets).expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].symbol.market_type, MarketType::Perpetual);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "market:0");
    assert_eq!(rules[0].base_asset, "ETH");
    assert_eq!(rules[0].quote_asset, "USD");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.0001"));
    assert!(rules[0].supports_post_only);

    let book: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/order_book_orders.json"
    ))
    .expect("book fixture");
    let snapshot = parse_lighter_orderbook_snapshot(
        &lighter_exchange_id(),
        lighter_symbol(MarketType::Perpetual),
        &book,
    )
    .expect("snapshot");
    assert_eq!(snapshot.sequence, Some(9_182_390_020));
    assert_eq!(snapshot.bids[0].price, 2064.53);
    assert_eq!(snapshot.asks[0].price, 2064.54);
    assert_eq!(snapshot.asks[0].quantity, 0.3285);
}
