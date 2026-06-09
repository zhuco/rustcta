use rustcta_exchange_api::{
    AmendOrderRequest, BatchAtomicity, BatchCancelOrdersRequest, BatchExecutionMode,
    BatchPlaceOrdersRequest, CancelOrderRequest, CapabilitySupport, ExchangeApiError,
    ExchangeClient, OpenOrdersRequest, OrderListConditionalLeg, OrderListLegType, OrderListRequest,
    PlaceOrderRequest, PublicStreamKind, PublicStreamSubscription, QueryOrderRequest,
    RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType};

use super::parser::{
    parse_lighter_amend_order_ack, parse_lighter_batch_cancel_orders_ack,
    parse_lighter_batch_place_orders_ack, parse_lighter_open_orders,
    parse_lighter_order_book_frame_meta, parse_lighter_orderbook_snapshot,
    parse_lighter_query_order, parse_lighter_recent_fills, parse_lighter_symbol_rules,
};
use super::private;
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
    assert!(!capabilities.supports_batch_cancel_order);
    assert_eq!(capabilities.max_order_book_depth, None);
    assert_eq!(capabilities.order_book.max_depth, None);
    assert!(!capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_place_orders
            .supports_client_order_id
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_cancel_orders
            .supports_partial_failure
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "amend_order"
            && endpoint.path.as_deref() == Some("/sendTx")
            && endpoint
                .credential_scopes
                .contains(&rustcta_exchange_api::CredentialScope::Trade)));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_place_orders"
            && endpoint.path.as_deref() == Some("/sendTxBatch")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_cancel_orders"
            && endpoint.path.as_deref() == Some("/sendTxBatch")));

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/unsupported_boundary.json"
    ))
    .expect("fixture");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_enabled"], false);
    assert_eq!(boundary["checksum"], "unsupported");
    assert_eq!(
        boundary["advanced_order_boundaries"]["amend_order"]["status"],
        "project_unimplemented"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["amend_order"]["runtime_error"],
        private::AMEND_ORDER_UNSUPPORTED
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["amend_order"]["official_surface"],
        "/sendTx"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_place_orders"]["status"],
        "project_unimplemented"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_place_orders"]["capabilities_v2"]["atomicity"],
        "partial"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_cancel_orders"]["runtime_error"],
        private::BATCH_CANCEL_UNSUPPORTED
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["place_order_list"]["status"],
        "unsupported"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["place_order_list"]["runtime_error"],
        private::ORDER_LIST_UNSUPPORTED
    );
}

#[test]
fn capabilities_should_keep_guarded_private_readbacks_unadvertised_until_history_promoted() {
    let adapter = LighterGatewayAdapter::new(LighterGatewayConfig {
        enabled_private_rest: true,
        auth_token: Some("token".to_string()),
        account_index: Some("42".to_string()),
        ..LighterGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_open_orders);
    assert!(!capabilities.supports_recent_fills);
    assert!(matches!(
        capabilities.capabilities_v2.private_rest,
        CapabilitySupport::Unsupported { .. }
    ));
    assert!(matches!(
        capabilities.capabilities_v2.order_history.support,
        CapabilitySupport::Unsupported { .. }
    ));
    assert!(matches!(
        capabilities.capabilities_v2.fills_history.support,
        CapabilitySupport::Unsupported { .. }
    ));
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_cancel_all_orders);
    assert!(!capabilities.supports_balances);
    assert!(!capabilities.supports_positions);
    assert!(!capabilities.supports_fees);
}

#[tokio::test]
async fn lighter_private_readbacks_should_fail_closed_without_guard() {
    let adapter = LighterGatewayAdapter::new(LighterGatewayConfig::default()).expect("adapter");
    let symbol = lighter_symbol(MarketType::Perpetual);

    let query_error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: lighter_context("query-disabled"),
            symbol: symbol.clone(),
            client_order_id: None,
            exchange_order_id: Some("lighter-order-1001".to_string()),
        })
        .await
        .expect_err("query disabled");
    assert!(matches!(
        query_error,
        ExchangeApiError::Unsupported {
            operation: private::PRIVATE_REST_DISABLED
        }
    ));

    let open_error = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: lighter_context("open-disabled"),
            exchange: lighter_exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol.clone()),
            page: None,
        })
        .await
        .expect_err("open disabled");
    assert!(matches!(
        open_error,
        ExchangeApiError::Unsupported {
            operation: private::PRIVATE_REST_DISABLED
        }
    ));

    let fills_error = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: lighter_context("fills-disabled"),
            exchange: lighter_exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: None,
            page: None,
        })
        .await
        .expect_err("fills disabled");
    assert!(matches!(
        fills_error,
        ExchangeApiError::Unsupported {
            operation: private::PRIVATE_REST_DISABLED
        }
    ));
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
            operation: private::BATCH_PLACE_UNSUPPORTED
        }
    ));

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: lighter_context("amend"),
            symbol: lighter_symbol(MarketType::Perpetual),
            client_order_id: Some("1234".to_string()),
            exchange_order_id: Some("lighter-order-1001".to_string()),
            new_client_order_id: None,
            new_quantity: "2".to_string(),
        })
        .await
        .expect_err("unsupported amend");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: private::AMEND_ORDER_UNSUPPORTED
        }
    ));

    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: lighter_context("batch-cancel"),
            exchange: lighter_exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: lighter_context("cancel-item"),
                symbol: lighter_symbol(MarketType::Perpetual),
                client_order_id: Some("cancel-client".to_string()),
                exchange_order_id: Some("lighter-order-3001".to_string()),
            }],
        })
        .await
        .expect_err("unsupported batch cancel");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: private::BATCH_CANCEL_UNSUPPORTED
        }
    ));

    let order_list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: lighter_context("oco"),
            symbol: lighter_symbol(MarketType::Perpetual),
            list_client_order_id: Some("lighter-oco-unsupported".to_string()),
            side: OrderSide::Sell,
            quantity: "0.1".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::Limit,
                price: Some("2200".to_string()),
                stop_price: None,
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("lighter-oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("1900".to_string()),
                stop_price: Some("1950".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("lighter-oco-below".to_string()),
            },
        })
        .await
        .expect_err("unsupported order list");
    assert!(matches!(
        order_list_error,
        ExchangeApiError::Unsupported {
            operation: private::ORDER_LIST_UNSUPPORTED
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
            "../../../../../tests/fixtures/exchanges/lighter/request_specs/send_tx_modify_request_spec_only.json"
        ))
        .expect("send tx modify spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/lighter/request_specs/send_tx_batch_request_spec_only.json"
        ))
        .expect("send tx batch request spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/lighter/request_specs/send_tx_batch_cancel_request_spec_only.json"
        ))
        .expect("send tx batch cancel spec"),
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

    let active_orders_spec = private::account_active_orders_request_spec_fixture();
    assert_eq!(active_orders_spec["method"], "GET");
    assert_eq!(active_orders_spec["path"], "/accountActiveOrders");
    assert_eq!(active_orders_spec["auth"], "bearer_token");
    assert_eq!(active_orders_spec["query"]["market_id"], "0");
    let trades_spec = private::trades_request_spec_fixture();
    assert_eq!(trades_spec["method"], "GET");
    assert_eq!(trades_spec["path"], "/trades");
    assert_eq!(trades_spec["auth"], "bearer_token");
}

#[test]
fn lighter_advanced_order_parser_fixtures_should_cover_offline_ack_boundaries() {
    let amend_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/parser/amend_order_ack.json"
    ))
    .expect("amend fixture");
    let amend_request = AmendOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: lighter_context("amend-parser"),
        symbol: lighter_symbol(MarketType::Perpetual),
        client_order_id: Some("lighter-client-1001".to_string()),
        exchange_order_id: Some("lighter-order-1001".to_string()),
        new_client_order_id: None,
        new_quantity: "1.25".to_string(),
    };
    let amend =
        parse_lighter_amend_order_ack(&lighter_exchange_id(), &amend_request, &amend_fixture)
            .expect("amend ack");
    assert_eq!(
        amend.order.exchange_order_id.as_deref(),
        Some("lighter-order-1001")
    );
    assert_eq!(
        amend.order.client_order_id.as_deref(),
        Some("lighter-client-1001")
    );
    assert_eq!(amend.order.quantity, "1.25");
    assert_eq!(amend.order.price.as_deref(), Some("2064.55"));

    let batch_place_request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: lighter_context("batch-place-parser"),
        exchange: lighter_exchange_id(),
        orders: vec![
            PlaceOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: lighter_context("batch-place-one"),
                symbol: lighter_symbol(MarketType::Perpetual),
                client_order_id: Some("lighter-batch-client-1".to_string()),
                side: OrderSide::Buy,
                position_side: None,
                order_type: OrderType::Limit,
                time_in_force: None,
                quantity: "1".to_string(),
                price: Some("2064.50".to_string()),
                quote_quantity: None,
                reduce_only: false,
                post_only: false,
            },
            PlaceOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: lighter_context("batch-place-two"),
                symbol: lighter_symbol(MarketType::Perpetual),
                client_order_id: Some("lighter-batch-client-2".to_string()),
                side: OrderSide::Buy,
                position_side: None,
                order_type: OrderType::Limit,
                time_in_force: None,
                quantity: "99".to_string(),
                price: Some("2064.50".to_string()),
                quote_quantity: None,
                reduce_only: false,
                post_only: false,
            },
        ],
    };
    let batch_place_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/parser/batch_place_orders_ack.json"
    ))
    .expect("batch place fixture");
    let batch_place = parse_lighter_batch_place_orders_ack(
        &lighter_exchange_id(),
        &batch_place_request,
        &batch_place_fixture,
    )
    .expect("batch place ack");
    assert_eq!(batch_place.orders.len(), 1);
    let report = batch_place.report.expect("place report");
    assert_eq!(report.total_items, 2);
    assert_eq!(report.succeeded_count(), 1);
    assert_eq!(report.failed_count(), 1);
    assert!(report.requires_reconciliation());

    let batch_cancel_request = BatchCancelOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: lighter_context("batch-cancel-parser"),
        exchange: lighter_exchange_id(),
        cancels: vec![
            CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: lighter_context("batch-cancel-one"),
                symbol: lighter_symbol(MarketType::Perpetual),
                client_order_id: Some("lighter-cancel-client-1".to_string()),
                exchange_order_id: Some("lighter-order-3001".to_string()),
            },
            CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: lighter_context("batch-cancel-two"),
                symbol: lighter_symbol(MarketType::Perpetual),
                client_order_id: Some("lighter-cancel-client-2".to_string()),
                exchange_order_id: Some("lighter-order-3002".to_string()),
            },
        ],
    };
    let batch_cancel_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/parser/batch_cancel_orders_ack.json"
    ))
    .expect("batch cancel fixture");
    let batch_cancel = parse_lighter_batch_cancel_orders_ack(
        &lighter_exchange_id(),
        &batch_cancel_request,
        &batch_cancel_fixture,
    )
    .expect("batch cancel ack");
    assert_eq!(batch_cancel.cancelled_count, 1);
    let report = batch_cancel.report.expect("cancel report");
    assert_eq!(report.succeeded_count(), 1);
    assert_eq!(report.failed_count(), 1);
    assert!(report.requires_reconciliation());
}

#[test]
fn lighter_private_readback_parser_fixtures_should_normalize_orders_and_fills() {
    let orders_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/parser/account_active_orders_success.json"
    ))
    .expect("orders fixture");
    let symbol = lighter_symbol(MarketType::Perpetual);
    let orders = parse_lighter_open_orders(&lighter_exchange_id(), &symbol, &orders_fixture)
        .expect("orders");
    assert_eq!(orders.len(), 1);
    assert_eq!(
        orders[0].exchange_order_id.as_deref(),
        Some("lighter-order-1001")
    );
    assert_eq!(orders[0].status, rustcta_types::OrderStatus::Open);
    assert_eq!(orders[0].filled_quantity, "0.50");
    assert!(orders[0].post_only);

    let queried = parse_lighter_query_order(
        &lighter_exchange_id(),
        &symbol,
        "lighter-order-1001",
        &orders_fixture,
    )
    .expect("query")
    .expect("order");
    assert_eq!(
        queried.client_order_id.as_deref(),
        Some("lighter-client-1001")
    );

    let trades_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lighter/parser/trades_success.json"
    ))
    .expect("trades fixture");
    let context = lighter_context("fills-parser");
    let fills = parse_lighter_recent_fills(
        &lighter_exchange_id(),
        context.tenant_id.expect("tenant"),
        context.account_id.expect("account"),
        &symbol,
        &trades_fixture,
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("lighter-trade-5001"));
    assert_eq!(fills[0].order_id.as_deref(), Some("lighter-order-1001"));
    assert_eq!(fills[0].price, 2064.55);
    assert_eq!(fills[0].quantity, 0.50);
    assert_eq!(fills[0].fee_amount, Some(0.0));
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
