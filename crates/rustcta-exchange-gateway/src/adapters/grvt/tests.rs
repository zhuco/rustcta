use rustcta_exchange_api::{
    AmendOrderRequest, BatchAtomicity, BatchCancelOrdersRequest, BatchExecutionMode,
    BatchPlaceOrdersRequest, CancelOrderRequest, CapabilitySupport, ExchangeApiError,
    ExchangeClient, OpenOrdersRequest, OrderListConditionalLeg, OrderListLegType, OrderListRequest,
    PlaceOrderRequest, PublicStreamKind, PublicStreamSubscription, QueryOrderRequest,
    RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};

use super::parser::{
    parse_grvt_amend_order_ack, parse_grvt_batch_cancel_orders_ack,
    parse_grvt_batch_place_orders_ack, parse_grvt_book_frame_meta, parse_grvt_fills,
    parse_grvt_orderbook_snapshot, parse_grvt_orders, parse_grvt_single_order,
    parse_grvt_symbol_rules,
};
use super::private;
use super::signing::grvt_session_auth_headers;
use super::streams::{
    grvt_ping_payload, grvt_public_subscribe_payload, grvt_stream_reconnect_policy_ms,
};
use super::test_support::{grvt_context, grvt_exchange_id, grvt_symbol, spawn_rest_server};
use super::{GrvtGatewayAdapter, GrvtGatewayConfig};
use crate::adapters::AdapterBackedGateway;
use crate::request_spec::{ActualHttpRequest, RequestSpec};

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
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_place_orders
            .supports_partial_failure
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert!(!capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert!(matches!(
        capabilities.capabilities_v2.batch_place_orders.support,
        CapabilitySupport::Unsupported { .. }
    ));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "amend_order"
            && endpoint.path.as_deref() == Some("/v1/amend_order")
            && matches!(endpoint.support, CapabilitySupport::Unsupported { .. })));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_place_orders"
            && endpoint.path.as_deref() == Some("/v2/bulk_orders")
            && matches!(endpoint.support, CapabilitySupport::Unsupported { .. })));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_cancel_orders"
            && endpoint.path.as_deref() == Some("/v2/bulk_orders")
            && matches!(endpoint.support, CapabilitySupport::Unsupported { .. })));

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/unsupported_boundary.json"
    ))
    .expect("fixture");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["scan_only"], false);
    assert_eq!(boundary["private_write_enabled"], false);
    assert_eq!(
        boundary["advanced_order_boundaries"]["amend_order"]["status"],
        "project_unimplemented"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["amend_order"]["runtime_error"],
        private::AMEND_ORDER_UNSUPPORTED
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["amend_order"]["request_spec_fixture"],
        "tests/fixtures/exchanges/grvt/request_specs/amend_order_session_spec_only.json"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["amend_order"]["parser_fixture"],
        "tests/fixtures/exchanges/grvt/parser/amend_order_ack.json"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_place_orders"]["status"],
        "project_unimplemented"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_place_orders"]["runtime_error"],
        private::BULK_ORDERS_UNSUPPORTED
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_place_orders"]["parser_fixture"],
        "tests/fixtures/exchanges/grvt/parser/bulk_orders_ack.json"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_cancel_orders"]["status"],
        "project_unimplemented"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_cancel_orders"]["runtime_error"],
        private::BULK_ORDERS_UNSUPPORTED
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_cancel_orders"]["parser_fixture"],
        "tests/fixtures/exchanges/grvt/parser/bulk_cancel_orders_ack.json"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["place_order_list"]["status"],
        "unsupported"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["oco"]["runtime_error"],
        private::ORDER_LIST_UNSUPPORTED
    );
}

#[tokio::test]
async fn grvt_readbacks_should_fail_closed_without_private_session() {
    let adapter = GrvtGatewayAdapter::new(GrvtGatewayConfig::default()).expect("adapter");

    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: grvt_context("query-disabled"),
            symbol: grvt_symbol(MarketType::Perpetual),
            client_order_id: None,
            exchange_order_id: Some("grvt-order-1".to_string()),
        })
        .await
        .expect_err("query disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "grvt.query_order"
        }
    ));

    let error = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: grvt_context("open-disabled"),
            exchange: grvt_exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(grvt_symbol(MarketType::Perpetual)),
            page: None,
        })
        .await
        .expect_err("open disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "grvt.get_open_orders"
        }
    ));

    let error = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: grvt_context("fills-disabled"),
            exchange: grvt_exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(grvt_symbol(MarketType::Perpetual)),
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
        error,
        ExchangeApiError::Unsupported {
            operation: "grvt.get_recent_fills"
        }
    ));
}

#[tokio::test]
async fn grvt_advanced_orders_should_remain_session_spec_only() {
    let adapter = GrvtGatewayAdapter::new(GrvtGatewayConfig::default()).expect("adapter");
    let order = PlaceOrderRequest {
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
    };
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: grvt_context("batch"),
        exchange: grvt_exchange_id(),
        orders: vec![order.clone()],
    };

    let error = adapter
        .batch_place_orders(request)
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: private::BULK_ORDERS_UNSUPPORTED
        }
    ));

    let error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: grvt_context("amend"),
            symbol: grvt_symbol(MarketType::Perpetual),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("grvt-order-1".to_string()),
            new_client_order_id: None,
            new_quantity: "0.002".to_string(),
        })
        .await
        .expect_err("unsupported amend");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: private::AMEND_ORDER_UNSUPPORTED
        }
    ));

    let error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: grvt_context("batch-cancel"),
            exchange: grvt_exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: grvt_context("cancel"),
                symbol: grvt_symbol(MarketType::Perpetual),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("grvt-order-1".to_string()),
            }],
        })
        .await
        .expect_err("unsupported batch cancel");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: private::BULK_ORDERS_UNSUPPORTED
        }
    ));

    let error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: grvt_context("oco"),
            symbol: grvt_symbol(MarketType::Perpetual),
            list_client_order_id: Some("grvt-oco-unsupported".to_string()),
            side: OrderSide::Sell,
            quantity: "0.001".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::Limit,
                price: Some("66000".to_string()),
                stop_price: None,
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("grvt-oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("63000".to_string()),
                stop_price: Some("63100".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("grvt-oco-below".to_string()),
            },
        })
        .await
        .expect_err("unsupported order list");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: private::ORDER_LIST_UNSUPPORTED
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
fn grvt_advanced_order_parser_fixtures_should_capture_offline_boundaries() {
    let amend_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/parser/amend_order_ack.json"
    ))
    .expect("amend fixture");
    let amend = parse_grvt_amend_order_ack(
        &grvt_exchange_id(),
        &AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: grvt_context("amend-parser"),
            symbol: grvt_symbol(MarketType::Perpetual),
            client_order_id: Some("grvt-amend-1".to_string()),
            exchange_order_id: Some("grvt-order-1001".to_string()),
            new_client_order_id: None,
            new_quantity: "0.002".to_string(),
        },
        &amend_fixture,
    )
    .expect("amend ack");
    assert_eq!(
        amend.order.exchange_order_id.as_deref(),
        Some("grvt-order-1001")
    );
    assert_eq!(amend.order.quantity, "0.002");
    assert_eq!(amend.order.status, OrderStatus::Open);

    let bulk_place_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/parser/bulk_orders_ack.json"
    ))
    .expect("bulk place fixture");
    let batch_place = parse_grvt_batch_place_orders_ack(
        &grvt_exchange_id(),
        &BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: grvt_context("bulk-place-parser"),
            exchange: grvt_exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: grvt_context("bulk-place-1"),
                    symbol: grvt_symbol(MarketType::Perpetual),
                    client_order_id: Some("grvt-bulk-create-1".to_string()),
                    side: OrderSide::Buy,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.001".to_string(),
                    price: Some("64000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: grvt_context("bulk-place-2"),
                    symbol: grvt_symbol(MarketType::Perpetual),
                    client_order_id: Some("grvt-bulk-create-2".to_string()),
                    side: OrderSide::Buy,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.001".to_string(),
                    price: Some("64000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
            ],
        },
        &bulk_place_fixture,
    )
    .expect("bulk place ack");
    assert_eq!(batch_place.orders.len(), 1);
    let report = batch_place.report.expect("bulk place report");
    assert_eq!(report.succeeded_count(), 1);
    assert_eq!(report.failed_count(), 1);
    assert!(report.requires_reconciliation());

    let bulk_cancel_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/parser/bulk_cancel_orders_ack.json"
    ))
    .expect("bulk cancel fixture");
    let batch_cancel = parse_grvt_batch_cancel_orders_ack(
        &grvt_exchange_id(),
        &BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: grvt_context("bulk-cancel-parser"),
            exchange: grvt_exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: grvt_context("bulk-cancel-1"),
                    symbol: grvt_symbol(MarketType::Perpetual),
                    client_order_id: Some("grvt-bulk-cancel-1".to_string()),
                    exchange_order_id: Some("grvt-order-3001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: grvt_context("bulk-cancel-2"),
                    symbol: grvt_symbol(MarketType::Perpetual),
                    client_order_id: Some("grvt-bulk-cancel-2".to_string()),
                    exchange_order_id: Some("grvt-order-3002".to_string()),
                },
            ],
        },
        &bulk_cancel_fixture,
    )
    .expect("bulk cancel ack");
    assert_eq!(batch_cancel.cancelled_count, 1);
    assert_eq!(batch_cancel.orders[0].status, OrderStatus::Cancelled);
    let report = batch_cancel.report.expect("bulk cancel report");
    assert_eq!(report.succeeded_count(), 1);
    assert_eq!(report.failed_count(), 1);
    assert!(report.requires_reconciliation());
}

#[test]
fn grvt_private_readback_parser_fixtures_should_normalize_orders_and_fills() {
    let order_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/order_success.json"
    ))
    .expect("order fixture");
    let order = parse_grvt_single_order(
        &grvt_exchange_id(),
        &grvt_symbol(MarketType::Perpetual),
        &order_fixture,
    )
    .expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("grvt-order-1"));
    assert_eq!(order.status, OrderStatus::Open);
    assert_eq!(order.filled_quantity, "0.004");

    let open_orders_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/open_orders_success.json"
    ))
    .expect("open orders fixture");
    let orders = parse_grvt_orders(
        &grvt_exchange_id(),
        &grvt_symbol(MarketType::Perpetual),
        &open_orders_fixture,
    )
    .expect("orders");
    assert_eq!(orders.len(), 2);
    assert_eq!(orders[1].status, OrderStatus::PartiallyFilled);

    let fills_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/recent_fills_success.json"
    ))
    .expect("fills fixture");
    let context = grvt_context("fills-parser");
    let fills = parse_grvt_fills(
        &grvt_exchange_id(),
        context.tenant_id.expect("tenant"),
        context.account_id.expect("account"),
        &grvt_symbol(MarketType::Perpetual),
        &fills_fixture,
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("grvt-fill-1"));
    assert_eq!(fills[0].price, 63950.0);
    assert_eq!(fills[0].quantity, 0.004);
    assert_eq!(fills[0].fee_asset.as_deref(), Some("USDT"));
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
            "../../../../../tests/fixtures/exchanges/grvt/request_specs/bulk_orders_session_spec_only.json"
        ))
        .expect("bulk orders spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/grvt/request_specs/cancel_order_unsupported.json"
        ))
        .expect("cancel order spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/grvt/request_specs/cancel_all_orders_unsupported.json"
        ))
        .expect("cancel all spec"),
        serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../../tests/fixtures/exchanges/grvt/request_specs/amend_order_session_spec_only.json"
        ))
        .expect("amend order spec"),
    ];
    for spec in specs {
        assert_eq!(spec["trade_enabled"], false);
        assert!(spec["expected_error"].as_str().is_some());
    }

    let read_specs = [
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
    ];
    for spec in read_specs {
        assert_eq!(spec["status"], "guarded_runtime");
        assert_eq!(spec["request_spec_required"], true);
        assert_eq!(spec["trade_enabled"], false);
    }

    let amend_spec: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/request_specs/amend_order_session_spec_only.json"
    ))
    .expect("amend spec");
    assert_eq!(amend_spec["status"], "project_unimplemented");
    assert_eq!(amend_spec["path"], "/v1/amend_order");
    assert_eq!(
        amend_spec["expected_error"],
        private::AMEND_ORDER_UNSUPPORTED
    );
    assert_eq!(
        amend_spec["signing_boundary"],
        "tests/fixtures/exchanges/grvt/signing_vectors/session_cookie_boundary.json"
    );

    let bulk_spec: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/request_specs/bulk_orders_session_spec_only.json"
    ))
    .expect("bulk spec");
    assert_eq!(bulk_spec["status"], "project_unimplemented");
    assert_eq!(bulk_spec["path"], "/v2/bulk_orders");
    assert_eq!(bulk_spec["native_batch"], true);
    assert_eq!(bulk_spec["atomicity"], "partial");
    assert_eq!(
        bulk_spec["expected_error"],
        private::BULK_ORDERS_UNSUPPORTED
    );

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

#[test]
fn grvt_readback_request_specs_should_match_runtime_shape() {
    let query: RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/request_specs/query_order_session_spec_only.json"
    ))
    .expect("query spec");
    query
        .assert_matches(
            &ActualHttpRequest::new("POST", private::QUERY_ORDER_PATH)
                .with_headers([
                    (
                        "Cookie".to_string(),
                        "<redacted-session-cookie>".to_string(),
                    ),
                    (
                        "X-Grvt-Account-Id".to_string(),
                        "<redacted-account-id>".to_string(),
                    ),
                ])
                .with_body(Some(
                    serde_json::json!({ "order_id": "<redacted-order-id>" }),
                )),
        )
        .expect("query request spec");

    let open_orders: RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/request_specs/open_orders_session_spec_only.json"
    ))
    .expect("open orders spec");
    open_orders
        .assert_matches(
            &ActualHttpRequest::new("POST", private::OPEN_ORDERS_PATH)
                .with_headers([
                    (
                        "Cookie".to_string(),
                        "<redacted-session-cookie>".to_string(),
                    ),
                    (
                        "X-Grvt-Account-Id".to_string(),
                        "<redacted-account-id>".to_string(),
                    ),
                ])
                .with_body(Some(serde_json::json!({ "instrument": "BTC_USDT_Perp" }))),
        )
        .expect("open-orders request spec");

    let fills: RequestSpec = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/request_specs/recent_fills_session_spec_only.json"
    ))
    .expect("fills spec");
    fills
        .assert_matches(
            &ActualHttpRequest::new("POST", private::RECENT_FILLS_PATH)
                .with_headers([
                    (
                        "Cookie".to_string(),
                        "<redacted-session-cookie>".to_string(),
                    ),
                    (
                        "X-Grvt-Account-Id".to_string(),
                        "<redacted-account-id>".to_string(),
                    ),
                ])
                .with_body(Some(serde_json::json!({ "instrument": "BTC_USDT_Perp" }))),
        )
        .expect("recent-fills request spec");
}

#[tokio::test]
async fn grvt_private_readback_runtime_should_send_session_post_requests() {
    let order: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/order_success.json"
    ))
    .expect("order");
    let open_orders: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/open_orders_success.json"
    ))
    .expect("open orders");
    let fills: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/grvt/recent_fills_success.json"
    ))
    .expect("fills");
    let (base_url, seen) = spawn_rest_server(vec![order, open_orders, fills]).await;
    let adapter = GrvtGatewayAdapter::new(GrvtGatewayConfig {
        trading_rest_base_url: base_url,
        session_cookie: Some("session=abc".to_string()),
        account_id: Some("grvt-account".to_string()),
        enabled_private_rest: true,
        ..GrvtGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);

    let query = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: grvt_context("query-live"),
            symbol: grvt_symbol(MarketType::Perpetual),
            client_order_id: None,
            exchange_order_id: Some("grvt-order-1".to_string()),
        })
        .await
        .expect("query");
    assert_eq!(
        query.order.expect("order").exchange_order_id.as_deref(),
        Some("grvt-order-1")
    );

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: grvt_context("open-live"),
            exchange: grvt_exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(grvt_symbol(MarketType::Perpetual)),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 2);

    let recent = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: grvt_context("fills-live"),
            exchange: grvt_exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(grvt_symbol(MarketType::Perpetual)),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect("recent fills");
    assert_eq!(recent.fills[0].fill_id.as_deref(), Some("grvt-fill-1"));

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].path, private::QUERY_ORDER_PATH);
    assert_eq!(
        seen[0].headers.get("cookie").map(String::as_str),
        Some("session=abc")
    );
    assert_eq!(
        seen[0].headers.get("x-grvt-account-id").map(String::as_str),
        Some("grvt-account")
    );
    assert_eq!(
        seen[0].body.as_ref().and_then(|body| body.get("order_id")),
        Some(&serde_json::json!("grvt-order-1"))
    );
    assert!(seen[0].query.is_empty());
    assert_eq!(seen[1].path, private::OPEN_ORDERS_PATH);
    assert_eq!(
        seen[1]
            .body
            .as_ref()
            .and_then(|body| body.get("instrument")),
        Some(&serde_json::json!("BTC_USDT_Perp"))
    );
    assert_eq!(seen[2].path, private::RECENT_FILLS_PATH);
    assert_eq!(
        seen[2].body.as_ref().and_then(|body| body.get("limit")),
        Some(&serde_json::json!(100))
    );
}
