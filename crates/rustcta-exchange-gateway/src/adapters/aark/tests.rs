use rustcta_exchange_api::{
    AmendOrderRequest, BatchAtomicity, BatchCancelOrdersRequest, BatchExecutionMode,
    BatchPlaceOrdersRequest, CancelOrderRequest, CapabilitySupport, ExchangeApiError,
    ExchangeClient, OpenOrdersRequest, OrderBookRequest, OrderListConditionalLeg, OrderListLegType,
    OrderListRequest, PageRequest, PlaceOrderRequest, PositionsRequest, PublicStreamKind,
    PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest, RequestContext, SymbolScope,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};
use serde_json::{json, Value};

use super::parser::{
    parse_amend_order_ack, parse_batch_place_orders_ack, parse_orderbook_snapshot, parse_orders,
    parse_recent_fills, parse_single_order, parse_symbol_rules,
};
use super::streams::{
    aark_check_prev_ts_continuity, aark_orderbook_update_timestamps, aark_orderbook_ws_policy,
    aark_private_auth_payload, aark_public_subscribe_payload, aark_reconnect_policy_ms,
    AarkPrevTsContinuity,
};
use super::transport::AarkRest;
use super::{private, signing, AarkGatewayAdapter, AarkGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("aark").expect("exchange")
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

fn symbol() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDC").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "PERP_BTC_USDC")
            .expect("symbol"),
    }
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "public_info" => {
            include_str!("../../../../../tests/fixtures/exchanges/aark/public_info.json")
        }
        "orderbook_snapshot" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/orderbook_snapshot.json"
        ),
        "private_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/private_order.json"
        ),
        "private_orders" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/private_orders.json"
        ),
        "private_trades" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/private_trades.json"
        ),
        "unsupported_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/unsupported_boundary.json"
        ),
        "empty_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/aark/empty_response.json")
        }
        "error_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/aark/error_response.json")
        }
        "missing_required_fields" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/missing_required_fields.json"
        ),
        "signing_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/signing_vectors/orderly_ed25519_boundary.json"
        ),
        "ws_subscribe" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/ws/public_orderbook_subscribe.json"
        ),
        "ws_orderbook_update" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/ws/public_orderbook_update.json"
        ),
        "ws_private_auth" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/ws/private_auth_payload.json"
        ),
        "amend_ack" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/parser/amend_order_ack.json"
        ),
        "batch_place_ack" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/parser/batch_place_orders_ack.json"
        ),
        _ => panic!("unknown fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "public_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/request_specs/public_info.json"
        ),
        "orderbook_signed_read" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/request_specs/orderbook_signed_read.json"
        ),
        "get_positions_signed_read" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/request_specs/get_positions_signed_read.json"
        ),
        "query_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/request_specs/query_order.json"
        ),
        "get_open_orders" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/request_specs/get_open_orders.json"
        ),
        "get_recent_fills" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/request_specs/get_recent_fills.json"
        ),
        "amend_order_signed_write" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/request_specs/amend_order_signed_write.json"
        ),
        "batch_place_orders_signed_write" => include_str!(
            "../../../../../tests/fixtures/exchanges/aark/request_specs/batch_place_orders_signed_write.json"
        ),
        _ => panic!("unknown request spec {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn limit_order(client_order_id: &str, price: &str) -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(client_order_id),
        symbol: symbol(),
        client_order_id: Some(client_order_id.to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some(price.to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

#[test]
fn parser_should_parse_orderly_private_readback_fixtures() {
    let order = parse_single_order(&exchange_id(), &symbol(), &fixture("private_order"))
        .expect("single order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("123456789"));
    assert_eq!(order.client_order_id.as_deref(), Some("cid-aark-1"));
    assert_eq!(order.status, OrderStatus::Open);
    assert_eq!(order.quantity, "0.01");
    assert_eq!(order.price.as_deref(), Some("65000"));

    let orders =
        parse_orders(&exchange_id(), Some(&symbol()), &fixture("private_orders")).expect("orders");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].exchange_order_id.as_deref(), Some("123456789"));

    let fills = parse_recent_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        Some(&symbol()),
        &fixture("private_trades"),
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("fill-aark-1"));
    assert_eq!(fills[0].fee_asset.as_deref(), Some("USDC"));
    assert_eq!(fills[0].price, 65000.0);
}

#[test]
fn parser_should_parse_orderly_perpetual_public_info() {
    let rules = parse_symbol_rules(&exchange_id(), &fixture("public_info")).expect("rules");

    assert_eq!(rules.len(), 2);
    let rule = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "PERP_BTC_USDC")
        .expect("btc rule");
    assert_eq!(rule.symbol.market_type, MarketType::Perpetual);
    assert_eq!(rule.base_asset, "BTC");
    assert_eq!(rule.quote_asset, "USDC");
    assert_eq!(rule.price_increment.as_deref(), Some("0.1"));
    assert_eq!(rule.quantity_increment.as_deref(), Some("0.0001"));
    assert_eq!(rule.min_quantity.as_deref(), Some("0.0001"));
    assert_eq!(rule.min_notional.as_deref(), Some("10"));
    assert_eq!(rule.price_precision, Some(1));
    assert_eq!(rule.quantity_precision, Some(4));
}

#[test]
fn parser_should_parse_orderly_orderbook_snapshot_fixture() {
    let snapshot =
        parse_orderbook_snapshot(&exchange_id(), symbol(), &fixture("orderbook_snapshot"))
            .expect("book");

    assert_eq!(snapshot.best_bid().expect("bid").price, 65000.1);
    assert_eq!(snapshot.best_bid().expect("bid").quantity, 1.25);
    assert_eq!(snapshot.best_ask().expect("ask").price, 65001.2);
    assert_eq!(snapshot.sequence, Some(424242));
    assert!(snapshot.exchange_timestamp.is_some());
}

#[test]
fn parser_should_parse_advanced_order_ack_fixtures() {
    let amend = parse_amend_order_ack(
        &exchange_id(),
        &AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol(),
            client_order_id: Some("aark-amend-boundary-1".to_string()),
            exchange_order_id: Some("987654321".to_string()),
            new_client_order_id: None,
            new_quantity: "0.01".to_string(),
        },
        &fixture("amend_ack"),
    )
    .expect("amend ack");
    assert_eq!(amend.order.exchange_order_id.as_deref(), Some("987654321"));
    assert_eq!(
        amend.order.client_order_id.as_deref(),
        Some("aark-amend-boundary-1")
    );
    assert_eq!(amend.order.status, OrderStatus::New);
    assert_eq!(amend.order.quantity, "0.01");
    assert_eq!(amend.order.price.as_deref(), Some("65000"));

    let batch_request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place"),
        exchange: exchange_id(),
        orders: vec![
            limit_order("aark-batch-1", "64000"),
            limit_order("aark-batch-2", "3000"),
        ],
    };
    let batch =
        parse_batch_place_orders_ack(&exchange_id(), &batch_request, &fixture("batch_place_ack"))
            .expect("batch ack");
    assert_eq!(batch.orders.len(), 1);
    assert_eq!(
        batch.orders[0].client_order_id.as_deref(),
        Some("aark-batch-1")
    );
    let report = batch.report.expect("partial report");
    assert_eq!(report.total_items, 2);
    assert_eq!(report.succeeded_count(), 1);
    assert_eq!(report.failed_count(), 1);
}

#[test]
fn capabilities_should_expose_public_info_and_guarded_readbacks_but_no_writes() {
    let adapter = AarkGatewayAdapter::new(AarkGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(!capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_query_order);
    assert!(!capabilities.supports_open_orders);
    assert!(!capabilities.supports_recent_fills);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_batch_place_order);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_symbol_rules"
            && endpoint.path.as_deref() == Some("/v1/public/info")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "query_order"
            && endpoint.path.as_deref() == Some("/v1/order/{order_id}")
            && matches!(endpoint.support, CapabilitySupport::Native)));
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_batch_place_order);
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert!(matches!(
        capabilities.capabilities_v2.batch_place_orders.support,
        CapabilitySupport::Unsupported { .. }
    ));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "amend_order"
            && endpoint.path.as_deref() == Some("/v1/order")
            && matches!(endpoint.support, CapabilitySupport::Unsupported { .. })));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_place_orders"
            && endpoint.path.as_deref() == Some("/v1/batch-order")
            && matches!(endpoint.support, CapabilitySupport::Unsupported { .. })));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "place_order_list"
            && endpoint.path.is_none()
            && matches!(endpoint.support, CapabilitySupport::Unsupported { .. })));

    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
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
        boundary["advanced_order_boundaries"]["batch_place_orders"]["status"],
        "project_unimplemented"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_place_orders"]["runtime_error"],
        private::BATCH_PLACE_UNSUPPORTED
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["place_order_list"]["status"],
        "unsupported"
    );
    assert_eq!(
        boundary["advanced_order_boundaries"]["batch_cancel_orders"]["runtime_error"],
        private::BATCH_CANCEL_UNSUPPORTED
    );

    let mut private_config = AarkGatewayConfig::default();
    private_config.enabled_private_rest = true;
    private_config.orderly_account_id = Some("acct_test_account_id".to_string());
    private_config.orderly_key = Some("ed25519:test-public-key".to_string());
    private_config.orderly_secret =
        Some("0000000000000000000000000000000000000000000000000000000000000001".to_string());
    let private_adapter = AarkGatewayAdapter::new(private_config).expect("private adapter");
    let private_capabilities = private_adapter.capabilities();
    assert!(private_capabilities.supports_private_rest);
    assert!(private_capabilities.supports_query_order);
    assert!(private_capabilities.supports_open_orders);
    assert!(private_capabilities.supports_recent_fills);
    assert!(!private_capabilities.supports_place_order);
    assert!(!private_capabilities.supports_cancel_order);
    assert!(!private_capabilities.supports_cancel_all_orders);
    assert!(!private_capabilities.supports_balances);
    assert!(!private_capabilities.supports_fees);
}

#[tokio::test]
async fn private_readbacks_should_fail_closed_without_guard() {
    let adapter = AarkGatewayAdapter::new(AarkGatewayConfig::default()).expect("adapter");
    let query_error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("123456789".to_string()),
        })
        .await
        .expect_err("query disabled");
    assert!(matches!(
        query_error,
        ExchangeApiError::Unsupported {
            operation: "aark.private_rest_disabled"
        }
    ));

    let open_error = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol()),
            page: Some(PageRequest::first_page(100)),
        })
        .await
        .expect_err("open disabled");
    assert!(matches!(
        open_error,
        ExchangeApiError::Unsupported {
            operation: "aark.private_rest_disabled"
        }
    ));

    let fills_error = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect_err("fills disabled");
    assert!(matches!(
        fills_error,
        ExchangeApiError::Unsupported {
            operation: "aark.private_rest_disabled"
        }
    ));
}

#[tokio::test]
async fn orderbook_and_private_writes_should_return_orderly_boundary() {
    let adapter = AarkGatewayAdapter::new(AarkGatewayConfig::default()).expect("adapter");
    let orderbook_error = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol(),
            depth: Some(50),
        })
        .await
        .expect_err("unsupported book");
    assert!(matches!(
        orderbook_error,
        ExchangeApiError::Unsupported {
            operation: private::ORDER_BOOK_UNSUPPORTED
        }
    ));

    let positions_error = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![symbol().exchange_symbol],
        })
        .await
        .expect_err("unsupported positions");
    assert!(matches!(
        positions_error,
        ExchangeApiError::Unsupported {
            operation: private::POSITIONS_UNSUPPORTED
        }
    ));

    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let place_error = adapter
        .place_order(order.clone())
        .await
        .expect_err("unsupported place");
    assert!(matches!(
        place_error,
        ExchangeApiError::Unsupported {
            operation: private::PLACE_ORDER_UNSUPPORTED
        }
    ));

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol(),
            client_order_id: Some("offline-fixture".to_string()),
            exchange_order_id: Some("987654321".to_string()),
            new_client_order_id: None,
            new_quantity: "0.02".to_string(),
        })
        .await
        .expect_err("unsupported amend");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: private::AMEND_ORDER_UNSUPPORTED
        }
    ));

    let batch_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch"),
            exchange: exchange_id(),
            orders: vec![order],
        })
        .await
        .expect_err("unsupported batch");
    assert!(matches!(
        batch_error,
        ExchangeApiError::Unsupported {
            operation: private::BATCH_PLACE_UNSUPPORTED
        }
    ));

    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel"),
                symbol: symbol(),
                client_order_id: Some("offline-fixture".to_string()),
                exchange_order_id: Some("987654321".to_string()),
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
            context: context("order-list"),
            symbol: symbol(),
            list_client_order_id: Some("aark-list".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::TakeProfitLimit,
                price: Some("70000".to_string()),
                stop_price: Some("69900".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("aark-list-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("60000".to_string()),
                stop_price: Some("60100".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("aark-list-below".to_string()),
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
fn request_specs_should_match_public_and_signed_read_boundaries() {
    let public_info = ActualHttpRequest::new("GET", AarkRest::public_info_path());
    request_spec("public_info")
        .assert_matches(&public_info)
        .expect("public info spec");

    let signed_orderbook =
        ActualHttpRequest::new("GET", AarkRest::signed_orderbook_path("PERP_BTC_USDC"))
            .with_headers([
                (
                    "orderly-account-id".to_string(),
                    "acct_test_account_id".to_string(),
                ),
                (
                    "orderly-key".to_string(),
                    "ed25519:test-public-key".to_string(),
                ),
                (
                    "orderly-signature".to_string(),
                    "test-signature-placeholder".to_string(),
                ),
                ("orderly-timestamp".to_string(), "1700000000000".to_string()),
            ]);
    request_spec("orderbook_signed_read")
        .assert_matches(&signed_orderbook)
        .expect("signed orderbook spec");

    let signed_headers = [
        (
            "orderly-account-id".to_string(),
            "<redacted-account-id>".to_string(),
        ),
        (
            "orderly-key".to_string(),
            "<redacted-orderly-key>".to_string(),
        ),
        (
            "orderly-signature".to_string(),
            "<redacted-ed25519-signature>".to_string(),
        ),
        ("orderly-timestamp".to_string(), "1700000000000".to_string()),
    ];
    let signed_positions =
        ActualHttpRequest::new("GET", "/v1/positions").with_headers(signed_headers.clone());
    request_spec("get_positions_signed_read")
        .assert_matches(&signed_positions)
        .expect("positions signed-read spec");
    assert_eq!(
        request_spec("get_positions_signed_read").operation,
        "get_positions"
    );

    let query_order = ActualHttpRequest::new("GET", AarkRest::query_order_path("123456789"))
        .with_headers([
            (
                "orderly-account-id".to_string(),
                "acct_test_account_id".to_string(),
            ),
            (
                "orderly-key".to_string(),
                "ed25519:test-public-key".to_string(),
            ),
            (
                "orderly-signature".to_string(),
                "test-signature-placeholder".to_string(),
            ),
            ("orderly-timestamp".to_string(), "1700000000000".to_string()),
        ]);
    request_spec("query_order")
        .assert_matches(&query_order)
        .expect("query order signed-read spec");

    let open_orders = ActualHttpRequest::new("GET", AarkRest::orders_path())
        .with_query([
            ("symbol".to_string(), "PERP_BTC_USDC".to_string()),
            ("status".to_string(), "INCOMPLETE".to_string()),
            ("size".to_string(), "100".to_string()),
        ])
        .with_headers([
            (
                "orderly-account-id".to_string(),
                "acct_test_account_id".to_string(),
            ),
            (
                "orderly-key".to_string(),
                "ed25519:test-public-key".to_string(),
            ),
            (
                "orderly-signature".to_string(),
                "test-signature-placeholder".to_string(),
            ),
            ("orderly-timestamp".to_string(), "1700000000000".to_string()),
        ]);
    request_spec("get_open_orders")
        .assert_matches(&open_orders)
        .expect("open orders signed-read spec");

    let recent_fills = ActualHttpRequest::new("GET", AarkRest::trades_path())
        .with_query([
            ("symbol".to_string(), "PERP_BTC_USDC".to_string()),
            ("size".to_string(), "100".to_string()),
        ])
        .with_headers([
            (
                "orderly-account-id".to_string(),
                "acct_test_account_id".to_string(),
            ),
            (
                "orderly-key".to_string(),
                "ed25519:test-public-key".to_string(),
            ),
            (
                "orderly-signature".to_string(),
                "test-signature-placeholder".to_string(),
            ),
            ("orderly-timestamp".to_string(), "1700000000000".to_string()),
        ]);
    request_spec("get_recent_fills")
        .assert_matches(&recent_fills)
        .expect("recent fills signed-read spec");

    let amend = ActualHttpRequest::new("PUT", "/v1/order")
        .with_headers(signed_headers.clone())
        .with_body(Some(json!({
            "order_id": "<redacted-order-id>",
            "symbol": "PERP_BTC_USDC",
            "order_price": "65000",
            "order_quantity": "0.01",
            "client_order_id": "aark-amend-boundary-1"
        })));
    request_spec("amend_order_signed_write")
        .assert_matches(&amend)
        .expect("amend signed-write spec");

    let batch = ActualHttpRequest::new("POST", "/v1/batch-order")
        .with_headers(signed_headers)
        .with_body(Some(json!({
            "orders": [
                {
                    "symbol": "PERP_BTC_USDC",
                    "order_type": "LIMIT",
                    "side": "BUY",
                    "order_price": "64000",
                    "order_quantity": "0.01",
                    "client_order_id": "aark-batch-1"
                }
            ]
        })));
    request_spec("batch_place_orders_signed_write")
        .assert_matches(&batch)
        .expect("batch place signed-write spec");

    let signing_fixture = fixture("signing_boundary");
    assert_eq!(
        signing_fixture["canonical_payload"],
        signing::aark_orderly_canonical_payload(
            1_700_000_000_000,
            "GET",
            "/v1/orderbook/PERP_BTC_USDC",
            ""
        )
    );
    assert_eq!(
        signing::aark_orderly_signing_boundary(),
        signing::AARK_ORDERLY_SIGNING_BOUNDARY
    );
}

#[test]
fn websocket_helpers_should_build_fixture_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    assert_eq!(
        aark_public_subscribe_payload(&subscription),
        fixture("ws_subscribe")
    );
    let policy = aark_orderbook_ws_policy();
    assert_eq!(policy.url, "wss://ws-evm.orderly.org/ws/stream");
    assert_eq!(policy.path_template, "/{account_id}");
    assert_eq!(policy.topic_template, "{symbol}@orderbookupdate");
    assert_eq!(policy.interval_ms, 200);
    assert_eq!(policy.sequence_fields, &["ts", "prevTs"]);
    assert_eq!(policy.checksum, None);
    assert!(policy.resync.contains("prevTs gap"));

    let update = fixture("ws_orderbook_update");
    let (ts, prev_ts) = aark_orderbook_update_timestamps(&update);
    assert_eq!(ts, Some(1_700_000_000_200));
    assert_eq!(prev_ts, Some(1_700_000_000_000));
    assert_eq!(
        aark_check_prev_ts_continuity(Some(1_700_000_000_000), prev_ts),
        AarkPrevTsContinuity::Continuous
    );
    let gap = aark_check_prev_ts_continuity(Some(1_700_000_000_100), prev_ts);
    assert_eq!(
        gap,
        AarkPrevTsContinuity::Gap {
            previous_ts: 1_700_000_000_100,
            prev_ts: 1_700_000_000_000
        }
    );
    assert!(gap.requires_resync());

    assert_eq!(
        aark_private_auth_payload(
            "ed25519:test-public-key",
            1_700_000_000_000,
            "test-signature-placeholder"
        ),
        fixture("ws_private_auth")
    );
    assert_eq!(aark_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[test]
fn boundary_fixtures_should_remain_sanitized_and_closed_for_trading() {
    let empty = fixture("empty_response");
    let error = fixture("error_response");
    let missing = fixture("missing_required_fields");
    let unsupported_spec = serde_json::from_str::<Value>(include_str!(
        "../../../../../tests/fixtures/exchanges/aark/request_specs/place_order_unsupported.json"
    ))
    .expect("unsupported request spec");

    assert_eq!(empty["success"], true);
    assert!(empty["data"]["rows"].as_array().is_some_and(Vec::is_empty));
    assert_eq!(error["success"], false);
    assert!(missing.get("symbol").is_some());
    assert_eq!(unsupported_spec["trade_enabled"], false);
    assert_eq!(
        unsupported_spec["expected_error"],
        json!(private::PLACE_ORDER_UNSUPPORTED)
    );
}
