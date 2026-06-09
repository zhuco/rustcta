use rustcta_exchange_api::{
    AmendOrderRequest, BatchAtomicity, BatchCancelOrdersRequest, BatchExecutionMode,
    BatchPlaceOrdersRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient,
    OpenOrdersRequest, OrderBookRequest, OrderListConditionalLeg, OrderListLegType,
    OrderListRequest, PageRequest, PlaceOrderRequest, PublicStreamKind, PublicStreamSubscription,
    QueryOrderRequest, RecentFillsRequest, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};
use serde_json::{json, Value};

use super::parser::{
    parse_amend_order_ack, parse_batch_cancel_orders_ack, parse_orderbook_snapshot, parse_orders,
    parse_recent_fills, parse_single_order, parse_symbol_rules,
};
use super::streams::{
    bsx_private_auth_payload, bsx_public_subscribe_payload, bsx_reconnect_policy_ms,
};
use super::transport::BsxRest;
use super::{private, signing, BsxGatewayAdapter, BsxGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bsx").expect("exchange")
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
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTC-PERP")
            .expect("symbol"),
    }
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "products" => include_str!("../../../../../tests/fixtures/exchanges/bsx/products.json"),
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/orderbook.json")
        }
        "unsupported_boundary" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/unsupported_boundary.json")
        }
        "order_success" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/order_success.json")
        }
        "open_orders_success" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/open_orders_success.json")
        }
        "recent_fills_success" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/recent_fills_success.json")
        }
        "empty_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/empty_response.json")
        }
        "error_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/error_response.json")
        }
        "missing_required_fields" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/missing_required_fields.json")
        }
        "eip712_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/signing_vectors/eip712_order_boundary.json"
        ),
        "ws_auth_signature" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/signing_vectors/ws_auth_hmac_sha256.json"
        ),
        "ws_subscribe" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/ws/public_orderbook_subscribe.json"
        ),
        "ws_private_auth" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/ws/private_auth_payload.json")
        }
        "parser/amend_order_ack" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/parser/amend_order_ack.json")
        }
        "parser/batch_cancel_orders_ack" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/parser/batch_cancel_orders_ack.json"
        ),
        _ => panic!("unknown fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "products" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/request_specs/products.json")
        }
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/bsx/request_specs/orderbook.json")
        }
        "open_orders_spec_only" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/request_specs/open_orders_spec_only.json"
        ),
        "query_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/request_specs/query_order.json"
        ),
        "get_open_orders" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/request_specs/get_open_orders.json"
        ),
        "get_recent_fills" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/request_specs/get_recent_fills.json"
        ),
        "cancel_order" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/request_specs/cancel_order_unsupported.json"
        ),
        "place_order_unsupported" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/request_specs/place_order_unsupported.json"
        ),
        "amend_order_source_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/request_specs/amend_order_source_boundary.json"
        ),
        "amend_order_request_spec_only" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/request_specs/amend_order_request_spec_only.json"
        ),
        "batch_cancel_orders_source_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/request_specs/batch_cancel_orders_source_boundary.json"
        ),
        "batch_cancel_orders_request_spec_only" => include_str!(
            "../../../../../tests/fixtures/exchanges/bsx/request_specs/batch_cancel_orders_request_spec_only.json"
        ),
        _ => panic!("unknown request spec {name}"),
    };
    serde_json::from_str(text).expect(name)
}

#[test]
fn parser_should_parse_bsx_products_as_perpetual_symbol_rules() {
    let rules = parse_symbol_rules(&exchange_id(), &fixture("products")).expect("rules");

    assert_eq!(rules.len(), 2);
    let rule = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "BTC-PERP")
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
fn parser_should_parse_public_orderbook_snapshot_fixture() {
    let snapshot =
        parse_orderbook_snapshot(&exchange_id(), symbol(), &fixture("orderbook")).expect("book");

    assert_eq!(snapshot.best_bid().expect("bid").price, 65000.1);
    assert_eq!(snapshot.best_bid().expect("bid").quantity, 1.25);
    assert_eq!(snapshot.best_ask().expect("ask").price, 65001.2);
    assert_eq!(snapshot.sequence, Some(424242));
    assert!(snapshot.exchange_timestamp.is_some());
}

#[test]
fn parser_should_reject_error_and_missing_required_fields() {
    parse_symbol_rules(&exchange_id(), &fixture("error_response")).expect_err("error response");
    parse_symbol_rules(&exchange_id(), &fixture("missing_required_fields"))
        .expect_err("missing required field");
}

#[test]
fn capabilities_should_expose_public_market_data_but_no_private_runtime() {
    let adapter = BsxGatewayAdapter::new(BsxGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_query_order);
    assert!(!capabilities.supports_open_orders);
    assert!(!capabilities.supports_recent_fills);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_cancel_orders
            .supports_partial_failure
    );
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities
        .capabilities_v2
        .credential_scopes
        .contains(&rustcta_exchange_api::CredentialScope::Trade));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_symbol_rules"
            && endpoint.path.as_deref() == Some("/products")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_order_book"
            && endpoint.path.as_deref() == Some("/products/{product_id}/book")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "amend_order"
            && endpoint.path.as_deref() == Some("/source/bsx/amend-order-boundary")
            && endpoint
                .credential_scopes
                .contains(&rustcta_exchange_api::CredentialScope::Trade)));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_cancel_orders"
            && endpoint.path.as_deref() == Some("/source/bsx/batch-cancel-orders-boundary")
            && endpoint
                .credential_scopes
                .contains(&rustcta_exchange_api::CredentialScope::Trade)));

    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_enabled"], false);
}

#[test]
fn capabilities_should_expose_guarded_private_readbacks_when_credentials_are_enabled() {
    let mut config = BsxGatewayConfig::default();
    config.enabled_private_rest = true;
    config.api_key = Some("test-api-key".to_string());
    config.api_secret = Some("test-api-secret".to_string());
    config.wallet_address = Some("0xtestwallet".to_string());
    let adapter = BsxGatewayAdapter::new(config).expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);
    assert!(capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "query_order"
            && endpoint.path.as_deref() == Some("/orders/{order_id}")
            && endpoint
                .credential_scopes
                .contains(&rustcta_exchange_api::CredentialScope::ReadOnly)));
}

#[tokio::test]
async fn private_writes_should_return_eip712_boundary() {
    let adapter = BsxGatewayAdapter::new(BsxGatewayConfig::default()).expect("adapter");
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

    let order_list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order-list"),
            symbol: symbol(),
            list_client_order_id: Some("bsx-oco-1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::Limit,
                price: Some("65100".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("bsx-oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("64000".to_string()),
                stop_price: Some("64200".to_string()),
                time_in_force: None,
                client_order_id: Some("bsx-oco-below".to_string()),
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

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol(),
            client_order_id: Some("client-amend-1001".to_string()),
            exchange_order_id: Some("bsx-order-1001".to_string()),
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

    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel-item"),
                symbol: symbol(),
                client_order_id: Some("cancel-client-1".to_string()),
                exchange_order_id: Some("bsx-order-2001".to_string()),
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
}

#[test]
fn request_specs_should_match_public_market_data_and_private_boundaries() {
    let products = ActualHttpRequest::new("GET", BsxRest::products_path());
    request_spec("products")
        .assert_matches(&products)
        .expect("products spec");

    let orderbook = ActualHttpRequest::new("GET", BsxRest::orderbook_path("BTC-PERP"));
    request_spec("orderbook")
        .assert_matches(&orderbook)
        .expect("orderbook spec");

    let timestamp_ns = 1_700_000_000_000_000_000_i128;
    let signature = signing::bsx_ws_auth_signature("test-api-key", "test-api-secret", timestamp_ns);
    let open_orders = ActualHttpRequest::new("GET", BsxRest::open_orders_path()).with_headers([
        ("BSX-KEY".to_string(), "test-api-key".to_string()),
        ("BSX-SIGNATURE".to_string(), signature),
        ("BSX-TIMESTAMP".to_string(), timestamp_ns.to_string()),
    ]);
    request_spec("open_orders_spec_only")
        .assert_matches(&open_orders)
        .expect("open orders spec");

    let read_headers = [
        ("BSX-KEY".to_string(), "test-api-key".to_string()),
        (
            "BSX-SIGNATURE".to_string(),
            "redacted-signature".to_string(),
        ),
        ("BSX-TIMESTAMP".to_string(), timestamp_ns.to_string()),
        ("BSX-ACCOUNT".to_string(), "0xtestwallet".to_string()),
    ];
    let query_order = ActualHttpRequest::new("GET", BsxRest::query_order_path("bsx-order-1001"))
        .with_headers(read_headers.clone());
    request_spec("query_order")
        .assert_matches(&query_order)
        .expect("query order spec");
    let get_open_orders = ActualHttpRequest::new("GET", BsxRest::open_orders_path())
        .with_query([
            ("limit".to_string(), "100".to_string()),
            ("product_id".to_string(), "BTC-PERP".to_string()),
        ])
        .with_headers(read_headers.clone());
    request_spec("get_open_orders")
        .assert_matches(&get_open_orders)
        .expect("get open orders spec");
    let get_recent_fills = ActualHttpRequest::new("GET", BsxRest::recent_fills_path())
        .with_query([
            ("limit".to_string(), "50".to_string()),
            ("product_id".to_string(), "BTC-PERP".to_string()),
        ])
        .with_headers(read_headers);
    request_spec("get_recent_fills")
        .assert_matches(&get_recent_fills)
        .expect("get recent fills spec");

    let cancel_order =
        ActualHttpRequest::new("DELETE", BsxRest::cancel_order_path("test-order-id")).with_headers(
            [
                ("BSX-KEY".to_string(), "test-api-key".to_string()),
                (
                    "BSX-SIGNATURE".to_string(),
                    signing::bsx_ws_auth_signature("test-api-key", "test-api-secret", timestamp_ns),
                ),
                ("BSX-TIMESTAMP".to_string(), timestamp_ns.to_string()),
            ],
        );
    request_spec("cancel_order")
        .assert_matches(&cancel_order)
        .expect("cancel order spec");

    let place_order =
        ActualHttpRequest::new("POST", BsxRest::place_order_path()).with_body(Some(json!({
            "product_id": "BTC-PERP",
            "side": "BUY",
            "order_type": "LIMIT",
            "size": "0.01",
            "price": "65000",
            "signature": "0xredacted_eip712_signature"
        })));
    request_spec("place_order_unsupported")
        .assert_matches(&place_order)
        .expect("place order unsupported spec");

    assert_eq!(
        request_spec("amend_order_source_boundary").operation,
        "amend_order"
    );
    assert_eq!(
        request_spec("batch_cancel_orders_source_boundary").operation,
        "batch_cancel_orders"
    );

    let signed_headers = [
        ("BSX-KEY".to_string(), "test-api-key".to_string()),
        (
            "BSX-SIGNATURE".to_string(),
            "0xredacted_session_or_eip712_signature".to_string(),
        ),
        (
            "BSX-TIMESTAMP".to_string(),
            "1700000000000000000".to_string(),
        ),
        ("BSX-ACCOUNT".to_string(), "0xredacted_account".to_string()),
    ];
    let amend_request = ActualHttpRequest::new("PATCH", "/source/bsx/amend-order-boundary")
        .with_headers(signed_headers.clone())
        .with_body(Some(json!({
            "product_id": "BTC-PERP",
            "order_id": "bsx-order-1001",
            "client_order_id": "client-amend-1001",
            "size": "0.0200",
            "price": "65000.5",
            "signature": "0xredacted_eip712_or_session_signature"
        })));
    request_spec("amend_order_request_spec_only")
        .assert_matches(&amend_request)
        .expect("amend order request spec");

    let batch_cancel_request =
        ActualHttpRequest::new("DELETE", "/source/bsx/batch-cancel-orders-boundary")
            .with_headers(signed_headers)
            .with_body(Some(json!({
                "product_id": "BTC-PERP",
                "orders": [
                    {
                        "order_id": "bsx-order-2001",
                        "client_order_id": "cancel-client-1"
                    },
                    {
                        "order_id": "bsx-order-2002",
                        "client_order_id": "cancel-client-2"
                    }
                ],
                "safe_cancel_shape": "selected_cancel_list_only_no_cancel_all_fallback"
            })));
    request_spec("batch_cancel_orders_request_spec_only")
        .assert_matches(&batch_cancel_request)
        .expect("batch cancel request spec");
}

#[test]
fn private_readback_parsers_should_parse_order_open_orders_and_fills() {
    let order =
        parse_single_order(&exchange_id(), symbol(), &fixture("order_success")).expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("bsx-order-1001"));
    assert_eq!(order.client_order_id.as_deref(), Some("client-order-1001"));
    assert_eq!(order.status, OrderStatus::New);
    assert_eq!(order.filled_quantity, "0.0025");

    let orders = parse_orders(
        &exchange_id(),
        Some(&symbol()),
        &fixture("open_orders_success"),
    )
    .expect("orders");
    assert_eq!(orders.len(), 2);
    assert_eq!(orders[1].status, OrderStatus::PartiallyFilled);

    let fills = parse_recent_fills(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        None,
        &fixture("recent_fills_success"),
    )
    .expect("fills");
    assert_eq!(fills.len(), 2);
    assert_eq!(fills[0].fill_id.as_deref(), Some("bsx-trade-2001"));
    assert_eq!(fills[0].canonical_symbol.to_string(), "BTC/USDC");
    assert_eq!(fills[0].price, 65000.0);
}

#[tokio::test]
async fn private_readbacks_should_fail_closed_without_guard_and_credentials() {
    let adapter = BsxGatewayAdapter::new(BsxGatewayConfig::default()).expect("adapter");

    let query_error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("bsx-order-1001".to_string()),
        })
        .await
        .expect_err("disabled query");
    assert!(matches!(
        query_error,
        ExchangeApiError::Unsupported {
            operation: private::PRIVATE_REST_DISABLED
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
        .expect_err("disabled open");
    assert!(matches!(
        open_error,
        ExchangeApiError::Unsupported {
            operation: private::PRIVATE_REST_DISABLED
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
            limit: Some(50),
            page: None,
        })
        .await
        .expect_err("disabled fills");
    assert!(matches!(
        fills_error,
        ExchangeApiError::Unsupported {
            operation: private::PRIVATE_REST_DISABLED
        }
    ));
}

#[test]
fn private_ack_parsers_should_cover_amend_and_batch_cancel_boundaries() {
    let amend = parse_amend_order_ack(&exchange_id(), symbol(), &fixture("parser/amend_order_ack"))
        .expect("amend ack");
    assert_eq!(
        amend.order.exchange_order_id.as_deref(),
        Some("bsx-order-1001")
    );
    assert_eq!(
        amend.order.client_order_id.as_deref(),
        Some("client-amend-1001")
    );
    assert_eq!(amend.order.quantity, "0.0200");
    assert_eq!(amend.order.price.as_deref(), Some("65000.5"));

    let request = BatchCancelOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-cancel-parser"),
        exchange: exchange_id(),
        cancels: vec![
            CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel-one"),
                symbol: symbol(),
                client_order_id: Some("cancel-client-1".to_string()),
                exchange_order_id: Some("bsx-order-2001".to_string()),
            },
            CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel-two"),
                symbol: symbol(),
                client_order_id: Some("cancel-client-2".to_string()),
                exchange_order_id: Some("bsx-order-2002".to_string()),
            },
            CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel-missing"),
                symbol: symbol(),
                client_order_id: Some("cancel-client-3".to_string()),
                exchange_order_id: Some("bsx-order-2003".to_string()),
            },
        ],
    };
    let batch = parse_batch_cancel_orders_ack(
        &exchange_id(),
        &request,
        &fixture("parser/batch_cancel_orders_ack"),
    )
    .expect("batch cancel ack");

    assert_eq!(batch.cancelled_count, 1);
    assert_eq!(batch.orders.len(), 1);
    let report = batch.report.expect("partial report");
    assert_eq!(report.total_items, 3);
    assert_eq!(report.succeeded_count(), 1);
    assert_eq!(report.failed_count(), 2);
    assert!(report.requires_reconciliation());
}

#[test]
fn websocket_helpers_should_build_fixture_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let signature_fixture = fixture("ws_auth_signature");
    let timestamp_ns = signature_fixture["timestamp_ns"]
        .as_str()
        .expect("timestamp")
        .parse::<i128>()
        .expect("timestamp ns");
    let signature = signing::bsx_ws_auth_signature("test-api-key", "test-api-secret", timestamp_ns);

    assert_eq!(
        bsx_public_subscribe_payload(&subscription),
        fixture("ws_subscribe")
    );
    assert_eq!(
        signing::bsx_ws_auth_message("test-api-key", timestamp_ns),
        signature_fixture["canonical_payload"]
    );
    assert_eq!(signature, signature_fixture["expected_signature"]);
    assert_eq!(
        bsx_private_auth_payload("test-api-key", timestamp_ns, &signature),
        fixture("ws_private_auth")
    );
    assert_eq!(
        signing::bsx_eip712_order_boundary(),
        signing::BSX_EIP712_ORDER_BOUNDARY
    );
    assert_eq!(bsx_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[test]
fn boundary_fixtures_should_remain_sanitized_and_closed_for_trading() {
    let empty = fixture("empty_response");
    let unsupported_spec = request_spec("place_order_unsupported");
    let eip712 = fixture("eip712_boundary");
    let amend_boundary = request_spec("amend_order_source_boundary");
    let amend_request = request_spec("amend_order_request_spec_only");
    let batch_cancel_boundary = request_spec("batch_cancel_orders_source_boundary");
    let batch_cancel_request = request_spec("batch_cancel_orders_request_spec_only");
    let positions_boundary: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bsx/request_specs/get_positions_source_boundary.json"
    ))
    .expect("positions source boundary");

    assert!(empty["products"].as_array().is_some_and(Vec::is_empty));
    assert_eq!(eip712["expected_error"], private::PLACE_ORDER_UNSUPPORTED);
    assert_eq!(positions_boundary["operation"], "get_positions");
    assert_eq!(positions_boundary["support"], "spec_only");
    assert_eq!(positions_boundary["runtime_enabled"], false);
    assert_eq!(amend_boundary.method, "PATCH");
    assert_eq!(amend_boundary.path, "/source/bsx/amend-order-boundary");
    assert_eq!(amend_request.method, "PATCH");
    assert_eq!(amend_request.path, "/source/bsx/amend-order-boundary");
    assert_eq!(batch_cancel_boundary.method, "DELETE");
    assert_eq!(
        batch_cancel_boundary.path,
        "/source/bsx/batch-cancel-orders-boundary"
    );
    assert_eq!(batch_cancel_request.method, "DELETE");
    assert_eq!(
        batch_cancel_request.path,
        "/source/bsx/batch-cancel-orders-boundary"
    );
    assert_eq!(
        unsupported_spec.body_contains.expect("body")["signature"],
        "0xredacted_eip712_signature"
    );
}

#[tokio::test]
async fn public_rest_disabled_should_return_unsupported_for_market_data() {
    let mut config = BsxGatewayConfig::default();
    config.enabled_public_rest = false;
    let adapter = BsxGatewayAdapter::new(config).expect("adapter");

    let error = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol(),
            depth: Some(50),
        })
        .await
        .expect_err("disabled public rest");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bsx.public_rest_disabled"
        }
    ));
}
