use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustcta_exchange_api::{
    AmendOrderRequest, BatchAtomicity, BatchCancelOrdersRequest, BatchExecutionMode,
    BatchPlaceOrdersRequest, CancelOrderRequest, CapabilitySupport, ExchangeApiError,
    ExchangeClient, OpenOrdersRequest, OrderBookRequest, OrderListConditionalLeg, OrderListLegType,
    OrderListRequest, PageRequest, PlaceOrderRequest, PositionsRequest, PublicStreamKind,
    PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest, ReconcileTrigger,
    RequestContext, SymbolScope, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus,
    OrderType, TenantId,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::parser::{
    parse_amend_order_ack, parse_batch_place_orders_ack, parse_orderbook_snapshot, parse_orders,
    parse_recent_fills, parse_single_order, parse_symbol_rules,
};
use super::streams::{
    modetrade_ping_payload, modetrade_private_auth_payload, modetrade_public_subscribe_payload,
    modetrade_public_unsubscribe_payload, modetrade_reconnect_policy_ms,
    parse_modetrade_public_orderbook_update,
};
use super::transport::ModetradeRest;
use super::{private, signing, ModetradeGatewayAdapter, ModetradeGatewayConfig};
use crate::request_spec::{ActualHttpRequest, RequestSpec};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("modetrade").expect("exchange")
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
            include_str!("../../../../../tests/fixtures/exchanges/modetrade/public_info.json")
        }
        "orderbook_snapshot" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/orderbook_snapshot.json"
        ),
        "unsupported_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/unsupported_boundary.json"
        ),
        "empty_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/modetrade/empty_response.json")
        }
        "error_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/modetrade/error_response.json")
        }
        "order_success" => {
            include_str!("../../../../../tests/fixtures/exchanges/modetrade/order_success.json")
        }
        "open_orders_success" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/open_orders_success.json"
        ),
        "recent_fills_success" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/recent_fills_success.json"
        ),
        "missing_required_fields" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/missing_required_fields.json"
        ),
        "signing_boundary" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/signing_vectors/orderly_ed25519_boundary.json"
        ),
        "ws_subscribe" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/ws/public_orderbook_subscribe.json"
        ),
        "ws_unsubscribe" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/ws/public_orderbook_unsubscribe.json"
        ),
        "ws_heartbeat_ping" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/ws/heartbeat_ping.json"
        ),
        "ws_orderbook_update" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/ws/public_orderbook_update.json"
        ),
        "ws_private_auth" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/ws/private_auth_payload.json"
        ),
        "parser_amend_order_ack" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/parser/amend_order_ack.json"
        ),
        "parser_batch_place_orders_ack" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/parser/batch_place_orders_ack.json"
        ),
        "parser_batch_place_orders_partial_ack" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/parser/batch_place_orders_partial_ack.json"
        ),
        "parser_batch_place_orders_missing_item_ack" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/parser/batch_place_orders_missing_item_ack.json"
        ),
        _ => panic!("unknown fixture {name}"),
    };
    serde_json::from_str(text).expect(name)
}

fn request_spec(name: &str) -> RequestSpec {
    let text = match name {
        "public_info" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/request_specs/public_info.json"
        ),
        "orderbook_signed_read" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/request_specs/orderbook_signed_read.json"
        ),
        "get_positions_signed_read" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/request_specs/get_positions_signed_read.json"
        ),
        "query_order_signed_read" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/request_specs/query_order_signed_read.json"
        ),
        "get_open_orders_signed_read" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/request_specs/get_open_orders_signed_read.json"
        ),
        "get_recent_fills_signed_read" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/request_specs/get_recent_fills_signed_read.json"
        ),
        "amend_order_signed_write" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/request_specs/amend_order_signed_write.json"
        ),
        "batch_place_orders_signed_write" => include_str!(
            "../../../../../tests/fixtures/exchanges/modetrade/request_specs/batch_place_orders_signed_write.json"
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
fn parser_should_parse_private_readback_fixtures() {
    let order =
        parse_single_order(&exchange_id(), &symbol(), &fixture("order_success")).expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("mt-order-1"));
    assert_eq!(order.client_order_id.as_deref(), Some("mt-client-1"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.filled_quantity, "0.0040");

    let open = parse_orders(
        &exchange_id(),
        Some(&symbol()),
        &fixture("open_orders_success"),
    )
    .expect("open orders");
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].exchange_order_id.as_deref(), Some("mt-open-1"));
    assert_eq!(open[0].status, OrderStatus::New);

    let request_context = context("fills");
    let fills = parse_recent_fills(
        &exchange_id(),
        request_context.tenant_id.expect("tenant"),
        request_context.account_id.expect("account"),
        Some(&symbol()),
        &fixture("recent_fills_success"),
    )
    .expect("fills");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].order_id.as_deref(), Some("mt-order-1"));
    assert_eq!(fills[0].fill_id.as_deref(), Some("mt-trade-1"));
    assert_eq!(fills[0].price, 63950.0);
    assert_eq!(fills[0].quantity, 0.004);
}

#[test]
fn capabilities_should_expose_public_info_but_no_trade_or_orderbook_runtime() {
    let adapter = ModetradeGatewayAdapter::new(ModetradeGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(!capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_query_order);
    assert!(!capabilities.supports_open_orders);
    assert!(!capabilities.supports_recent_fills);
    assert!(!capabilities.supports_batch_place_order);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(capabilities.capabilities_v2.private_rest.is_supported());
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
        .any(|endpoint| {
            endpoint.operation == "query_order"
                && endpoint.method.as_deref() == Some("GET")
                && endpoint.path.as_deref() == Some("/v1/order/{order_id}")
                && endpoint.support.is_supported()
        }));
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
    assert!(matches!(
        &capabilities.capabilities_v2.batch_place_orders.support,
        CapabilitySupport::Unsupported { reason }
            if reason == private::BATCH_PLACE_UNSUPPORTED
    ));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| {
            endpoint.operation == "amend_order"
                && endpoint.method.as_deref() == Some("PUT")
                && endpoint.path.as_deref() == Some("/v1/order")
                && matches!(&endpoint.support, CapabilitySupport::Unsupported { .. })
        }));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| {
            endpoint.operation == "batch_place_orders"
                && endpoint.method.as_deref() == Some("POST")
                && endpoint.path.as_deref() == Some("/v1/batch-order")
                && matches!(&endpoint.support, CapabilitySupport::Unsupported { .. })
        }));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| {
            endpoint.operation == "place_order_list"
                && endpoint.path.is_none()
                && matches!(&endpoint.support, CapabilitySupport::Unsupported { .. })
        }));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| {
            endpoint.operation == "batch_cancel_orders"
                && endpoint.path.is_none()
                && matches!(&endpoint.support, CapabilitySupport::Unsupported { .. })
        }));

    let boundary = fixture("unsupported_boundary");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_enabled"], false);
    assert!(boundary["runtime_disabled_boundaries"]["amend_order"]
        .as_array()
        .expect("amend blockers")
        .iter()
        .any(|item| item == "audited Ed25519 signer runtime"));
    assert!(
        boundary["runtime_disabled_boundaries"]["batch_place_orders"]
            .as_array()
            .expect("batch blockers")
            .iter()
            .any(|item| item == "missing-item reconciliation")
    );
    assert!(boundary["runtime_disabled_boundaries"]["place_order_list"]
        .as_array()
        .expect("order-list blockers")
        .iter()
        .any(|item| item
            == "no verified lossless shared order-list mapping for the ModeTrade/Orderly profile"));
}

#[tokio::test]
async fn orderbook_and_private_writes_should_return_orderly_boundary() {
    let adapter = ModetradeGatewayAdapter::new(ModetradeGatewayConfig::default()).expect("adapter");
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

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("123456789".to_string()),
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

    let cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel"),
                symbol: symbol(),
                client_order_id: None,
                exchange_order_id: Some("123456789".to_string()),
            }],
        })
        .await
        .expect_err("unsupported batch cancel");
    assert!(matches!(
        cancel_error,
        ExchangeApiError::Unsupported {
            operation: private::BATCH_CANCEL_UNSUPPORTED
        }
    ));

    let order_list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order-list"),
            symbol: symbol(),
            list_client_order_id: Some("modetrade-order-list-boundary".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::TakeProfitLimit,
                price: Some("70000".to_string()),
                stop_price: Some("70000".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("modetrade-order-list-tp".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("60000".to_string()),
                stop_price: Some("60000".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("modetrade-order-list-sl".to_string()),
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

#[tokio::test]
async fn private_readbacks_should_fail_closed_without_guard() {
    let adapter = ModetradeGatewayAdapter::new(ModetradeGatewayConfig::default()).expect("adapter");

    let query_error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("mt-order-1".to_string()),
        })
        .await
        .expect_err("private REST disabled");
    assert!(matches!(
        query_error,
        ExchangeApiError::Unsupported {
            operation: "modetrade.query_order"
        }
    ));

    let open_error = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-disabled"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol()),
            page: None,
        })
        .await
        .expect_err("private REST disabled");
    assert!(matches!(
        open_error,
        ExchangeApiError::Unsupported {
            operation: "modetrade.get_open_orders"
        }
    ));

    let fills_error = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills-disabled"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(25),
            page: None,
        })
        .await
        .expect_err("private REST disabled");
    assert!(matches!(
        fills_error,
        ExchangeApiError::Unsupported {
            operation: "modetrade.get_recent_fills"
        }
    ));
}

#[tokio::test]
async fn private_readbacks_should_use_orderly_signed_get_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("order_success"),
        fixture("open_orders_success"),
        fixture("recent_fills_success"),
    ])
    .await;
    let adapter = ModetradeGatewayAdapter::new(ModetradeGatewayConfig {
        rest_base_url: base_url,
        enabled_private_rest: true,
        orderly_account_id: Some("acct_test_account_id".to_string()),
        orderly_key: Some("ed25519:test-public-key".to_string()),
        orderly_secret: Some(
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        ),
        ..ModetradeGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);

    let queried = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol(),
            client_order_id: None,
            exchange_order_id: Some("mt-order-1".to_string()),
        })
        .await
        .expect("query");
    assert_eq!(
        queried
            .order
            .as_ref()
            .and_then(|order| order.exchange_order_id.as_deref()),
        Some("mt-order-1")
    );

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol()),
            page: Some(PageRequest::first_page(50)),
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);

    let fills = adapter
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
            limit: Some(25),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/v1/order/mt-order-1");
    assert_eq!(
        requests[0]
            .headers
            .get("orderly-account-id")
            .map(String::as_str),
        Some("acct_test_account_id")
    );
    assert_eq!(
        requests[0].headers.get("orderly-key").map(String::as_str),
        Some("ed25519:test-public-key")
    );
    assert!(requests[0].headers.contains_key("orderly-signature"));
    assert!(requests[0].headers.contains_key("orderly-timestamp"));

    assert_eq!(requests[1].method, "GET");
    assert_eq!(requests[1].path, private::OPEN_ORDERS_PATH);
    assert_eq!(
        requests[1].query.get("symbol").map(String::as_str),
        Some("PERP_BTC_USDC")
    );
    assert_eq!(
        requests[1].query.get("status").map(String::as_str),
        Some("INCOMPLETE")
    );
    assert_eq!(
        requests[1].query.get("size").map(String::as_str),
        Some("50")
    );

    assert_eq!(requests[2].method, "GET");
    assert_eq!(requests[2].path, private::RECENT_FILLS_PATH);
    assert_eq!(
        requests[2].query.get("symbol").map(String::as_str),
        Some("PERP_BTC_USDC")
    );
    assert_eq!(
        requests[2].query.get("size").map(String::as_str),
        Some("25")
    );
}

#[test]
fn request_specs_should_match_public_and_signed_read_boundaries() {
    let public_info = ActualHttpRequest::new("GET", ModetradeRest::public_info_path());
    request_spec("public_info")
        .assert_matches(&public_info)
        .expect("public info spec");

    let signed_orderbook =
        ActualHttpRequest::new("GET", ModetradeRest::signed_orderbook_path("PERP_BTC_USDC"))
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

    let signed_positions = ActualHttpRequest::new("GET", "/v1/positions").with_headers([
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
    ]);
    request_spec("get_positions_signed_read")
        .assert_matches(&signed_positions)
        .expect("positions signed-read spec");
    assert_eq!(
        request_spec("get_positions_signed_read").operation,
        "get_positions"
    );

    let signed_read_headers = [
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
    request_spec("query_order_signed_read")
        .assert_matches(
            &ActualHttpRequest::new("GET", private::query_order_path("mt-order-1"))
                .with_headers(signed_read_headers.clone()),
        )
        .expect("query order signed-read spec");
    request_spec("get_open_orders_signed_read")
        .assert_matches(
            &ActualHttpRequest::new("GET", private::OPEN_ORDERS_PATH)
                .with_query([
                    ("symbol".to_string(), "PERP_BTC_USDC".to_string()),
                    ("status".to_string(), "INCOMPLETE".to_string()),
                    ("size".to_string(), "50".to_string()),
                ])
                .with_headers(signed_read_headers.clone()),
        )
        .expect("open orders signed-read spec");
    request_spec("get_recent_fills_signed_read")
        .assert_matches(
            &ActualHttpRequest::new("GET", private::RECENT_FILLS_PATH)
                .with_query([
                    ("symbol".to_string(), "PERP_BTC_USDC".to_string()),
                    ("size".to_string(), "25".to_string()),
                ])
                .with_headers(signed_read_headers),
        )
        .expect("recent fills signed-read spec");

    let signing_fixture = fixture("signing_boundary");
    assert_eq!(
        signing_fixture["canonical_payload"],
        signing::modetrade_orderly_canonical_payload(
            1_700_000_000_000,
            "GET",
            "/v1/orderbook/PERP_BTC_USDC",
            ""
        )
    );
    assert_eq!(
        signing::modetrade_orderly_signing_boundary(),
        signing::MODETRADE_ORDERLY_SIGNING_BOUNDARY
    );
}

#[test]
fn request_specs_should_match_signed_write_boundaries() {
    let headers = [
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

    let amend = ActualHttpRequest::new("PUT", "/v1/order")
        .with_headers(headers.clone())
        .with_body(Some(json!({
            "order_id": "<redacted-order-id>",
            "symbol": "PERP_BTC_USDC",
            "order_price": "65000",
            "order_quantity": "0.01",
            "client_order_id": "modetrade-amend-boundary-1"
        })));
    request_spec("amend_order_signed_write")
        .assert_matches(&amend)
        .expect("amend write spec");
    assert_eq!(
        request_spec("amend_order_signed_write").operation,
        "amend_order"
    );

    let batch = ActualHttpRequest::new("POST", "/v1/batch-order")
        .with_headers(headers)
        .with_body(Some(json!({
            "orders": [
                {
                    "symbol": "PERP_BTC_USDC",
                    "order_type": "LIMIT",
                    "side": "BUY",
                    "order_price": "64000",
                    "order_quantity": "0.01",
                    "client_order_id": "modetrade-batch-1"
                }
            ]
        })));
    request_spec("batch_place_orders_signed_write")
        .assert_matches(&batch)
        .expect("batch place write spec");
    assert_eq!(
        request_spec("batch_place_orders_signed_write").operation,
        "batch_place_orders"
    );
}

#[test]
fn parser_should_parse_advanced_order_ack_fixtures() {
    let amend = parse_amend_order_ack(
        &exchange_id(),
        &symbol(),
        &fixture("parser_amend_order_ack"),
    )
    .expect("amend ack");
    assert_eq!(amend.order.exchange_order_id.as_deref(), Some("123456789"));
    assert_eq!(
        amend.order.client_order_id.as_deref(),
        Some("modetrade-amend-boundary-1")
    );
    assert_eq!(amend.order.quantity, "0.01");

    let batch_request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place"),
        exchange: exchange_id(),
        orders: vec![limit_order("modetrade-batch-1", "64000")],
    };
    let batch = parse_batch_place_orders_ack(
        &exchange_id(),
        &batch_request,
        &fixture("parser_batch_place_orders_ack"),
    )
    .expect("batch ack");
    assert_eq!(batch.orders.len(), 1);
    assert_eq!(
        batch.orders[0].client_order_id.as_deref(),
        Some("modetrade-batch-1")
    );
    assert_eq!(batch.orders[0].price.as_deref(), Some("64000"));
    let report = batch.report.expect("batch report");
    assert_eq!(report.total_items, 1);
    assert_eq!(report.succeeded_count(), 1);
    assert_eq!(report.failed_count(), 0);
}

#[test]
fn parser_should_report_batch_place_partial_failure_and_missing_item() {
    let batch_request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-partial"),
        exchange: exchange_id(),
        orders: vec![
            limit_order("modetrade-batch-1", "64000"),
            limit_order("modetrade-batch-2", "3000"),
        ],
    };

    let partial = parse_batch_place_orders_ack(
        &exchange_id(),
        &batch_request,
        &fixture("parser_batch_place_orders_partial_ack"),
    )
    .expect("partial batch ack");
    assert_eq!(partial.orders.len(), 1);
    let partial_report = partial.report.expect("partial report");
    assert_eq!(partial_report.total_items, 2);
    assert_eq!(partial_report.succeeded_count(), 1);
    assert_eq!(partial_report.failed_count(), 1);
    assert!(partial_report.requires_reconciliation());
    assert_eq!(
        partial_report.results[1]
            .reconcile_plan
            .as_ref()
            .expect("partial reconcile plan")
            .trigger,
        ReconcileTrigger::BatchPlacePartialFailure
    );

    let missing = parse_batch_place_orders_ack(
        &exchange_id(),
        &batch_request,
        &fixture("parser_batch_place_orders_missing_item_ack"),
    )
    .expect("missing item batch ack");
    assert_eq!(missing.orders.len(), 1);
    let missing_report = missing.report.expect("missing item report");
    assert_eq!(missing_report.total_items, 2);
    assert_eq!(missing_report.succeeded_count(), 1);
    assert_eq!(missing_report.failed_count(), 1);
    assert!(missing_report.requires_reconciliation());
    let missing_result = &missing_report.results[1];
    assert_eq!(
        missing_result.client_order_id.as_deref(),
        Some("modetrade-batch-2")
    );
    assert_eq!(
        missing_result
            .reconcile_plan
            .as_ref()
            .expect("missing item reconcile plan")
            .trigger,
        ReconcileTrigger::BatchResponseMissingItem
    );
    assert_eq!(partial.orders[0].status, OrderStatus::New);
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
        modetrade_public_subscribe_payload(&subscription),
        fixture("ws_subscribe")
    );
    assert_eq!(
        modetrade_public_unsubscribe_payload(&subscription),
        fixture("ws_unsubscribe")
    );
    assert_eq!(modetrade_ping_payload(), fixture("ws_heartbeat_ping"));
    assert_eq!(
        modetrade_private_auth_payload(
            "ed25519:test-public-key",
            1_700_000_000_000,
            "test-signature-placeholder"
        ),
        fixture("ws_private_auth")
    );
    let parsed_update = parse_modetrade_public_orderbook_update(
        &exchange_id(),
        &subscription,
        &fixture("ws_orderbook_update"),
    )
    .expect("ws update");
    assert_eq!(parsed_update.sequence, Some(424243));
    assert_eq!(parsed_update.best_bid().expect("bid").price, 65000.0);
    assert_eq!(parsed_update.best_ask().expect("ask").quantity, 0.9);
    assert_eq!(modetrade_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}

#[test]
fn boundary_fixtures_should_remain_sanitized_and_closed_for_trading() {
    let empty = fixture("empty_response");
    let error = fixture("error_response");
    let missing = fixture("missing_required_fields");
    let unsupported_spec = serde_json::from_str::<Value>(include_str!(
        "../../../../../tests/fixtures/exchanges/modetrade/request_specs/place_order_unsupported.json"
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

#[derive(Debug, Clone)]
struct SeenRequest {
    method: String,
    path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
}

async fn spawn_rest_server(responses: Vec<Value>) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_requests = Arc::clone(&seen);
    let responses = Arc::new(Mutex::new(responses.into_iter()));

    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut buffer = vec![0_u8; 8192];
            let bytes_read = stream.read(&mut buffer).await.unwrap();
            let request_text = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
            seen_requests
                .lock()
                .unwrap()
                .push(parse_seen_request(&request_text));
            let body = responses
                .lock()
                .unwrap()
                .next()
                .unwrap_or_else(|| json!({}));
            let body_text = body.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body_text.len(),
                body_text
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    (format!("http://{address}"), seen)
}

fn parse_seen_request(request_text: &str) -> SeenRequest {
    let (head, _) = request_text
        .split_once("\r\n\r\n")
        .unwrap_or((request_text, ""));
    let request_line = request_text.lines().next().unwrap_or_default();
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default().to_string();
    let target = request_parts.next().unwrap_or_default();
    let (path, query_text) = target.split_once('?').unwrap_or((target, ""));
    let query = query_text
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (key.to_string(), value.to_string())
        })
        .collect();
    let headers = head
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    SeenRequest {
        method,
        path: path.to_string(),
        query,
        headers,
    }
}
