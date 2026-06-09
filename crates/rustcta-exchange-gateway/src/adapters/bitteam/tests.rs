use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, OpenOrdersRequest, OrderBookRequest,
    OrderListConditionalLeg, OrderListLegType, OrderListRequest, PlaceOrderRequest,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    QueryOrderRequest, RecentFillsRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::private::{
    build_basic_private_request_spec, request_spec_body_from_order, BITTEAM_BALANCE_PATH,
    BITTEAM_CANCEL_ORDER_PATH, BITTEAM_OPEN_ORDERS_PATH, BITTEAM_PLACE_ORDER_PATH,
    BITTEAM_QUERY_ORDER_PATH, BITTEAM_RECENT_FILLS_PATH,
};
use super::signing::basic_authorization_header;
use super::streams::bitteam_rest_reconciliation_fallback;
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{BitteamGatewayAdapter, BitteamGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "symbol_rules_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/symbol_rules_success.json"
        ),
        "order_book_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/order_book_success.json"
        ),
        "order_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/order_success.json"
        ),
        "open_orders_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/open_orders_success.json"
        ),
        "recent_fills_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/recent_fills_success.json"
        ),
        "request_specs/place_order_limit_buy.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/request_specs/place_order_limit_buy.json"
        ),
        "request_specs/get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/request_specs/get_balances.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/request_specs/cancel_order.json"
        ),
        "request_specs/query_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/request_specs/query_order.json"
        ),
        "request_specs/get_open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/request_specs/get_open_orders.json"
        ),
        "request_specs/get_recent_fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/request_specs/get_recent_fills.json"
        ),
        "signing_vectors/basic_auth.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/signing_vectors/basic_auth.json"
        ),
        "unsupported_boundary.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/unsupported_boundary.json"
        ),
        "ws/public_streams_unsupported.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bitteam/ws/public_streams_unsupported.json"
        ),
        _ => panic!("unknown bitteam fixture {name}"),
    };
    serde_json::from_str(text).expect("bitteam fixture")
}

#[test]
fn bitteam_capabilities_should_expose_scan_only_spot_public_rest() {
    let adapter = BitteamGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_query_order);
    assert!(!capabilities.supports_open_orders);
    assert!(!capabilities.supports_recent_fills);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());

    let boundary = fixture("unsupported_boundary.json");
    assert_eq!(boundary["scan_only"], false);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_rest_default_enabled"], false);
}

#[test]
fn bitteam_capabilities_should_enable_only_guarded_private_readbacks() {
    let adapter = BitteamGatewayAdapter::new(BitteamGatewayConfig {
        api_key: "btm_public_test_key".to_string(),
        api_secret: "btm_private_test_secret".to_string(),
        enabled_private_rest: true,
        ..BitteamGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);
    assert_eq!(capabilities.max_recent_fill_limit, Some(1000));
    assert!(!capabilities.supports_balances);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities
        .capabilities_v2
        .order_history
        .support
        .is_supported());
    assert!(capabilities
        .capabilities_v2
        .fills_history
        .support
        .is_supported());
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| {
            endpoint.operation == "query_order" && endpoint.support.is_supported()
        }));
}

#[test]
fn bitteam_named_registration_should_accept_aliases() {
    AdapterBackedGateway::with_named_adapters("bitteam-test", ["bitteam"]).expect("bitteam");
    AdapterBackedGateway::with_named_adapters("bitteam-alias-test", ["bit.team"])
        .expect("bit.team alias");
}

#[tokio::test]
async fn bitteam_should_load_symbol_rules_from_public_ccxt_pairs() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("symbol_rules_success.json")]).await;
    let adapter = BitteamGatewayAdapter::new(BitteamGatewayConfig {
        rest_base_url: base_url,
        ..BitteamGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    let rule = &response.rules[0];
    assert_eq!(rule.base_asset, "BTC");
    assert_eq!(rule.quote_asset, "USDT");
    assert_eq!(rule.symbol.exchange_symbol.symbol, "BTC_USDT");
    assert_eq!(rule.price_increment.as_deref(), Some("0.01"));
    assert_eq!(rule.quantity_increment.as_deref(), Some("0.000001"));
    assert_eq!(rule.min_notional.as_deref(), Some("5"));
    assert_eq!(seen.lock().unwrap()[0].path, "/trade/api/ccxt/pairs");
}

#[tokio::test]
async fn bitteam_should_load_order_book_snapshot() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("order_book_success.json")]).await;
    let adapter = BitteamGatewayAdapter::new(BitteamGatewayConfig {
        rest_base_url: base_url,
        ..BitteamGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(2),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids.len(), 2);
    assert_eq!(response.order_book.asks.len(), 2);
    assert_eq!(response.order_book.bids[0].price, 64250.5);
    assert_eq!(response.order_book.asks[0].quantity, 0.21);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/trade/api/orderbooks/BTC_USDT");
}

#[tokio::test]
async fn bitteam_private_readbacks_should_fail_closed_without_guard() {
    let adapter = BitteamGatewayAdapter::default_public().expect("adapter");

    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("btm_order_1".to_string()),
        })
        .await
        .expect_err("private rest disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bitteam.query_order"
        }
    ));
}

#[tokio::test]
async fn bitteam_should_query_order_with_basic_auth() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("order_success.json")]).await;
    let adapter = private_adapter(base_url);

    let response = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("btm_order_1".to_string()),
        })
        .await
        .expect("query order");

    let order = response.order.expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("btm_order_1"));
    assert_eq!(order.status, OrderStatus::Open);
    assert_eq!(order.filled_quantity, "0.004");

    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/trade/api/ccxt/order");
    assert_eq!(
        request.query.get("orderId").map(String::as_str),
        Some("btm_order_1")
    );
    assert_eq!(
        request.query.get("pair").map(String::as_str),
        Some("BTC_USDT")
    );
    assert_eq!(
        request.headers.get("authorization").map(String::as_str),
        Some("Basic YnRtX3B1YmxpY190ZXN0X2tleTpidG1fcHJpdmF0ZV90ZXN0X3NlY3JldA==")
    );
    assert!(request.body.is_none());
}

#[tokio::test]
async fn bitteam_should_get_open_orders_with_basic_auth() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("open_orders_success.json")]).await;
    let adapter = private_adapter(base_url);

    let response = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");

    assert_eq!(response.orders.len(), 1);
    assert_eq!(
        response.orders[0].exchange_order_id.as_deref(),
        Some("btm_order_1")
    );

    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/trade/api/ccxt/ordersOfUser");
    assert_eq!(
        request.query.get("pair").map(String::as_str),
        Some("BTC_USDT")
    );
    assert_eq!(
        request.query.get("type").map(String::as_str),
        Some("active")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("1000"));
    assert_eq!(request.query.get("offset").map(String::as_str), Some("0"));
    assert_eq!(
        request.headers.get("authorization").map(String::as_str),
        Some("Basic YnRtX3B1YmxpY190ZXN0X2tleTpidG1fcHJpdmF0ZV90ZXN0X3NlY3JldA==")
    );
    assert!(request.body.is_none());
}

#[tokio::test]
async fn bitteam_should_get_recent_fills_with_basic_auth() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("recent_fills_success.json")]).await;
    let adapter = private_adapter(base_url);

    let response = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("btm_order_1".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(10),
            page: None,
        })
        .await
        .expect("recent fills");

    assert_eq!(response.fills.len(), 1);
    assert_eq!(response.fills[0].fill_id.as_deref(), Some("btm_trade_1"));
    assert_eq!(response.fills[0].order_id.as_deref(), Some("btm_order_1"));
    assert_eq!(response.fills[0].price, 64010.0);
    assert_eq!(response.fills[0].quantity, 0.004);

    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/trade/api/ccxt/tradesOfUser");
    assert_eq!(
        request.query.get("pair").map(String::as_str),
        Some("BTC_USDT")
    );
    assert_eq!(
        request.query.get("orderId").map(String::as_str),
        Some("btm_order_1")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("10"));
    assert_eq!(request.query.get("offset").map(String::as_str), Some("0"));
    assert_eq!(
        request.headers.get("authorization").map(String::as_str),
        Some("Basic YnRtX3B1YmxpY190ZXN0X2tleTpidG1fcHJpdmF0ZV90ZXN0X3NlY3JldA==")
    );
    assert!(request.body.is_none());
}

#[tokio::test]
async fn bitteam_balance_and_writes_should_stay_unsupported_even_with_read_credentials() {
    let adapter = BitteamGatewayAdapter::new(BitteamGatewayConfig {
        api_key: "btm_public_test_key".to_string(),
        api_secret: "btm_private_test_secret".to_string(),
        enabled_private_rest: true,
        ..BitteamGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.config.private_rest_configured());
    assert!(adapter.capabilities().supports_private_rest);
    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("private reads disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bitteam.balances_request_spec_only"
        }
    ));

    let place_error = adapter
        .place_order(place_order_request())
        .await
        .expect_err("writes disabled");
    assert!(matches!(
        place_error,
        ExchangeApiError::Unsupported {
            operation: "bitteam.place_order_request_spec_only"
        }
    ));

    let cancel_error = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("btm_order_1".to_string()),
        })
        .await
        .expect_err("cancel disabled");
    assert!(matches!(
        cancel_error,
        ExchangeApiError::Unsupported {
            operation: "bitteam.cancel_order_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn bitteam_advanced_orders_should_be_explicitly_unsupported() {
    let adapter = BitteamGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol_scope(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            new_client_order_id: None,
            new_quantity: "0.2".to_string(),
        })
        .await
        .expect_err("amend unsupported");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: "bitteam.amend_order_unsupported"
        }
    ));

    let order_list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oco"),
            symbol: symbol_scope(),
            list_client_order_id: Some("oco-1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.1".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some("70000".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("60000".to_string()),
                stop_price: Some("61000".to_string()),
                time_in_force: None,
                client_order_id: Some("oco-below".to_string()),
            },
        })
        .await
        .expect_err("order-list unsupported");
    assert!(matches!(
        order_list_error,
        ExchangeApiError::Unsupported {
            operation: "bitteam.order_list_unsupported"
        }
    ));

    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch"),
        exchange: exchange_id(),
        orders: vec![place_order_request()],
    };

    let error = adapter
        .batch_place_orders(request)
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bitteam.batch_place_orders_unverified"
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
                symbol: symbol_scope(),
                client_order_id: None,
                exchange_order_id: Some("order-1".to_string()),
            }],
        })
        .await
        .expect_err("batch cancel unsupported");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: "bitteam.batch_cancel_orders_unverified"
        }
    ));
}

#[tokio::test]
async fn bitteam_streams_should_keep_rest_reconciliation_boundary() {
    let adapter = BitteamGatewayAdapter::default_public().expect("adapter");
    let public_error = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .await
        .expect_err("public ws unsupported");
    assert!(matches!(
        public_error,
        ExchangeApiError::Unsupported {
            operation: "bitteam.public_streams_unverified"
        }
    ));

    let private_error = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect_err("private ws unsupported");
    assert!(matches!(
        private_error,
        ExchangeApiError::Unsupported {
            operation: "bitteam.private_streams_unverified"
        }
    ));
    let ws_boundary = fixture("ws/public_streams_unsupported.json");
    assert_eq!(ws_boundary["public_streams"], "unsupported_unverified");
    assert_eq!(ws_boundary["private_streams"], "unsupported_unverified");
    assert_eq!(
        ws_boundary["rest_reconciliation_fallback"][0],
        "/trade/api/ccxt/ordersOfUser"
    );
    assert!(bitteam_rest_reconciliation_fallback().contains("ordersOfUser"));
}

#[test]
fn bitteam_basic_auth_signing_vector_should_match_fixture() {
    let vector = fixture("signing_vectors/basic_auth.json");
    let header = basic_authorization_header(
        vector["api_key"].as_str().unwrap(),
        vector["api_secret"].as_str().unwrap(),
    )
    .expect("basic auth");
    assert_eq!(header, vector["expected_authorization"].as_str().unwrap());
}

#[test]
fn bitteam_private_request_specs_should_be_sanitized_and_request_spec_only() {
    let place = fixture("request_specs/place_order_limit_buy.json");
    assert_eq!(place["operation"], "bitteam.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], BITTEAM_PLACE_ORDER_PATH);
    assert_eq!(place["trade_enabled"], false);
    assert_eq!(place["body"]["pairId"], 12501);
    assert_eq!(place["headers"]["authorization"], "<redacted:basic-auth>");

    let balances = fixture("request_specs/get_balances.json");
    assert_eq!(balances["operation"], "bitteam.get_balances");
    assert_eq!(balances["method"], "GET");
    assert_eq!(balances["path"], BITTEAM_BALANCE_PATH);
    assert_eq!(balances["request_spec_only"], true);

    let cancel = fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["operation"], "bitteam.cancel_order");
    assert_eq!(cancel["method"], "POST");
    assert_eq!(cancel["path"], BITTEAM_CANCEL_ORDER_PATH);
    assert_eq!(cancel["trade_enabled"], false);
    assert_eq!(cancel["headers"]["authorization"], "<redacted:basic-auth>");

    let open_orders = fixture("request_specs/get_open_orders.json");
    assert_eq!(open_orders["operation"], "bitteam.get_open_orders");
    assert_eq!(open_orders["method"], "GET");
    assert_eq!(open_orders["path"], BITTEAM_OPEN_ORDERS_PATH);
    assert_eq!(open_orders["request_spec_only"], false);
    assert_eq!(open_orders["runtime_enabled"], true);
    assert_eq!(open_orders["credential_scope"], "read_only");

    let recent_fills = fixture("request_specs/get_recent_fills.json");
    assert_eq!(recent_fills["operation"], "bitteam.get_recent_fills");
    assert_eq!(recent_fills["method"], "GET");
    assert_eq!(recent_fills["path"], BITTEAM_RECENT_FILLS_PATH);
    assert_eq!(recent_fills["request_spec_only"], false);
    assert_eq!(recent_fills["runtime_enabled"], true);
    assert_eq!(recent_fills["credential_scope"], "read_only");

    let query_order = fixture("request_specs/query_order.json");
    assert_eq!(query_order["operation"], "bitteam.query_order");
    assert_eq!(query_order["method"], "GET");
    assert_eq!(query_order["path"], BITTEAM_QUERY_ORDER_PATH);
    assert_eq!(query_order["request_spec_only"], false);
    assert_eq!(query_order["runtime_enabled"], true);
    assert_eq!(query_order["query"]["orderId"], "btm_order_1");

    let spec = build_basic_private_request_spec(
        "GET",
        BITTEAM_BALANCE_PATH,
        None,
        "btm_public_test_key",
        "btm_private_test_secret",
    )
    .expect("request spec");
    assert_eq!(
        spec.authorization,
        "Basic YnRtX3B1YmxpY190ZXN0X2tleTpidG1fcHJpdmF0ZV90ZXN0X3NlY3JldA=="
    );
}

#[test]
fn bitteam_private_order_request_body_should_match_ccxt_shape() {
    let request = place_order_request();
    let body = request_spec_body_from_order(&request, 12501);
    assert_eq!(
        body,
        json!({
            "pairId": 12501,
            "side": "buy",
            "type": "limit",
            "amount": "0.01",
            "price": "64000"
        })
    );
}

fn place_order_request() -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol_scope(),
        client_order_id: None,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("64000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

fn private_adapter(rest_base_url: String) -> BitteamGatewayAdapter {
    BitteamGatewayAdapter::new(BitteamGatewayConfig {
        rest_base_url,
        api_key: "btm_public_test_key".to_string(),
        api_secret: "btm_private_test_secret".to_string(),
        enabled_private_rest: true,
        ..BitteamGatewayConfig::default()
    })
    .expect("private bitteam adapter")
}
