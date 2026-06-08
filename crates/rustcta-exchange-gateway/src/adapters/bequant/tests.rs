use rustcta_exchange_api::{
    BalancesRequest, CancelAllOrdersRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient,
    FeesRequest, OpenOrdersRequest, OrderBookRequest, PlaceOrderRequest, PrivateStreamKind,
    PublicStreamKind, PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeErrorClass, MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::{
    heartbeat_ping_payload, heartbeat_pong_method, parse_public_orderbook_message,
    private_auth_spec, private_subscription_spec, public_subscription_spec,
};
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{BequantGatewayAdapter, BequantGatewayConfig};

fn bequant_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "symbols_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bequant/symbols_success.json")
        }
        "orderbook_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bequant/orderbook_success.json")
        }
        "balances_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bequant/balances_success.json")
        }
        "open_orders_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bequant/open_orders_success.json")
        }
        "trades_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bequant/trades_success.json")
        }
        "error_response.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bequant/error_response.json")
        }
        "ws/orderbook_snapshot.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bequant/ws/orderbook_snapshot.json"
        ),
        "request_specs/place_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bequant/request_specs/place_order_limit.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bequant/request_specs/cancel_order.json"
        ),
        "signing_vectors/basic_auth.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/bequant/signing_vectors/basic_auth.json"
        ),
        _ => panic!("unknown bequant fixture {name}"),
    };
    serde_json::from_str(text).expect("bequant fixture")
}

#[tokio::test]
async fn bequant_adapter_should_load_symbol_rules_and_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![
        bequant_fixture("symbols_success.json"),
        bequant_fixture("orderbook_success.json"),
    ])
    .await;
    let adapter = BequantGatewayAdapter::new(BequantGatewayConfig {
        rest_base_url: base_url,
        ..BequantGatewayConfig::default()
    })
    .expect("adapter");

    let rules = adapter
        .get_symbol_rules(rustcta_exchange_api::SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("rules");
    assert_eq!(rules.rules.len(), 1);
    assert_eq!(rules.rules[0].base_asset, "BTC");
    assert_eq!(
        rules.rules[0].quantity_increment.as_deref(),
        Some("0.00001")
    );

    let book = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(2),
        })
        .await
        .expect("book");
    assert_eq!(book.order_book.bids[0].price, 65000.0);
    assert_eq!(book.order_book.asks[0].quantity, 0.75);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/public/symbol");
    assert_eq!(requests[1].path, "/public/orderbook/BTCUSDT");
    assert_eq!(
        requests[1].query.get("depth").map(String::as_str),
        Some("2")
    );
}

#[test]
fn bequant_parser_fixtures_should_cover_public_and_private_shapes() {
    let rules = parse_symbol_rules(
        &exchange_id(),
        &[symbol_scope()],
        &bequant_fixture("symbols_success.json"),
    )
    .expect("rules");
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTCUSDT");

    let book = parse_orderbook_snapshot(
        &exchange_id(),
        symbol_scope(),
        &bequant_fixture("orderbook_success.json"),
    )
    .expect("book");
    assert_eq!(book.sequence, Some(12345));

    let balances = super::private_parser::parse_account_balances(
        &exchange_id(),
        context("balances").tenant_id.unwrap(),
        context("balances").account_id.unwrap(),
        &[],
        &bequant_fixture("balances_success.json"),
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].asset, "BTC");

    let fills = super::private_parser::parse_recent_fills(
        &exchange_id(),
        context("fills").tenant_id.unwrap(),
        context("fills").account_id.unwrap(),
        &symbol_scope(),
        &bequant_fixture("trades_success.json"),
    )
    .expect("fills");
    assert_eq!(fills[0].fill_id.as_deref(), Some("7001"));
}

#[test]
fn bequant_basic_auth_and_request_specs_should_match_fixtures() {
    let vector = bequant_fixture("signing_vectors/basic_auth.json");
    assert_eq!(
        super::signing::basic_auth_value(
            vector["api_key"].as_str().unwrap(),
            vector["api_secret"].as_str().unwrap(),
        ),
        vector["expected_authorization"].as_str().unwrap()
    );

    let place = bequant_fixture("request_specs/place_order_limit.json");
    assert_eq!(place["operation"], "bequant.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], "/spot/order");
    assert_eq!(place["body"]["client_order_id"], "CLIENTLIMIT1");

    let cancel = bequant_fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["method"], "DELETE");
    assert_eq!(cancel["path"], "/spot/order/CLIENTLIMIT1");
}

#[tokio::test]
async fn bequant_private_should_be_unsupported_without_credentials() {
    let adapter = BequantGatewayAdapter::default_public().expect("adapter");
    assert!(adapter.capabilities().supports_public_rest);
    assert!(!adapter.capabilities().supports_private_rest);

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn bequant_private_rest_should_use_basic_auth_and_route_order_lifecycle() {
    let (base_url, seen) = spawn_rest_server(vec![
        bequant_fixture("balances_success.json"),
        json!({
            "id": "1001",
            "client_order_id": "CLIENTLIMIT1",
            "symbol": "BTCUSDT",
            "side": "buy",
            "type": "limit",
            "status": "new",
            "quantity": "0.02",
            "price": "65000",
            "cum_quantity": "0",
            "created_at": "2026-06-08T00:00:00Z"
        }),
        json!({
            "id": "1001",
            "client_order_id": "CLIENTLIMIT1",
            "symbol": "BTCUSDT",
            "side": "buy",
            "type": "limit",
            "status": "canceled",
            "quantity": "0.02",
            "price": "65000",
            "cum_quantity": "0"
        }),
        json!({
            "id": "1001",
            "client_order_id": "CLIENTLIMIT1",
            "symbol": "BTCUSDT",
            "side": "buy",
            "type": "limit",
            "status": "filled",
            "quantity": "0.02",
            "price": "65000",
            "cum_quantity": "0.02"
        }),
        bequant_fixture("open_orders_success.json"),
        json!({"make_rate": "0.0009", "take_rate": "0.0010", "symbol": "BTCUSDT"}),
        bequant_fixture("trades_success.json"),
        bequant_fixture("open_orders_success.json"),
    ])
    .await;
    let adapter = BequantGatewayAdapter::new(BequantGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("bequant-key".to_string()),
        api_secret: Some("bequant-secret".to_string()),
        enabled_private_rest: true,
        ..BequantGatewayConfig::default()
    })
    .expect("adapter");

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances[0].balances.len(), 2);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol_scope(),
            client_order_id: Some("CLIENTLIMIT1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.02".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: true,
        })
        .await
        .expect("place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("1001"));

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol_scope(),
            client_order_id: Some("CLIENTLIMIT1".to_string()),
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("cancel");
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);

    let query = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: Some("CLIENTLIMIT1".to_string()),
            exchange_order_id: None,
        })
        .await
        .expect("query");
    assert_eq!(query.order.unwrap().status, OrderStatus::Filled);

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open");
    assert_eq!(open.orders.len(), 1);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.0009");

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(50),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("7001"));

    let cancel_all = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
        })
        .await
        .expect("cancel-all");
    assert_eq!(cancel_all.cancelled_count, 1);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/spot/balance");
    assert_eq!(
        requests[0].header("authorization"),
        Some("Basic YmVxdWFudC1rZXk6YmVxdWFudC1zZWNyZXQ=")
    );
    assert_eq!(requests[1].method, "POST");
    assert_eq!(requests[1].path, "/spot/order");
    assert_eq!(
        requests[1]
            .body_form
            .get("client_order_id")
            .map(String::as_str),
        Some("CLIENTLIMIT1")
    );
    assert_eq!(requests[2].method, "DELETE");
    assert_eq!(requests[2].path, "/spot/order/CLIENTLIMIT1");
    assert_eq!(requests[3].path, "/spot/order/CLIENTLIMIT1");
    assert_eq!(requests[4].path, "/spot/order");
    assert_eq!(requests[5].path, "/spot/fee/BTCUSDT");
    assert_eq!(requests[6].path, "/spot/history/trade");
    assert_eq!(requests[7].method, "DELETE");
    assert_eq!(requests[7].path, "/spot/order");
}

#[tokio::test]
async fn bequant_adapter_should_classify_error_response_fixture() {
    let (base_url, _seen) = spawn_rest_server(vec![bequant_fixture("error_response.json")]).await;
    let adapter = BequantGatewayAdapter::new(BequantGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BequantGatewayConfig::default()
    })
    .expect("adapter");

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances-error"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("error fixture");

    match error {
        ExchangeApiError::Exchange(error) => {
            assert_eq!(error.class, ExchangeErrorClass::RateLimited);
            assert_eq!(error.code.as_deref(), Some("429"));
        }
        other => panic!("expected classified exchange error, got {other:?}"),
    }
}

#[test]
fn bequant_ws_specs_and_parser_should_cover_public_private_and_heartbeat() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let spec = public_subscription_spec(&subscription, "wss://api.bequant.io/api/3/ws/public")
        .expect("public spec");
    assert_eq!(spec.channel, "orderbook/D20/1000ms");
    assert_eq!(spec.subscribe_payload["method"], "subscribe");

    let private = private_subscription_spec(
        PrivateStreamKind::Orders,
        "wss://api.bequant.io/api/3/ws/trading",
        "wss://api.bequant.io/api/3/ws/wallet",
    )
    .expect("private spec");
    assert_eq!(private.subscribe_payload["method"], "spot_subscribe");

    let auth = private_auth_spec(
        "wss://api.bequant.io/api/3/ws/trading",
        "fixture-key",
        "fixture-secret",
    );
    assert_eq!(auth.payload["method"], "login");
    assert_eq!(
        auth.authorization,
        "Basic Zml4dHVyZS1rZXk6Zml4dHVyZS1zZWNyZXQ="
    );

    let snapshot = parse_public_orderbook_message(
        &exchange_id(),
        symbol_scope(),
        &bequant_fixture("ws/orderbook_snapshot.json"),
    )
    .expect("parse")
    .expect("snapshot");
    assert_eq!(snapshot.bids[0].price, 65000.0);

    assert_eq!(heartbeat_ping_payload()["method"], "ping");
    assert!(heartbeat_pong_method(&json!({"method": "pong"})).expect("pong"));
}
