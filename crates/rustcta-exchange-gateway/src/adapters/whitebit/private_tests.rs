use rustcta_exchange_api::{
    BalancesRequest, BatchAtomicity, BatchCancelOrdersRequest, BatchExecutionMode,
    BatchPlaceOrdersRequest, CancelAllOrdersRequest, CancelOrderRequest, CapabilitySupport,
    ExchangeApiError, ExchangeClient, ExchangeStreamEvent, FeesRequest, OpenOrdersRequest,
    PlaceOrderRequest, PositionsRequest, PrivateStreamKind, PrivateStreamSubscription,
    QueryOrderRequest, QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, ExchangeSymbol, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
};
use serde_json::json;

use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

use super::streams::{
    parse_whitebit_private_stream_message, whitebit_private_authorize_payload,
    whitebit_private_subscribe_payload, whitebit_ws_is_pong, whitebit_ws_ping_payload,
    whitebit_ws_server_time_payload, WhiteBitWsSessionEvent,
};
use super::test_support::{
    assert_signed_request, assert_signed_request_method, context, exchange_id, perp_symbol_scope,
    spawn_rest_server, symbol_scope,
};
use super::{WhiteBitGatewayAdapter, WhiteBitGatewayConfig};

#[tokio::test]
async fn whitebit_adapter_should_keep_private_operations_unsupported_until_credentials_move() {
    let adapter = WhiteBitGatewayAdapter::default_public().expect("adapter");
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
        .expect_err("private operation should be unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[test]
fn whitebit_signing_vector_fixture_should_verify_payload_hmac() {
    signing_vector("order_new_limit.json")
        .verify()
        .expect("signing vector");
}

#[tokio::test]
async fn whitebit_request_spec_fixture_should_cover_private_place_order() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": {
            "order_id": "2001",
            "market": "BTCUSDT",
            "client_id": "CLIENTLIMIT1",
            "side": "buy",
            "type": "limit",
            "status": "open",
            "price": "65000",
            "amount": "0.02",
            "filled_amount": "0"
        }
    })])
    .await;
    let adapter = private_adapter(base_url);

    adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("request-spec"),
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
            post_only: false,
        })
        .await
        .expect("place order");

    request_spec("order_new_limit.json")
        .assert_matches(&seen.lock().unwrap()[0].actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn whitebit_adapter_should_sign_private_get_requests_with_headers() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": [{
            "ccy": "BTC",
            "available": "0.5",
            "frozen": "0.1",
            "total": "0.6"
        }]
    })])
    .await;
    let adapter = WhiteBitGatewayAdapter::new(WhiteBitGatewayConfig {
        rest_base_url: base_url,
        api_key: "whitebit-key".to_string(),
        api_secret: "whitebit-secret".to_string(),
        enabled_private_rest: true,
        ..WhiteBitGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("signed-balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect("balances");

    assert!(adapter.capabilities().supports_private_rest);
    assert_eq!(response.balances.len(), 1);
    assert_eq!(response.balances[0].balances[0].asset, "BTC");
    assert_eq!(response.balances[0].balances[0].available, 0.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v4/trade-account/balance");
    assert_signed_request(&request, "whitebit-key", "whitebit-secret");
}

#[tokio::test]
async fn whitebit_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "data": {
                "order_id": "2001",
                "market": "BTCUSDT",
                "client_id": "CLIENTLIMIT1",
                "side": "buy",
                "type": "limit",
                "option": "normal",
                "status": "open",
                "price": "65000",
                "amount": "0.02",
                "filled_amount": "0",
                "created_at": 1743054548000_i64
            }
        }),
        json!({
            "code": 0,
            "data": {
                "order_id": "2002",
                "market": "BTCUSDT",
                "client_id": "CLIENTQUOTE1",
                "side": "buy",
                "type": "market",
                "status": "open",
                "amount": "25.5",
                "deal_amount": "0",
                "created_at": 1743054549000_i64
            }
        }),
        json!({
            "code": 0,
            "data": {
                "order_id": "2001",
                "market": "BTCUSDT",
                "client_id": "CLIENTLIMIT1",
                "status": "cancel"
            }
        }),
        json!({
            "code": 0,
            "data": [{"order_id": "2003"}, {"order_id": "2004"}]
        }),
    ])
    .await;
    let adapter = WhiteBitGatewayAdapter::new(WhiteBitGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..WhiteBitGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_place_order);
    assert!(capabilities.supports_cancel_order);
    assert!(capabilities.supports_cancel_all_orders);
    assert!(capabilities.supports_quote_market_order);
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place-order"),
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
            post_only: false,
        })
        .await
        .expect("place order");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("2001"));
    assert_eq!(
        placed.order.client_order_id.as_deref(),
        Some("CLIENTLIMIT1")
    );

    let quote = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote-order"),
            symbol: symbol_scope(),
            client_order_id: Some("CLIENTQUOTE1".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25.5".to_string(),
        })
        .await
        .expect("quote order");
    assert_eq!(quote.order.exchange_order_id.as_deref(), Some("2002"));
    assert_eq!(quote.order.order_type, OrderType::Market);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-order"),
            symbol: symbol_scope(),
            client_order_id: Some("CLIENTLIMIT1".to_string()),
            exchange_order_id: Some("2001".to_string()),
        })
        .await
        .expect("cancel order");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);

    let cancel_all = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
        })
        .await
        .expect("cancel all");
    assert_eq!(cancel_all.cancelled_count, 2);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);

    assert_signed_request_method(&requests[0], "POST", "key", "secret");
    assert_eq!(requests[0].path, "/api/v4/order/new");
    let body = requests[0].body.as_ref().expect("place body");
    assert_eq!(body["request"], "/api/v4/order/new");
    assert_eq!(body["market"], "BTC_USDT");
    assert_eq!(body["side"], "buy");
    assert_eq!(body["amount"], "0.02");
    assert_eq!(body["price"], "65000");
    assert_eq!(body["clientOrderId"], "CLIENTLIMIT1");

    assert_signed_request_method(&requests[1], "POST", "key", "secret");
    assert_eq!(requests[1].path, "/api/v4/order/market");
    let body = requests[1].body.as_ref().expect("quote body");
    assert_eq!(body["request"], "/api/v4/order/market");
    assert_eq!(body["amount"], "25.5");
    assert_eq!(body["clientOrderId"], "CLIENTQUOTE1");

    assert_signed_request_method(&requests[2], "POST", "key", "secret");
    assert_eq!(requests[2].path, "/api/v4/order/cancel");
    let body = requests[2].body.as_ref().expect("cancel body");
    assert_eq!(body["market"], "BTC_USDT");
    assert_eq!(body["orderId"], "2001");
    assert_eq!(body["clientOrderId"], "CLIENTLIMIT1");

    assert_signed_request_method(&requests[3], "POST", "key", "secret");
    assert_eq!(requests[3].path, "/api/v4/order/cancel/all");
    let body = requests[3].body.as_ref().expect("cancel all body");
    assert_eq!(body["market"], "BTC_USDT");
    assert_eq!(body["type"], json!(["spot"]));
}

#[tokio::test]
async fn whitebit_adapter_should_route_perpetual_order_balance_position_and_fees() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "data": {
                "orderId": "3001",
                "market": "BTC_PERP",
                "clientOrderId": "PERPLIMIT1",
                "side": "sell",
                "type": "limit",
                "status": "new",
                "price": "68000",
                "amount": "0.01",
                "dealStock": "0",
                "positionSide": "SHORT",
                "reduceOnly": true
            }
        }),
        json!({
            "code": 0,
            "data": [
                {"ticker": "USDT", "available": "750", "freeze": "25", "total": "775"}
            ]
        }),
        json!({
            "code": 0,
            "data": {
                "market": "BTC_PERP",
                "maker": "0.001",
                "taker": "0.001",
                "futures_maker": "0.0001",
                "futures_taker": "0.00035"
            }
        }),
        json!({
            "code": 0,
            "data": [{
                "market": "BTC_PERP",
                "amount": "0.02",
                "side": "short",
                "basePrice": "65000",
                "marketPrice": "64000",
                "liquidationPrice": "70000",
                "unrealizedPnl": "20",
                "leverage": "10"
            }]
        }),
    ])
    .await;
    let adapter = WhiteBitGatewayAdapter::new(WhiteBitGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..WhiteBitGatewayConfig::default()
    })
    .expect("adapter");

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-place"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("PERPLIMIT1".to_string()),
            side: OrderSide::Sell,
            position_side: Some(PositionSide::Short),
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.01".to_string(),
            price: Some("68000".to_string()),
            quote_quantity: None,
            reduce_only: true,
            post_only: false,
        })
        .await
        .expect("perp order");
    assert_eq!(placed.order.market_type, MarketType::Perpetual);
    assert_eq!(placed.order.position_side, Some(PositionSide::Short));
    assert!(placed.order.reduce_only);

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("perp balances");
    assert_eq!(balances.balances[0].market_type, MarketType::Perpetual);
    assert_eq!(balances.balances[0].balances[0].locked, 25.0);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-fees"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("perp fees");
    assert_eq!(fees.fees[0].maker_rate, "0.0001");
    assert_eq!(fees.fees[0].taker_rate, "0.00035");

    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![
                ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTC_PERP")
                    .expect("perp symbol"),
            ],
        })
        .await
        .expect("positions");
    assert_eq!(positions.positions.len(), 1);
    assert_eq!(positions.positions[0].side, PositionSide::Short);
    assert_eq!(positions.positions[0].quantity, 0.02);
    assert_eq!(positions.positions[0].leverage, Some(10.0));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/api/v4/order/collateral/limit");
    let body = requests[0].body.as_ref().expect("perp order body");
    assert_eq!(body["market"], "BTC_PERP");
    assert_eq!(body["positionSide"], "SHORT");
    assert_eq!(body["reduceOnly"], true);
    assert_eq!(requests[1].path, "/api/v4/collateral-account/balance");
    assert_eq!(requests[2].path, "/api/v4/market/fee");
    assert_eq!(
        requests[3].path,
        "/api/v4/collateral-account/positions/open"
    );

    assert_eq!(seen.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn whitebit_adapter_should_route_batch_order_mutations_and_perp_readbacks() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "data": [
                {
                    "orderId": "4101",
                    "clientOrderId": "BATCH1",
                    "market": "BTC_USDT",
                    "side": "buy",
                    "type": "limit",
                    "status": "new",
                    "price": "65000",
                    "amount": "0.01"
                },
                {
                    "orderId": "4102",
                    "clientOrderId": "BATCH2",
                    "market": "BTC_USDT",
                    "side": "sell",
                    "type": "limit",
                    "status": "new",
                    "price": "66000",
                    "amount": "0.02"
                }
            ]
        }),
        json!({
            "code": 0,
            "data": [
                {"orderId": "4101", "market": "BTC_USDT", "status": "cancel"},
                {"orderId": "4102", "market": "BTC_USDT", "status": "cancel"}
            ]
        }),
        json!({
            "code": 0,
            "data": {
                "orderId": "5101",
                "market": "BTC_PERP",
                "clientOrderId": "PERPCANCEL",
                "side": "sell",
                "type": "limit",
                "status": "cancel",
                "amount": "0.01",
                "price": "68000"
            }
        }),
        json!({
            "code": 0,
            "data": [{
                "orderId": "5101",
                "market": "BTC_PERP",
                "clientOrderId": "PERPCANCEL",
                "side": "sell",
                "type": "limit",
                "status": "open",
                "amount": "0.01",
                "price": "68000"
            }]
        }),
    ])
    .await;
    let adapter = WhiteBitGatewayAdapter::new(WhiteBitGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..WhiteBitGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);
    assert!(capabilities.supports_private_streams);
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.max_items,
        Some(20)
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_cancel_orders
            .supports_partial_failure
    );
    assert!(matches!(
        capabilities.capabilities_v2.stream_runtime.private,
        CapabilitySupport::Native
    ));
    assert!(capabilities.capabilities_v2.stream_runtime.auth.required);

    let batch = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-place-1"),
                    symbol: symbol_scope(),
                    client_order_id: Some("BATCH1".to_string()),
                    side: OrderSide::Buy,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.01".to_string(),
                    price: Some("65000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-place-2"),
                    symbol: symbol_scope(),
                    client_order_id: Some("BATCH2".to_string()),
                    side: OrderSide::Sell,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.02".to_string(),
                    price: Some("66000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("batch place");
    assert_eq!(batch.orders.len(), 2);
    assert_eq!(batch.orders[0].exchange_order_id.as_deref(), Some("4101"));

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-1"),
                    symbol: symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("4101".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-2"),
                    symbol: symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("4102".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 2);
    assert_eq!(cancelled.orders[0].status, OrderStatus::Cancelled);

    let perp_cancel = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-cancel"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("PERPCANCEL".to_string()),
            exchange_order_id: Some("5101".to_string()),
        })
        .await
        .expect("perp cancel");
    assert!(perp_cancel.cancelled);
    assert_eq!(perp_cancel.order.market_type, MarketType::Perpetual);

    let perp_query = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-query"),
            symbol: perp_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("5101".to_string()),
        })
        .await
        .expect("perp query")
        .order
        .expect("perp order");
    assert_eq!(perp_query.market_type, MarketType::Perpetual);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/api/v4/order/bulk");
    assert_eq!(
        requests[0].body.as_ref().expect("batch body")["orders"][0]["clientOrderId"],
        "BATCH1"
    );
    assert_eq!(requests[1].path, "/api/v4/order/cancel/bulk");
    assert_eq!(
        requests[1].body.as_ref().expect("batch cancel body")["orders"][1]["orderId"],
        "4102"
    );
    assert_eq!(requests[2].path, "/api/v4/order/cancel");
    assert_eq!(
        requests[2].body.as_ref().expect("perp cancel body")["market"],
        "BTC_PERP"
    );
    assert_eq!(requests[3].path, "/api/v4/orders");
}

#[tokio::test]
async fn whitebit_adapter_should_build_private_websocket_subscription_and_heartbeat_specs() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": { "websocket_token": "ws-token" }
    })])
    .await;
    let adapter = WhiteBitGatewayAdapter::new(WhiteBitGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..WhiteBitGatewayConfig::default()
    })
    .expect("adapter");

    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Positions,
    };
    let authorize = whitebit_private_authorize_payload("ws-token", 9).expect("authorize");
    assert_eq!(authorize["method"], "authorize");
    assert_eq!(authorize["params"], json!(["ws-token", "public"]));
    let payload = whitebit_private_subscribe_payload(&subscription, 10).expect("subscribe");
    assert_eq!(payload["method"], "positionsMargin_subscribe");
    assert_eq!(whitebit_ws_ping_payload(11)["method"], "ping");
    assert_eq!(whitebit_ws_server_time_payload(12)["method"], "server.time");
    assert!(whitebit_ws_is_pong(&json!({"result": "pong"})));

    let subscription_id = adapter
        .subscribe_private_stream(subscription)
        .await
        .expect("private stream subscription");
    assert_eq!(
        subscription_id,
        "whitebit:wss://api.whitebit.com/ws:positionsMargin_subscribe:account"
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v4/profile/websocket_token");
    assert_signed_request_method(&request, "POST", "key", "secret");
}

#[tokio::test]
async fn whitebit_private_websocket_session_should_auth_subscribe_heartbeat_and_parse_events() {
    let (base_url, _seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": { "websocket_token": "ws-token" }
    })])
    .await;
    let adapter = WhiteBitGatewayAdapter::new(WhiteBitGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..WhiteBitGatewayConfig::default()
    })
    .expect("adapter");
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-session"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };

    let mut session = adapter
        .private_ws_session(subscription)
        .await
        .expect("private session")
        .with_symbol_hint(symbol_scope());
    let initial = session.initial_requests();
    assert_eq!(initial.len(), 2);
    assert_eq!(initial[0]["method"], "authorize");
    assert_eq!(initial[0]["params"], json!(["ws-token", "public"]));
    assert_eq!(initial[1]["method"], "ordersPending_subscribe");

    let ping = session.heartbeat_request(33, chrono::Utc::now());
    assert_eq!(ping["method"], "ping");
    let pong_events = session
        .handle_text_message(r#"{"id":33,"result":"pong"}"#)
        .expect("pong");
    assert!(pong_events.iter().any(|event| {
        matches!(
            event,
            WhiteBitWsSessionEvent::Stream(items)
                if matches!(items.first(), Some(ExchangeStreamEvent::Heartbeat { .. }))
        )
    }));

    let order_events = session
        .handle_text_message(
            r#"{"method":"ordersPending_update","params":[[{"orderId":"7001","clientOrderId":"CID7001","market":"BTC_USDT","side":"buy","type":"limit","status":"part_deal","amount":"0.02","price":"65000","dealStock":"0.01","avgPrice":"65010","timestamp":1743054548000}]]}"#,
        )
        .expect("order event");
    assert!(order_events.iter().any(|event| {
        matches!(
            event,
            WhiteBitWsSessionEvent::Stream(items)
                if items.iter().any(|item| matches!(item, ExchangeStreamEvent::OrderUpdate(order) if order.exchange_order_id.as_deref() == Some("7001")))
        )
    }));

    let balance_events = parse_whitebit_private_stream_message(
        &exchange_id(),
        context("balance-event").tenant_id.expect("tenant"),
        AccountId::new("account").expect("account"),
        Some(symbol_scope()),
        &json!({
            "method": "balanceSpot_update",
            "params": [[{"ticker":"USDT","available":"100","freeze":"5","total":"105"}]]
        }),
    )
    .expect("balance event");
    assert!(matches!(
        &balance_events[0],
        ExchangeStreamEvent::BalanceSnapshot(snapshot)
            if snapshot.balances[0].balances[0].asset == "USDT"
                && snapshot.balances[0].balances[0].locked == 5.0
    ));

    let position_events = parse_whitebit_private_stream_message(
        &exchange_id(),
        context("position-event").tenant_id.expect("tenant"),
        AccountId::new("account").expect("account"),
        Some(perp_symbol_scope()),
        &json!({
            "method": "positionsMargin_update",
            "params": [[{"market":"BTC_PERP","amount":"0.03","side":"long","basePrice":"65000","marketPrice":"65100","leverage":"5"}]]
        }),
    )
    .expect("position event");
    assert!(matches!(
        &position_events[0],
        ExchangeStreamEvent::PositionSnapshot(snapshot)
            if snapshot.positions[0].quantity == 0.03
                && snapshot.positions[0].side == PositionSide::Long
    ));
}

#[tokio::test]
async fn whitebit_adapter_should_parse_private_readback_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "data": [
                {"ccy": "BTC", "available": "0.5", "frozen": "0.1", "total": "0.6"},
                {"ccy": "USDT", "available": "1000", "frozen": "25", "total": "1025"}
            ]
        }),
        json!({
            "code": 0,
            "data": {
                "order_id": "1001",
                "client_id": "CID1001",
                "market": "BTCUSDT",
                "side": "buy",
                "type": "limit",
                "option": "normal",
                "status": "part_deal",
                "price": "65000",
                "amount": "0.01",
                "filled_amount": "0.006",
                "avg_price": "65010",
                "created_at": 1743054550000_i64,
                "updated_at": 1743054551000_i64
            }
        }),
        json!({
            "code": 0,
            "data": [{
                "order_id": "1002",
                "client_id": "CID1002",
                "market": "BTCUSDT",
                "side": "sell",
                "type": "limit",
                "status": "open",
                "price": "70000",
                "amount": "0.02",
                "filled_amount": "0"
            }]
        }),
        json!({
            "code": 0,
            "data": {
                "market": "BTCUSDT",
                "maker_fee_rate": "0.001",
                "taker_fee_rate": "0.0015"
            }
        }),
        json!({
            "code": 0,
            "data": [{
                "deal_id": "9001",
                "order_id": "1001",
                "client_id": "CID1001",
                "market": "BTCUSDT",
                "side": "buy",
                "price": "65010",
                "amount": "0.006",
                "fee": "0.39",
                "fee_ccy": "USDT",
                "created_at": 1743054550000_i64
            }]
        }),
    ])
    .await;
    let adapter = WhiteBitGatewayAdapter::new(WhiteBitGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..WhiteBitGatewayConfig::default()
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
    assert_eq!(balances.balances[0].balances[0].asset, "BTC");
    assert_eq!(balances.balances[0].balances[0].locked, 0.1);

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("order")
        .order
        .expect("order state");
    assert_eq!(order.exchange_order_id.as_deref(), Some("1001"));
    assert_eq!(order.client_order_id.as_deref(), Some("CID1001"));
    assert_eq!(order.side, OrderSide::Buy);
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.quantity, "0.01");
    assert_eq!(order.filled_quantity, "0.006");
    assert_eq!(order.average_fill_price.as_deref(), Some("65010"));

    let open_orders = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open_orders.orders.len(), 1);
    assert_eq!(
        open_orders.orders[0].exchange_order_id.as_deref(),
        Some("1002")
    );
    assert_eq!(open_orders.orders[0].side, OrderSide::Sell);
    assert_eq!(open_orders.orders[0].status, OrderStatus::New);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees.len(), 1);
    assert_eq!(fees.fees[0].maker_rate, "0.001");
    assert_eq!(fees.fees[0].taker_rate, "0.0015");

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(100),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("9001"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("1001"));
    assert_eq!(fills.fills[0].client_order_id.as_deref(), Some("CID1001"));
    assert_eq!(fills.fills[0].side, OrderSide::Buy);
    assert_eq!(fills.fills[0].price, 65010.0);
    assert_eq!(fills.fills[0].quantity, 0.006);
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("USDT"));
    assert_eq!(fills.fills[0].fee_amount, Some(0.39));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    assert_eq!(requests[0].path, "/api/v4/trade-account/balance");
    assert_eq!(requests[1].path, "/api/v4/orders");
    assert_eq!(
        requests[1]
            .body
            .as_ref()
            .and_then(|body| body.get("orderId"))
            .and_then(|value| value.as_str()),
        Some("1001")
    );
    assert_eq!(requests[2].path, "/api/v4/orders");
    assert_eq!(requests[3].path, "/api/v4/market/fee");
    assert_eq!(requests[4].path, "/api/v4/trade-account/order");
    assert_eq!(
        requests[4]
            .body
            .as_ref()
            .and_then(|body| body.get("limit"))
            .and_then(|value| value.as_u64()),
        Some(100)
    );
}

fn private_adapter(base_url: String) -> WhiteBitGatewayAdapter {
    WhiteBitGatewayAdapter::new(WhiteBitGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..WhiteBitGatewayConfig::default()
    })
    .expect("adapter")
}

fn request_spec(name: &str) -> RequestSpec {
    let raw = match name {
        "order_new_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/whitebit/request_specs/order_new_limit.json"
        ),
        _ => unreachable!("unknown whitebit request spec"),
    };
    serde_json::from_str(raw).expect("request spec")
}

fn signing_vector(name: &str) -> SigningVector {
    let raw = match name {
        "order_new_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/whitebit/signing_vectors/order_new_limit.json"
        ),
        _ => unreachable!("unknown whitebit signing vector"),
    };
    serde_json::from_str(raw).expect("signing vector")
}
