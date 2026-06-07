use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelOrderRequest,
    ExchangeClient, PlaceOrderRequest, QuoteMarketOrderRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType, PositionSide, TimeInForce};
use serde_json::json;

use super::private::orangex_auth_client_signature_params;
use super::test_support::{
    actual_request, context, perp_symbol_scope, request_spec, signing_vector, spawn_rest_server,
    spot_symbol_scope,
};
use super::{OrangeXGatewayAdapter, OrangeXGatewayConfig, OrangeXMarginMode};

#[tokio::test]
async fn orangex_private_requests_should_use_bearer_json_rpc_without_secret_leakage() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "jsonrpc": "2.0",
        "id": "1",
        "result": {
            "order": {
                "order_id": "77409325612535808"
            }
        }
    })])
    .await;
    let adapter = OrangeXGatewayAdapter::new(OrangeXGatewayConfig {
        rest_base_url: base_url,
        access_token: Some("token".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..OrangeXGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("CTA-abc/123".to_string()),
            side: OrderSide::Buy,
            position_side: Some(PositionSide::Long),
            order_type: OrderType::PostOnly,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.01".to_string(),
            price: Some("44000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: true,
        })
        .await
        .expect("place");

    assert_eq!(
        response.order.exchange_order_id.as_deref(),
        Some("77409325612535808")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.header("authorization"), Some("bearer token"));
    assert_eq!(request.body.as_ref().unwrap()["method"], "/private/buy");
    assert_eq!(
        request.body.as_ref().unwrap()["params"]["instrument_name"],
        "BTC-USDT-PERPETUAL"
    );
    assert_eq!(
        request.body.as_ref().unwrap()["params"]["time_in_force"],
        "good_til_cancelled"
    );
    assert!(!request.body.unwrap().to_string().contains("secret"));
    request_spec("place_order_perp")
        .assert_matches(&actual_request(seen.lock().unwrap()[0].clone()))
        .expect("orangex place request spec");
}

#[tokio::test]
async fn orangex_private_balance_and_cancel_specs_should_match_official_methods() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "jsonrpc": "2.0",
            "id": "1",
            "result": {
                "SPOT": {
                    "details": [{
                        "coin_type": "USDT",
                        "available": "10",
                        "freeze": "2",
                        "total": "12"
                    }]
                }
            }
        }),
        json!({
            "jsonrpc": "2.0",
            "id": "2",
            "result": {
                "order_id": "77409325612535808"
            }
        }),
    ])
    .await;
    let adapter = OrangeXGatewayAdapter::new(OrangeXGatewayConfig {
        rest_base_url: base_url,
        access_token: Some("token".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..OrangeXGatewayConfig::default()
    })
    .expect("adapter");

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances[0].balances[0].available, 10.0);

    let cancel = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("77409325612535808".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancel.cancelled);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(
        requests[0].body.as_ref().unwrap()["method"],
        "/private/get_assets_info"
    );
    assert_eq!(
        requests[0].body.as_ref().unwrap()["params"]["asset_type"][0],
        "SPOT"
    );
    assert_eq!(
        requests[1].body.as_ref().unwrap()["method"],
        "/private/cancel"
    );
    assert_eq!(
        requests[1].body.as_ref().unwrap()["params"]["order_id"],
        "77409325612535808"
    );
    for (request, spec) in requests
        .into_iter()
        .zip(["get_balances_spot", "cancel_order_spot"])
    {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("orangex balance/cancel request spec");
    }
}

#[tokio::test]
async fn orangex_private_requests_should_fetch_bearer_token_with_client_signature() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "jsonrpc": "2.0",
            "id": "1",
            "result": {
                "access_token": "fresh-token",
                "expires_in": 3600
            }
        }),
        json!({
            "jsonrpc": "2.0",
            "id": "2",
            "result": {
                "order": {
                    "order_id": "88409325612535808"
                }
            }
        }),
    ])
    .await;
    let adapter = OrangeXGatewayAdapter::new(OrangeXGatewayConfig {
        rest_base_url: base_url,
        client_id: Some("client".to_string()),
        client_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..OrangeXGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("auth-place"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("CTA.auth-1".to_string()),
            side: OrderSide::Sell,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.01".to_string(),
            price: Some("45000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");

    assert_eq!(
        response.order.exchange_order_id.as_deref(),
        Some("88409325612535808")
    );
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].body.as_ref().unwrap()["method"], "/public/auth");
    assert_eq!(
        requests[0].body.as_ref().unwrap()["params"]["grant_type"],
        "client_signature"
    );
    assert_eq!(
        requests[1].header("authorization"),
        Some("bearer fresh-token")
    );
    assert_eq!(
        requests[1].body.as_ref().unwrap()["method"],
        "/private/sell"
    );
    assert!(!requests[0]
        .body
        .as_ref()
        .unwrap()
        .to_string()
        .contains("secret"));
    assert!(!requests[1]
        .body
        .as_ref()
        .unwrap()
        .to_string()
        .contains("secret"));
    for (request, spec) in requests
        .into_iter()
        .zip(["auth_token_client_signature", "place_order_spot_sell"])
    {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("orangex auth/place request spec");
    }
}

#[tokio::test]
async fn orangex_batch_place_orders_should_use_sequential_buy_sell_rpc_fallback() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "jsonrpc": "2.0",
            "id": "1",
            "result": { "order": { "order_id": "place-buy-1" } }
        }),
        json!({
            "jsonrpc": "2.0",
            "id": "2",
            "result": { "order": { "order_id": "place-sell-2" } }
        }),
    ])
    .await;
    let adapter = OrangeXGatewayAdapter::new(OrangeXGatewayConfig {
        rest_base_url: base_url,
        access_token: Some("token".to_string()),
        enabled_private_rest: true,
        ..OrangeXGatewayConfig::default()
    })
    .expect("adapter");

    let first = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-1"),
        symbol: spot_symbol_scope(),
        client_order_id: Some("CTA.batch-1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("44000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let second = PlaceOrderRequest {
        side: OrderSide::Sell,
        client_order_id: Some("CTA.batch-2".to_string()),
        price: Some("44100".to_string()),
        ..first.clone()
    };

    let response = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: super::test_support::exchange_id(),
            orders: vec![first, second],
        })
        .await
        .expect("batch place");

    assert_eq!(response.orders.len(), 2);
    assert_eq!(
        response.orders[0].exchange_order_id.as_deref(),
        Some("place-buy-1")
    );
    assert_eq!(
        response.orders[1].exchange_order_id.as_deref(),
        Some("place-sell-2")
    );
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].body.as_ref().unwrap()["method"], "/private/buy");
    assert_eq!(
        requests[1].body.as_ref().unwrap()["method"],
        "/private/sell"
    );
    for (request, spec) in requests
        .into_iter()
        .zip(["batch_place_buy_fallback", "batch_place_sell_fallback"])
    {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("orangex batch place request spec");
    }
}

#[tokio::test]
async fn orangex_batch_cancel_orders_should_use_sequential_cancel_rpc_fallback() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "jsonrpc": "2.0",
            "id": "1",
            "result": { "order_id": "cancel-1" }
        }),
        json!({
            "jsonrpc": "2.0",
            "id": "2",
            "result": { "order_id": "cancel-2" }
        }),
    ])
    .await;
    let adapter = OrangeXGatewayAdapter::new(OrangeXGatewayConfig {
        rest_base_url: base_url,
        access_token: Some("token".to_string()),
        enabled_private_rest: true,
        ..OrangeXGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: super::test_support::exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-1"),
                    symbol: spot_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("cancel-1".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("cancel-2".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");

    assert_eq!(response.cancelled_count, 2);
    assert_eq!(response.orders.len(), 2);
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].body.as_ref().unwrap()["params"]["order_id"],
        "cancel-1"
    );
    assert_eq!(
        requests[1].body.as_ref().unwrap()["params"]["order_id"],
        "cancel-2"
    );
    for (request, spec) in requests
        .into_iter()
        .zip(["batch_cancel_spot_fallback", "batch_cancel_perp_fallback"])
    {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("orangex batch cancel request spec");
    }
}

#[tokio::test]
async fn orangex_private_capabilities_should_be_disabled_without_token() {
    let adapter = OrangeXGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);

    let error = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            side: OrderSide::Buy,
            quote_quantity: "10".to_string(),
        })
        .await
        .expect_err("unsupported without token");
    assert!(matches!(
        error,
        rustcta_exchange_api::ExchangeApiError::Unsupported { .. }
    ));
}

#[test]
fn orangex_private_capabilities_should_include_batch_when_auth_available() {
    let adapter = OrangeXGatewayAdapter::new(OrangeXGatewayConfig {
        access_token: Some("token".to_string()),
        enabled_private_rest: true,
        ..OrangeXGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_private_streams);
    let private_streams = capabilities
        .private_stream_capabilities
        .expect("private stream capability metadata");
    assert!(!private_streams.supports_orders);
    assert!(!private_streams.supports_fills);
    assert!(!private_streams.supports_balances);
    assert!(!private_streams.supports_positions);
}

#[test]
fn orangex_auth_signature_params_should_follow_official_prehash() {
    let params = orangex_auth_client_signature_params(
        "client",
        "secret",
        "1600000000000",
        "nonce",
        "account:read trade:read_write",
    );
    assert_eq!(params["grant_type"], "client_signature");
    assert_eq!(params["client_id"], "client");
    assert_eq!(params["signature"].as_str().unwrap().len(), 64);
    signing_vector("auth_client_signature")
        .verify()
        .expect("orangex signing vector");
}

#[tokio::test]
async fn orangex_perpetual_settings_should_use_official_adjust_rpc_methods() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "jsonrpc": "2.0",
            "id": "1",
            "result": { "instrument_name": "BTC-USDT-PERPETUAL", "margin_type": "cross" }
        }),
        json!({
            "jsonrpc": "2.0",
            "id": "2",
            "result": { "instrument_name": "BTC-USDT-PERPETUAL", "leverage": "20" }
        }),
    ])
    .await;
    let adapter = OrangeXGatewayAdapter::new(OrangeXGatewayConfig {
        rest_base_url: base_url,
        access_token: Some("token".to_string()),
        enabled_private_rest: true,
        ..OrangeXGatewayConfig::default()
    })
    .expect("adapter");

    let margin = adapter
        .adjust_perpetual_margin_type(perp_symbol_scope(), OrangeXMarginMode::Cross)
        .await
        .expect("margin");
    assert_eq!(margin.operation, "orangex.adjust_perpetual_margin_type");

    let leverage = adapter
        .adjust_perpetual_leverage(perp_symbol_scope(), 20)
        .await
        .expect("leverage");
    assert_eq!(leverage.operation, "orangex.adjust_perpetual_leverage");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(
        requests[0].body.as_ref().unwrap()["method"],
        "/private/adjust_perpetual_margin_type"
    );
    assert_eq!(
        requests[0].body.as_ref().unwrap()["params"]["instrument_name"],
        "BTC-USDT-PERPETUAL"
    );
    assert_eq!(
        requests[0].body.as_ref().unwrap()["params"]["margin_type"],
        "cross"
    );
    assert_eq!(
        requests[1].body.as_ref().unwrap()["method"],
        "/private/adjust_perpetual_leverage"
    );
    assert_eq!(
        requests[1].body.as_ref().unwrap()["params"]["leverage"],
        "20"
    );
    for (request, spec) in requests
        .into_iter()
        .zip(["adjust_margin_type_perp", "adjust_leverage_perp"])
    {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("orangex settings request spec");
    }
}
