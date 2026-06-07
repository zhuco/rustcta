use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelAllOrdersRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient,
    OrderListConditionalLeg, OrderListLegType, OrderListRequest, OrderSide, OrderType,
    PlaceOrderRequest, PositionsRequest, QueryOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeSymbol, MarketType, PositionSide};
use serde_json::json;

use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{CoinbaseGatewayAdapter, CoinbaseGatewayConfig};
use crate::request_spec::RequestSpec;

#[tokio::test]
async fn coinbase_private_rest_should_remain_disabled_without_bearer_token() {
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        bearer_token: String::new(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");
    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            assets: vec![],
        })
        .await
        .expect_err("private rest disabled");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn coinbase_adapter_should_load_balances_with_bearer_auth() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("balances_success")]).await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url,
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            assets: vec![],
        })
        .await
        .expect("balances");

    assert_eq!(response.balances[0].balances[0].asset, "USD");
    assert_eq!(response.balances[0].balances[0].available, 100.0);
    assert_eq!(response.balances[0].balances[0].locked, 0.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/accounts");
    assert_eq!(request.header("authorization"), Some("Bearer test.jwt"));
    load_request_spec("get_balances.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn coinbase_adapter_should_classify_error_response_fixture() {
    let (base_url, _seen) = spawn_rest_server(vec![fixture("error_response")]).await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url,
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances-error"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            assets: vec![],
        })
        .await
        .expect_err("coinbase error fixture");

    match error {
        ExchangeApiError::Exchange(error) => {
            assert_eq!(error.code.as_deref(), Some("ORDER_NOT_FOUND"));
            assert!(error.message.contains("order not found"));
        }
        other => panic!("expected exchange error, got {other:?}"),
    }
}

#[tokio::test]
async fn coinbase_adapter_should_load_intx_balances_with_portfolio_uuid() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "portfolio_balances": [{
            "portfolio_uuid": "portfolio-1",
            "balances": [{
                "asset": {"asset_id": "USDC", "asset_name": "USD Coin"},
                "quantity": "1250.5",
                "hold": "10.5",
                "transfer_hold": "2.0",
                "pledged_quantity": "3.0",
                "max_withdraw_amount": "1235.0"
            }]
        }]
    })])
    .await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        international_rest_base_url: base_url,
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        perpetual_portfolio_uuid: Some("portfolio-1".to_string()),
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("intx-balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["USDC".to_string()],
        })
        .await
        .expect("intx balances");

    let balance = &response.balances[0].balances[0];
    assert_eq!(balance.asset, "USDC");
    assert_eq!(balance.total, 1250.5);
    assert_eq!(balance.available, 1235.0);
    assert_eq!(balance.locked, 15.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/intx/balances/portfolio-1");
    assert_eq!(request.header("authorization"), Some("Bearer test.jwt"));
}

#[tokio::test]
async fn coinbase_adapter_should_build_limit_order_body() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "success": true,
        "success_response": {
            "order_id": "order-1",
            "product_id": "BTC-USD",
            "side": "BUY",
            "client_order_id": "client-1"
        }
    })])
    .await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url,
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol_scope(),
            client_order_id: Some("client-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.01".to_string(),
            price: Some("50000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: true,
        })
        .await
        .expect("place order");

    assert_eq!(response.order.exchange_order_id.as_deref(), Some("order-1"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/orders");
    load_request_spec("place_order.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
    let body = request.body.expect("body");
    assert_eq!(body["client_order_id"], "client-1");
    assert_eq!(body["product_id"], "BTC-USD");
    assert_eq!(body["side"], "BUY");
    assert_eq!(
        body["order_configuration"]["limit_limit_gtc"]["base_size"],
        "0.01"
    );
    assert_eq!(
        body["order_configuration"]["limit_limit_gtc"]["limit_price"],
        "50000"
    );
    assert_eq!(
        body["order_configuration"]["limit_limit_gtc"]["post_only"],
        true
    );
}

#[tokio::test]
async fn coinbase_adapter_should_cancel_by_exchange_order_id_only() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "results": [{"success": true, "order_id": "order-1"}]
    })])
    .await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url,
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("order-1".to_string()),
        })
        .await
        .expect("cancel");

    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/orders/batch_cancel");
    load_request_spec("cancel_order.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
    assert_eq!(request.body.expect("body")["order_ids"][0], "order-1");
}

#[tokio::test]
async fn coinbase_adapter_should_batch_cancel_by_exchange_order_ids() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "results": [
            {"success": true, "order_id": "order-1"},
            {"success": true, "order_id": "order-2"}
        ]
    })])
    .await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url,
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-1"),
                    symbol: symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("order-1".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-2"),
                    symbol: symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("order-2".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");

    assert_eq!(response.cancelled_count, 2);
    assert_eq!(response.orders.len(), 2);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/orders/batch_cancel");
    load_request_spec("batch_cancel_orders.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
    let body = request.body.expect("body");
    assert_eq!(body["order_ids"][0], "order-1");
    assert_eq!(body["order_ids"][1], "order-2");
}

#[tokio::test]
async fn coinbase_adapter_should_batch_place_orders_with_composed_create_order_calls() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "success": true,
            "success_response": {
                "order_id": "order-1",
                "product_id": "BTC-USD",
                "side": "BUY",
                "client_order_id": "client-1"
            }
        }),
        json!({
            "success": true,
            "success_response": {
                "order_id": "order-2",
                "product_id": "BTC-USD",
                "side": "SELL",
                "client_order_id": "client-2"
            }
        }),
    ])
    .await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url,
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("place-1"),
                    symbol: symbol_scope(),
                    client_order_id: Some("client-1".to_string()),
                    side: OrderSide::Buy,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.01".to_string(),
                    price: Some("50000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("place-2"),
                    symbol: symbol_scope(),
                    client_order_id: Some("client-2".to_string()),
                    side: OrderSide::Sell,
                    position_side: None,
                    order_type: OrderType::PostOnly,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.02".to_string(),
                    price: Some("60000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: true,
                },
            ],
        })
        .await
        .expect("batch place");

    assert_eq!(response.orders.len(), 2);
    assert_eq!(
        response.orders[0].exchange_order_id.as_deref(),
        Some("order-1")
    );
    assert_eq!(
        response.orders[1].exchange_order_id.as_deref(),
        Some("order-2")
    );
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.method == "POST"));
    assert!(requests.iter().all(|request| request.path == "/orders"));
    load_request_spec("batch_place_orders.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("request spec");
    assert_eq!(
        requests[0].body.as_ref().expect("body")["client_order_id"],
        "client-1"
    );
    assert_eq!(
        requests[1].body.as_ref().expect("body")["order_configuration"]["limit_limit_gtc"]
            ["post_only"],
        true
    );
}

#[tokio::test]
async fn coinbase_adapter_should_amend_order_size_by_exchange_order_id() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "success": true,
        "errors": []
    })])
    .await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url,
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("order-1".to_string()),
            new_client_order_id: None,
            new_quantity: "0.02".to_string(),
        })
        .await
        .expect("amend");

    assert_eq!(response.order.exchange_order_id.as_deref(), Some("order-1"));
    assert_eq!(response.order.quantity, "0.02");
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/orders/edit");
    let body = request.body.expect("body");
    assert_eq!(body["order_id"], "order-1");
    assert_eq!(body["size"], "0.02");
    assert!(body.get("price").is_none());
}

#[tokio::test]
async fn coinbase_adapter_should_query_order_by_client_order_id_fallback() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "order": {
            "order_id": "order-1",
            "client_order_id": "client-1",
            "product_id": "BTC-USD",
            "side": "BUY",
            "status": "OPEN",
            "order_configuration": {
                "limit_limit_gtc": {
                    "base_size": "0.01",
                    "limit_price": "50000",
                    "post_only": false
                }
            },
            "filled_size": "0"
        }
    })])
    .await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url,
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-client-id"),
            symbol: symbol_scope(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
        })
        .await
        .expect("query order");

    assert_eq!(
        response.order.unwrap().client_order_id.as_deref(),
        Some("client-1")
    );
    assert_eq!(
        seen.lock().unwrap()[0].path,
        "/orders/historical/client-1".to_string()
    );
}

#[tokio::test]
async fn coinbase_adapter_should_place_sell_oco_as_trigger_bracket() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "success": true,
        "success_response": {
            "order_id": "bracket-1",
            "product_id": "BTC-USD",
            "side": "SELL",
            "client_order_id": "list-1"
        }
    })])
    .await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url,
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order-list"),
            symbol: symbol_scope(),
            list_client_order_id: Some("list-1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::Limit,
                price: Some("60000".to_string()),
                stop_price: None,
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: None,
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLoss,
                price: None,
                stop_price: Some("45500".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: None,
            },
        })
        .await
        .expect("order list");

    assert_eq!(response.order_list_id.as_deref(), Some("bracket-1"));
    assert_eq!(
        response.orders[0].exchange_order_id.as_deref(),
        Some("bracket-1")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/orders");
    let body = request.body.expect("body");
    assert_eq!(body["client_order_id"], "list-1");
    assert_eq!(body["side"], "SELL");
    let bracket = &body["order_configuration"]["trigger_bracket_gtc"];
    assert_eq!(bracket["base_size"], "0.01");
    assert_eq!(bracket["limit_price"], "60000");
    assert_eq!(bracket["stop_trigger_price"], "45500");
}

#[tokio::test]
async fn coinbase_oco_stop_limit_price_should_remain_unsupported() {
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order-list-stop-limit"),
            symbol: symbol_scope(),
            list_client_order_id: Some("list-1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::Limit,
                price: Some("60000".to_string()),
                stop_price: None,
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: None,
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("45000".to_string()),
                stop_price: Some("45500".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: None,
            },
        })
        .await
        .expect_err("stop-limit price is not losslessly mapped");

    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn coinbase_oto_order_list_should_remain_unsupported() {
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let error = adapter
        .place_order_list(OrderListRequest::Oto {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oto-order-list"),
            symbol: symbol_scope(),
            list_client_order_id: Some("list-oto".to_string()),
            working: rustcta_exchange_api::OrderListOrderLeg {
                side: OrderSide::Buy,
                order_type: OrderListLegType::Limit,
                quantity: "0.01".to_string(),
                price: Some("50000".to_string()),
                stop_price: None,
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("working-1".to_string()),
            },
            pending: rustcta_exchange_api::OrderListOrderLeg {
                side: OrderSide::Sell,
                order_type: OrderListLegType::TakeProfitLimit,
                quantity: "0.01".to_string(),
                price: Some("60000".to_string()),
                stop_price: None,
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("pending-1".to_string()),
            },
        })
        .await
        .expect_err("oto order list unsupported");

    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn coinbase_adapter_should_load_intx_positions_with_portfolio_uuid() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "positions": [{
            "product_id": "BTC-PERP-INTX",
            "symbol": "BTC-PERP-INTX",
            "position_side": "POSITION_SIDE_LONG",
            "net_size": "0.5",
            "entry_vwap": {"value": "50000", "currency": "USDC"},
            "mark_price": {"value": "51000", "currency": "USDC"},
            "liquidation_price": {"value": "30000", "currency": "USDC"},
            "unrealized_pnl": {"value": "500", "currency": "USDC"},
            "leverage": "5"
        }]
    })])
    .await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        international_rest_base_url: base_url,
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        perpetual_portfolio_uuid: Some("portfolio-1".to_string()),
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![ExchangeSymbol::new(
                exchange_id(),
                MarketType::Perpetual,
                "BTC-PERP-INTX",
            )
            .expect("symbol")],
        })
        .await
        .expect("positions");

    assert_eq!(response.positions.len(), 1);
    assert_eq!(response.positions[0].side, PositionSide::Long);
    assert_eq!(response.positions[0].quantity, 0.5);
    assert_eq!(response.positions[0].entry_price, Some(50000.0));
    assert_eq!(response.positions[0].mark_price, Some(51000.0));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/intx/positions/portfolio-1");
    assert_eq!(request.header("authorization"), Some("Bearer test.jwt"));
}

#[tokio::test]
async fn coinbase_adapter_should_cancel_all_symbol_open_orders_with_batch_cancel() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "orders": [
                {
                    "order_id": "order-1",
                    "client_order_id": "client-1",
                    "product_id": "BTC-USD",
                    "side": "BUY",
                    "status": "OPEN",
                    "order_configuration": {
                        "limit_limit_gtc": {
                            "base_size": "0.01",
                            "limit_price": "50000",
                            "post_only": false
                        }
                    },
                    "filled_size": "0"
                },
                {
                    "order_id": "order-2",
                    "client_order_id": "client-2",
                    "product_id": "BTC-USD",
                    "side": "SELL",
                    "status": "OPEN",
                    "order_configuration": {
                        "limit_limit_gtc": {
                            "base_size": "0.02",
                            "limit_price": "60000",
                            "post_only": false
                        }
                    },
                    "filled_size": "0"
                }
            ]
        }),
        json!({
            "results": [
                {"success": true, "order_id": "order-1"},
                {"success": true, "order_id": "order-2"}
            ]
        }),
    ])
    .await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url,
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            symbol: Some(symbol_scope()),
        })
        .await
        .expect("cancel all");

    assert_eq!(response.cancelled_count, 2);
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/orders/historical/batch");
    assert_eq!(
        requests[0].query.get("order_status"),
        Some(&"OPEN".to_string())
    );
    assert_eq!(
        requests[0].query.get("product_id"),
        Some(&"BTC-USD".to_string())
    );
    assert_eq!(requests[1].method, "POST");
    assert_eq!(requests[1].path, "/orders/batch_cancel");
    let body = requests[1].body.clone().expect("cancel body");
    assert_eq!(body["order_ids"][0], "order-1");
    assert_eq!(body["order_ids"][1], "order-2");
}

#[tokio::test]
async fn coinbase_adapter_should_route_recent_fills_with_request_spec() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "fills": [{
            "trade_id": "trade-1",
            "order_id": "order-1",
            "side": "BUY",
            "price": "50000",
            "size": "0.01",
            "trade_value": "500",
            "commission": "0.25",
            "commission_asset": "USD",
            "trade_time": "2026-01-02T03:04:05Z"
        }]
    })])
    .await;
    let adapter = CoinbaseGatewayAdapter::new(CoinbaseGatewayConfig {
        spot_rest_base_url: base_url,
        bearer_token: "test.jwt".to_string(),
        enabled_private_rest: true,
        ..CoinbaseGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("order-1".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(25),
            page: None,
        })
        .await
        .expect("recent fills");

    assert_eq!(response.fills[0].fill_id.as_deref(), Some("trade-1"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/orders/historical/fills");
    assert_eq!(request.query.get("limit").map(String::as_str), Some("25"));
    load_request_spec("recent_fills.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
}

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "balances_success" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinbase/balances_success.json")
        }
        "error_response" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinbase/error_response.json")
        }
        _ => unreachable!("unknown fixture"),
    };
    serde_json::from_str(text).expect("fixture json")
}

fn load_request_spec(path: &str) -> RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/coinbase/request_specs/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("request spec fixture");
    serde_json::from_str(&text).expect("request spec fixture")
}
