use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchAtomicity, BatchCancelOrdersRequest,
    BatchExecutionMode, BatchPlaceOrdersRequest, CancelAllOrdersRequest, CancelOrderRequest,
    CapabilitySupport, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    OrderListConditionalLeg, OrderListLegType, OrderListOrderLeg, OrderListRequest,
    PlaceOrderRequest, PositionsRequest, QueryOrderRequest, QuoteMarketOrderRequest,
    RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

use super::test_support::{
    assert_private_request, context, exchange_id, perp_symbol_scope, spawn_rest_server,
    symbol_scope,
};
use super::{CryptoComGatewayAdapter, CryptoComGatewayConfig};

#[tokio::test]
async fn cryptocom_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = CryptoComGatewayAdapter::default_public().expect("adapter");
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
fn cryptocom_signing_vector_fixture_should_verify_json_rpc_hmac() {
    signing_vector("private_create_order_limit.json")
        .verify()
        .expect("signing vector");
}

#[tokio::test]
async fn cryptocom_request_spec_fixture_should_cover_private_place_order() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "result": {"order_id": "2001", "client_oid": "CLIENTLIMIT1"}
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

    request_spec("private_create_order_limit.json")
        .assert_matches(&seen.lock().unwrap()[0].actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn cryptocom_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "result": {"order_id": "2001", "client_oid": "CLIENTLIMIT1"}}),
        json!({"code": 0, "result": {"order_id": "2002", "client_oid": "CLIENTQUOTE1"}}),
        json!({
            "code": 0,
            "result": {
                "order_info": {
                    "order_id": "2001",
                    "client_oid": "CLIENTLIMIT1",
                    "instrument_name": "BTC_USDT",
                    "side": "BUY",
                    "order_type": "LIMIT",
                    "time_in_force": "GOOD_TILL_CANCEL",
                    "status": "ACTIVE",
                    "quantity": "0.02",
                    "limit_price": "65000",
                    "cumulative_quantity": "0"
                }
            }
        }),
        json!({"code": 0, "result": {"order_id": "2001", "client_oid": "CLIENTLIMIT1"}}),
        json!({"code": 0, "result": {"order_id": "2001", "client_oid": "CLIENTLIMIT1"}}),
        json!({"code": 0, "result": {"data": [{"order_id": "2003"}, {"order_id": "2004"}]}}),
    ])
    .await;
    let adapter = CryptoComGatewayAdapter::new(CryptoComGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..CryptoComGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_place_order);
    assert!(capabilities.supports_cancel_order);
    assert!(capabilities.supports_cancel_all_orders);
    assert!(capabilities.supports_quote_market_order);
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);
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
        Some(10)
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_place_orders
            .supports_partial_failure
    );
    assert!(matches!(
        capabilities.capabilities_v2.stream_runtime.private,
        CapabilitySupport::Native
    ));
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .auth
            .requires_relogin_on_reconnect
    );
    assert!(capabilities.supports_positions);
    assert!(capabilities.supports_amend_order);
    assert!(capabilities.supports_order_list);

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
    assert_eq!(placed.order.status, OrderStatus::New);
    assert_eq!(placed.order.quantity, "0.02");

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

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend-order"),
            symbol: symbol_scope(),
            client_order_id: Some("CLIENTLIMIT1".to_string()),
            exchange_order_id: Some("2001".to_string()),
            new_client_order_id: None,
            new_quantity: "0.015".to_string(),
        })
        .await
        .expect("amend order");
    assert_eq!(amended.order.exchange_order_id.as_deref(), Some("2001"));
    assert_eq!(
        amended.order.client_order_id.as_deref(),
        Some("CLIENTLIMIT1")
    );
    assert_eq!(amended.order.quantity, "0.015");
    assert_eq!(amended.order.price.as_deref(), Some("65000"));

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
    assert_eq!(requests.len(), 6);
    assert_private_request(&requests[0], "private/create-order", "key");
    assert_eq!(
        requests[0].body.as_ref().unwrap()["params"]["instrument_name"],
        "BTC_USDT"
    );
    assert_eq!(
        requests[0].body.as_ref().unwrap()["params"]["type"],
        "LIMIT"
    );
    assert_eq!(
        requests[0].body.as_ref().unwrap()["params"]["client_oid"],
        "CLIENTLIMIT1"
    );
    assert_private_request(&requests[1], "private/create-order", "key");
    assert_eq!(
        requests[1].body.as_ref().unwrap()["params"]["notional"],
        "25.5"
    );
    assert_private_request(&requests[2], "private/get-order-detail", "key");
    assert_eq!(
        requests[2].body.as_ref().unwrap()["params"]["order_id"],
        "2001"
    );
    assert_private_request(&requests[3], "private/amend-order", "key");
    assert_eq!(
        requests[3].body.as_ref().unwrap()["params"]["new_price"],
        "65000"
    );
    assert_eq!(
        requests[3].body.as_ref().unwrap()["params"]["new_quantity"],
        "0.015"
    );
    assert_private_request(&requests[4], "private/cancel-order", "key");
    assert_private_request(&requests[5], "private/cancel-all-orders", "key");
}

#[tokio::test]
async fn cryptocom_adapter_should_route_advanced_order_lists() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "result": {"list_id": "oco-1"}}),
        json!({"code": 0, "result": {"list_id": "oto-1"}}),
    ])
    .await;
    let adapter = CryptoComGatewayAdapter::new(CryptoComGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..CryptoComGatewayConfig::default()
    })
    .expect("adapter");

    let oco = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oco"),
            symbol: symbol_scope(),
            list_client_order_id: Some("OCO1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some("70000".to_string()),
                stop_price: None,
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("OCO-LIMIT".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLoss,
                price: None,
                stop_price: Some("60000".to_string()),
                time_in_force: None,
                client_order_id: Some("OCO-STOP".to_string()),
            },
        })
        .await
        .expect("oco");
    assert_eq!(oco.order_list_id.as_deref(), Some("oco-1"));

    let oto = adapter
        .place_order_list(OrderListRequest::Oto {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oto"),
            symbol: symbol_scope(),
            list_client_order_id: Some("OTO1".to_string()),
            working: OrderListOrderLeg {
                side: OrderSide::Buy,
                order_type: OrderListLegType::Limit,
                quantity: "0.01".to_string(),
                price: Some("65000".to_string()),
                stop_price: None,
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("OTO-WORK".to_string()),
            },
            pending: OrderListOrderLeg {
                side: OrderSide::Sell,
                order_type: OrderListLegType::TakeProfitLimit,
                quantity: "0.01".to_string(),
                price: Some("72000".to_string()),
                stop_price: Some("71000".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("OTO-TP".to_string()),
            },
        })
        .await
        .expect("oto");
    assert_eq!(oto.order_list_id.as_deref(), Some("oto-1"));

    let requests = seen.lock().unwrap().clone();
    assert_private_request(&requests[0], "private/advanced/create-oco", "key");
    assert_eq!(
        requests[0].body.as_ref().unwrap()["params"]["order_list"][0]["exec_inst"],
        json!(["POST_ONLY"])
    );
    assert_eq!(
        requests[0].body.as_ref().unwrap()["params"]["order_list"][1]["ref_price"],
        "60000"
    );
    assert_private_request(&requests[1], "private/advanced/create-oto", "key");
    assert_eq!(
        requests[1].body.as_ref().unwrap()["params"]["order_list"][1]["type"],
        "TAKE_PROFIT_LIMIT"
    );
    assert_eq!(
        requests[1].body.as_ref().unwrap()["params"]["order_list"][1]["ref_price"],
        "71000"
    );
}

#[tokio::test]
async fn cryptocom_adapter_should_route_native_batch_order_lists() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "result": [
                {"code": 0, "index": 0, "client_oid": "BATCH1", "order_id": "3001"},
                {"code": 306, "index": 1, "client_oid": "BATCH2", "message": "INSUFFICIENT_AVAILABLE_BALANCE", "order_id": "3002"}
            ]
        }),
        json!({
            "code": 0,
            "result": [
                {"code": 0, "index": 0, "client_oid": "BATCH1", "order_id": "3001"},
                {"code": 212, "index": 1, "client_oid": "BATCH2", "order_id": "3002", "message": "INVALID_ORDERID"}
            ]
        }),
    ])
    .await;
    let adapter = CryptoComGatewayAdapter::new(CryptoComGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..CryptoComGatewayConfig::default()
    })
    .expect("adapter");

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
                    order_type: OrderType::Market,
                    time_in_force: None,
                    quantity: "0.02".to_string(),
                    price: None,
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("batch place");
    assert_eq!(batch.orders[0].exchange_order_id.as_deref(), Some("3001"));
    assert_eq!(batch.orders[0].status, OrderStatus::New);
    assert_eq!(batch.orders[1].status, OrderStatus::Rejected);

    let cancels = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-1"),
                    symbol: symbol_scope(),
                    client_order_id: Some("BATCH1".to_string()),
                    exchange_order_id: Some("3001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-2"),
                    symbol: symbol_scope(),
                    client_order_id: Some("BATCH2".to_string()),
                    exchange_order_id: Some("3002".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancels.cancelled_count, 1);
    assert_eq!(cancels.orders[0].status, OrderStatus::Cancelled);
    assert_eq!(cancels.orders[1].status, OrderStatus::Rejected);

    let requests = seen.lock().unwrap().clone();
    assert_private_request(&requests[0], "private/create-order-list", "key");
    assert_eq!(
        requests[0].body.as_ref().unwrap()["params"]["contingency_type"],
        "LIST"
    );
    assert_eq!(
        requests[0].body.as_ref().unwrap()["params"]["order_list"][0]["client_oid"],
        "BATCH1"
    );
    assert_private_request(&requests[1], "private/cancel-order-list", "key");
    assert_eq!(
        requests[1].body.as_ref().unwrap()["params"]["order_list"][1]["order_id"],
        "3002"
    );
}

#[tokio::test]
async fn cryptocom_adapter_should_parse_private_readback_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "result": {
                "data": [{
                    "position_balances": [
                        {"instrument_name": "BTC", "quantity": "0.6", "reserved_qty": "0.1", "max_withdrawal_balance": "0.5"},
                        {"instrument_name": "USDT", "quantity": "1025", "reserved_qty": "25", "max_withdrawal_balance": "1000"}
                    ]
                }]
            }
        }),
        json!({
            "code": 0,
            "result": {
                "data": [{
                    "account_id": "acc",
                    "quantity": "-0.1984",
                    "cost": "-10159.573500",
                    "open_position_pnl": "-497.743736",
                    "open_pos_cost": "-10159.352200",
                    "session_pnl": "2.236145",
                    "update_timestamp_ms": 1613552240770_i64,
                    "instrument_name": "BTCUSD-PERP",
                    "type": "PERPETUAL_SWAP"
                }]
            }
        }),
        json!({
            "code": 0,
            "result": {
                "order_info": {
                    "order_id": "1001",
                    "client_oid": "CID1001",
                    "instrument_name": "BTC_USDT",
                    "side": "BUY",
                    "order_type": "LIMIT",
                    "time_in_force": "GOOD_TILL_CANCEL",
                    "status": "ACTIVE",
                    "quantity": "0.01",
                    "limit_price": "65000",
                    "cumulative_quantity": "0.006",
                    "avg_price": "65010",
                    "create_time": 1743054550000_i64,
                    "update_time": 1743054551000_i64
                }
            }
        }),
        json!({
            "code": 0,
            "result": {
                "data": [{
                    "order_id": "1002",
                    "client_oid": "CID1002",
                    "instrument_name": "BTC_USDT",
                    "side": "SELL",
                    "order_type": "LIMIT",
                    "status": "ACTIVE",
                    "quantity": "0.02",
                    "limit_price": "70000",
                    "cumulative_quantity": "0"
                }]
            }
        }),
        json!({
            "code": 0,
            "result": {
                "effective_spot_maker_rate_bps": "6.5",
                "effective_spot_taker_rate_bps": "6.9"
            }
        }),
        json!({
            "code": 0,
            "result": {
                "data": [{
                    "trade_id": "9001",
                    "order_id": "1001",
                    "client_oid": "CID1001",
                    "instrument_name": "BTC_USDT",
                    "side": "BUY",
                    "traded_price": "65010",
                    "traded_quantity": "0.006",
                    "fees": "0.39",
                    "fee_instrument_name": "USDT",
                    "taker_side": "BUY",
                    "create_time": 1743054550000_i64
                }]
            }
        }),
    ])
    .await;
    let adapter = CryptoComGatewayAdapter::new(CryptoComGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..CryptoComGatewayConfig::default()
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
    assert_eq!(balances.balances[0].balances[0].asset, "BTC");
    assert_eq!(balances.balances[0].balances[0].locked, 0.1);

    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![perp_symbol_scope().exchange_symbol],
        })
        .await
        .expect("positions");
    assert_eq!(positions.positions.len(), 1);
    assert_eq!(
        positions.positions[0].side,
        rustcta_types::PositionSide::Short
    );
    assert_eq!(positions.positions[0].quantity, 0.1984);
    assert_eq!(positions.positions[0].unrealized_pnl, Some(-497.743736));

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
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.filled_quantity, "0.006");

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
    assert_eq!(open_orders.orders[0].side, OrderSide::Sell);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.00065");
    assert_eq!(fees.fees[0].taker_rate, "0.00069");

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
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("9001"));
    assert_eq!(fills.fills[0].price, 65010.0);
    assert_eq!(fills.fills[0].fee_amount, Some(0.39));
    assert_eq!(fills.fills[0].liquidity_role, LiquidityRole::Taker);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/private/user-balance");
    assert_eq!(requests[1].path, "/private/get-positions");
    assert_eq!(
        requests[1].body.as_ref().unwrap()["params"]["instrument_name"],
        "BTCUSD-PERP"
    );
    assert_eq!(requests[2].path, "/private/get-order-detail");
    assert_eq!(requests[3].path, "/private/get-open-orders");
    assert_eq!(requests[4].path, "/private/get-fee-rate");
    assert_eq!(requests[5].path, "/private/get-trades");
}

fn private_adapter(base_url: String) -> CryptoComGatewayAdapter {
    CryptoComGatewayAdapter::new(CryptoComGatewayConfig {
        rest_base_url: base_url,
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..CryptoComGatewayConfig::default()
    })
    .expect("adapter")
}

fn request_spec(name: &str) -> RequestSpec {
    let raw = match name {
        "private_create_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/cryptocom/request_specs/private_create_order_limit.json"
        ),
        _ => unreachable!("unknown cryptocom request spec"),
    };
    serde_json::from_str(raw).expect("request spec")
}

fn signing_vector(name: &str) -> SigningVector {
    let raw = match name {
        "private_create_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/cryptocom/signing_vectors/private_create_order_limit.json"
        ),
        _ => unreachable!("unknown cryptocom signing vector"),
    };
    serde_json::from_str(raw).expect("signing vector")
}
