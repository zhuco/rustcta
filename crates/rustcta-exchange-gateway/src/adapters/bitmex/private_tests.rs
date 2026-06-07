use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelAllOrdersRequest, CancelOrderRequest, ExchangeClient, FeesRequest, OpenOrdersRequest,
    PlaceOrderRequest, PositionsRequest, QueryOrderRequest, RecentFillsRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, TimeInForce};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private_parser::{
    parse_margin_balances, parse_order_state, parse_orders, parse_positions, parse_recent_fills,
};
use super::test_support::{
    assert_signed_bitmex_request, context, exchange_id, perp_symbol_scope, private_config,
    spawn_rest_server,
};
use super::BitmexGatewayAdapter;
use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

#[tokio::test]
async fn bitmex_adapter_should_send_signed_limit_order_request() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "orderID": "order-1",
        "clOrdID": "client-1",
        "symbol": "XBTUSD",
        "side": "Buy",
        "ordType": "Limit",
        "timeInForce": "GoodTillCancel",
        "ordStatus": "New",
        "orderQty": 100,
        "price": 99.5,
        "cumQty": 0,
        "execInst": "ParticipateDoNotInitiate,ReduceOnly",
        "timestamp": "2026-06-07T00:00:00.000Z"
    })])
    .await;
    let adapter = BitmexGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("client-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::PostOnly,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "100".to_string(),
            price: Some("99.5".to_string()),
            quote_quantity: None,
            reduce_only: true,
            post_only: true,
        })
        .await
        .expect("place");

    assert_eq!(response.order.exchange_order_id.as_deref(), Some("order-1"));
    assert_eq!(response.order.status, OrderStatus::New);
    assert!(response.order.post_only);
    assert!(response.order.reduce_only);
    let request = seen.lock().unwrap()[0].clone();
    assert_signed_bitmex_request(&request, "POST", "/api/v1/order");
    let body = request.body.expect("json body");
    assert_eq!(
        body.get("symbol").and_then(|value| value.as_str()),
        Some("XBTUSD")
    );
    assert_eq!(
        body.get("clOrdID").and_then(|value| value.as_str()),
        Some("client-1")
    );
    assert_eq!(
        body.get("orderQty").and_then(|value| value.as_f64()),
        Some(100.0)
    );
    assert_eq!(
        body.get("execInst").and_then(|value| value.as_str()),
        Some("ReduceOnly,ParticipateDoNotInitiate")
    );
}

#[tokio::test]
async fn bitmex_adapter_should_send_stop_market_stop_px() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "orderID": "stop-1",
        "clOrdID": "stop-client-1",
        "symbol": "XBTUSD",
        "side": "Sell",
        "ordType": "Stop",
        "ordStatus": "New",
        "orderQty": 10,
        "stopPx": 90.5,
        "cumQty": 0
    })])
    .await;
    let adapter = BitmexGatewayAdapter::new(private_config(base_url)).expect("adapter");

    adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place-stop"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("stop-client-1".to_string()),
            side: OrderSide::Sell,
            position_side: None,
            order_type: OrderType::StopMarket,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "10".to_string(),
            price: Some("90.5".to_string()),
            quote_quantity: None,
            reduce_only: true,
            post_only: false,
        })
        .await
        .expect("stop place");

    let request = seen.lock().unwrap()[0].clone();
    assert_signed_bitmex_request(&request, "POST", "/api/v1/order");
    let body = request.body.expect("json body");
    assert_eq!(
        body.get("ordType").and_then(|value| value.as_str()),
        Some("Stop")
    );
    assert_eq!(
        body.get("stopPx").and_then(|value| value.as_f64()),
        Some(90.5)
    );
    assert!(body.get("price").is_none());
}

#[tokio::test]
async fn bitmex_adapter_should_send_signed_cancel_and_query_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!([{
            "orderID": "order-1",
            "clOrdID": "client-1",
            "symbol": "XBTUSD",
            "side": "Sell",
            "ordType": "Limit",
            "ordStatus": "Canceled",
            "orderQty": 100,
            "price": 101,
            "cumQty": 0
        }]),
        json!([{
            "orderID": "order-1",
            "clOrdID": "client-1",
            "symbol": "XBTUSD",
            "side": "Sell",
            "ordType": "Limit",
            "ordStatus": "Canceled",
            "orderQty": 100,
            "price": 101,
            "cumQty": 0
        }]),
    ])
    .await;
    let adapter = BitmexGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let cancel = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
        })
        .await
        .expect("cancel");
    assert!(cancel.cancelled);

    let query = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
        })
        .await
        .expect("query");
    assert!(query.order.is_some());

    let requests = seen.lock().unwrap().clone();
    assert_signed_bitmex_request(&requests[0], "DELETE", "/api/v1/order");
    assert_eq!(
        requests[0].query.get("clOrdID").map(String::as_str),
        Some("client-1")
    );
    assert_signed_bitmex_request(&requests[1], "GET", "/api/v1/order");
    assert!(requests[1]
        .query
        .get("filter")
        .is_some_and(|value| value.contains("client-1") || value.contains("client%2D1")));
}

#[tokio::test]
async fn bitmex_adapter_should_send_native_batch_place_and_cancel_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!([
            {
                "orderID": "order-1",
                "clOrdID": "client-1",
                "symbol": "XBTUSD",
                "side": "Buy",
                "ordType": "Limit",
                "ordStatus": "New",
                "orderQty": 100,
                "price": 99.5,
                "cumQty": 0
            },
            {
                "orderID": "order-2",
                "clOrdID": "client-2",
                "symbol": "XBTUSD",
                "side": "Sell",
                "ordType": "Limit",
                "ordStatus": "New",
                "orderQty": 50,
                "price": 101.5,
                "cumQty": 0
            }
        ]),
        json!([
            {
                "orderID": "order-1",
                "clOrdID": "client-1",
                "symbol": "XBTUSD",
                "side": "Buy",
                "ordType": "Limit",
                "ordStatus": "Canceled",
                "orderQty": 100,
                "price": 99.5,
                "cumQty": 0
            }
        ]),
        json!([
            {
                "orderID": "order-2",
                "clOrdID": "client-2",
                "symbol": "XBTUSD",
                "side": "Sell",
                "ordType": "Limit",
                "ordStatus": "Canceled",
                "orderQty": 50,
                "price": 101.5,
                "cumQty": 0
            }
        ]),
    ])
    .await;
    let adapter = BitmexGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let first = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-1"),
        symbol: perp_symbol_scope(),
        client_order_id: Some("client-1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "100".to_string(),
        price: Some("99.5".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let mut second = first.clone();
    second.context = context("batch-place-2");
    second.client_order_id = Some("client-2".to_string());
    second.side = OrderSide::Sell;
    second.quantity = "50".to_string();
    second.price = Some("101.5".to_string());

    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: super::test_support::exchange_id(),
            orders: vec![first, second],
        })
        .await
        .expect("batch place");
    assert_eq!(placed.orders.len(), 2);

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: super::test_support::exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("order-1".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("client-2".to_string()),
                    exchange_order_id: None,
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 2);

    let requests = seen.lock().unwrap().clone();
    assert_signed_bitmex_request(&requests[0], "POST", "/api/v1/order/bulk");
    let body = requests[0].body.clone().expect("bulk body");
    assert_eq!(
        body.get("orders")
            .and_then(|value| value.as_array())
            .map(Vec::len),
        Some(2)
    );
    assert_signed_bitmex_request(&requests[1], "DELETE", "/api/v1/order");
    assert!(requests[1]
        .query
        .get("orderID")
        .is_some_and(|value| value.contains("order-1")));
    assert!(requests[1].query.get("clOrdID").is_none());
    assert_signed_bitmex_request(&requests[2], "DELETE", "/api/v1/order");
    assert!(requests[2]
        .query
        .get("clOrdID")
        .is_some_and(|value| value.contains("client-2")));
    assert!(requests[2].query.get("orderID").is_none());
}

#[tokio::test]
async fn bitmex_adapter_should_send_signed_amend_cancel_all_and_open_orders_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "orderID": "order-1",
            "clOrdID": "client-2",
            "symbol": "XBTUSD",
            "side": "Buy",
            "ordType": "Limit",
            "ordStatus": "New",
            "orderQty": 25,
            "price": 99.5,
            "cumQty": 0
        }),
        json!([{
            "orderID": "order-1",
            "clOrdID": "client-2",
            "symbol": "XBTUSD",
            "side": "Buy",
            "ordType": "Limit",
            "ordStatus": "Canceled",
            "orderQty": 25,
            "price": 99.5,
            "cumQty": 0
        }]),
        json!([{
            "orderID": "order-open",
            "clOrdID": "client-open",
            "symbol": "XBTUSD",
            "side": "Sell",
            "ordType": "Limit",
            "ordStatus": "New",
            "orderQty": 10,
            "price": 101.5,
            "cumQty": 0
        }]),
    ])
    .await;
    let adapter = BitmexGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: perp_symbol_scope(),
            exchange_order_id: Some("order-1".to_string()),
            client_order_id: None,
            new_client_order_id: Some("client-2".to_string()),
            new_quantity: "25".to_string(),
        })
        .await
        .expect("amend");
    assert_eq!(amended.order.client_order_id.as_deref(), Some("client-2"));

    let cancel_all = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
        })
        .await
        .expect("cancel all");
    assert_eq!(cancel_all.cancelled_count, 1);

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);

    let requests = seen.lock().unwrap().clone();
    assert_signed_bitmex_request(&requests[0], "PUT", "/api/v1/order");
    let amend_body = requests[0].body.clone().expect("amend body");
    assert_eq!(
        amend_body.get("orderID").and_then(|value| value.as_str()),
        Some("order-1")
    );
    assert_eq!(
        amend_body.get("clOrdID").and_then(|value| value.as_str()),
        Some("client-2")
    );
    assert_eq!(
        amend_body.get("orderQty").and_then(|value| value.as_f64()),
        Some(25.0)
    );
    assert_signed_bitmex_request(&requests[1], "DELETE", "/api/v1/order/all");
    assert_eq!(
        requests[1].query.get("symbol").map(String::as_str),
        Some("XBTUSD")
    );
    assert_signed_bitmex_request(&requests[2], "GET", "/api/v1/order");
    assert_eq!(
        requests[2].query.get("count").map(String::as_str),
        Some("100")
    );
    assert_eq!(
        requests[2].query.get("reverse").map(String::as_str),
        Some("true")
    );
    assert!(requests[2]
        .query
        .get("filter")
        .is_some_and(|value| value.contains("open") && value.contains("XBTUSD")));
}

#[tokio::test]
async fn bitmex_adapter_should_parse_balances_positions_fees_and_fills() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "currency": "XBt",
            "walletBalance": 100000000,
            "marginBalance": 100000000,
            "availableMargin": 75000000
        }),
        json!([{
            "symbol": "XBTUSD",
            "isOpen": true,
            "currentQty": -10,
            "avgEntryPrice": 100,
            "markPrice": 99,
            "liquidationPrice": 150,
            "leverage": 2
        }]),
        json!([{
            "symbol": "XBTUSD",
            "typ": "FFWCSX",
            "state": "Open",
            "underlying": "XBT",
            "quoteCurrency": "USD",
            "makerFee": -0.0001,
            "takerFee": 0.00075
        }]),
        json!([{
            "execID": "fill-1",
            "orderID": "order-1",
            "clOrdID": "client-1",
            "symbol": "XBTUSD",
            "side": "Buy",
            "execType": "Trade",
            "lastLiquidityInd": "RemovedLiquidity",
            "lastPx": 99.5,
            "lastQty": 5,
            "commission": 0.00075,
            "execComm": 3731,
            "settlCurrency": "XBt",
            "timestamp": "2026-06-07T00:00:00.000Z"
        }]),
    ])
    .await;
    let adapter = BitmexGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["BTC".to_string()],
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances[0].balances[0].asset, "BTC");
    assert_eq!(balances.balances[0].balances[0].total, 1.0);

    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![perp_symbol_scope().exchange_symbol],
        })
        .await
        .expect("positions");
    assert_eq!(positions.positions[0].quantity, 10.0);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "-0.0001");

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: super::test_support::exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
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
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("fill-1"));

    let requests = seen.lock().unwrap().clone();
    assert_signed_bitmex_request(&requests[0], "GET", "/api/v1/user/margin");
    assert_signed_bitmex_request(&requests[1], "GET", "/api/v1/position");
    assert_eq!(requests[2].path, "/api/v1/instrument/active");
    assert_signed_bitmex_request(&requests[3], "GET", "/api/v1/execution/tradeHistory");
}

#[test]
fn bitmex_parser_fixtures_should_cover_success_empty_error_and_missing_fields() {
    let exchange = exchange_id();
    let tenant_id = rustcta_types::TenantId::new("tenant").expect("tenant");
    let account_id = rustcta_types::AccountId::new("account").expect("account");
    let symbol = perp_symbol_scope();

    let instruments: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitmex/instruments.json"
    ))
    .expect("instruments fixture");
    let rules = parse_symbol_rules(&exchange, &instruments).expect("rules");
    assert_eq!(rules.len(), 2);
    assert!(rules
        .iter()
        .any(|rule| rule.symbol.market_type == MarketType::Spot));
    assert!(rules
        .iter()
        .any(|rule| rule.symbol.market_type == MarketType::Perpetual));

    let book_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitmex/orderbook_l2.json"
    ))
    .expect("book fixture");
    let book =
        parse_orderbook_snapshot(&exchange, symbol.clone(), &book_fixture).expect("orderbook");
    assert_eq!(book.bids[0].price, 99.5);
    assert_eq!(book.asks[0].price, 100.5);

    let balance_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitmex/margin.json"
    ))
    .expect("margin fixture");
    let balances = parse_margin_balances(
        &exchange,
        tenant_id.clone(),
        account_id.clone(),
        &["BTC".to_string()],
        &balance_fixture,
    )
    .expect("balances");
    assert_eq!(balances[0].balances[0].total, 1.0);

    let position_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitmex/position.json"
    ))
    .expect("position fixture");
    let positions = parse_positions(
        &exchange,
        tenant_id.clone(),
        account_id.clone(),
        &position_fixture,
    )
    .expect("positions");
    assert_eq!(positions[0].quantity, 10.0);

    let order_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitmex/order_ack.json"
    ))
    .expect("order fixture");
    let order = parse_order_state(&exchange, Some(&symbol), &order_fixture).expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("order-1"));

    let fills_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitmex/fill.json"
    ))
    .expect("fill fixture");
    let fills = parse_recent_fills(&exchange, tenant_id, account_id, &symbol, &fills_fixture)
        .expect("fills");
    assert_eq!(fills[0].fill_id.as_deref(), Some("fill-1"));

    let empty_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitmex/empty.json"
    ))
    .expect("empty fixture");
    assert!(parse_symbol_rules(&exchange, &empty_fixture)
        .expect("empty rules")
        .is_empty());
    assert!(parse_orders(&exchange, Some(&symbol), &empty_fixture)
        .expect("empty orders")
        .is_empty());

    let error_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitmex/error.json"
    ))
    .expect("error fixture");
    assert!(parse_symbol_rules(&exchange, &error_fixture).is_err());

    let missing_symbol_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitmex/order_missing_symbol.json"
    ))
    .expect("missing symbol fixture");
    assert!(parse_order_state(&exchange, None, &missing_symbol_fixture).is_err());

    let missing_price_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitmex/orderbook_missing_price.json"
    ))
    .expect("missing price fixture");
    assert!(parse_orderbook_snapshot(&exchange, symbol, &missing_price_fixture).is_err());
}

#[test]
fn bitmex_future_fixture_should_remain_outside_declared_product_support() {
    let exchange = exchange_id();
    let future_fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitmex/future_instrument.json"
    ))
    .expect("future fixture");
    let rules = parse_symbol_rules(&exchange, &future_fixture).expect("future rules");
    assert!(rules.is_empty());
}

#[test]
fn bitmex_task14_request_spec_and_signing_vector_fixtures_should_load() {
    let request_specs = [
        (
            "balances",
            include_str!("../../../../../tests/fixtures/exchanges/bitmex/request_specs/balances.json"),
        ),
        (
            "positions",
            include_str!("../../../../../tests/fixtures/exchanges/bitmex/request_specs/positions.json"),
        ),
        (
            "place_order",
            include_str!("../../../../../tests/fixtures/exchanges/bitmex/request_specs/place_order.json"),
        ),
        (
            "batch_place_orders",
            include_str!("../../../../../tests/fixtures/exchanges/bitmex/request_specs/batch_place_orders.json"),
        ),
        (
            "amend_order",
            include_str!("../../../../../tests/fixtures/exchanges/bitmex/request_specs/amend_order.json"),
        ),
        (
            "cancel_order",
            include_str!("../../../../../tests/fixtures/exchanges/bitmex/request_specs/cancel_order.json"),
        ),
        (
            "batch_cancel_orders",
            include_str!("../../../../../tests/fixtures/exchanges/bitmex/request_specs/batch_cancel_orders.json"),
        ),
        (
            "cancel_all_orders",
            include_str!("../../../../../tests/fixtures/exchanges/bitmex/request_specs/cancel_all_orders.json"),
        ),
        (
            "query_order",
            include_str!("../../../../../tests/fixtures/exchanges/bitmex/request_specs/query_order.json"),
        ),
        (
            "open_orders",
            include_str!("../../../../../tests/fixtures/exchanges/bitmex/request_specs/open_orders.json"),
        ),
        (
            "recent_fills",
            include_str!("../../../../../tests/fixtures/exchanges/bitmex/request_specs/recent_fills.json"),
        ),
    ];

    for (operation, raw) in request_specs {
        let spec: RequestSpec = serde_json::from_str(raw).expect("request spec fixture");
        assert_eq!(spec.exchange, "bitmex");
        assert_eq!(spec.operation, operation);
        assert_eq!(spec.transport.as_deref(), Some("rest"));
    }

    for raw in [
        include_str!(
            "../../../../../tests/fixtures/exchanges/bitmex/signing_vectors/rest_get_instrument.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bitmex/signing_vectors/rest_place_order.json"
        ),
        include_str!("../../../../../tests/fixtures/exchanges/bitmex/signing_vectors/ws_auth.json"),
    ] {
        let vector: SigningVector = serde_json::from_str(raw).expect("signing vector fixture");
        assert_eq!(vector.exchange, "bitmex");
        vector.verify().expect("signing vector verifies");
    }
}
