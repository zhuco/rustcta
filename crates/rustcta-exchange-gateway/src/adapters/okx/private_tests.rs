use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelAllOrdersRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest,
    OpenOrdersRequest, PlaceOrderRequest, PositionsRequest, QueryOrderRequest,
    QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

use super::test_support::{
    assert_signed_okx_request, assert_signed_okx_request_method, context, exchange_id,
    futures_symbol_scope, option_symbol_scope, perpetual_symbol_scope, private_config,
    spawn_rest_server, symbol_scope,
};
use super::{OkxGatewayAdapter, OkxGatewayConfig};

#[test]
fn okx_signing_vector_fixtures_should_verify() {
    for path in [
        "okx/signing_vectors/place_order.json",
        "okx/signing_vectors/query_order.json",
    ] {
        let vector = load_signing_vector(path);
        vector.verify().expect("fixture signature");
    }
}

#[tokio::test]
async fn okx_adapter_should_keep_private_operations_and_streams_unsupported() {
    let adapter = OkxGatewayAdapter::default_public().expect("adapter");
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

#[tokio::test]
async fn okx_adapter_should_sign_private_readback_requests_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "0",
            "msg": "",
            "data": [{
                "details": [
                    {"ccy": "BTC", "cashBal": "0.12", "availBal": "0.10", "frozenBal": "0.02"},
                    {"ccy": "USDT", "cashBal": "125.5", "availBal": "125.5", "frozenBal": "0"}
                ]
            }]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{
                "instId": "BTC-USDT",
                "ordId": "1001",
                "clOrdId": "CLIENT1",
                "side": "buy",
                "ordType": "limit",
                "state": "partially_filled",
                "sz": "0.01",
                "px": "65000",
                "accFillSz": "0.004",
                "avgPx": "64950",
                "cTime": "1710000000000",
                "uTime": "1710000000123"
            }]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{
                "instId": "BTC-USDT",
                "ordId": "1002",
                "clOrdId": "CLIENT2",
                "side": "sell",
                "ordType": "post_only",
                "state": "live",
                "sz": "0.02",
                "px": "70000",
                "accFillSz": "0",
                "avgPx": "",
                "cTime": "1710000001000",
                "uTime": "1710000001000"
            }]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{"instId": "BTC-USDT", "maker": "-0.0008", "taker": "-0.001"}]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{
                "instId": "BTC-USDT",
                "tradeId": "fill-1",
                "ordId": "1001",
                "clOrdId": "CLIENT1",
                "side": "buy",
                "fillPx": "64950",
                "fillSz": "0.004",
                "feeCcy": "USDT",
                "fee": "-0.2598",
                "execType": "T",
                "fillTime": "1710000000200"
            }]
        }),
    ])
    .await;
    let adapter = OkxGatewayAdapter::new(private_config(base_url)).expect("adapter");
    assert!(adapter.capabilities().supports_private_rest);
    assert!(adapter.capabilities().supports_balances);
    assert!(adapter.capabilities().supports_query_order);

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["BTC".to_string()],
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances.len(), 1);
    assert_eq!(balances.balances[0].balances.len(), 1);
    assert_eq!(balances.balances[0].balances[0].asset, "BTC");
    assert_eq!(balances.balances[0].balances[0].available, 0.10);
    assert_eq!(balances.balances[0].balances[0].locked, 0.02);

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-order"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("query order")
        .order
        .expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("1001"));
    assert_eq!(order.client_order_id.as_deref(), Some("CLIENT1"));
    assert_eq!(order.side, OrderSide::Buy);
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.quantity, "0.01");
    assert_eq!(order.filled_quantity, "0.004");
    assert_eq!(order.average_fill_price.as_deref(), Some("64950"));

    let open_orders = adapter
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
    assert_eq!(open_orders.orders.len(), 1);
    assert_eq!(
        open_orders.orders[0].exchange_order_id.as_deref(),
        Some("1002")
    );
    assert!(open_orders.orders[0].post_only);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees.len(), 1);
    assert_eq!(fees.fees[0].maker_rate, "-0.0008");
    assert_eq!(fees.fees[0].taker_rate, "-0.001");

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: Some("CLIENT1".to_string()),
            exchange_order_id: Some("1001".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(50),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("fill-1"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("1001"));
    assert_eq!(fills.fills[0].client_order_id.as_deref(), Some("CLIENT1"));
    assert_eq!(fills.fills[0].side, OrderSide::Buy);
    assert_eq!(fills.fills[0].liquidity_role, LiquidityRole::Taker);
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("USDT"));
    assert_eq!(fills.fills[0].fee_amount, Some(0.2598));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    load_request_spec("okx/request_specs/get_balances.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("request spec");
    assert_signed_okx_request(&requests[0], "/api/v5/account/balance");
    assert!(requests[0].query.is_empty());

    assert_signed_okx_request(&requests[1], "/api/v5/trade/order");
    load_request_spec("okx/request_specs/query_order.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("request spec");
    assert_eq!(
        requests[1].query.get("instId").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_eq!(
        requests[1].query.get("ordId").map(String::as_str),
        Some("1001")
    );

    assert_signed_okx_request(&requests[2], "/api/v5/trade/orders-pending");
    load_request_spec("okx/request_specs/get_open_orders.json")
        .assert_matches(&requests[2].actual_http_request())
        .expect("request spec");
    assert_eq!(
        requests[2].query.get("instType").map(String::as_str),
        Some("SPOT")
    );
    assert_eq!(
        requests[2].query.get("instId").map(String::as_str),
        Some("BTC-USDT")
    );

    assert_signed_okx_request(&requests[3], "/api/v5/account/trade-fee");
    load_request_spec("okx/request_specs/get_fees.json")
        .assert_matches(&requests[3].actual_http_request())
        .expect("request spec");
    assert_eq!(
        requests[3].query.get("instType").map(String::as_str),
        Some("SPOT")
    );
    assert_eq!(
        requests[3].query.get("instId").map(String::as_str),
        Some("BTC-USDT")
    );

    assert_signed_okx_request(&requests[4], "/api/v5/trade/fills-history");
    load_request_spec("okx/request_specs/get_recent_fills.json")
        .assert_matches(&requests[4].actual_http_request())
        .expect("request spec");
    assert_eq!(
        requests[4].query.get("instType").map(String::as_str),
        Some("SPOT")
    );
    assert_eq!(
        requests[4].query.get("instId").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_eq!(
        requests[4].query.get("ordId").map(String::as_str),
        Some("1001")
    );
    assert_eq!(
        requests[4].query.get("limit").map(String::as_str),
        Some("50")
    );
}

#[tokio::test]
async fn okx_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "0",
            "msg": "",
            "data": [{"ordId": "2001", "clOrdId": "LIMIT1", "sCode": "0", "sMsg": ""}]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{"ordId": "2002", "clOrdId": "QUOTE1", "sCode": "0", "sMsg": ""}]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{"ordId": "2001", "clOrdId": "LIMIT1", "sCode": "0", "sMsg": ""}]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{
                "instId": "BTC-USDT",
                "ordId": "2003",
                "clOrdId": "CANCELALL1",
                "side": "sell",
                "ordType": "limit",
                "state": "live",
                "px": "70000",
                "sz": "0.02",
                "accFillSz": "0"
            }]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{"ordId": "2003", "clOrdId": "CANCELALL1", "sCode": "0", "sMsg": ""}]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{"ordId": "2004", "clOrdId": "AMENDNEW", "sCode": "0", "sMsg": ""}]
        }),
    ])
    .await;
    let adapter = OkxGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_place_order);
    assert!(capabilities.supports_cancel_order);
    assert!(capabilities.supports_cancel_all_orders);
    assert!(capabilities.supports_quote_market_order);
    assert!(capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place-order"),
            symbol: symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
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
    assert_eq!(placed.order.client_order_id.as_deref(), Some("LIMIT1"));

    let quote = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote-order"),
            symbol: symbol_scope(),
            client_order_id: Some("QUOTE1".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "125.5".to_string(),
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
            client_order_id: Some("LIMIT1".to_string()),
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
    assert_eq!(cancel_all.cancelled_count, 1);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend-order"),
            symbol: symbol_scope(),
            client_order_id: Some("AMENDOLD".to_string()),
            exchange_order_id: Some("2004".to_string()),
            new_client_order_id: Some("AMENDNEW".to_string()),
            new_quantity: "0.015".to_string(),
        })
        .await
        .expect("amend order");
    assert_eq!(amended.order.exchange_order_id.as_deref(), Some("2004"));
    assert_eq!(amended.order.client_order_id.as_deref(), Some("AMENDNEW"));
    assert_eq!(amended.order.quantity, "0.015");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 6);

    assert_signed_okx_request_method(&requests[0], "POST", "/api/v5/trade/order");
    load_request_spec("okx/request_specs/place_order.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("request spec");
    let body = requests[0].body.as_ref().expect("place body");
    assert_eq!(body["instId"], "BTC-USDT");
    assert_eq!(body["tdMode"], "cash");
    assert_eq!(body["side"], "buy");
    assert_eq!(body["ordType"], "limit");
    assert_eq!(body["sz"], "0.02");
    assert_eq!(body["px"], "65000");
    assert_eq!(body["clOrdId"], "LIMIT1");

    assert_signed_okx_request_method(&requests[1], "POST", "/api/v5/trade/order");
    load_request_spec("okx/request_specs/place_quote_market_order.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("request spec");
    let body = requests[1].body.as_ref().expect("quote body");
    assert_eq!(body["ordType"], "market");
    assert_eq!(body["sz"], "125.5");
    assert_eq!(body["tgtCcy"], "quote_ccy");
    assert_eq!(body["clOrdId"], "QUOTE1");

    assert_signed_okx_request_method(&requests[2], "POST", "/api/v5/trade/cancel-order");
    load_request_spec("okx/request_specs/cancel_order.json")
        .assert_matches(&requests[2].actual_http_request())
        .expect("request spec");
    let body = requests[2].body.as_ref().expect("cancel body");
    assert_eq!(body["instId"], "BTC-USDT");
    assert_eq!(body["ordId"], "2001");
    assert_eq!(body["clOrdId"], "LIMIT1");

    assert_signed_okx_request(&requests[3], "/api/v5/trade/orders-pending");
    assert_eq!(
        requests[3].query.get("instId").map(String::as_str),
        Some("BTC-USDT")
    );

    assert_signed_okx_request_method(&requests[4], "POST", "/api/v5/trade/cancel-batch-orders");
    load_request_spec("okx/request_specs/cancel_all_orders.json")
        .assert_matches(&requests[4].actual_http_request())
        .expect("request spec");
    let body = requests[4].body.as_ref().unwrap().as_array().unwrap();
    assert_eq!(body.len(), 1);
    assert_eq!(body[0]["instId"], "BTC-USDT");
    assert_eq!(body[0]["ordId"], "2003");

    assert_signed_okx_request_method(&requests[5], "POST", "/api/v5/trade/amend-order");
    load_request_spec("okx/request_specs/amend_order.json")
        .assert_matches(&requests[5].actual_http_request())
        .expect("request spec");
    let body = requests[5].body.as_ref().expect("amend body");
    assert_eq!(body["instId"], "BTC-USDT");
    assert_eq!(body["ordId"], "2004");
    assert_eq!(body["clOrdId"], "AMENDOLD");
    assert_eq!(body["newClOrdId"], "AMENDNEW");
    assert_eq!(body["newSz"], "0.015");
}

#[tokio::test]
async fn okx_adapter_should_route_native_batch_place_and_cancel_with_partial_reports() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "0",
            "msg": "",
            "data": [
                {"ordId": "5001", "clOrdId": "BATCH1", "sCode": "0", "sMsg": ""},
                {"ordId": "", "clOrdId": "BATCH2", "sCode": "51008", "sMsg": "Insufficient balance"}
            ]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [
                {"ordId": "5001", "clOrdId": "BATCH1", "sCode": "0", "sMsg": ""},
                {"ordId": "5002", "clOrdId": "BATCH2", "sCode": "51603", "sMsg": "Order does not exist"}
            ]
        }),
    ])
    .await;
    let adapter = OkxGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);

    let first_order = PlaceOrderRequest {
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
    };
    let second_order = PlaceOrderRequest {
        client_order_id: Some("BATCH2".to_string()),
        price: Some("64000".to_string()),
        ..first_order.clone()
    };

    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![first_order, second_order],
        })
        .await
        .expect("batch place");
    assert_eq!(placed.orders.len(), 1);
    let place_report = placed.report.expect("place report");
    assert_eq!(place_report.total_items, 2);
    assert_eq!(place_report.succeeded_count(), 1);
    assert_eq!(place_report.failed_count(), 1);
    assert!(place_report.requires_reconciliation());
    assert_eq!(
        place_report.results[1].client_order_id.as_deref(),
        Some("BATCH2")
    );

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
                    client_order_id: Some("BATCH1".to_string()),
                    exchange_order_id: Some("5001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-2"),
                    symbol: symbol_scope(),
                    client_order_id: Some("BATCH2".to_string()),
                    exchange_order_id: Some("5002".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 1);
    let cancel_report = cancelled.report.expect("cancel report");
    assert_eq!(cancel_report.total_items, 2);
    assert_eq!(cancel_report.succeeded_count(), 1);
    assert_eq!(cancel_report.failed_count(), 1);
    assert!(cancel_report.requires_reconciliation());

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_signed_okx_request_method(&requests[0], "POST", "/api/v5/trade/batch-orders");
    load_request_spec("okx/request_specs/batch_place_orders.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("batch place request spec");
    assert_signed_okx_request_method(&requests[1], "POST", "/api/v5/trade/cancel-batch-orders");
    load_request_spec("okx/request_specs/batch_cancel_orders.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("batch cancel request spec");
}

fn load_request_spec(path: &str) -> RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("request spec fixture");
    serde_json::from_str(&text).expect("request spec fixture")
}

fn load_signing_vector(path: &str) -> SigningVector {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("signing vector fixture");
    serde_json::from_str(&text).expect("signing vector fixture")
}

#[tokio::test]
async fn okx_adapter_should_place_perpetual_order_as_swap() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{"ordId": "3001", "clOrdId": "PERP1", "sCode": "0", "sMsg": ""}]
    })])
    .await;
    let adapter = OkxGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-place"),
            symbol: perpetual_symbol_scope(),
            client_order_id: Some("PERP1".to_string()),
            side: OrderSide::Sell,
            position_side: Some(rustcta_types::PositionSide::Net),
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "1".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: true,
            post_only: false,
        })
        .await
        .expect("perp place");

    assert_eq!(response.order.market_type, MarketType::Perpetual);
    assert!(response.order.reduce_only);
    let request = seen.lock().unwrap()[0].clone();
    assert_signed_okx_request_method(&request, "POST", "/api/v5/trade/order");
    let body = request.body.as_ref().expect("place body");
    assert_eq!(body["instId"], "BTC-USDT-SWAP");
    assert_eq!(body["tdMode"], "cross");
    assert_eq!(body["reduceOnly"], true);
    assert_eq!(body["posSide"], "net");
    assert!(body.get("tgtCcy").is_none());
}

#[tokio::test]
async fn okx_adapter_should_load_futures_positions_and_place_futures_order() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "0",
            "msg": "",
            "data": [{
                "instType": "FUTURES",
                "instId": "BTC-USD-240329",
                "posSide": "net",
                "pos": "-2",
                "avgPx": "65000",
                "markPx": "64000",
                "liqPx": "70000",
                "upl": "2000",
                "lever": "10"
            }]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{"ordId": "4001", "clOrdId": "FUT1", "sCode": "0", "sMsg": ""}]
        }),
    ])
    .await;
    let adapter = OkxGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let futures_symbol = futures_symbol_scope();

    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Futures),
            symbols: vec![futures_symbol.exchange_symbol.clone()],
        })
        .await
        .expect("positions");

    assert_eq!(positions.positions.len(), 1);
    assert_eq!(positions.positions[0].market_type, MarketType::Futures);
    assert_eq!(
        positions.positions[0].side,
        rustcta_types::PositionSide::Short
    );
    assert_eq!(positions.positions[0].quantity, 2.0);
    assert_eq!(positions.positions[0].entry_price, Some(65000.0));
    assert_eq!(positions.positions[0].mark_price, Some(64000.0));
    assert_eq!(positions.positions[0].leverage, Some(10.0));

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-place"),
            symbol: futures_symbol.clone(),
            client_order_id: Some("FUT1".to_string()),
            side: OrderSide::Buy,
            position_side: Some(rustcta_types::PositionSide::Net),
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "1".to_string(),
            price: Some("64500".to_string()),
            quote_quantity: None,
            reduce_only: true,
            post_only: false,
        })
        .await
        .expect("futures place");

    assert_eq!(placed.order.market_type, MarketType::Futures);
    assert!(placed.order.reduce_only);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);

    assert_signed_okx_request(&requests[0], "/api/v5/account/positions");
    load_request_spec("okx/request_specs/get_positions_futures.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("positions request spec");
    assert_eq!(
        requests[0].query.get("instType").map(String::as_str),
        Some("FUTURES")
    );
    assert_eq!(
        requests[0].query.get("instId").map(String::as_str),
        Some("BTC-USD-240329")
    );

    assert_signed_okx_request_method(&requests[1], "POST", "/api/v5/trade/order");
    load_request_spec("okx/request_specs/place_order_futures.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("futures place request spec");
    let body = requests[1].body.as_ref().expect("futures place body");
    assert_eq!(body["instId"], "BTC-USD-240329");
    assert_eq!(body["tdMode"], "cross");
    assert_eq!(body["side"], "buy");
    assert_eq!(body["ordType"], "limit");
    assert_eq!(body["sz"], "1");
    assert_eq!(body["px"], "64500");
    assert_eq!(body["reduceOnly"], true);
    assert_eq!(body["posSide"], "net");
}

#[tokio::test]
async fn okx_adapter_should_load_option_positions_and_place_option_order() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "0",
            "msg": "",
            "data": [{
                "instType": "OPTION",
                "instId": "BTC-USD-240329-65000-C",
                "posSide": "long",
                "pos": "1",
                "avgPx": "0.08",
                "markPx": "0.09",
                "upl": "0.01",
                "lever": "1"
            }]
        }),
        json!({
            "code": "0",
            "msg": "",
            "data": [{"ordId": "6001", "clOrdId": "OPT1", "sCode": "0", "sMsg": ""}]
        }),
    ])
    .await;
    let adapter = OkxGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let option_symbol = option_symbol_scope();

    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("option-positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Option),
            symbols: vec![option_symbol.exchange_symbol.clone()],
        })
        .await
        .expect("positions");

    assert_eq!(positions.positions.len(), 1);
    assert_eq!(positions.positions[0].market_type, MarketType::Option);
    assert_eq!(
        positions.positions[0].side,
        rustcta_types::PositionSide::Long
    );
    assert_eq!(positions.positions[0].quantity, 1.0);
    assert_eq!(positions.positions[0].entry_price, Some(0.08));

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("option-place"),
            symbol: option_symbol.clone(),
            client_order_id: Some("OPT1".to_string()),
            side: OrderSide::Buy,
            position_side: Some(rustcta_types::PositionSide::Long),
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "1".to_string(),
            price: Some("0.09".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("option place");

    assert_eq!(placed.order.market_type, MarketType::Option);
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("6001"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);

    assert_signed_okx_request(&requests[0], "/api/v5/account/positions");
    load_request_spec("okx/request_specs/get_positions_option.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("option positions request spec");
    assert_eq!(
        requests[0].query.get("instType").map(String::as_str),
        Some("OPTION")
    );
    assert_eq!(
        requests[0].query.get("instId").map(String::as_str),
        Some("BTC-USD-240329-65000-C")
    );

    assert_signed_okx_request_method(&requests[1], "POST", "/api/v5/trade/order");
    load_request_spec("okx/request_specs/place_order_option.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("option place request spec");
    let body = requests[1].body.as_ref().expect("option place body");
    assert_eq!(body["instId"], "BTC-USD-240329-65000-C");
    assert_eq!(body["tdMode"], "cross");
    assert_eq!(body["posSide"], "long");
    assert_eq!(body["px"], "0.09");
}

#[tokio::test]
async fn okx_adapter_should_return_unsupported_when_private_credentials_are_missing() {
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig {
        enabled_private_rest: true,
        ..OkxGatewayConfig::default()
    })
    .expect("adapter");
    assert!(!adapter.capabilities().supports_private_rest);
    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("missing-creds"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect_err("missing credentials should be unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}
