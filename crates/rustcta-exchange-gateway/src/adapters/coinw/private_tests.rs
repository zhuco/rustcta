use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchExecutionMode, BatchPlaceOrdersRequest,
    CancelOrderRequest, CapabilitySupport, ExchangeApiError, ExchangeClient, FeesRequest,
    OpenOrdersRequest, PlaceOrderRequest, PositionsRequest, QueryOrderRequest,
    QuoteMarketOrderRequest, RecentFillsRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeErrorClass, MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::private_parser::{parse_balances, parse_positions};
use super::test_support::{
    context, exchange_id, perp_symbol_scope, spawn_rest_server, spawn_rest_server_with_status,
    spot_symbol_scope,
};
use super::{CoinwGatewayAdapter, CoinwGatewayConfig};

fn private_config(base_url: String) -> CoinwGatewayConfig {
    CoinwGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..CoinwGatewayConfig::default()
    }
}

#[tokio::test]
async fn coinw_adapter_should_sign_and_parse_spot_balances() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "200",
        "data": {
            "BTC": {"available": "1.2", "onOrders": "0.3"},
            "USDT": {"available": "50", "onOrders": "10"}
        },
        "msg": "SUCCESS",
        "failed": false
    })])
    .await;
    let adapter = CoinwGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["BTC".to_string()],
        })
        .await
        .expect("balances");

    assert_eq!(response.balances.len(), 1);
    assert_eq!(response.balances[0].balances[0].asset, "BTC");
    assert_eq!(response.balances[0].balances[0].total, 1.5);
    assert_eq!(response.balances[0].balances[0].available, 1.2);
    assert_eq!(response.balances[0].balances[0].locked, 0.3);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/api/v1/private");
    assert_eq!(
        request.query.get("command").map(String::as_str),
        Some("returnCompleteBalances")
    );
    assert_eq!(
        request.query.get("api_key").map(String::as_str),
        Some("key")
    );
    assert!(request.query.contains_key("sign"));
    assert!(!request.query.values().any(|value| value.contains("secret")));
}

#[tokio::test]
async fn coinw_adapter_should_route_spot_order_lifecycle() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": "200", "data": {"orderNumber": "1001"}, "msg": "SUCCESS", "failed": false}),
        json!({"code": "200", "data": [
            {
                "orderNumber": "1001",
                "currencyPair": "BTC_USDT",
                "type": "buy",
                "total": "0.1",
                "startingAmount": "100",
                "prize": "1000",
                "success_count": "0.02",
                "status": "2",
                "date": "2026-06-07 00:00:00"
            }
        ], "msg": "SUCCESS", "failed": false}),
        json!({"code": "200", "data": {
            "orderNumber": "1001",
            "currencyPair": "BTC_USDT",
            "type": "buy",
            "total": "0.1",
            "prize": "1000",
            "success_count": "0.1",
            "status": "3",
            "date": "2026-06-07 00:00:00"
        }, "msg": "SUCCESS", "failed": false}),
        json!({"code": "200", "data": [
            {
                "tradeID": "t1",
                "orderNumber": "1001",
                "currencyPair": "BTC_USDT",
                "type": "buy",
                "amount": "0.1",
                "total": "100",
                "prize": "1000",
                "fee": "0.001",
                "date": "2026-06-07 00:00:00",
                "status": "3"
            }
        ], "msg": "SUCCESS", "failed": false}),
        json!({"code": "200", "data": {"clientOrderId": "cid-1"}, "msg": "SUCCESS", "failed": false}),
    ])
    .await;
    let adapter = CoinwGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let symbol = spot_symbol_scope();

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol.clone(),
            client_order_id: Some("cid-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "0.1".to_string(),
            price: Some("1000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("1001"));

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol.clone()),
            page: None,
        })
        .await
        .expect("open");
    assert_eq!(open.orders[0].status, OrderStatus::PartiallyFilled);

    let queried = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol.clone(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("query");
    assert_eq!(queried.order.expect("order").status, OrderStatus::Filled);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol.clone()),
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
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("t1"));
    assert_eq!(fills.fills[0].fee_amount, Some(0.001));

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol,
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);

    let seen = seen.lock().unwrap();
    assert_eq!(
        seen[0].query.get("command").map(String::as_str),
        Some("doTrade")
    );
    assert_eq!(
        seen[1].query.get("command").map(String::as_str),
        Some("returnOpenOrders")
    );
    assert_eq!(
        seen[2].query.get("command").map(String::as_str),
        Some("returnOrderStatus")
    );
    assert_eq!(
        seen[3].query.get("command").map(String::as_str),
        Some("returnUTradeHistory")
    );
    assert_eq!(
        seen[4].query.get("command").map(String::as_str),
        Some("cancelOrder")
    );
}

#[tokio::test]
async fn coinw_adapter_should_route_quote_market_order() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "200",
        "data": {"orderNumber": "2001"},
        "msg": "SUCCESS",
        "failed": false
    })])
    .await;
    let adapter = CoinwGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("quote-1".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25".to_string(),
        })
        .await
        .expect("quote order");

    assert_eq!(response.order.exchange_order_id.as_deref(), Some("2001"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(
        request.query.get("isMarket").map(String::as_str),
        Some("true")
    );
    assert_eq!(request.query.get("funds").map(String::as_str), Some("25"));
}

#[tokio::test]
async fn coinw_adapter_should_route_futures_private_rest() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"value": "f-1", "ts": 1775443378356_u64}, "msg": ""}),
        json!({"code": 0, "data": {
            "rows": [
                {
                    "id": "f-1",
                    "instrument": "BTC",
                    "direction": "long",
                    "currentPiece": "0.1",
                    "totalPiece": "0.1",
                    "orderPrice": "1000",
                    "orderStatus": "unFinish",
                    "positionModel": 0,
                    "quantity": "0.1",
                    "quantityUnit": 2,
                    "takerFee": "0.0006",
                    "primaryId": "p-1"
                }
            ]
        }, "msg": ""}),
        json!({"code": 0, "data": {
            "rows": [
                {
                    "orderId": "f-1",
                    "instrument": "BTC",
                    "direction": "long",
                    "fee": "0.01",
                    "feeRate": "0.0006",
                    "openPrice": "1000",
                    "baseSize": "0.1",
                    "orderStatus": "Finish",
                    "takerMaker": "1",
                    "thirdOrderId": "perp-1"
                }
            ]
        }, "msg": ""}),
    ])
    .await;
    let adapter = CoinwGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let symbol = perp_symbol_scope();

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-place"),
            symbol: symbol.clone(),
            client_order_id: Some("perp-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "0.1".to_string(),
            price: Some("1000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("perp place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("f-1"));

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol.clone()),
            page: None,
        })
        .await
        .expect("perp open");
    assert_eq!(open.orders[0].exchange_order_id.as_deref(), Some("f-1"));

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(20),
            page: None,
        })
        .await
        .expect("perp fills");
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("f-1"));
    assert_eq!(fills.fills[0].fee_rate, Some(0.0006));

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].path, "/v1/perpum/order");
    assert_eq!(
        seen[0].headers.get("api_key").map(String::as_str),
        Some("key")
    );
    assert!(seen[0].headers.contains_key("sign"));
    assert!(seen[0].headers.contains_key("timestamp"));
    assert!(seen[0].body.contains(r#""instrument":"BTC""#));
    assert_eq!(seen[1].method, "GET");
    assert_eq!(seen[1].path, "/v1/perpum/orders/open");
    assert_eq!(
        seen[1].query.get("instrument").map(String::as_str),
        Some("BTC")
    );
    assert_eq!(seen[2].path, "/v1/perpum/orders/deals");
}

#[tokio::test]
async fn coinw_adapter_should_parse_futures_balances_positions_and_fees() {
    let (base_url, _seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {
            "availableUsdt": "100.5",
            "alFreeze": "5.0",
            "alMargin": "105.5",
            "ts": 1775443378356_u64
        }, "msg": ""}),
        json!({"code": 0, "data": [
            {
                "base": "BTC",
                "baseSize": "0.25",
                "direction": "long",
                "leverage": "5",
                "openPrice": "1000",
                "profitUnreal": "12.5",
                "liquidationPrice": "500"
            }
        ], "msg": ""}),
        json!({"code": 0, "data": [
            {"base": "BTC", "makerFee": "0.0001", "takerFee": "0.0006"}
        ], "msg": ""}),
    ])
    .await;
    let adapter = CoinwGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let symbol = perp_symbol_scope();

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
    assert_eq!(balances.balances[0].balances[0].available, 100.5);
    assert_eq!(balances.balances[0].balances[0].locked, 5.0);

    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![symbol.exchange_symbol.clone()],
        })
        .await
        .expect("positions");
    assert_eq!(positions.positions[0].quantity, 0.25);
    assert_eq!(positions.positions[0].leverage, Some(5.0));

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.0001");
    assert_eq!(fees.fees[0].taker_rate, "0.0006");

    let requests = _seen.lock().unwrap().clone();
    assert_eq!(requests[1].method, "GET");
    assert_eq!(requests[1].path, "/v1/perpum/positions");
    assert_eq!(
        requests[1].query.get("instrument").map(String::as_str),
        Some("BTC")
    );
    assert_eq!(
        requests[1].headers.get("api_key").map(String::as_str),
        Some("key")
    );
    assert!(requests[1].headers.contains_key("timestamp"));
    assert!(requests[1].headers.contains_key("sign"));
    assert!(!requests[1]
        .headers
        .values()
        .any(|value| value.contains("secret")));
}

#[tokio::test]
async fn coinw_adapter_should_route_batch_place_and_cancel_fallbacks() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": "200", "data": {"orderNumber": "s-1"}, "msg": "SUCCESS", "failed": false}),
        json!({"code": 0, "data": {"value": "p-1"}, "msg": ""}),
        json!({"code": "200", "data": {"clientOrderId": "spot-cancel"}, "msg": "SUCCESS", "failed": false}),
        json!({"code": 0, "msg": ""}),
        json!({"code": 0, "msg": ""}),
        json!({"code": 0, "msg": ""}),
        json!({"code": 0, "msg": ""}),
        json!({"code": 0, "msg": ""}),
    ])
    .await;
    let adapter = CoinwGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-spot"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("spot-batch".to_string()),
                    side: OrderSide::Buy,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: None,
                    quantity: "0.1".to_string(),
                    price: Some("1000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-perp"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("perp-batch".to_string()),
                    side: OrderSide::Buy,
                    position_side: None,
                    order_type: OrderType::Market,
                    time_in_force: None,
                    quantity: "0.1".to_string(),
                    price: None,
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("batch place");
    assert_eq!(placed.orders.len(), 2);
    assert_eq!(placed.orders[0].exchange_order_id.as_deref(), Some("s-1"));
    assert_eq!(placed.orders[1].exchange_order_id.as_deref(), Some("p-1"));

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-spot"),
                    symbol: spot_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("s-1".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-perp"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("p-1".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 2);

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].path, "/api/v1/private");
    assert_eq!(seen[1].path, "/v1/perpum/order");
    assert_eq!(seen[2].path, "/api/v1/private");
    let perp_cancel_bodies = seen[3..8]
        .iter()
        .map(|request| request.body.as_str())
        .collect::<Vec<_>>();
    assert!(perp_cancel_bodies.iter().all(|body| body.contains("p-1")));
    assert!(perp_cancel_bodies
        .iter()
        .any(|body| body.contains(r#""posType":"plan""#)));
    assert!(perp_cancel_bodies
        .iter()
        .any(|body| body.contains(r#""posType":"execute""#)));
}

#[test]
fn coinw_private_parser_fixtures_should_cover_success_and_empty_payloads() {
    let exchange_id = exchange_id();
    let tenant_id = context("fixture").tenant_id.expect("tenant");
    let account_id = context("fixture").account_id.expect("account");
    let spot_balances = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinw/spot_balances_success.json"
    ))
    .expect("spot balances fixture");
    let perp_balances = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinw/perp_balances_success.json"
    ))
    .expect("perp balances fixture");
    let positions = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinw/perp_positions_success.json"
    ))
    .expect("positions fixture");
    let empty = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinw/empty_data_array.json"
    ))
    .expect("empty fixture");

    let spot = parse_balances(
        &exchange_id,
        tenant_id.clone(),
        account_id.clone(),
        MarketType::Spot,
        &["BTC".to_string()],
        &spot_balances,
    )
    .expect("spot balances");
    assert_eq!(spot[0].balances[0].total, 1.5);

    let perp = parse_balances(
        &exchange_id,
        tenant_id.clone(),
        account_id.clone(),
        MarketType::Perpetual,
        &["USDT".to_string()],
        &perp_balances,
    )
    .expect("perp balances");
    assert_eq!(perp[0].balances[0].available, 100.5);

    let parsed_positions =
        parse_positions(&exchange_id, tenant_id, account_id, &[], &positions).expect("positions");
    assert_eq!(parsed_positions[0].quantity, 0.25);

    let empty_positions = parse_positions(
        &exchange_id,
        context("empty").tenant_id.expect("tenant"),
        context("empty").account_id.expect("account"),
        &[],
        &empty,
    )
    .expect("empty positions");
    assert!(empty_positions.is_empty());
}

#[tokio::test]
async fn coinw_error_fixture_should_classify_rate_limit() {
    let error_payload = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinw/error_rate_limited.json"
    ))
    .expect("error fixture");
    let (base_url, _seen) = spawn_rest_server_with_status(429, vec![error_payload]).await;
    let adapter = CoinwGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("error"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("rate limited");

    match error {
        ExchangeApiError::Exchange(error) => {
            assert_eq!(error.class, ExchangeErrorClass::RateLimited);
            assert_eq!(error.code.as_deref(), Some("29001"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn coinw_capabilities_v2_should_declare_conservative_batch_and_stream_runtime() {
    let adapter = CoinwGatewayAdapter::new(private_config("http://127.0.0.1:9".to_string()))
        .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(matches!(
        capabilities.capabilities_v2.public_rest,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        capabilities.capabilities_v2.private_streams,
        CapabilitySupport::RestFallback { .. }
    ));
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::ComposedSequential
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.max_items,
        Some(20)
    );
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .resync
            .order_book
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "coinw.place_order"
            && endpoint.market_types == vec![MarketType::Spot]
            && endpoint.path.as_deref() == Some("/api/v1/private?command=doTrade")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "coinw.place_order"
            && endpoint.market_types == vec![MarketType::Perpetual]
            && endpoint.path.as_deref() == Some("/v1/perpum/order")));
}

#[test]
fn coinw_request_spec_fixtures_should_declare_spot_and_futures_private_signing() {
    let spot: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinw/request_specs/spot_place_order_limit.json"
    ))
    .expect("spot request spec");
    let futures: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/coinw/request_specs/futures_place_order_limit.json"
    ))
    .expect("futures request spec");

    assert_eq!(spot["operation"], "coinw.place_order");
    assert_eq!(spot["product"], "spot");
    assert_eq!(spot["path"], "/api/v1/private");
    assert_eq!(spot["query"]["command"], "doTrade");
    assert_eq!(spot["signing"], "spot_md5_query");
    assert_eq!(spot["required_auth_fields"][0], "api_key");
    assert_eq!(spot["required_auth_fields"][1], "sign");

    assert_eq!(futures["operation"], "coinw.place_order");
    assert_eq!(futures["product"], "perpetual");
    assert_eq!(futures["path"], "/v1/perpum/order");
    assert_eq!(futures["body"]["instrument"], "BTC");
    assert_eq!(futures["signing"], "futures_hmac_headers");
    assert_eq!(futures["required_auth_headers"][0], "api_key");
    assert_eq!(futures["required_auth_headers"][1], "timestamp");
    assert_eq!(futures["required_auth_headers"][2], "sign");
}
