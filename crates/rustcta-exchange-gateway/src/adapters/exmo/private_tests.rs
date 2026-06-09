use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelAllOrdersRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient,
    OpenOrdersRequest, OrderListConditionalLeg, OrderListLegType, OrderListRequest, PageCursor,
    PageRequest, PlaceOrderRequest, QueryOrderRequest, QuoteMarketOrderRequest, RecentFillsRequest,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::private_parser::{parse_open_orders, parse_recent_fills};
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope, SeenRequest};
use super::{ExmoGatewayAdapter, ExmoGatewayConfig};

fn exmo_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "request_specs/place_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/exmo/request_specs/place_order_limit.json"
        ),
        "request_specs/get_recent_fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/exmo/request_specs/get_recent_fills.json"
        ),
        "signing_vectors/private_form_hmac_sha512.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/exmo/signing_vectors/private_form_hmac_sha512.json"
        ),
        "unsupported_boundary.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/exmo/unsupported_boundary.json")
        }
        _ => panic!("unknown EXMO fixture {name}"),
    };
    serde_json::from_str(text).expect("EXMO fixture")
}

#[tokio::test]
async fn exmo_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = ExmoGatewayAdapter::default_public().expect("adapter");
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
async fn exmo_advanced_order_surfaces_should_remain_explicitly_unsupported() {
    let adapter = ExmoGatewayAdapter::new(ExmoGatewayConfig {
        api_key: Some("api-key".to_string()),
        api_secret: Some("api-secret".to_string()),
        enabled_private_rest: true,
        ..ExmoGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_cancel_all_orders);

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
            operation: "exmo.amend_order"
        }
    ));

    let list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oco"),
            symbol: symbol_scope(),
            list_client_order_id: Some("oco-1".to_string()),
            side: OrderSide::Sell,
            quantity: "0.1".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::LimitMaker,
                price: Some("12000".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("9000".to_string()),
                stop_price: Some("9500".to_string()),
                time_in_force: None,
                client_order_id: Some("oco-below".to_string()),
            },
        })
        .await
        .expect_err("order-list unsupported");
    assert!(matches!(
        list_error,
        ExchangeApiError::Unsupported {
            operation: "place_order_list"
        }
    ));

    let place = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-order"),
        symbol: symbol_scope(),
        client_order_id: Some("batch-place-1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("10000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let batch_place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![place],
        })
        .await
        .expect_err("batch place unsupported");
    assert!(matches!(
        batch_place_error,
        ExchangeApiError::Unsupported {
            operation: "exmo.batch_place_orders"
        }
    ));

    let cancel = CancelOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("cancel"),
        symbol: symbol_scope(),
        client_order_id: None,
        exchange_order_id: Some("order-1".to_string()),
    };
    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![cancel],
        })
        .await
        .expect_err("batch cancel unsupported");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: "exmo.batch_cancel_orders"
        }
    ));

    let cancel_all_error = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
        })
        .await
        .expect_err("cancel all unsupported");
    assert!(matches!(
        cancel_all_error,
        ExchangeApiError::Unsupported {
            operation: "exmo.cancel_all_orders"
        }
    ));
}

#[test]
fn exmo_signing_should_hmac_sha512_form_body() {
    let vector = exmo_fixture("signing_vectors/private_form_hmac_sha512.json");
    let params = vector["params"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| {
            (
                item["key"].as_str().unwrap().to_string(),
                item["value"].as_str().unwrap().to_string(),
            )
        })
        .collect::<Vec<_>>();
    let (body, signature) =
        super::signing::rest_signed_body_and_signature(vector["secret"].as_str().unwrap(), &params)
            .expect("signature");
    assert_eq!(body, vector["expected_body"].as_str().unwrap());
    assert_eq!(signature, vector["expected_signature"].as_str().unwrap());
}

#[test]
fn exmo_request_specs_and_unsupported_fixture_should_document_boundaries() {
    let place = exmo_fixture("request_specs/place_order_limit.json");
    assert_eq!(place["operation"], "exmo.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], "/v1.1/order_create");
    assert_eq!(place["body"]["pair"], "BTC_USD");
    assert_eq!(place["body"]["exec_type"], "post_only");

    let fills = exmo_fixture("request_specs/get_recent_fills.json");
    assert_eq!(fills["operation"], "exmo.get_recent_fills");
    assert_eq!(fills["body"]["limit"], "100");
    assert_eq!(fills["body"]["offset"], "20");

    let unsupported = exmo_fixture("unsupported_boundary.json");
    assert_eq!(unsupported["margin"], "unsupported");
    assert_eq!(unsupported["excode"], "unsupported");
    assert_eq!(unsupported["wallet_withdraw"], "unsupported");
}

#[tokio::test]
async fn exmo_adapter_should_route_private_order_requests_with_signed_form_body() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"result": true, "error": "", "order_id": 123456, "client_id": 100500}),
        json!({"result": true, "error": "", "order_id": 123456}),
        json!({"result": true, "error": "", "order_id": 123457, "client_id": 100501}),
    ])
    .await;
    let adapter = ExmoGatewayAdapter::new(ExmoGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("api-key".to_string()),
        api_secret: Some("api-secret".to_string()),
        enabled_private_rest: true,
        ..ExmoGatewayConfig::default()
    })
    .expect("adapter");

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol_scope(),
            client_order_id: Some("100500".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::PostOnly,
            time_in_force: Some(TimeInForce::GTX),
            quantity: "0.01".to_string(),
            price: Some("10000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: true,
        })
        .await
        .expect("place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("123456"));

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol_scope(),
            client_order_id: Some("100500".to_string()),
            exchange_order_id: Some("123456".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);

    let quote = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote"),
            symbol: symbol_scope(),
            client_order_id: Some("100501".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "100".to_string(),
        })
        .await
        .expect("quote market");
    assert_eq!(quote.order.exchange_order_id.as_deref(), Some("123457"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert_signed_request(&requests[0], "/v1.1/order_create");
    assert_eq!(
        requests[0].body.get("pair").map(String::as_str),
        Some("BTC_USD")
    );
    assert_eq!(
        requests[0].body.get("exec_type").map(String::as_str),
        Some("post_only")
    );
    assert_eq!(
        requests[0].body.get("client_id").map(String::as_str),
        Some("100500")
    );
    assert_signed_request(&requests[1], "/v1.1/order_cancel");
    assert_eq!(
        requests[1].body.get("order_id").map(String::as_str),
        Some("123456")
    );
    assert_signed_request(&requests[2], "/v1.1/order_create");
    assert_eq!(
        requests[2].body.get("type").map(String::as_str),
        Some("market_buy_total")
    );
}

#[tokio::test]
async fn exmo_adapter_should_route_history_with_limit_offset_pagination() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "BTC_USD": [{
                "order_id": "14",
                "client_id": "100500",
                "created": "1435517311",
                "type": "buy",
                "pair": "BTC_USD",
                "price": "100",
                "quantity": "1",
                "amount": "100"
            }]
        }),
        json!({
            "BTC_USD": [{
                "trade_id": 20056872,
                "client_id": 100500,
                "date": 1435488248,
                "type": "buy",
                "pair": "BTC_USD",
                "quantity": "1",
                "price": "100",
                "amount": "100",
                "order_id": 7,
                "exec_type": "taker",
                "commission_amount": "0.02",
                "commission_currency": "BTC",
                "commission_percent": "0.2"
            }]
        }),
        json!({
            "type": "buy",
            "in_currency": "BTC",
            "in_amount": "1",
            "out_currency": "USD",
            "out_amount": "100",
            "trades": [{
                "trade_id": 3,
                "client_id": 100500,
                "date": 1435488248,
                "type": "buy",
                "pair": "BTC_USD",
                "order_id": 12345,
                "quantity": 1,
                "price": 100,
                "amount": 100
            }]
        }),
    ])
    .await;
    let adapter = ExmoGatewayAdapter::new(ExmoGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("api-key".to_string()),
        api_secret: Some("api-secret".to_string()),
        enabled_private_rest: true,
        ..ExmoGatewayConfig::default()
    })
    .expect("adapter");

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
    assert_eq!(
        open_orders.orders[0].exchange_order_id.as_deref(),
        Some("14")
    );

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
            limit: None,
            page: Some(PageRequest {
                limit: Some(100),
                cursor: Some(PageCursor::Offset { offset: 20 }),
            }),
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("20056872"));

    let query = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("12345".to_string()),
        })
        .await
        .expect("query");
    assert_eq!(query.order.unwrap().status, OrderStatus::Filled);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/v1.1/user_open_orders");
    assert_eq!(requests[1].path, "/v1.1/user_trades");
    assert_eq!(
        requests[1].body.get("limit").map(String::as_str),
        Some("100")
    );
    assert_eq!(
        requests[1].body.get("offset").map(String::as_str),
        Some("20")
    );
    assert_eq!(requests[2].path, "/v1.1/order_trades");
}

#[test]
fn exmo_private_parsers_should_cover_orders_and_fills() {
    let orders = parse_open_orders(
        &exchange_id(),
        Some(&symbol_scope()),
        &json!({
            "BTC_USD": [{
                "order_id": "14",
                "client_id": "100500",
                "created": "1435517311",
                "type": "buy",
                "pair": "BTC_USD",
                "price": "100",
                "quantity": "1",
                "amount": "100"
            }]
        }),
    )
    .expect("orders");
    assert_eq!(orders[0].status, OrderStatus::Unknown);
    assert_eq!(orders[0].exchange_order_id.as_deref(), Some("14"));

    let fills = parse_recent_fills(
        &exchange_id(),
        context("fills").tenant_id.unwrap(),
        context("fills").account_id.unwrap(),
        &symbol_scope(),
        &json!({
            "BTC_USD": [{
                "trade_id": 20056872,
                "client_id": 100500,
                "date": 1435488248,
                "type": "buy",
                "pair": "BTC_USD",
                "quantity": "1",
                "price": "100",
                "amount": "100",
                "order_id": 7,
                "exec_type": "taker",
                "commission_amount": "0.02",
                "commission_currency": "BTC",
                "commission_percent": "0.2"
            }]
        }),
    )
    .expect("fills");
    assert_eq!(fills[0].fill_id.as_deref(), Some("20056872"));
    assert_eq!(fills[0].liquidity_role, rustcta_types::LiquidityRole::Taker);
}

#[tokio::test]
async fn exmo_adapter_should_reject_non_numeric_client_order_id() {
    let adapter = ExmoGatewayAdapter::new(ExmoGatewayConfig {
        api_key: Some("api-key".to_string()),
        api_secret: Some("api-secret".to_string()),
        enabled_private_rest: true,
        ..ExmoGatewayConfig::default()
    })
    .expect("adapter");
    let error = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place-bad-client-id"),
            symbol: symbol_scope(),
            client_order_id: Some("not-numeric".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.01".to_string(),
            price: Some("10000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect_err("invalid client id");
    assert!(matches!(error, ExchangeApiError::InvalidRequest { .. }));
}

fn assert_signed_request(request: &SeenRequest, path: &str) {
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, path);
    assert_eq!(
        request
            .headers
            .get("content-type")
            .map(|value| value.as_str()),
        Some("application/x-www-form-urlencoded")
    );
    assert!(request
        .headers
        .get("key")
        .is_some_and(|value| !value.is_empty()));
    assert!(request
        .headers
        .get("sign")
        .is_some_and(|value| value.len() == 128));
    assert!(request
        .body
        .get("nonce")
        .is_some_and(|value| !value.is_empty()));
    assert!(!request.raw_body.contains("api-secret"));
}
