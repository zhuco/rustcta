use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelAllOrdersRequest, CancelOrderRequest, CapabilitySupport, ExchangeApiError,
    ExchangeClient, FeesRequest, OpenOrdersRequest, OrderListConditionalLeg, OrderListLegType,
    OrderListRequest, PlaceOrderRequest, QueryOrderRequest, QuoteMarketOrderRequest,
    RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

use super::signing::sign_raw_query;
use super::test_support::{
    assert_signed_request, assert_signed_request_method, context, exchange_id, private_config,
    spawn_rest_server, symbol_scope,
};
use super::CoinsPhGatewayAdapter;

fn coinsph_fixture(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/coinsph/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("coinsph fixture");
    serde_json::from_str(&text).expect("coinsph fixture json")
}

#[test]
fn coinsph_signing_should_match_official_query_example() {
    let query = "symbol=BTCPHP&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1538323200000";
    let signature = sign_raw_query(
        "lH3ELTNiFxCQTmi9pPcWWikhsjO04Yoqw3euoHUuOLC3GYBW64ZqzQsiOEHXQS76",
        query,
    )
    .expect("signature");
    assert_eq!(
        signature,
        "d7b09aa959094bafd1de10be3985651691fff6cc04b5cd94aea8cc1ca02e0ed8"
    );
}

#[test]
fn coinsph_signing_vector_fixture_should_verify() {
    let vector = load_signing_vector("coinsph/signing_vectors/place_order_limit.json");
    vector.verify().expect("fixture signature");
}

#[tokio::test]
async fn coinsph_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = CoinsPhGatewayAdapter::default_public().expect("adapter");
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
async fn coinsph_adapter_should_return_unsupported_for_funding_boundaries() {
    let (base_url, _seen) = spawn_rest_server(vec![]).await;
    let adapter = CoinsPhGatewayAdapter::new(private_config(base_url)).expect("adapter");

    assert!(matches!(
        adapter
            .place_quote_market_order(QuoteMarketOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("quote-order"),
                symbol: symbol_scope("BTCPHP"),
                client_order_id: Some("cli-quote".to_string()),
                side: OrderSide::Buy,
                quote_quantity: "1000.00".to_string(),
            })
            .await
            .expect_err("quote market unsupported"),
        ExchangeApiError::Unsupported { .. }
    ));
    assert!(matches!(
        adapter
            .amend_order(AmendOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("amend"),
                symbol: symbol_scope("BTCPHP"),
                client_order_id: None,
                exchange_order_id: Some("33".to_string()),
                new_client_order_id: Some("cli-new".to_string()),
                new_quantity: "0.01000000".to_string(),
            })
            .await
            .expect_err("amend unsupported"),
        ExchangeApiError::Unsupported { .. }
    ));
    assert!(matches!(
        adapter
            .cancel_all_orders(CancelAllOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel-all"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Spot),
                symbol: Some(symbol_scope("BTCPHP")),
            })
            .await
            .expect_err("cancel all unsupported"),
        ExchangeApiError::Unsupported { .. }
    ));
}

#[tokio::test]
async fn coinsph_adapter_should_pin_advanced_order_unsupported_boundary() {
    let (base_url, _seen) = spawn_rest_server(vec![]).await;
    let adapter = CoinsPhGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_cancel_all_orders);
    assert!(matches!(
        capabilities.capabilities_v2.batch_place_orders.support,
        CapabilitySupport::Unsupported { .. }
    ));
    assert!(matches!(
        capabilities.capabilities_v2.batch_cancel_orders.support,
        CapabilitySupport::Unsupported { .. }
    ));
    for operation in [
        "amend_order",
        "place_order_list",
        "batch_place_orders",
        "batch_cancel_orders",
        "cancel_all_orders",
    ] {
        assert!(
            capabilities
                .capabilities_v2
                .endpoints
                .iter()
                .any(|endpoint| endpoint.operation == operation
                    && matches!(endpoint.support, CapabilitySupport::Unsupported { .. })),
            "missing unsupported capabilities_v2 endpoint for {operation}"
        );
    }

    let boundary = coinsph_fixture("unsupported_boundary.json");
    assert_eq!(boundary["advanced_order_runtime"], false);
    assert_eq!(
        boundary["unsupported_operations"]["batch_cancel_orders"],
        "coinsph.batch_cancel_orders"
    );

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: symbol_scope("BTCPHP"),
            client_order_id: None,
            exchange_order_id: Some("33".to_string()),
            new_client_order_id: Some("cli-new".to_string()),
            new_quantity: "0.01000000".to_string(),
        })
        .await
        .expect_err("amend unsupported");
    assert!(matches!(
        amend_error,
        ExchangeApiError::Unsupported {
            operation: "coinsph.amend_order"
        }
    ));

    let order_list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("oco"),
            symbol: symbol_scope("BTCPHP"),
            list_client_order_id: Some("coinsph-oco".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01000000".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::Limit,
                price: Some("3600000.00".to_string()),
                stop_price: None,
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("coinsph-oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("3400000.00".to_string()),
                stop_price: Some("3450000.00".to_string()),
                time_in_force: Some(TimeInForce::GTC),
                client_order_id: Some("coinsph-oco-below".to_string()),
            },
        })
        .await
        .expect_err("order-list unsupported");
    assert!(matches!(
        order_list_error,
        ExchangeApiError::Unsupported {
            operation: "coinsph.place_order_list"
        }
    ));

    let batch_place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: Vec::new(),
        })
        .await
        .expect_err("batch place unsupported");
    assert!(matches!(
        batch_place_error,
        ExchangeApiError::Unsupported {
            operation: "coinsph.batch_place_orders"
        }
    ));

    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: Vec::new(),
        })
        .await
        .expect_err("batch cancel unsupported");
    assert!(matches!(
        batch_cancel_error,
        ExchangeApiError::Unsupported {
            operation: "coinsph.batch_cancel_orders"
        }
    ));
}

#[tokio::test]
async fn coinsph_adapter_should_load_balances_from_signed_account_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "balances": [
            {"asset": "PHP", "free": "1000.00", "locked": "25.50"},
            {"asset": "BTC", "free": "0.10000000", "locked": "0.02000000"}
        ]
    })])
    .await;
    let adapter = CoinsPhGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["PHP".to_string()],
        })
        .await
        .expect("balances");

    assert_eq!(response.balances.len(), 1);
    assert_eq!(response.balances[0].balances[0].asset, "PHP");
    assert_eq!(response.balances[0].balances[0].available, 1000.0);
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("coinsph/request_specs/get_balances.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
    assert_eq!(request.path, "/openapi/v1/account");
    assert_signed_request(&request);
}

#[tokio::test]
async fn coinsph_adapter_should_route_private_order_read_write_surfaces() {
    let order_ack = json!({
        "symbol": "BTCPHP",
        "orderId": 30,
        "clientOrderId": "cli-place",
        "price": "3500000.00",
        "origQty": "0.01000000",
        "executedQty": "0.00000000",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "transactTime": 1538323200000_i64
    });
    let cancel_ack = json!({
        "symbol": "BTCPHP",
        "orderId": 30,
        "clientOrderId": "cli-place",
        "price": "3500000.00",
        "origQty": "0.01000000",
        "executedQty": "0.00000000",
        "status": "CANCELED",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "updateTime": 1538323200001_i64
    });
    let query_ack = json!({
        "symbol": "BTCPHP",
        "orderId": 31,
        "clientOrderId": "cli-query",
        "price": "3510000.00",
        "origQty": "0.02000000",
        "executedQty": "0.01000000",
        "cummulativeQuoteQty": "35100.00",
        "status": "PARTIALLY_FILLED",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "SELL",
        "time": 1538323200002_i64,
        "updateTime": 1538323200003_i64
    });
    let open_orders = json!([{
        "symbol": "BTCPHP",
        "orderId": 32,
        "clientOrderId": "cli-open",
        "price": "3520000.00",
        "origQty": "0.03000000",
        "executedQty": "0.00000000",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "SELL",
        "time": 1538323200004_i64,
        "updateTime": 1538323200005_i64
    }]);
    let fills = json!([{
        "symbol": "BTCPHP",
        "id": 28457,
        "orderId": 100234,
        "clientOrderId": "cli-fill",
        "price": "3500000.00",
        "qty": "0.00400000",
        "quoteQty": "14000.00",
        "commission": "7.00",
        "commissionAsset": "PHP",
        "time": 1538323200006_i64,
        "isBuyer": true,
        "isMaker": false
    }]);
    let fees = json!([{
        "symbol": "BTCPHP",
        "makerCommission": "0.0010",
        "takerCommission": "0.0015"
    }]);
    let (base_url, seen) = spawn_rest_server(vec![
        order_ack,
        cancel_ack,
        query_ack,
        open_orders,
        fills,
        fees,
    ])
    .await;
    let adapter = CoinsPhGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place-order"),
            symbol: symbol_scope("BTCPHP"),
            client_order_id: Some("cli-place".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.01000000".to_string(),
            price: Some("3500000.00".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place order");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("30"));

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-order"),
            symbol: symbol_scope("BTCPHP"),
            client_order_id: Some("cli-place".to_string()),
            exchange_order_id: Some("30".to_string()),
        })
        .await
        .expect("cancel order");
    assert!(cancelled.cancelled);

    let queried = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-order"),
            symbol: symbol_scope("BTCPHP"),
            client_order_id: Some("cli-query".to_string()),
            exchange_order_id: Some("31".to_string()),
        })
        .await
        .expect("query order");
    let order = queried.order.expect("order");
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.average_fill_price.as_deref(), Some("3510000"));

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope("BTCPHP")),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open.orders.len(), 1);

    let recent_fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope("BTCPHP")),
            client_order_id: Some("cli-fill".to_string()),
            exchange_order_id: Some("100234".to_string()),
            from_trade_id: Some("28457".to_string()),
            start_time: None,
            end_time: None,
            limit: Some(50),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(recent_fills.fills[0].fill_id.as_deref(), Some("28457"));
    assert_eq!(recent_fills.fills[0].side, OrderSide::Buy);
    assert_eq!(recent_fills.fills[0].fee_asset.as_deref(), Some("PHP"));

    let fee_response = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope("BTCPHP")],
        })
        .await
        .expect("fees");
    assert_eq!(fee_response.fees[0].maker_rate, "0.0010");

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].path, "/openapi/v1/order");
    load_request_spec("coinsph/request_specs/place_order.json")
        .assert_matches(&seen[0].actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&seen[0], "POST");
    assert_eq!(
        seen[0].query.get("symbol").map(String::as_str),
        Some("BTCPHP")
    );

    assert_eq!(seen[1].path, "/openapi/v1/order");
    load_request_spec("coinsph/request_specs/cancel_order.json")
        .assert_matches(&seen[1].actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&seen[1], "DELETE");

    assert_eq!(seen[2].path, "/openapi/v1/order");
    load_request_spec("coinsph/request_specs/query_order.json")
        .assert_matches(&seen[2].actual_http_request())
        .expect("request spec");
    assert_signed_request(&seen[2]);

    assert_eq!(seen[3].path, "/openapi/v1/openOrders");
    load_request_spec("coinsph/request_specs/get_open_orders.json")
        .assert_matches(&seen[3].actual_http_request())
        .expect("request spec");
    assert_signed_request(&seen[3]);

    assert_eq!(seen[4].path, "/openapi/v1/myTrades");
    load_request_spec("coinsph/request_specs/get_recent_fills.json")
        .assert_matches(&seen[4].actual_http_request())
        .expect("request spec");
    assert_signed_request(&seen[4]);

    assert_eq!(seen[5].path, "/openapi/v1/asset/tradeFee");
    load_request_spec("coinsph/request_specs/get_fees.json")
        .assert_matches(&seen[5].actual_http_request())
        .expect("request spec");
    assert_signed_request(&seen[5]);
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
