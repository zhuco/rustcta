use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelAllOrdersRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest,
    OpenOrdersRequest, PageCursor, PageRequest, PlaceOrderRequest, QueryOrderRequest,
    QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{LiquidityRole, MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope, SeenRequest};
use super::{KuCoinGatewayAdapter, KuCoinGatewayConfig};

fn kucoin_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "request_specs/place_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/kucoin/request_specs/place_order_limit.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/kucoin/request_specs/cancel_order.json"
        ),
        "request_specs/trade_fees.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/kucoin/request_specs/trade_fees.json"
        ),
        "signing_vectors/private_get_accounts.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/kucoin/signing_vectors/private_get_accounts.json"
        ),
        _ => panic!("unknown kucoin fixture {name}"),
    };
    serde_json::from_str(text).expect("kucoin fixture")
}

#[tokio::test]
async fn kucoin_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = KuCoinGatewayAdapter::default_public().expect("adapter");
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

fn assert_signed_request(request: &SeenRequest, path: &str) {
    assert_signed_request_method(request, "GET", path);
}

fn assert_signed_request_method(request: &SeenRequest, method: &str, path: &str) {
    assert_eq!(request.method, method);
    assert_eq!(request.path, path);
    assert_eq!(
        request.headers.get("kc-api-key").map(String::as_str),
        Some("key")
    );
    assert!(request
        .headers
        .get("kc-api-sign")
        .is_some_and(|value| !value.is_empty()));
    assert!(request
        .headers
        .get("kc-api-timestamp")
        .is_some_and(|value| !value.is_empty()));
    assert!(request
        .headers
        .get("kc-api-passphrase")
        .is_some_and(|value| !value.is_empty() && value != "passphrase"));
    assert_eq!(
        request
            .headers
            .get("kc-api-key-version")
            .map(String::as_str),
        Some("2")
    );
}

#[test]
fn kucoin_signing_should_base64_encode_hmac_sha256() {
    let vector = kucoin_fixture("signing_vectors/private_get_accounts.json");
    let signature = super::signing::sign_base64(
        vector["secret"].as_str().unwrap(),
        vector["payload"].as_str().unwrap(),
    )
    .expect("signature");
    assert_eq!(signature, vector["expected_signature"].as_str().unwrap());
}

#[test]
fn kucoin_request_spec_fixtures_should_cover_private_writes() {
    let place = kucoin_fixture("request_specs/place_order_limit.json");
    assert_eq!(place["operation"], "kucoin.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], "/api/v1/hf/orders");
    assert_eq!(place["body"]["symbol"], "BTC-USDT");
    assert_eq!(place["body"]["clientOid"], "LIMIT1");

    let cancel = kucoin_fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["operation"], "kucoin.cancel_order");
    assert_eq!(cancel["method"], "DELETE");
    assert_eq!(cancel["path"], "/api/v1/hf/orders/2001");
    assert_eq!(cancel["query"]["symbol"], "BTC-USDT");

    let fees = kucoin_fixture("request_specs/trade_fees.json");
    assert_eq!(fees["operation"], "kucoin.get_fees");
    assert_eq!(fees["method"], "GET");
    assert_eq!(fees["path"], "/api/v1/trade-fees");
    assert_eq!(fees["query"]["symbols"], "BTC-USDT");
}

#[tokio::test]
async fn kucoin_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "200000",
            "data": {"orderId": "2001", "clientOid": "LIMIT1"}
        }),
        json!({
            "code": "200000",
            "data": {"orderId": "2002", "clientOid": "QUOTE1"}
        }),
        json!({
            "code": "200000",
            "data": {"orderId": "2001"}
        }),
        json!({
            "code": "200000",
            "data": {"cancelledOrderIds": ["2003", "2004"]}
        }),
        json!({
            "code": "200000",
            "data": {"newOrderId": "2005", "clientOid": "AMEND1"}
        }),
    ])
    .await;
    let adapter = KuCoinGatewayAdapter::new(KuCoinGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        api_passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..KuCoinGatewayConfig::default()
    })
    .expect("adapter");

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
            quantity: "0.01".to_string(),
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
    assert_eq!(cancel_all.cancelled_count, 2);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend-order"),
            symbol: symbol_scope(),
            client_order_id: Some("AMEND1".to_string()),
            exchange_order_id: Some("2005".to_string()),
            new_client_order_id: None,
            new_quantity: "0.005".to_string(),
        })
        .await
        .expect("amend order");
    assert_eq!(amended.order.exchange_order_id.as_deref(), Some("2005"));
    assert_eq!(amended.order.client_order_id.as_deref(), Some("AMEND1"));
    assert_eq!(amended.order.quantity, "0.005");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);

    assert_signed_request_method(&requests[0], "POST", "/api/v1/hf/orders");
    let body = requests[0].body.as_ref().expect("place body");
    assert_eq!(body["symbol"], "BTC-USDT");
    assert_eq!(body["side"], "buy");
    assert_eq!(body["type"], "limit");
    assert_eq!(body["size"], "0.01");
    assert_eq!(body["price"], "65000");
    assert_eq!(body["clientOid"], "LIMIT1");
    assert_eq!(body["tradeType"], "TRADE");

    assert_signed_request_method(&requests[1], "POST", "/api/v1/hf/orders");
    let body = requests[1].body.as_ref().expect("quote body");
    assert_eq!(body["symbol"], "BTC-USDT");
    assert_eq!(body["side"], "buy");
    assert_eq!(body["type"], "market");
    assert_eq!(body["funds"], "125.5");
    assert_eq!(body["clientOid"], "QUOTE1");
    assert_eq!(body["tradeType"], "TRADE");

    assert_signed_request_method(&requests[2], "DELETE", "/api/v1/hf/orders/2001");
    assert_eq!(
        requests[2].query.get("symbol").map(String::as_str),
        Some("BTC-USDT")
    );

    assert_signed_request_method(&requests[3], "DELETE", "/api/v1/hf/orders");
    assert_eq!(
        requests[3].query.get("symbol").map(String::as_str),
        Some("BTC-USDT")
    );

    assert_signed_request_method(&requests[4], "POST", "/api/v1/hf/orders/alter");
    let body = requests[4].body.as_ref().expect("amend body");
    assert_eq!(body["symbol"], "BTC-USDT");
    assert_eq!(body["orderId"], "2005");
    assert_eq!(body["clientOid"], "AMEND1");
    assert_eq!(body["newSize"], "0.005");
}

#[tokio::test]
async fn kucoin_adapter_should_compose_batch_place_and_cancel_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "200000",
            "data": {"orderId": "3001", "clientOid": "BATCH1"}
        }),
        json!({
            "code": "200000",
            "data": {"orderId": "3002", "clientOid": "BATCH2"}
        }),
        json!({
            "code": "200000",
            "data": {"orderId": "3001"}
        }),
        json!({
            "code": "200000",
            "data": {"orderId": "3002"}
        }),
    ])
    .await;
    let adapter = KuCoinGatewayAdapter::new(KuCoinGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        api_passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..KuCoinGatewayConfig::default()
    })
    .expect("adapter");

    let mut order_one = place_limit_order("BATCH1", "0.01");
    order_one.context = context("batch-place-one");
    let mut order_two = place_limit_order("BATCH2", "0.02");
    order_two.context = context("batch-place-two");

    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![order_one, order_two],
        })
        .await
        .expect("batch place");
    assert_eq!(placed.orders.len(), 2);
    assert_eq!(placed.orders[0].exchange_order_id.as_deref(), Some("3001"));
    assert_eq!(placed.orders[1].exchange_order_id.as_deref(), Some("3002"));

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                cancel_request("3001", "BATCH1"),
                cancel_request("3002", "BATCH2"),
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 2);
    assert_eq!(cancelled.orders[0].status, OrderStatus::Cancelled);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);
    assert_signed_request_method(&requests[0], "POST", "/api/v1/hf/orders");
    assert_eq!(requests[0].body.as_ref().unwrap()["clientOid"], "BATCH1");
    assert_signed_request_method(&requests[1], "POST", "/api/v1/hf/orders");
    assert_eq!(requests[1].body.as_ref().unwrap()["clientOid"], "BATCH2");
    assert_signed_request_method(&requests[2], "DELETE", "/api/v1/hf/orders/3001");
    assert_signed_request_method(&requests[3], "DELETE", "/api/v1/hf/orders/3002");
}

#[tokio::test]
async fn kucoin_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "200000",
            "data": [
                {"type": "trade", "currency": "BTC", "available": "0.5", "holds": "0.1"},
                {"type": "trade", "currency": "USDT", "available": "123.45", "holds": "1.55"}
            ]
        }),
        json!({
            "code": "200000",
            "data": {
                "id": "1001",
                "symbol": "BTC-USDT",
                "clientOid": "CID1001",
                "side": "buy",
                "type": "limit",
                "isActive": true,
                "price": "65000",
                "size": "0.01",
                "dealSize": "0.006",
                "dealFunds": "390.06",
                "createdAt": 1743054548123i64
            }
        }),
        json!({
            "code": "200000",
            "data": {
                "items": [{
                    "id": "1002",
                    "symbol": "BTC-USDT",
                    "clientOid": "CID1002",
                    "side": "sell",
                    "type": "limit",
                    "isActive": true,
                    "price": "70000",
                    "size": "0.02",
                    "dealSize": "0",
                    "createdAt": 1743054549000i64
                }]
            }
        }),
        json!({
            "code": "200000",
            "data": [{
                "symbol": "BTC-USDT",
                "makerFeeRate": "0.001",
                "takerFeeRate": "0.0015"
            }]
        }),
        json!({
            "code": "200000",
            "data": {
                "items": [{
                    "tradeId": "9001",
                    "orderId": "1001",
                    "clientOid": "CID1001",
                    "symbol": "BTC-USDT",
                    "side": "buy",
                    "price": "65010",
                    "size": "0.006",
                    "fee": "0.39",
                    "feeCurrency": "USDT",
                    "liquidity": "taker",
                    "createdAt": 1743054550000i64
                }]
            }
        }),
    ])
    .await;
    let adapter = KuCoinGatewayAdapter::new(KuCoinGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        api_passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..KuCoinGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.capabilities().supports_private_rest);
    assert!(adapter.capabilities().supports_balances);
    assert!(adapter.capabilities().supports_query_order);

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
    assert_eq!(balances.balances.len(), 1);
    assert_eq!(balances.balances[0].balances.len(), 2);
    assert_eq!(balances.balances[0].balances[0].asset, "BTC");
    assert_eq!(balances.balances[0].balances[0].available, 0.5);

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
    assert_eq!(order.status, OrderStatus::New);
    assert_eq!(order.price.as_deref(), Some("65000"));
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
            page: Some(PageRequest::next_page(
                Some(50),
                PageCursor::Offset { offset: 100 },
            )),
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
            limit: None,
            page: Some(PageRequest::next_page(
                Some(100),
                PageCursor::Offset { offset: 200 },
            )),
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("9001"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("1001"));
    assert_eq!(fills.fills[0].client_order_id.as_deref(), Some("CID1001"));
    assert_eq!(fills.fills[0].side, OrderSide::Buy);
    assert_eq!(fills.fills[0].liquidity_role, LiquidityRole::Taker);
    assert_eq!(fills.fills[0].price, 65_010.0);
    assert_eq!(fills.fills[0].quantity, 0.006);
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("USDT"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    assert_signed_request(&requests[0], "/api/v1/accounts");
    assert!(requests[0].query.is_empty());
    assert_signed_request(&requests[1], "/api/v1/hf/orders/1001");
    assert_eq!(
        requests[1].query.get("symbol").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_signed_request(&requests[2], "/api/v1/hf/orders/active");
    assert_eq!(
        requests[2].query.get("status").map(String::as_str),
        Some("active")
    );
    assert_eq!(
        requests[2].query.get("pageSize").map(String::as_str),
        Some("50")
    );
    assert_eq!(
        requests[2].query.get("currentPage").map(String::as_str),
        Some("3")
    );
    assert_signed_request(&requests[3], "/api/v1/trade-fees");
    assert_eq!(
        requests[3].query.get("symbols").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_signed_request(&requests[4], "/api/v1/hf/fills");
    assert_eq!(
        requests[4].query.get("pageSize").map(String::as_str),
        Some("100")
    );
    assert_eq!(
        requests[4].query.get("currentPage").map(String::as_str),
        Some("3")
    );
}

fn place_limit_order(client_order_id: &str, quantity: &str) -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(client_order_id),
        symbol: symbol_scope(),
        client_order_id: Some(client_order_id.to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: quantity.to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

fn cancel_request(exchange_order_id: &str, client_order_id: &str) -> CancelOrderRequest {
    CancelOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(client_order_id),
        symbol: symbol_scope(),
        client_order_id: Some(client_order_id.to_string()),
        exchange_order_id: Some(exchange_order_id.to_string()),
    }
}
