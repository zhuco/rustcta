use rustcta_exchange_api::{
    BalancesRequest, BatchExecutionMode, CancelAllOrdersRequest, CancelOrderRequest,
    CapabilitySupport, CredentialScope, ExchangeApiError, ExchangeClient, FeesRequest,
    OpenOrdersRequest, PlaceOrderRequest, QueryOrderRequest, QuoteMarketOrderRequest,
    RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::test_support::{
    assert_signed_request, assert_signed_request_method, context, exchange_id,
    perpetual_symbol_scope, spawn_rest_server, symbol_scope,
};
use super::{MexcGatewayAdapter, MexcGatewayConfig};
use crate::request_spec::RequestSpec;

#[tokio::test]
async fn mexc_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = MexcGatewayAdapter::default_public().expect("adapter");
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
fn mexc_adapter_should_declare_capabilities_v2_for_toolchain_audit() {
    let adapter = MexcGatewayAdapter::new(MexcGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..MexcGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(matches!(
        &capabilities.capabilities_v2.public_rest,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        &capabilities.capabilities_v2.private_rest,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        &capabilities.capabilities_v2.public_streams,
        CapabilitySupport::RestFallback { .. }
    ));
    assert!(matches!(
        &capabilities.capabilities_v2.private_streams,
        CapabilitySupport::RestFallback { .. }
    ));
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::ComposedSequential
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.mode,
        BatchExecutionMode::Native
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_cancel_orders
            .same_symbol_required
    );
    assert_eq!(
        capabilities.capabilities_v2.fills_history.max_limit,
        Some(1000)
    );
    assert!(capabilities.capabilities_v2.fills_history.supports_since);
    assert!(capabilities.capabilities_v2.fills_history.supports_from_id);
    assert_eq!(
        capabilities.capabilities_v2.credential_scopes,
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    );
    assert!(capabilities.capabilities_v2.stream_runtime.resync.orders);
}

#[tokio::test]
async fn mexc_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "symbol": "BTCUSDT",
            "orderId": 2001,
            "clientOrderId": "LIMIT1",
            "price": "65000",
            "origQty": "0.02",
            "executedQty": "0",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "transactTime": 1700000000000i64
        }),
        json!({
            "symbol": "BTCUSDT",
            "orderId": 2002,
            "clientOrderId": "QUOTE1",
            "price": "0",
            "origQty": "25.5",
            "executedQty": "0",
            "status": "NEW",
            "type": "MARKET",
            "side": "BUY",
            "transactTime": 1700000001000i64
        }),
        json!({
            "symbol": "BTCUSDT",
            "orderId": 2001,
            "clientOrderId": "LIMIT1",
            "status": "CANCELED"
        }),
        json!([
            {"symbol": "BTCUSDT", "orderId": 2002, "clientOrderId": "QUOTE1", "status": "CANCELED"},
            {"symbol": "BTCUSDT", "orderId": 2003, "clientOrderId": "SELL1", "status": "CANCELED"}
        ]),
    ])
    .await;
    let adapter = MexcGatewayAdapter::new(MexcGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..MexcGatewayConfig::default()
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
    assert_eq!(placed.order.order_type, OrderType::Limit);

    let quote = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote-order"),
            symbol: symbol_scope(),
            client_order_id: Some("QUOTE1".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25.5".to_string(),
        })
        .await
        .expect("quote market order");
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
    assert_eq!(cancel_all.orders.len(), 2);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);

    load_request_spec("place_order.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("place order request spec");
    assert_signed_request_method(&requests[0], "POST", "/api/v3/order");
    assert_eq!(
        requests[0].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(
        requests[0].query.get("side").map(String::as_str),
        Some("BUY")
    );
    assert_eq!(
        requests[0].query.get("type").map(String::as_str),
        Some("LIMIT")
    );
    assert_eq!(
        requests[0].query.get("quantity").map(String::as_str),
        Some("0.02")
    );
    assert_eq!(
        requests[0].query.get("price").map(String::as_str),
        Some("65000")
    );
    assert_eq!(
        requests[0].query.get("timeInForce").map(String::as_str),
        Some("GTC")
    );
    assert_eq!(
        requests[0]
            .query
            .get("newClientOrderId")
            .map(String::as_str),
        Some("LIMIT1")
    );

    assert_signed_request_method(&requests[1], "POST", "/api/v3/order");
    load_request_spec("place_quote_market_order.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("quote market order request spec");
    assert_eq!(
        requests[1].query.get("type").map(String::as_str),
        Some("MARKET")
    );
    assert_eq!(
        requests[1].query.get("quoteOrderQty").map(String::as_str),
        Some("25.5")
    );
    assert_eq!(
        requests[1]
            .query
            .get("newClientOrderId")
            .map(String::as_str),
        Some("QUOTE1")
    );
    assert!(!requests[1].query.contains_key("quantity"));

    load_request_spec("cancel_order.json")
        .assert_matches(&requests[2].actual_http_request())
        .expect("cancel order request spec");
    assert_signed_request_method(&requests[2], "DELETE", "/api/v3/order");
    assert_eq!(
        requests[2].query.get("orderId").map(String::as_str),
        Some("2001")
    );
    assert_eq!(
        requests[2]
            .query
            .get("origClientOrderId")
            .map(String::as_str),
        Some("LIMIT1")
    );

    assert_signed_request_method(&requests[3], "DELETE", "/api/v3/openOrders");
    load_request_spec("cancel_all_orders.json")
        .assert_matches(&requests[3].actual_http_request())
        .expect("cancel all request spec");
    assert_eq!(
        requests[3].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
}

#[tokio::test]
async fn mexc_adapter_should_place_perpetual_order_on_contract_api() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "success": true,
        "code": 0,
        "data": {"orderId": "3001", "externalOid": "PERP1"}
    })])
    .await;
    let adapter = MexcGatewayAdapter::new(MexcGatewayConfig {
        contract_rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..MexcGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-place"),
            symbol: perpetual_symbol_scope(),
            client_order_id: Some("PERP1".to_string()),
            side: OrderSide::Sell,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "2".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: true,
            post_only: false,
        })
        .await
        .expect("perp place");

    assert_eq!(response.order.market_type, MarketType::Perpetual);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/api/v1/private/order/submit");
    assert_eq!(
        request.headers.get("apikey").map(String::as_str),
        Some("key")
    );
    assert!(request
        .headers
        .get("request-time")
        .is_some_and(|value| !value.is_empty()));
    assert!(request
        .headers
        .get("signature")
        .is_some_and(|value| !value.is_empty()));
    let body = request.body.as_ref().expect("contract order body");
    assert_eq!(body["symbol"], "BTC_USDT");
    assert_eq!(body["side"], 4);
    assert_eq!(body["type"], 1);
    assert_eq!(body["vol"], "2");
    assert_eq!(body["price"], "65000");
    assert_eq!(body["externalOid"], "PERP1");
}

#[test]
fn mexc_signing_should_match_known_hmac() {
    let vector = signing_vector("place_order_limit.json");
    let signature = super::signing::sign_raw_query(
        vector["secret"].as_str().expect("secret"),
        vector["payload"].as_str().expect("payload"),
    )
    .expect("signature");
    assert_eq!(
        signature,
        vector["expected_signature"]
            .as_str()
            .expect("expected signature")
    );
}

fn load_request_spec(path: &str) -> RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/mexc/request_specs/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("request spec fixture");
    serde_json::from_str(&text).expect("request spec fixture")
}

fn signing_vector(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/mexc/signing_vectors/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("signing vector fixture");
    serde_json::from_str(&text).expect("signing vector fixture")
}

#[tokio::test]
async fn mexc_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "balances": [
                {"asset": "BTC", "free": "0.10000000", "locked": "0.02000000"},
                {"asset": "USDT", "free": "123.45", "locked": "1.55"}
            ]
        }),
        json!({
            "symbol": "BTCUSDT",
            "orderId": 1001,
            "clientOrderId": "CLIENT1",
            "price": "65000",
            "origQty": "0.01",
            "executedQty": "0.004",
            "cummulativeQuoteQty": "259.8",
            "status": "PARTIALLY_FILLED",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "time": 1700000000000i64,
            "updateTime": 1700000001000i64
        }),
        json!([{
            "symbol": "BTCUSDT",
            "orderId": 1002,
            "clientOrderId": "CLIENT2",
            "price": "70000",
            "origQty": "0.02",
            "executedQty": "0",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "SELL",
            "time": 1700000002000i64,
            "updateTime": 1700000002000i64
        }]),
        json!([{
            "symbol": "BTCUSDT",
            "makerCommission": "0.0008",
            "takerCommission": "0.001"
        }]),
        json!([{
            "symbol": "BTCUSDT",
            "id": 2001,
            "orderId": 1001,
            "clientOrderId": "CLIENT1",
            "price": "64950",
            "qty": "0.004",
            "commission": "0.2598",
            "commissionAsset": "USDT",
            "isBuyer": true,
            "time": 1700000001000i64
        }]),
    ])
    .await;
    let adapter = MexcGatewayAdapter::new(MexcGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..MexcGatewayConfig::default()
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
    assert_eq!(balances.balances[0].balances[0].available, 0.1);

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
    assert_eq!(order.client_order_id.as_deref(), Some("CLIENT1"));
    assert_eq!(order.side, OrderSide::Buy);
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.quantity, "0.01");
    assert_eq!(order.filled_quantity, "0.004");
    assert_eq!(order.average_fill_price.as_deref(), Some("64950"));

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
    assert_eq!(fees.fees[0].maker_rate, "0.0008");
    assert_eq!(fees.fees[0].taker_rate, "0.001");

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
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("2001"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("1001"));
    assert_eq!(fills.fills[0].side, OrderSide::Buy);
    assert_eq!(fills.fills[0].price, 64_950.0);
    assert_eq!(fills.fills[0].quantity, 0.004);
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("USDT"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    load_request_spec("get_balances.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("get balances request spec");
    assert_signed_request(&requests[0], "/api/v3/account");
    assert!(requests[0].query.get("symbol").is_none());
    load_request_spec("query_order.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("query order request spec");
    assert_signed_request(&requests[1], "/api/v3/order");
    assert_eq!(
        requests[1].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(
        requests[1].query.get("orderId").map(String::as_str),
        Some("1001")
    );
    load_request_spec("get_open_orders.json")
        .assert_matches(&requests[2].actual_http_request())
        .expect("get open orders request spec");
    assert_signed_request(&requests[2], "/api/v3/openOrders");
    load_request_spec("get_fees.json")
        .assert_matches(&requests[3].actual_http_request())
        .expect("get fees request spec");
    assert_signed_request(&requests[3], "/api/v3/tradeFee");
    load_request_spec("get_recent_fills.json")
        .assert_matches(&requests[4].actual_http_request())
        .expect("get recent fills request spec");
    assert_signed_request(&requests[4], "/api/v3/myTrades");
    assert_eq!(
        requests[4].query.get("limit").map(String::as_str),
        Some("100")
    );
}
