use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchExecutionMode, CancelAllOrdersRequest,
    CancelOrderRequest, CapabilitySupport, CredentialScope, ExchangeApiError, ExchangeClient,
    FeesRequest, OpenOrdersRequest, PlaceOrderRequest, PositionsRequest, QueryOrderRequest,
    QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeSymbol, MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::json;

use super::test_support::{
    assert_signed_request, assert_signed_request_method, context, exchange_id,
    perpetual_symbol_scope, spawn_rest_server, symbol_scope,
};
use super::{GateIoGatewayAdapter, GateIoGatewayConfig};
use crate::request_spec::RequestSpec;

#[tokio::test]
async fn gateio_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = GateIoGatewayAdapter::default_public().expect("adapter");
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
fn gateio_adapter_should_declare_capabilities_v2_for_toolchain_audit() {
    let adapter = GateIoGatewayAdapter::new(GateIoGatewayConfig {
        api_key: Some("gate-key".to_string()),
        api_secret: Some("gate-secret".to_string()),
        enabled_private_rest: true,
        ..GateIoGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Spot, MarketType::Perpetual]
    );
    assert!(capabilities.supports_positions);
    assert!(capabilities.supports_reduce_only);
    assert!(capabilities.supports_post_only);
    assert!(capabilities
        .supports_time_in_force
        .contains(&TimeInForce::IOC));
    assert!(capabilities
        .supports_time_in_force
        .contains(&TimeInForce::FOK));
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
    assert!(capabilities.capabilities_v2.fills_history.supports_from_id);
    assert_eq!(
        capabilities.capabilities_v2.credential_scopes,
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    );
    assert!(capabilities.capabilities_v2.stream_runtime.resync.orders);
}

#[test]
fn gateio_signing_should_match_known_hmac_vector() {
    let vector = signing_vector("place_order_limit.json");
    let signature = super::signing::sign_gateio_request(
        vector["secret"].as_str().expect("secret"),
        vector["method"].as_str().expect("method"),
        "/api/v4/spot/orders",
        "currency_pair=BTC_USDT",
        vector["body"].as_str().expect("body"),
        vector["timestamp"].as_str().expect("timestamp"),
    );
    assert_eq!(
        signature,
        vector["expected_signature"]
            .as_str()
            .expect("expected signature")
    );
}

fn load_request_spec(path: &str) -> RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/gateio/request_specs/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("request spec fixture");
    serde_json::from_str(&text).expect("request spec fixture")
}

fn signing_vector(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/gateio/signing_vectors/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("signing vector fixture");
    serde_json::from_str(&text).expect("signing vector fixture")
}

fn gate_fixture(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/gate/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("gate fixture");
    serde_json::from_str(&text).expect("gate fixture json")
}

#[tokio::test]
async fn gateio_adapter_should_route_perpetual_place_cancel_and_positions() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "id": "15675394",
            "contract": "BTC_USDT",
            "size": -100,
            "left": 100,
            "price": "65000.1",
            "tif": "ioc",
            "text": "t-PERP1",
            "status": "open",
            "is_reduce_only": true,
            "create_time_ms": "1743054549000"
        }),
        json!({
            "id": "15675394",
            "contract": "BTC_USDT",
            "size": -100,
            "left": 0,
            "price": "65000.1",
            "text": "t-PERP1",
            "finish_as": "cancelled",
            "status": "finished",
            "is_reduce_only": true,
            "create_time_ms": "1743054549000"
        }),
        gate_fixture("position.json"),
    ])
    .await;
    let adapter = GateIoGatewayAdapter::new(GateIoGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("gate-key".to_string()),
        api_secret: Some("gate-secret".to_string()),
        enabled_private_rest: true,
        ..GateIoGatewayConfig::default()
    })
    .expect("adapter");

    let symbol = perpetual_symbol_scope("BTCUSDT");
    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-place"),
            symbol: symbol.clone(),
            client_order_id: Some("PERP1".to_string()),
            side: OrderSide::Sell,
            position_side: Some(PositionSide::Short),
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::IOC),
            quantity: "100".to_string(),
            price: Some("65000.1".to_string()),
            quote_quantity: None,
            reduce_only: true,
            post_only: false,
        })
        .await
        .expect("place perpetual order");
    assert_eq!(placed.order.market_type, MarketType::Perpetual);
    assert_eq!(placed.order.side, OrderSide::Sell);
    assert_eq!(placed.order.position_side, Some(PositionSide::Short));
    assert!(placed.order.reduce_only);
    assert_eq!(placed.order.quantity, "100");

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-cancel"),
            symbol: symbol.clone(),
            client_order_id: None,
            exchange_order_id: Some("15675394".to_string()),
        })
        .await
        .expect("cancel perpetual order");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);

    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![
                ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTCUSDT")
                    .expect("position symbol"),
            ],
        })
        .await
        .expect("positions");
    assert_eq!(positions.positions.len(), 1);
    assert_eq!(positions.positions[0].market_type, MarketType::Perpetual);
    assert_eq!(positions.positions[0].side, PositionSide::Long);
    assert_eq!(positions.positions[0].quantity, 100.0);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert_signed_request_method(&requests[0], "POST", "/futures/usdt/orders");
    load_request_spec("perpetual_place_order.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("perpetual place order request spec");
    assert_eq!(
        requests[0]
            .headers
            .get("x-gate-size-decimal")
            .map(String::as_str),
        Some("1")
    );
    let body = requests[0].body.as_ref().expect("perp place body");
    assert_eq!(body["contract"], "BTC_USDT");
    assert_eq!(body["size"], "-100");
    assert_eq!(body["tif"], "ioc");
    assert_eq!(body["text"], "t-PERP1");
    assert_eq!(body["reduce_only"], true);

    assert_signed_request_method(&requests[1], "DELETE", "/futures/usdt/orders/15675394");
    load_request_spec("perpetual_cancel_order.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("perpetual cancel order request spec");
    assert_eq!(
        requests[1].query.get("contract").map(String::as_str),
        Some("BTC_USDT")
    );

    assert_signed_request_method(&requests[2], "GET", "/futures/usdt/positions/BTC_USDT");
    load_request_spec("perpetual_positions.json")
        .assert_matches(&requests[2].actual_http_request())
        .expect("perpetual positions request spec");
}

#[tokio::test]
async fn gateio_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "id": "2001",
            "currency_pair": "BTC_USDT",
            "text": "t-LIMIT1",
            "side": "buy",
            "type": "limit",
            "time_in_force": "gtc",
            "status": "open",
            "price": "65000",
            "amount": "0.02",
            "left": "0.02",
            "create_time_ms": "1743054548123"
        }),
        json!({
            "id": "2002",
            "currency_pair": "BTC_USDT",
            "text": "t-QUOTE1",
            "side": "buy",
            "type": "market",
            "time_in_force": "ioc",
            "status": "open",
            "price": "0",
            "amount": "25.5",
            "left": "25.5",
            "create_time_ms": "1743054549123"
        }),
        json!({
            "id": "2001",
            "currency_pair": "BTC_USDT",
            "text": "t-LIMIT1",
            "status": "cancelled"
        }),
        json!([
            {"id": "2003", "currency_pair": "BTC_USDT", "text": "t-CANCELALL1", "status": "cancelled"},
            {"id": "2004", "currency_pair": "BTC_USDT", "text": "t-CANCELALL2", "status": "cancelled"}
        ]),
        json!({
            "id": "2005",
            "currency_pair": "BTC_USDT",
            "text": "t-AMEND1",
            "side": "buy",
            "type": "limit",
            "status": "open",
            "price": "65000",
            "amount": "0.015",
            "left": "0.015",
            "create_time_ms": "1743054550123"
        }),
    ])
    .await;
    let adapter = GateIoGatewayAdapter::new(GateIoGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("gate-key".to_string()),
        api_secret: Some("gate-secret".to_string()),
        enabled_private_rest: true,
        ..GateIoGatewayConfig::default()
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
            symbol: symbol_scope("BTCUSDT"),
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
    assert_eq!(placed.order.client_order_id.as_deref(), Some("t-LIMIT1"));
    assert_eq!(placed.order.status, OrderStatus::New);

    let quote = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote-order"),
            symbol: symbol_scope("BTC_USDT"),
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
            symbol: symbol_scope("BTC-USDT"),
            client_order_id: None,
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
            symbol: Some(symbol_scope("BTCUSDT")),
        })
        .await
        .expect("cancel all");
    assert_eq!(cancel_all.cancelled_count, 2);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend-order"),
            symbol: symbol_scope("BTC_USDT"),
            client_order_id: None,
            exchange_order_id: Some("2005".to_string()),
            new_client_order_id: None,
            new_quantity: "0.015".to_string(),
        })
        .await
        .expect("amend order");
    assert_eq!(amended.order.exchange_order_id.as_deref(), Some("2005"));
    assert_eq!(amended.order.quantity, "0.015");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);

    load_request_spec("place_order.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("place order request spec");
    assert_signed_request_method(&requests[0], "POST", "/spot/orders");
    let body = requests[0].body.as_ref().expect("place body");
    assert_eq!(body["currency_pair"], "BTC_USDT");
    assert_eq!(body["side"], "buy");
    assert_eq!(body["type"], "limit");
    assert_eq!(body["amount"], "0.02");
    assert_eq!(body["price"], "65000");
    assert_eq!(body["time_in_force"], "gtc");
    assert_eq!(body["text"], "t-LIMIT1");

    assert_signed_request_method(&requests[1], "POST", "/spot/orders");
    load_request_spec("place_quote_market_order.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("quote market order request spec");
    let body = requests[1].body.as_ref().expect("quote body");
    assert_eq!(body["currency_pair"], "BTC_USDT");
    assert_eq!(body["side"], "buy");
    assert_eq!(body["type"], "market");
    assert_eq!(body["amount"], "25.5");
    assert_eq!(body["time_in_force"], "ioc");
    assert_eq!(body["text"], "t-QUOTE1");

    load_request_spec("cancel_order.json")
        .assert_matches(&requests[2].actual_http_request())
        .expect("cancel order request spec");
    assert_signed_request_method(&requests[2], "DELETE", "/spot/orders/2001");
    assert_eq!(
        requests[2].query.get("currency_pair").map(String::as_str),
        Some("BTC_USDT")
    );

    assert_signed_request_method(&requests[3], "DELETE", "/spot/orders");
    load_request_spec("cancel_all_orders.json")
        .assert_matches(&requests[3].actual_http_request())
        .expect("cancel all request spec");
    assert_eq!(
        requests[3].query.get("currency_pair").map(String::as_str),
        Some("BTC_USDT")
    );

    assert_signed_request_method(&requests[4], "PATCH", "/spot/orders/2005");
    load_request_spec("amend_order.json")
        .assert_matches(&requests[4].actual_http_request())
        .expect("amend order request spec");
    let body = requests[4].body.as_ref().expect("amend body");
    assert_eq!(body["currency_pair"], "BTC_USDT");
    assert_eq!(body["account"], "spot");
    assert_eq!(body["amount"], "0.015");
}

#[tokio::test]
async fn gateio_adapter_should_sign_private_readback_requests_and_parse_responses() {
    let order = json!({
        "id": "1001",
        "text": "t-CLIENT1",
        "currency_pair": "BTC_USDT",
        "side": "buy",
        "type": "limit",
        "time_in_force": "gtc",
        "status": "open",
        "price": "65000",
        "amount": "0.01",
        "left": "0.004",
        "avg_deal_price": "65010",
        "create_time_ms": "1710000000000",
        "update_time_ms": "1710000001000"
    });
    let fill = json!({
        "id": "trade-1",
        "order_id": "1001",
        "text": "t-CLIENT1",
        "currency_pair": "BTC_USDT",
        "side": "buy",
        "price": "65000",
        "amount": "0.006",
        "fee": "0.000006",
        "fee_currency": "BTC",
        "role": "maker",
        "create_time_ms": "1710000002000"
    });
    let (base_url, seen) = spawn_rest_server(vec![
        json!([
            {"currency": "BTC", "available": "0.5", "locked": "0.1"},
            {"currency": "ETH", "available": "0", "locked": "0"}
        ]),
        order.clone(),
        json!([{"currency_pair": "BTC_USDT", "orders": [order.clone()]}]),
        json!({"maker_fee": "0.001", "taker_fee": "0.002"}),
        json!([fill]),
    ])
    .await;
    let adapter = GateIoGatewayAdapter::new(GateIoGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("gate-key".to_string()),
        api_secret: Some("gate-secret".to_string()),
        enabled_private_rest: true,
        ..GateIoGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.capabilities().supports_private_rest);
    assert!(adapter.capabilities().supports_balances);

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
    assert_eq!(balances.balances[0].balances[0].total, 0.6);

    let queried = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope("BTCUSDT"),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("query order")
        .order
        .expect("order");
    assert_eq!(queried.exchange_order_id.as_deref(), Some("1001"));
    assert_eq!(queried.client_order_id.as_deref(), Some("t-CLIENT1"));
    assert_eq!(queried.side, OrderSide::Buy);
    assert_eq!(queried.status, OrderStatus::PartiallyFilled);
    assert_eq!(queried.quantity, "0.01");
    assert_eq!(queried.filled_quantity, "0.006");
    assert_eq!(queried.average_fill_price.as_deref(), Some("65010"));

    let open_orders = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope("BTC_USDT")),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open_orders.orders.len(), 1);
    assert_eq!(
        open_orders.orders[0].client_order_id.as_deref(),
        Some("t-CLIENT1")
    );

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope("BTC_USDT")],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees.len(), 1);
    assert_eq!(fees.fees[0].maker_rate, "0.001");
    assert_eq!(fees.fees[0].taker_rate, "0.002");

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope("BTC-USDT")),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
            from_trade_id: Some("trade-0".to_string()),
            start_time: None,
            end_time: None,
            limit: Some(50),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("trade-1"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("1001"));
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("BTC"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    load_request_spec("get_balances.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("get balances request spec");
    assert_signed_request(&requests[0], "/spot/accounts");
    load_request_spec("query_order.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("query order request spec");
    assert_signed_request(&requests[1], "/spot/orders/1001");
    load_request_spec("get_open_orders.json")
        .assert_matches(&requests[2].actual_http_request())
        .expect("get open orders request spec");
    assert_signed_request(&requests[2], "/spot/open_orders");
    load_request_spec("get_fees.json")
        .assert_matches(&requests[3].actual_http_request())
        .expect("get fees request spec");
    assert_signed_request(&requests[3], "/spot/fee");
    load_request_spec("get_recent_fills.json")
        .assert_matches(&requests[4].actual_http_request())
        .expect("get recent fills request spec");
    assert_signed_request(&requests[4], "/spot/my_trades");
    assert_eq!(
        requests[1].query.get("currency_pair").map(String::as_str),
        Some("BTC_USDT")
    );
    assert_eq!(
        requests[4].query.get("order_id").map(String::as_str),
        Some("1001")
    );
    assert_eq!(
        requests[4].query.get("last_id").map(String::as_str),
        Some("trade-0")
    );
    assert_eq!(
        requests[4].query.get("limit").map(String::as_str),
        Some("50")
    );
}
