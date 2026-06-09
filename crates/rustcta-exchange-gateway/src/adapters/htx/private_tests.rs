use rustcta_exchange_api::{
    BalancesRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest,
    OpenOrdersRequest, PlaceOrderRequest, PositionsRequest, SymbolScope, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType, PositionSide,
};
use serde_json::json;

use super::config::HtxGatewayProfile;
use super::test_support::{
    context, exchange_id, load_request_spec, perp_symbol, spawn_rest_server, spot_symbol,
};
use super::{HtxGatewayAdapter, HtxGatewayConfig};

#[tokio::test]
async fn htx_private_operations_should_require_credentials() {
    let adapter = HtxGatewayAdapter::default_public().expect("adapter");
    assert!(!adapter.capabilities().supports_private_rest);
    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: Vec::new(),
        })
        .await
        .expect_err("unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn htx_adapter_should_route_spot_open_orders_as_signed_get() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "status": "ok",
        "data": [
            {
                "id": 1002,
                "symbol": "btcusdt",
                "type": "buy-limit",
                "state": "submitted",
                "amount": "0.01",
                "price": "65000",
                "field-amount": "0"
            }
        ]
    })])
    .await;
    let adapter = HtxGatewayAdapter::new(HtxGatewayConfig {
        spot_rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..HtxGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol()),
            page: None,
        })
        .await
        .expect("open orders");

    assert_eq!(response.orders.len(), 1);
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("request_specs/open_orders_spot.json")
        .assert_matches(&request.to_actual_request())
        .expect("open orders request spec");
    assert_eq!(request.method, "GET");
    assert!(request.body.is_none());
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("btcusdt")
    );
}

#[tokio::test]
async fn htx_adapter_should_route_perp_order_lifecycle() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"status": "ok", "data": {"order_id": 1001, "order_id_str": "1001"}}),
        json!({"status": "ok", "data": {"success": [{"order_id": "1001"}]}}),
    ])
    .await;
    let adapter = HtxGatewayAdapter::new(HtxGatewayConfig {
        linear_rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..HtxGatewayConfig::default()
    })
    .expect("adapter");

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: perp_symbol(),
            client_order_id: Some("CID1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "1".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("1001"));

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: perp_symbol(),
            client_order_id: Some("CID1".to_string()),
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/linear-swap-api/v1/swap_cross_order");
    load_request_spec("request_specs/place_order_perp_limit.json")
        .assert_matches(&requests[0].to_actual_request())
        .expect("place request spec");
    assert_eq!(
        requests[0].body.as_ref().unwrap()["contract_code"],
        "BTC-USDT"
    );
    assert_eq!(requests[0].body.as_ref().unwrap()["direction"], "buy");
    assert_eq!(requests[1].path, "/linear-swap-api/v1/swap_cross_cancel");
    load_request_spec("request_specs/cancel_order_perp.json")
        .assert_matches(&requests[1].to_actual_request())
        .expect("cancel request spec");
}

#[tokio::test]
async fn huobi_legacy_profile_should_route_positions_as_signed_linear_swap_read() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "status": "ok",
        "data": [
            {
                "contract_code": "BTC-USDT",
                "direction": "buy",
                "volume": 2.0,
                "cost_open": "65000",
                "last_price": "65100",
                "liquidation_price": "40000",
                "profit_unreal": "200",
                "lever_rate": 5
            }
        ]
    })])
    .await;
    let adapter = HtxGatewayAdapter::new(HtxGatewayConfig {
        profile: HtxGatewayProfile::HuobiLegacy,
        linear_rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..HtxGatewayConfig::huobi_legacy()
    })
    .expect("adapter");
    let huobi = ExchangeId::new("huobi").expect("huobi");

    let response = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("huobi-positions"),
            exchange: huobi.clone(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![
                ExchangeSymbol::new(huobi.clone(), MarketType::Perpetual, "BTC-USDT")
                    .expect("symbol"),
            ],
        })
        .await
        .expect("positions");

    assert_eq!(response.positions.len(), 1);
    let position = &response.positions[0];
    assert_eq!(position.exchange_id, huobi);
    assert_eq!(
        position.canonical_symbol,
        CanonicalSymbol::new("BTC", "USDT").expect("canonical")
    );
    assert_eq!(position.side, PositionSide::Long);
    assert_eq!(position.quantity, 2.0);
    assert_eq!(position.entry_price, Some(65000.0));
    assert_eq!(position.mark_price, Some(65100.0));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/linear-swap-api/v1/swap_cross_position_info");
    assert_eq!(request.body.as_ref().unwrap()["contract_code"], "BTC-USDT");
    assert_eq!(
        request.query.get("AccessKeyId").map(String::as_str),
        Some("test-key")
    );
    assert_eq!(
        request.query.get("SignatureMethod").map(String::as_str),
        Some("HmacSHA256")
    );
    assert_eq!(
        request.query.get("SignatureVersion").map(String::as_str),
        Some("2")
    );
    assert!(request.query.contains_key("Timestamp"));
    assert!(request.query.contains_key("Signature"));
    assert!(!request
        .query
        .values()
        .any(|value| value.contains("test-secret")));
    load_huobi_request_spec("request_specs/get_positions_linear_cross.json")
        .assert_matches(&request.to_actual_request())
        .expect("huobi positions request spec");
}

#[tokio::test]
async fn huobi_legacy_profile_should_route_balances_as_signed_linear_swap_read() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "status": "ok",
        "data": [
            {
                "margin_asset": "USDT",
                "margin_balance": "1000.5",
                "available_margin": "900.25",
                "withdraw_available": "900.25",
                "margin_frozen": "100.25"
            }
        ]
    })])
    .await;
    let adapter = HtxGatewayAdapter::new(HtxGatewayConfig {
        profile: HtxGatewayProfile::HuobiLegacy,
        linear_rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..HtxGatewayConfig::huobi_legacy()
    })
    .expect("adapter");
    let huobi = ExchangeId::new("huobi").expect("huobi");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("huobi-balances"),
            exchange: huobi.clone(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("balances");

    assert_eq!(response.balances.len(), 1);
    let balance = &response.balances[0];
    assert_eq!(balance.exchange_id, huobi);
    assert_eq!(balance.market_type, MarketType::Perpetual);
    assert_eq!(balance.balances.len(), 1);
    assert_eq!(balance.balances[0].asset, "USDT");
    assert_eq!(balance.balances[0].total, 1000.5);
    assert_eq!(balance.balances[0].available, 900.25);
    assert_eq!(balance.balances[0].locked, 100.25);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/linear-swap-api/v1/swap_cross_account_info");
    assert_eq!(
        request.query.get("AccessKeyId").map(String::as_str),
        Some("test-key")
    );
    assert_eq!(
        request.query.get("SignatureMethod").map(String::as_str),
        Some("HmacSHA256")
    );
    assert!(request.query.contains_key("Timestamp"));
    assert!(request.query.contains_key("Signature"));
    assert!(!request
        .query
        .values()
        .any(|value| value.contains("test-secret")));
    load_huobi_request_spec("request_specs/get_balances_linear_cross.json")
        .assert_matches(&request.to_actual_request())
        .expect("huobi balances request spec");
}

#[tokio::test]
async fn huobi_legacy_profile_should_route_fees_as_signed_linear_swap_read() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "status": "ok",
        "data": [
            {
                "contract_code": "BTC-USDT",
                "maker_fee_rate": "0.0002",
                "taker_fee_rate": "0.0005"
            }
        ]
    })])
    .await;
    let adapter = HtxGatewayAdapter::new(HtxGatewayConfig {
        profile: HtxGatewayProfile::HuobiLegacy,
        linear_rest_base_url: base_url,
        api_key: Some("test-key".to_string()),
        api_secret: Some("test-secret".to_string()),
        enabled_private_rest: true,
        ..HtxGatewayConfig::huobi_legacy()
    })
    .expect("adapter");
    let huobi = ExchangeId::new("huobi").expect("huobi");
    let symbol = SymbolScope {
        exchange: huobi.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(huobi.clone(), MarketType::Perpetual, "BTC-USDT")
            .expect("symbol"),
    };

    let response = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("huobi-fees"),
            symbols: vec![symbol],
        })
        .await
        .expect("fees");

    assert_eq!(response.fees.len(), 1);
    assert_eq!(response.fees[0].symbol.exchange, huobi);
    assert_eq!(response.fees[0].maker_rate, "0.0002");
    assert_eq!(response.fees[0].taker_rate, "0.0005");
    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/linear-swap-api/v1/swap_fee");
    assert_eq!(request.body.as_ref().unwrap()["contract_code"], "BTC-USDT");
    assert_eq!(
        request.query.get("AccessKeyId").map(String::as_str),
        Some("test-key")
    );
    assert!(request.query.contains_key("Signature"));
    assert!(!request
        .query
        .values()
        .any(|value| value.contains("test-secret")));
    load_huobi_request_spec("request_specs/get_fees_linear_swap.json")
        .assert_matches(&request.to_actual_request())
        .expect("huobi fees request spec");
}

fn load_huobi_request_spec(path: &str) -> crate::request_spec::RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/huobi/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("huobi request spec fixture");
    serde_json::from_str(&text).expect("huobi request spec fixture")
}
