use std::collections::HashMap;

use chrono::TimeZone;
use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, ExchangeStreamEvent, FeesRequest,
    OpenOrdersRequest, PlaceOrderRequest, PrivateStreamKind, PrivateStreamSubscription,
    QueryOrderRequest, QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeSymbol, MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::signing::sign_lbank_params;
use super::streams::{lbank_ping_payload, LBankWsControlMessage, LBankWsSessionEvent};
use super::test_support::{
    assert_signed_form_request, assert_signed_json_request, context, exchange_id, private_config,
    spawn_rest_server, spot_symbol_scope,
};
use super::LBankGatewayAdapter;

#[tokio::test]
async fn lbank_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = LBankGatewayAdapter::default_public().expect("adapter");
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
fn lbank_private_parser_fixtures_should_cover_success_and_missing_fields() {
    let exchange = exchange_id();
    let tenant_id = rustcta_types::TenantId::new("tenant").expect("tenant");
    let account_id = rustcta_types::AccountId::new("account").expect("account");

    let balances = super::private_parser::parse_balances(
        &exchange,
        tenant_id.clone(),
        account_id.clone(),
        &[],
        &serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/lbank/spot_balance.json"
        ))
        .expect("spot balance fixture"),
    )
    .expect("balances");
    assert_eq!(balances[0].balances.len(), 2);
    assert_eq!(balances[0].balances[0].asset, "BTC");

    let order = super::private_parser::parse_order_state(
        &exchange,
        Some(&spot_symbol_scope()),
        &serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/lbank/spot_order_ack.json"
        ))
        .expect("spot order fixture"),
    )
    .expect("spot order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("2001"));
    assert_eq!(order.client_order_id.as_deref(), Some("LIMIT1"));

    let contract_order = super::private_parser::parse_contract_order_state(
        &exchange,
        Some(&super::test_support::perp_symbol_scope()),
        &serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/lbank/contract_order_ack.json"
        ))
        .expect("contract order fixture"),
    )
    .expect("contract order");
    assert_eq!(contract_order.market_type, MarketType::Perpetual);

    let missing_symbol = serde_json::json!({
        "result": "true",
        "data": {"order_id": "missing-symbol", "type": "buy", "status": 0}
    });
    assert!(super::private_parser::parse_order_state(&exchange, None, &missing_symbol).is_err());
}

#[tokio::test]
async fn lbank_adapter_should_create_private_stream_subscription_key() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "result": "true",
        "error_code": 0,
        "key": "sub-key"
    })])
    .await;
    let adapter = LBankGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let subscription_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-stream"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: rustcta_types::AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect("private stream");

    assert!(subscription_id.contains("wss://www.lbkex.net/ws/V2/"));
    assert!(subscription_id.contains(":orderUpdate:account"));
    let requests = seen.lock().unwrap().clone();
    assert_signed_form_request(&requests[0], "/v2/subscribe/get_key.do");
}

#[tokio::test]
async fn lbank_private_ws_session_should_manage_subscribe_key_and_heartbeat() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "result": "true",
            "error_code": 0,
            "data": "sub-key"
        }),
        json!({
            "result": "true",
            "error_code": 0
        }),
        json!({
            "result": true,
            "error_code": 0
        }),
    ])
    .await;
    let adapter = LBankGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let mut session = adapter
        .private_ws_session(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-session"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: rustcta_types::AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Balances,
        })
        .await
        .expect("session");

    assert_eq!(session.url, "wss://www.lbkex.net/ws/V2/");
    assert_eq!(session.subscribe_key(), "sub-key");
    assert_eq!(session.initial_requests()[0]["subscribe"], "assetUpdate");
    assert_eq!(session.initial_requests()[0]["subscribeKey"], "sub-key");
    let ping_at = chrono::Utc
        .timestamp_millis_opt(1_700_000_000_001)
        .single()
        .expect("timestamp");
    assert_eq!(
        session.heartbeat_request(ping_at),
        lbank_ping_payload("rustcta-1700000000001")
    );

    let events = session
        .handle_text_message(r#"{"action":"pong","pong":"rustcta-1700000000001"}"#)
        .expect("pong");
    assert!(matches!(
        events.first(),
        Some(LBankWsSessionEvent::Private(
            LBankWsControlMessage::HeartbeatPong { nonce }
        )) if nonce.as_deref() == Some("rustcta-1700000000001")
    ));
    assert!(matches!(
        events.iter().find(|event| matches!(event, LBankWsSessionEvent::Stream(_))),
        Some(LBankWsSessionEvent::Stream(items))
            if matches!(items.first(), Some(ExchangeStreamEvent::Heartbeat { .. }))
    ));

    let order_events = session
        .handle_text_message(
            r#"{"type":"orderUpdate","data":{"symbol":"btc_usdt","orderId":"2001","clientOrderId":"LIMIT1","type":"buy","status":1,"amount":"0.01","deal_amount":"0.004","price":"65000","time":1700000000000}}"#,
        )
        .expect("order update");
    assert!(matches!(
        order_events.iter().find(|event| {
            matches!(
                event,
                LBankWsSessionEvent::Stream(items)
                    if matches!(items.first(), Some(ExchangeStreamEvent::OrderUpdate(_)))
            )
        }),
        Some(LBankWsSessionEvent::Stream(_))
    ));

    let balance_events = session
        .handle_text_message(
            r#"{"type":"assetUpdate","data":{"balances":[{"assetCode":"USDT","assetAmt":"102.5","usableAmt":"100","locked":"2.5"}]}}"#,
        )
        .expect("balance update");
    assert!(matches!(
        balance_events.iter().find(|event| {
            matches!(
                event,
                LBankWsSessionEvent::Stream(items)
                    if matches!(items.first(), Some(ExchangeStreamEvent::BalanceSnapshot(_)))
            )
        }),
        Some(LBankWsSessionEvent::Stream(_))
    ));

    adapter
        .refresh_spot_subscribe_key(session.subscribe_key())
        .await
        .expect("refresh");
    adapter
        .destroy_spot_subscribe_key(session.subscribe_key())
        .await
        .expect("destroy");

    let requests = seen.lock().unwrap().clone();
    assert_signed_form_request(&requests[0], "/v2/subscribe/get_key.do");
    assert_signed_form_request(&requests[1], "/v2/subscribe/refresh_key.do");
    assert_eq!(
        requests[1].form.get("subscribeKey").map(String::as_str),
        Some("sub-key")
    );
    assert_signed_form_request(&requests[2], "/v2/subscribe/destroy_key.do");
    assert_eq!(
        requests[2].form.get("subscribeKey").map(String::as_str),
        Some("sub-key")
    );
}

#[tokio::test]
async fn lbank_adapter_should_route_contract_account_and_place_order() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "result": true,
            "error_code": 0,
            "data": {
                "asset": "USDT",
                "balance": "150.5",
                "available": "120.25",
                "positions": [{
                    "symbol": "BTCUSDT",
                    "side": "LONG",
                    "volume": "0.02",
                    "entryPrice": "65000",
                    "markPrice": "65100",
                    "leverage": "5"
                }]
            }
        }),
        json!({
            "result": true,
            "error_code": 0,
            "data": {
                "asset": "USDT",
                "balance": "150.5",
                "available": "120.25",
                "positions": [{
                    "symbol": "BTCUSDT",
                    "side": "LONG",
                    "volume": "0.02",
                    "entryPrice": "65000",
                    "markPrice": "65100",
                    "leverage": "5"
                }]
            }
        }),
        json!({
            "result": true,
            "error_code": 0,
            "data": {
                "orderId": "9001",
                "clientOrderId": "PERP1",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "orderPriceType": 4,
                "offsetFlag": 0,
                "price": "65000",
                "volume": "0.02",
                "status": 0
            }
        }),
    ])
    .await;
    let adapter = LBankGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("contract-balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("contract balances");
    assert_eq!(balances.balances[0].market_type, MarketType::Perpetual);
    assert_eq!(balances.balances[0].balances[0].available, 120.25);

    let positions = adapter
        .get_positions(rustcta_exchange_api::PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![
                ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTCUSDT")
                    .expect("symbol"),
            ],
        })
        .await
        .expect("positions");
    assert_eq!(positions.positions.len(), 1);
    assert_eq!(positions.positions[0].quantity, 0.02);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("contract-place"),
            symbol: super::test_support::perp_symbol_scope(),
            client_order_id: Some("PERP1".to_string()),
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
        .expect("contract place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("9001"));
    assert_eq!(placed.order.market_type, MarketType::Perpetual);

    let requests = seen.lock().unwrap().clone();
    assert_signed_json_request(&requests[0], "/cfd/openApi/v1/prv/account");
    assert_eq!(requests[0].json["productGroup"], "SwapU");
    assert_eq!(requests[0].json["asset"], "USDT");
    assert_signed_json_request(&requests[1], "/cfd/openApi/v1/prv/account");
    assert_signed_json_request(&requests[2], "/cfd/openApi/v1/prv/placeOrder");
    assert_eq!(requests[2].json["clientOrderId"], "PERP1");
    assert_eq!(requests[2].json["orderPriceType"], "4");
    assert_eq!(requests[2].json["offsetFlag"], "0");
    assert_eq!(requests[2].json["side"], "BUY");
}

#[test]
fn lbank_signing_should_match_placeholder_contract_hmac_vector() {
    let mut params = HashMap::new();
    params.insert("api_key".to_string(), "<lbank-api-key>".to_string());
    params.insert("asset".to_string(), "USDT".to_string());
    params.insert(
        "echostr".to_string(),
        "rustcta-lbank-test-echostr-00000001".to_string(),
    );
    params.insert("productGroup".to_string(), "SwapU".to_string());
    params.insert("signature_method".to_string(), "HmacSHA256".to_string());
    params.insert("timestamp".to_string(), "1665990154559".to_string());

    let signature = sign_lbank_params("<lbank-api-secret>", &params).expect("signature");
    assert_eq!(
        signature,
        "5f756201e24da3656f4a721359356c40dd7b41fc211c24ae54d70ac556752362"
    );
}

#[test]
fn lbank_signing_vector_fixture_should_match_adapter_signer() {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/lbank/signing_vectors/contract_account_official_hmac_sha256.json"
    ))
    .expect("signing vector json");
    let params = vector["params"]
        .as_object()
        .expect("params object")
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                value.as_str().expect("string param").to_string(),
            )
        })
        .collect::<HashMap<_, _>>();

    assert_eq!(
        super::signing::build_query_string(&params),
        vector["canonical_string"]
            .as_str()
            .expect("canonical string")
    );
    assert_eq!(
        format!(
            "{:x}",
            md5::compute(vector["canonical_string"].as_str().unwrap())
        )
        .to_ascii_uppercase(),
        vector["md5_uppercase"].as_str().expect("md5 uppercase")
    );
    assert_eq!(
        sign_lbank_params(vector["secret"].as_str().expect("secret"), &params).expect("signature"),
        vector["expected_signature"]
            .as_str()
            .expect("expected signature")
    );
}

#[test]
fn lbank_mapping_and_request_spec_fixtures_should_be_parseable() {
    let mapping_text = include_str!("endpoint_mapping.yaml");
    assert!(mapping_text.contains("exchange: lbank"));
    assert!(mapping_text.contains("path: /v2/supplement/create_order.do"));
    assert!(mapping_text.contains("path: /cfd/openApi/v1/prv/placeOrder"));
    assert!(mapping_text.contains("request_spec_required: true"));
    assert!(mapping_text.contains("unsupported:"));

    let capabilities_text = include_str!("capabilities_v2.yaml");
    assert!(capabilities_text.contains("linear_perp: confirmed_account_and_place_order_subset"));
    assert!(capabilities_text.contains("rate_limit_plan:"));
    assert!(capabilities_text.contains("pagination:"));
    assert!(capabilities_text.contains("reconciliation:"));
    assert!(capabilities_text.contains("auth_renewal_policy:"));
    assert!(capabilities_text.contains("unsupported_gaps:"));

    let required_specs = [
        (
            "place_order",
            "/v2/supplement/create_order.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_place_order.json"
            ),
        ),
        (
            "cancel_order",
            "/v2/supplement/cancel_order.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_cancel_order.json"
            ),
        ),
        (
            "batch_place_orders",
            "/v2/batch_create_order.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_batch_place_orders.json"
            ),
        ),
        (
            "batch_cancel_orders",
            "/v2/supplement/cancel_order.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_batch_cancel_orders.json"
            ),
        ),
        (
            "cancel_all_orders",
            "/v2/supplement/cancel_order_by_symbol.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_cancel_all_orders.json"
            ),
        ),
        (
            "get_balances",
            "/v2/supplement/user_info_account.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_get_balances.json"
            ),
        ),
        (
            "get_fees",
            "/v2/supplement/customer_trade_fee.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_get_fees.json"
            ),
        ),
        (
            "query_order",
            "/v2/supplement/orders_info.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_query_order.json"
            ),
        ),
        (
            "get_open_orders",
            "/v2/supplement/orders_info_no_deal.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_get_open_orders.json"
            ),
        ),
        (
            "get_recent_fills",
            "/v2/order_transaction_detail.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_get_recent_fills_by_order.json"
            ),
        ),
        (
            "get_recent_fills_history",
            "/v2/supplement/transaction_history.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_get_recent_fills_history.json"
            ),
        ),
        (
            "place_quote_market_order",
            "/v2/supplement/create_order.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_place_quote_market_order.json"
            ),
        ),
        (
            "private_stream_key_create",
            "/v2/subscribe/get_key.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_private_stream_key_create.json"
            ),
        ),
        (
            "private_stream_key_refresh",
            "/v2/subscribe/refresh_key.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_private_stream_key_refresh.json"
            ),
        ),
        (
            "private_stream_key_destroy",
            "/v2/subscribe/destroy_key.do",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/spot_private_stream_key_destroy.json"
            ),
        ),
        (
            "get_balances",
            "/cfd/openApi/v1/prv/account",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/contract_get_balances.json"
            ),
        ),
        (
            "get_positions",
            "/cfd/openApi/v1/prv/account",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/contract_get_positions.json"
            ),
        ),
        (
            "place_order",
            "/cfd/openApi/v1/prv/placeOrder",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/contract_place_order.json"
            ),
        ),
        (
            "batch_place_orders",
            "/cfd/openApi/v1/prv/placeOrder",
            include_str!(
                "../../../../../tests/fixtures/exchanges/lbank/request_specs/contract_batch_place_orders.json"
            ),
        ),
    ];
    for (operation, path, spec_text) in required_specs {
        let spec: serde_json::Value = serde_json::from_str(spec_text).expect("request spec json");
        assert_eq!(spec["exchange"], "lbank");
        assert_eq!(spec["operation"], operation);
        assert_eq!(spec["path"], path);
        assert_eq!(spec["auth"], "md5_hmac_sha256");
    }
}

#[tokio::test]
async fn lbank_adapter_should_route_signed_private_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "result": "true",
            "error_code": 0,
            "data": {
                "order_id": "2001",
                "custom_id": "LIMIT1",
                "symbol": "btc_usdt",
                "type": "buy",
                "status": 0,
                "price": "65000",
                "amount": "0.02"
            }
        }),
        json!({
            "result": "true",
            "error_code": 0,
            "data": {
                "order_id": "2002",
                "custom_id": "QUOTE1",
                "symbol": "btc_usdt",
                "type": "buy_market",
                "status": 0,
                "price": "25.5"
            }
        }),
        json!({
            "result": "true",
            "error_code": 0,
            "data": {
                "orderId": "2001",
                "clientOrderId": "LIMIT1",
                "symbol": "btc_usdt",
                "type": "buy",
                "status": -1
            }
        }),
        json!({
            "result": "true",
            "error_code": 0,
            "data": [{
                "orderId": "2002",
                "clientOrderId": "QUOTE1",
                "symbol": "btc_usdt",
                "type": "buy_market",
                "status": -1
            }]
        }),
    ])
    .await;
    let adapter = LBankGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_positions);
    assert!(capabilities.supports_place_order);
    assert!(capabilities.supports_cancel_order);
    assert!(capabilities.supports_cancel_all_orders);
    assert!(capabilities.supports_recent_fills);
    assert!(capabilities.supports_quote_market_order);
    assert!(!capabilities.supports_amend_order);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: spot_symbol_scope(),
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
        .expect("place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("2001"));

    let quote = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("QUOTE1".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25.5".to_string(),
        })
        .await
        .expect("quote");
    assert_eq!(quote.order.order_type, OrderType::Market);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            exchange_order_id: Some("2001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);

    let cancel_all = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
        })
        .await
        .expect("cancel all");
    assert_eq!(cancel_all.cancelled_count, 1);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);
    assert_signed_form_request(&requests[0], "/v2/supplement/create_order.do");
    assert_eq!(
        requests[0].form.get("symbol").map(String::as_str),
        Some("btc_usdt")
    );
    assert_eq!(
        requests[0].form.get("type").map(String::as_str),
        Some("buy")
    );
    assert_eq!(
        requests[0].form.get("amount").map(String::as_str),
        Some("0.02")
    );
    assert_eq!(
        requests[0].form.get("price").map(String::as_str),
        Some("65000")
    );
    assert_eq!(
        requests[0].form.get("custom_id").map(String::as_str),
        Some("LIMIT1")
    );

    assert_signed_form_request(&requests[1], "/v2/supplement/create_order.do");
    assert_eq!(
        requests[1].form.get("type").map(String::as_str),
        Some("buy_market")
    );
    assert_eq!(
        requests[1].form.get("price").map(String::as_str),
        Some("25.5")
    );
    assert!(!requests[1].form.contains_key("amount"));

    assert_signed_form_request(&requests[2], "/v2/supplement/cancel_order.do");
    assert_eq!(
        requests[2].form.get("orderId").map(String::as_str),
        Some("2001")
    );
    assert_eq!(
        requests[2]
            .form
            .get("origClientOrderId")
            .map(String::as_str),
        Some("LIMIT1")
    );

    assert_signed_form_request(&requests[3], "/v2/supplement/cancel_order_by_symbol.do");
    assert_eq!(
        requests[3].form.get("symbol").map(String::as_str),
        Some("btc_usdt")
    );
}

#[tokio::test]
async fn lbank_adapter_should_route_spot_batch_order_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "result": "true",
            "error_code": 0,
            "data": [
                {
                    "order_id": "3001",
                    "custom_id": "BATCH1",
                    "symbol": "btc_usdt",
                    "type": "buy",
                    "status": 0,
                    "price": "65000",
                    "amount": "0.01"
                },
                {
                    "order_id": "3002",
                    "custom_id": "BATCH2",
                    "symbol": "btc_usdt",
                    "type": "sell",
                    "status": 0,
                    "price": "65100",
                    "amount": "0.02"
                }
            ]
        }),
        json!({
            "result": "true",
            "error_code": 0,
            "data": {
                "orderId": "3001",
                "clientOrderId": "BATCH1",
                "symbol": "btc_usdt",
                "type": "buy",
                "status": -1
            }
        }),
        json!({
            "result": "true",
            "error_code": 0,
            "data": {
                "orderId": "3002",
                "clientOrderId": "BATCH2",
                "symbol": "btc_usdt",
                "type": "sell",
                "status": -1
            }
        }),
    ])
    .await;
    let adapter = LBankGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);

    let order_one = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-1"),
        symbol: spot_symbol_scope(),
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
    let order_two = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-2"),
        symbol: spot_symbol_scope(),
        client_order_id: Some("BATCH2".to_string()),
        side: OrderSide::Sell,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.02".to_string(),
        price: Some("65100".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let batch = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![order_one, order_two],
        })
        .await
        .expect("batch place");
    assert_eq!(batch.orders.len(), 2);
    assert_eq!(batch.orders[0].exchange_order_id.as_deref(), Some("3001"));
    assert_eq!(batch.orders[1].exchange_order_id.as_deref(), Some("3002"));

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-1"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("BATCH1".to_string()),
                    exchange_order_id: Some("3001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-2"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("BATCH2".to_string()),
                    exchange_order_id: Some("3002".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 2);

    let requests = seen.lock().unwrap().clone();
    assert_signed_form_request(&requests[0], "/v2/batch_create_order.do");
    let orders = requests[0]
        .form
        .get("orders")
        .and_then(|orders| urlencoding::decode(orders).ok())
        .and_then(|orders| serde_json::from_str::<serde_json::Value>(&orders).ok())
        .expect("orders json");
    assert_eq!(orders[0]["symbol"], "btc_usdt");
    assert_eq!(orders[0]["custom_id"], "BATCH1");
    assert_eq!(orders[1]["type"], "sell");

    assert_signed_form_request(&requests[1], "/v2/supplement/cancel_order.do");
    assert_eq!(
        requests[1].form.get("orderId").map(String::as_str),
        Some("3001")
    );
    assert_signed_form_request(&requests[2], "/v2/supplement/cancel_order.do");
    assert_eq!(
        requests[2].form.get("orderId").map(String::as_str),
        Some("3002")
    );
}

#[tokio::test]
async fn lbank_adapter_should_batch_place_contract_orders_via_confirmed_place_order() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "result": "true",
            "data": {
                "orderId": "c-3001",
                "clientOrderId": "CBATCH1",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "status": 0,
                "price": "65000",
                "volume": "0.01"
            }
        }),
        json!({
            "result": "true",
            "data": {
                "orderId": "c-3002",
                "clientOrderId": "CBATCH2",
                "symbol": "BTCUSDT",
                "side": "SELL",
                "status": 0,
                "price": "65100",
                "volume": "0.02"
            }
        }),
    ])
    .await;
    let adapter = LBankGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let batch = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("contract-batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("contract-batch-place-1"),
                    symbol: super::test_support::perp_symbol_scope(),
                    client_order_id: Some("CBATCH1".to_string()),
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
                    context: context("contract-batch-place-2"),
                    symbol: super::test_support::perp_symbol_scope(),
                    client_order_id: Some("CBATCH2".to_string()),
                    side: OrderSide::Sell,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.02".to_string(),
                    price: Some("65100".to_string()),
                    quote_quantity: None,
                    reduce_only: true,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("contract batch place");

    assert_eq!(batch.orders.len(), 2);
    assert_eq!(batch.orders[0].market_type, MarketType::Perpetual);
    assert_eq!(batch.orders[0].exchange_order_id.as_deref(), Some("c-3001"));
    assert_eq!(batch.orders[1].exchange_order_id.as_deref(), Some("c-3002"));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_signed_json_request(&requests[0], "/cfd/openApi/v1/prv/placeOrder");
    assert_signed_json_request(&requests[1], "/cfd/openApi/v1/prv/placeOrder");
    assert_eq!(requests[0].json["clientOrderId"], "CBATCH1");
    assert_eq!(requests[0].json["symbol"], "BTCUSDT");
    assert_eq!(requests[1].json["offsetFlag"], "1");
}

#[tokio::test]
async fn lbank_adapter_should_route_contract_raw_signed_extension_post() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "result": "true",
        "error_code": 0,
        "data": {
            "status": "ok"
        }
    })])
    .await;
    let adapter = LBankGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let mut params = HashMap::new();
    params.insert("symbol".to_string(), "BTCUSDT".to_string());
    params.insert("orderId".to_string(), "contract-order-1".to_string());
    let ack = adapter
        .post_contract_signed_raw("/cfd/openApi/v1/prv/cancelOrder", &params)
        .await
        .expect("raw signed contract post");
    assert_eq!(ack.operation, "lbank.contract.raw_signed_post");
    assert_eq!(ack.data.expect("data")["status"], "ok");

    let error = adapter
        .post_contract_signed_raw("/v2/supplement/cancel_order.do", &params)
        .await
        .expect_err("non-contract raw endpoint should fail");
    assert!(matches!(error, ExchangeApiError::InvalidRequest { .. }));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_signed_json_request(&requests[0], "/cfd/openApi/v1/prv/cancelOrder");
    assert_eq!(requests[0].json["symbol"], "BTCUSDT");
    assert_eq!(requests[0].json["orderId"], "contract-order-1");
}

#[tokio::test]
async fn lbank_adapter_should_reject_unconfirmed_contract_order_lifecycle_before_network() {
    let (base_url, seen) = spawn_rest_server(Vec::new()).await;
    let adapter = LBankGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let symbol = super::test_support::perp_symbol_scope();

    let cancel_error = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("contract-cancel"),
            symbol: symbol.clone(),
            client_order_id: Some("CONTRACT-CID".to_string()),
            exchange_order_id: Some("contract-order-1".to_string()),
        })
        .await
        .expect_err("contract cancel should remain unsupported without a stable endpoint spec");
    assert_lbank_contract_lifecycle_unsupported(cancel_error);

    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("contract-batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("contract-batch-cancel-1"),
                symbol: symbol.clone(),
                client_order_id: None,
                exchange_order_id: Some("contract-order-1".to_string()),
            }],
        })
        .await
        .expect_err(
            "contract batch cancel should remain unsupported without a stable endpoint spec",
        );
    assert_lbank_contract_lifecycle_unsupported(batch_cancel_error);

    let cancel_all_error = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("contract-cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol.clone()),
        })
        .await
        .expect_err("contract cancel all should remain unsupported without a stable endpoint spec");
    assert_lbank_contract_lifecycle_unsupported(cancel_all_error);

    let query_error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("contract-query"),
            symbol: symbol.clone(),
            client_order_id: None,
            exchange_order_id: Some("contract-order-1".to_string()),
        })
        .await
        .expect_err("contract query should remain unsupported without a stable endpoint spec");
    assert_lbank_contract_lifecycle_unsupported(query_error);

    let open_error = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("contract-open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol.clone()),
            page: None,
        })
        .await
        .expect_err(
            "contract open orders should remain unsupported without a stable endpoint spec",
        );
    assert_lbank_contract_lifecycle_unsupported(open_error);

    let fills_error = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("contract-fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(symbol),
            client_order_id: None,
            exchange_order_id: Some("contract-order-1".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(50),
            page: None,
        })
        .await
        .expect_err("contract fills should remain unsupported without a stable endpoint spec");
    assert_lbank_contract_lifecycle_unsupported(fills_error);

    assert!(seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn lbank_adapter_should_parse_private_readbacks() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "result": "true",
            "error_code": 0,
            "data": {
                "balances": [
                    {"assetCode": "BTC", "assetAmt": "0.12", "usableAmt": "0.10"},
                    {"assetCode": "USDT", "assetAmt": "125.00", "usableAmt": "123.00"}
                ]
            }
        }),
        json!({
            "result": "true",
            "error_code": 0,
            "data": [{"symbol": "btc_usdt", "makerCommission": "0.10", "takerCommission": "0.10"}]
        }),
        json!({
            "result": "true",
            "error_code": 0,
            "data": {
                "symbol": "btc_usdt",
                "orderId": "2001",
                "clientOrderId": "LIMIT1",
                "type": "buy",
                "status": 1,
                "price": "65000",
                "origQty": "0.02",
                "executedQty": "0.01"
            }
        }),
        json!({
            "result": "true",
            "error_code": 0,
            "data": [{
                "symbol": "btc_usdt",
                "orderId": "2001",
                "clientOrderId": "LIMIT1",
                "type": "buy",
                "status": 0,
                "price": "65000",
                "origQty": "0.02",
                "executedQty": "0"
            }]
        }),
        json!({
            "result": "true",
            "error_code": 0,
            "data": [{
                "symbol": "btc_usdt",
                "id": "fill-1",
                "orderId": "2001",
                "tradeType": "buy",
                "price": "65000",
                "amount": "0.01",
                "volume": "650",
                "tradeFee": "0.65",
                "tradeFeeAsset": "USDT",
                "tradeFeeRate": "0.001",
                "time": 1700000000000i64
            }]
        }),
    ])
    .await;
    let adapter = LBankGatewayAdapter::new(private_config(base_url)).expect("adapter");

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
    assert_eq!(balances.balances[0].balances.len(), 1);
    assert_eq!(balances.balances[0].balances[0].available, 0.10);

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.10");

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            exchange_order_id: Some("2001".to_string()),
        })
        .await
        .expect("query")
        .order
        .expect("order");
    assert_eq!(order.status, OrderStatus::PartiallyFilled);

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            page: None,
        })
        .await
        .expect("open");
    assert_eq!(open.orders.len(), 1);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("2001".to_string()),
            from_trade_id: Some("fill-0".to_string()),
            start_time: None,
            end_time: None,
            limit: Some(50),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("fill-1"));
    assert_eq!(fills.fills[0].quote_quantity, Some(650.0));

    let requests = seen.lock().unwrap().clone();
    assert_signed_form_request(&requests[0], "/v2/supplement/user_info_account.do");
    assert_signed_form_request(&requests[1], "/v2/supplement/customer_trade_fee.do");
    assert_eq!(
        requests[1].form.get("category").map(String::as_str),
        Some("btc_usdt")
    );
    assert_signed_form_request(&requests[2], "/v2/supplement/orders_info.do");
    assert_signed_form_request(&requests[3], "/v2/supplement/orders_info_no_deal.do");
    assert_eq!(
        requests[3].form.get("current_page").map(String::as_str),
        Some("1")
    );
    assert_eq!(
        requests[3].form.get("page_length").map(String::as_str),
        Some("100")
    );
    assert_signed_form_request(&requests[4], "/v2/order_transaction_detail.do");
    assert_eq!(
        requests[4].form.get("order_id").map(String::as_str),
        Some("2001")
    );
    assert!(!requests[4].form.contains_key("fromId"));
    assert!(!requests[4].form.contains_key("limit"));
}

fn assert_lbank_contract_lifecycle_unsupported(error: ExchangeApiError) {
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "lbank.non_spot_market_type"
        }
    ));
}
