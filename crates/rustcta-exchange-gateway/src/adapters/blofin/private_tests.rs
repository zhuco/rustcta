use std::collections::BTreeMap;

use rustcta_exchange_api::{
    AccountId, BatchAtomicity, BatchExecutionMode, CancelOrderRequest, ExchangeApiError,
    ExchangeClient, PlaceOrderRequest, RequestContext, SymbolScope, TenantId, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderStatus, OrderType,
    PositionSide,
};
use serde_json::{json, Value};

use crate::request_spec::{
    ActualHttpRequest, FieldExpectation, FieldMatch, RequestAuth, RequestSpec,
};
use crate::signing_spec::{SigningAlgorithm, SigningPayloadFormat, SigningVector};

use super::private::{
    blofin_algo_cancel_body, blofin_algo_order_body, blofin_cancel_body,
    blofin_close_position_body, blofin_place_order_body, blofin_query_params,
    blofin_set_leverage_body, blofin_tpsl_cancel_body, blofin_tpsl_order_body,
    blofin_transfer_body, BlofinAlgoCancelRequest, BlofinAlgoOrderRequest, BlofinAttachedAlgoOrder,
    BlofinClosePositionRequest, BlofinQueryParams, BlofinSetLeverageRequest,
    BlofinTpslCancelRequest, BlofinTpslOrderRequest, BlofinTransferRequest,
};
use super::private_parser::{
    parse_balances, parse_fills, parse_order, parse_orders, parse_positions,
};
use super::public::BlofinMarginMode;
use super::signing::sign_request;
use super::transport::request_path;
use super::{BlofinGatewayAdapter, BlofinGatewayConfig};

fn fixture(name: &str) -> Value {
    let raw = match name {
        "balance" => include_str!("../../../../../tests/fixtures/exchanges/blofin/balance.json"),
        "position" => {
            include_str!("../../../../../tests/fixtures/exchanges/blofin/position.json")
        }
        "order_ack" => {
            include_str!("../../../../../tests/fixtures/exchanges/blofin/order_ack.json")
        }
        "fill" => include_str!("../../../../../tests/fixtures/exchanges/blofin/fill.json"),
        "error" => include_str!("../../../../../tests/fixtures/exchanges/blofin/error.json"),
        other => panic!("unknown blofin fixture {other}"),
    };
    serde_json::from_str(raw).unwrap()
}

fn btc_scope() -> SymbolScope {
    let exchange = ExchangeId::new("blofin").unwrap();
    SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange, MarketType::Perpetual, "BTC-USDT").unwrap(),
    }
}

fn context() -> RequestContext {
    RequestContext::new(chrono::DateTime::from_timestamp_millis(1_700_000_000_000).unwrap())
}

fn tenant_id() -> TenantId {
    TenantId::new("tenant").unwrap()
}

fn account_id() -> AccountId {
    AccountId::new("acct").unwrap()
}

#[test]
fn capabilities_v2_should_declare_perp_stream_batch_and_history_policies() {
    let adapter = BlofinGatewayAdapter::new(BlofinGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("pass".to_string()),
        ..BlofinGatewayConfig::default()
    })
    .unwrap();
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(capabilities.capabilities_v2.private_rest.is_supported());
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.max_items,
        Some(20)
    );
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .orderbook_requires_snapshot_after_reconnect
    );
    assert!(capabilities.capabilities_v2.fills_history.supports_since);
    assert_eq!(
        capabilities.capabilities_v2.fills_history.max_limit,
        Some(100)
    );
}

#[test]
fn private_parser_fixtures_should_cover_success_empty_error_and_key_missing() {
    let exchange = ExchangeId::new("blofin").unwrap();
    let balances = parse_balances(
        &exchange,
        tenant_id(),
        account_id(),
        &[],
        &fixture("balance"),
    )
    .unwrap();
    assert_eq!(balances[0].market_type, MarketType::Perpetual);
    assert_eq!(balances[0].balances[0].asset, "USDT");
    assert_eq!(balances[0].balances[0].available, 900.0);

    let positions =
        parse_positions(&exchange, tenant_id(), account_id(), &fixture("position")).unwrap();
    assert_eq!(
        positions[0]
            .exchange_symbol
            .as_ref()
            .map(|symbol| symbol.symbol.as_str()),
        Some("BTC-USDT")
    );
    assert_eq!(positions[0].quantity, 0.01);

    let order = parse_order(&exchange, None, &fixture("order_ack"))
        .unwrap()
        .unwrap();
    assert_eq!(order.exchange_order_id.as_deref(), Some("ord_fixture_1"));
    assert_eq!(order.client_order_id.as_deref(), Some("cid_fixture_1"));
    assert_eq!(order.status, OrderStatus::Open);

    let fills = parse_fills(&exchange, tenant_id(), account_id(), None, &fixture("fill")).unwrap();
    assert_eq!(fills[0].fill_id.as_deref(), Some("trade_fixture_1"));
    assert_eq!(fills[0].price, 65000.0);
    assert_eq!(fills[0].quantity, 0.01);

    assert!(parse_orders(&exchange, None, &fixture("error"))
        .unwrap()
        .is_empty());
    assert!(parse_order(
        &exchange,
        None,
        &json!({
            "code": "0",
            "msg": "success",
            "data": [{"orderId": "ord_missing_symbol", "side": "buy"}]
        })
    )
    .is_err());
}

#[test]
fn place_order_request_spec_should_match_signed_rest_shape() {
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(),
        symbol: btc_scope(),
        client_order_id: Some("cid-1".to_string()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::Net),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let body = blofin_place_order_body("cross", &request).unwrap();
    let timestamp = "1700000000000";
    let nonce = "nonce-1";
    let signature = sign_request(
        "secret",
        "/api/v1/trade/order",
        "POST",
        timestamp,
        nonce,
        &body.to_string(),
    )
    .unwrap();
    let spec = RequestSpec {
        exchange: "blofin".to_string(),
        operation: "blofin.place_order".to_string(),
        product: Some("linear_perp".to_string()),
        transport: Some("rest".to_string()),
        method: "POST".to_string(),
        path: "/api/v1/trade/order".to_string(),
        auth: Some(RequestAuth::Signed),
        query: BTreeMap::new(),
        headers: BTreeMap::from([
            (
                "ACCESS-KEY".to_string(),
                FieldExpectation {
                    match_kind: FieldMatch::Exact,
                    value: Some("key".to_string()),
                },
            ),
            (
                "ACCESS-SIGN".to_string(),
                FieldExpectation {
                    match_kind: FieldMatch::NonEmpty,
                    value: None,
                },
            ),
            (
                "ACCESS-TIMESTAMP".to_string(),
                FieldExpectation {
                    match_kind: FieldMatch::Exact,
                    value: Some(timestamp.to_string()),
                },
            ),
            (
                "ACCESS-NONCE".to_string(),
                FieldExpectation {
                    match_kind: FieldMatch::Exact,
                    value: Some(nonce.to_string()),
                },
            ),
            (
                "ACCESS-PASSPHRASE".to_string(),
                FieldExpectation {
                    match_kind: FieldMatch::Exact,
                    value: Some("pass".to_string()),
                },
            ),
        ]),
        body: None,
        body_contains: Some(json!({
            "instId": "BTC-USDT",
            "marginMode": "cross",
            "positionSide": "net",
            "side": "buy",
            "orderType": "limit",
            "size": "0.01",
            "price": "65000",
            "clientOrderId": "cid-1"
        })),
        strict_query: false,
        strict_headers: false,
        strict_body: false,
        forbid_query_values_containing: vec!["secret".to_string()],
        forbid_header_values_containing: vec!["secret".to_string()],
        signing_vector: Some("blofin-place-order-hex-base64".to_string()),
    };
    let actual = ActualHttpRequest::new("POST", "/api/v1/trade/order")
        .with_headers([
            ("ACCESS-KEY".to_string(), "key".to_string()),
            ("ACCESS-SIGN".to_string(), signature),
            ("ACCESS-TIMESTAMP".to_string(), timestamp.to_string()),
            ("ACCESS-NONCE".to_string(), nonce.to_string()),
            ("ACCESS-PASSPHRASE".to_string(), "pass".to_string()),
        ])
        .with_body(Some(body));

    spec.assert_matches(&actual).unwrap();
}

#[test]
fn cancel_order_request_spec_should_include_order_identity() {
    let body = blofin_cancel_body(&CancelOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(),
        symbol: btc_scope(),
        client_order_id: Some("cid-1".to_string()),
        exchange_order_id: Some("ord-1".to_string()),
    })
    .unwrap();
    let spec = RequestSpec {
        exchange: "blofin".to_string(),
        operation: "blofin.cancel_order".to_string(),
        product: Some("linear_perp".to_string()),
        transport: Some("rest".to_string()),
        method: "POST".to_string(),
        path: "/api/v1/trade/cancel-order".to_string(),
        auth: Some(RequestAuth::Signed),
        query: BTreeMap::new(),
        headers: BTreeMap::new(),
        body: None,
        body_contains: Some(json!({
            "instId": "BTC-USDT",
            "orderId": "ord-1",
            "clientOrderId": "cid-1"
        })),
        strict_query: false,
        strict_headers: false,
        strict_body: false,
        forbid_query_values_containing: Vec::new(),
        forbid_header_values_containing: Vec::new(),
        signing_vector: None,
    };

    spec.assert_matches(
        &ActualHttpRequest::new("POST", "/api/v1/trade/cancel-order").with_body(Some(body)),
    )
    .unwrap();
}

#[test]
fn query_order_request_path_should_sort_query_for_signing() {
    let params = blofin_query_params(&BlofinQueryParams {
        inst_id: Some("btcusdt"),
        order_id: Some("ord-1"),
        client_order_id: Some("cid-1"),
        ..BlofinQueryParams::default()
    })
    .unwrap();

    assert_eq!(
        request_path("/api/v1/trade/order-detail", &params),
        "/api/v1/trade/order-detail?clientOrderId=cid-1&instId=BTC-USDT&orderId=ord-1"
    );
}

#[test]
fn signing_vector_should_match_blofin_hex_then_base64_algorithm() {
    let request_path = "/api/v1/trade/order";
    let method = "POST";
    let timestamp = "1700000000000";
    let nonce = "nonce-1";
    let body = r#"{"instId":"BTC-USDT","marginMode":"cross","orderType":"limit","positionSide":"net","price":"65000","reduceOnly":"false","side":"buy","size":"0.01"}"#;
    let payload = format!("{request_path}{method}{timestamp}{nonce}{body}");
    let vector = SigningVector {
        exchange: "blofin".to_string(),
        name: "blofin-place-order-hex-base64".to_string(),
        algorithm: SigningAlgorithm::HmacSha256Hex,
        secret: "secret".to_string(),
        payload,
        expected_signature: "46f5ccd39d35aa1269a6d0bc5da780d4b38f0cbe343ce636b6ac7cef562ad6bc"
            .to_string(),
        payload_format: SigningPayloadFormat::RawPayload,
        timestamp: Some(timestamp.to_string()),
        method: Some(method.to_string()),
        request_path: Some(request_path.to_string()),
        body: Some(body.to_string()),
    };
    vector.verify().unwrap();
    assert_eq!(
        sign_request("secret", request_path, method, timestamp, nonce, body).unwrap(),
        "NDZmNWNjZDM5ZDM1YWExMjY5YTZkMGJjNWRhNzgwZDRiMzhmMGNiZTM0M2NlNjM2YjZhYzdjZWY1NjJhZDZiYw=="
    );
}

#[test]
fn task14_request_spec_and_signing_vector_fixtures_should_load() {
    let request_specs = [
        (
            "balances",
            include_str!("../../../../../tests/fixtures/exchanges/blofin/request_specs/balances.json"),
        ),
        (
            "positions",
            include_str!("../../../../../tests/fixtures/exchanges/blofin/request_specs/positions.json"),
        ),
        (
            "place_order",
            include_str!("../../../../../tests/fixtures/exchanges/blofin/request_specs/place_order.json"),
        ),
        (
            "batch_place_orders",
            include_str!("../../../../../tests/fixtures/exchanges/blofin/request_specs/batch_place_orders.json"),
        ),
        (
            "cancel_order",
            include_str!("../../../../../tests/fixtures/exchanges/blofin/request_specs/cancel_order.json"),
        ),
        (
            "batch_cancel_orders",
            include_str!("../../../../../tests/fixtures/exchanges/blofin/request_specs/batch_cancel_orders.json"),
        ),
        (
            "cancel_all_orders",
            include_str!("../../../../../tests/fixtures/exchanges/blofin/request_specs/cancel_all_orders.json"),
        ),
        (
            "query_order",
            include_str!("../../../../../tests/fixtures/exchanges/blofin/request_specs/query_order.json"),
        ),
        (
            "open_orders",
            include_str!("../../../../../tests/fixtures/exchanges/blofin/request_specs/open_orders.json"),
        ),
        (
            "recent_fills",
            include_str!("../../../../../tests/fixtures/exchanges/blofin/request_specs/recent_fills.json"),
        ),
    ];

    for (operation, raw) in request_specs {
        let spec: RequestSpec = serde_json::from_str(raw).expect("request spec fixture");
        assert_eq!(spec.exchange, "blofin");
        assert_eq!(spec.operation, operation);
        assert_eq!(spec.transport.as_deref(), Some("rest"));
    }

    for raw in [
        include_str!(
            "../../../../../tests/fixtures/exchanges/blofin/signing_vectors/rest_place_order_hex.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/blofin/signing_vectors/rest_place_order_access_sign.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/blofin/signing_vectors/ws_login_access_sign.json"
        ),
    ] {
        let vector: SigningVector = serde_json::from_str(raw).expect("signing vector fixture");
        assert_eq!(vector.exchange, "blofin");
        vector.verify().expect("signing vector verifies");
    }
}

#[test]
fn set_leverage_body_should_match_official_fields() {
    let body = blofin_set_leverage_body(&BlofinSetLeverageRequest {
        inst_id: "btcusdt",
        leverage: "20",
        margin_mode: BlofinMarginMode::Isolated,
        position_side: Some(PositionSide::Long),
    })
    .unwrap();

    assert_eq!(
        body,
        json!({
            "instId": "BTC-USDT",
            "leverage": "20",
            "marginMode": "isolated",
            "positionSide": "long",
        })
    );
}

#[test]
fn tpsl_order_body_should_require_trigger_and_map_optional_prices() {
    let body = blofin_tpsl_order_body(&BlofinTpslOrderRequest {
        inst_id: "ETH-USDT",
        margin_mode: BlofinMarginMode::Cross,
        position_side: PositionSide::Short,
        side: OrderSide::Sell,
        size: "1",
        reduce_only: Some(true),
        client_order_id: Some("cid-1"),
        broker_id: None,
        tp_trigger_price: Some("3000"),
        tp_order_price: Some("-1"),
        tp_trigger_price_type: Some("last"),
        sl_trigger_price: None,
        sl_order_price: None,
        sl_trigger_price_type: None,
    })
    .unwrap();

    assert_eq!(body["instId"], "ETH-USDT");
    assert_eq!(body["marginMode"], "cross");
    assert_eq!(body["positionSide"], "short");
    assert_eq!(body["side"], "sell");
    assert_eq!(body["size"], "1");
    assert_eq!(body["reduceOnly"], "true");
    assert_eq!(body["tpOrderPrice"], "-1");

    let error = blofin_tpsl_order_body(&BlofinTpslOrderRequest {
        inst_id: "ETH-USDT",
        margin_mode: BlofinMarginMode::Cross,
        position_side: PositionSide::Net,
        side: OrderSide::Buy,
        size: "1",
        reduce_only: None,
        client_order_id: None,
        broker_id: None,
        tp_trigger_price: None,
        tp_order_price: None,
        tp_trigger_price_type: None,
        sl_trigger_price: None,
        sl_order_price: None,
        sl_trigger_price_type: None,
    })
    .unwrap_err();
    assert!(matches!(error, ExchangeApiError::InvalidRequest { .. }));
}

#[test]
fn algo_order_body_should_include_attached_tpsl_array() {
    let body = blofin_algo_order_body(&BlofinAlgoOrderRequest {
        inst_id: "ETHUSDT",
        margin_mode: BlofinMarginMode::Cross,
        position_side: PositionSide::Net,
        side: OrderSide::Buy,
        size: "2",
        order_type: "trigger",
        order_price: Some("-1"),
        trigger_price: Some("2500"),
        trigger_price_type: Some("last"),
        reduce_only: Some(false),
        client_order_id: Some("algo-1"),
        broker_id: Some("brk"),
        attach_algo_orders: vec![BlofinAttachedAlgoOrder {
            tp_trigger_price: Some("2800"),
            tp_order_price: Some("-1"),
            tp_trigger_price_type: Some("last"),
            sl_trigger_price: Some("2200"),
            sl_order_price: Some("-1"),
            sl_trigger_price_type: Some("mark"),
        }],
    })
    .unwrap();

    assert_eq!(body["instId"], "ETH-USDT");
    assert_eq!(body["orderType"], "trigger");
    assert_eq!(body["orderPrice"], "-1");
    assert_eq!(body["reduceOnly"], "false");
    assert_eq!(body["attachAlgoOrders"][0]["slTriggerPriceType"], "mark");
}

#[test]
fn cancel_and_close_bodies_should_validate_identifiers() {
    assert!(matches!(
        blofin_tpsl_cancel_body(&BlofinTpslCancelRequest {
            inst_id: Some("BTC-USDT"),
            tpsl_id: None,
            client_order_id: None,
        }),
        Err(ExchangeApiError::InvalidRequest { .. })
    ));
    assert!(matches!(
        blofin_algo_cancel_body(&BlofinAlgoCancelRequest {
            inst_id: None,
            algo_id: None,
            client_order_id: None,
        }),
        Err(ExchangeApiError::InvalidRequest { .. })
    ));

    let close = blofin_close_position_body(&BlofinClosePositionRequest {
        inst_id: "BTCUSDT",
        margin_mode: BlofinMarginMode::Cross,
        position_side: PositionSide::Long,
        client_order_id: Some("close-1"),
        broker_id: None,
    })
    .unwrap();
    assert_eq!(close["instId"], "BTC-USDT");
    assert_eq!(close["positionSide"], "long");
}

#[test]
fn transfer_body_and_query_params_should_use_official_names() {
    let transfer = blofin_transfer_body(&BlofinTransferRequest {
        currency: "USDT",
        amount: "1.5",
        from_account: "funding",
        to_account: "futures",
        client_id: Some("xfer-1"),
        sub_account: Some("sub"),
        main_to_sub_account: Some(true),
    });
    assert_eq!(transfer["fromAccount"], "funding");
    assert_eq!(transfer["clientId"], "xfer-1");
    assert_eq!(transfer["mainToSubAccount"], true);

    let params = blofin_query_params(&BlofinQueryParams {
        inst_id: Some("btcusdt"),
        algo_id: Some("a1"),
        order_type: Some("trigger"),
        before: Some("11"),
        limit: Some(500),
        ..BlofinQueryParams::default()
    })
    .unwrap();
    assert_eq!(params.get("instId").map(String::as_str), Some("BTC-USDT"));
    assert_eq!(params.get("algoId").map(String::as_str), Some("a1"));
    assert_eq!(params.get("limit").map(String::as_str), Some("100"));
}
