use rustcta_exchange_api::{
    ExchangeClient, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, TenantId};
use serde_json::json;

use super::streams::{
    bingx_heartbeat_response, bingx_private_subscribe_payload, bingx_public_subscribe_payload,
    parse_bingx_private_stream_message, parse_bingx_public_stream_message,
    BingxPrivateStreamMessage, BingxPublicStreamMessage,
};
use super::test_support::{
    assert_request_matches_spec, context, exchange_id, perp_symbol_scope, spawn_rest_server,
    spot_symbol_scope,
};
use super::{BingxGatewayAdapter, BingxGatewayConfig};

#[test]
fn bingx_public_stream_payload_should_map_channels_and_heartbeat() {
    let book = bingx_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("book-stream"),
        symbol: perp_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("book payload");
    assert_eq!(book["reqType"], "sub");
    assert_eq!(book["dataType"], "BTC-USDT@depth50");

    let candle = bingx_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("candle-stream"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::Candles {
            interval: "1m".to_string(),
        },
    })
    .expect("candle payload");
    assert_eq!(candle["dataType"], "BTC-USDT@kline_1m");

    assert_eq!(bingx_heartbeat_response("Ping"), Some("Pong"));
    assert_eq!(bingx_heartbeat_response("ping"), Some("Pong"));
}

#[test]
fn bingx_stream_parser_should_parse_public_book_and_private_order() {
    let public = parse_bingx_public_stream_message(
        &exchange_id(),
        perp_symbol_scope(),
        &json!({
            "dataType": "BTC-USDT@depth50",
            "data": {
                "T": 1700000000000i64,
                "bids": [["65000", "0.2"]],
                "asks": [["65010", "0.1"]]
            }
        }),
    )
    .expect("public stream");
    match public {
        BingxPublicStreamMessage::OrderBook(book) => {
            assert_eq!(book.bids[0].price, 65000.0);
            assert_eq!(book.asks[0].quantity, 0.1);
        }
        other => panic!("unexpected public message {other:?}"),
    }

    let private = parse_bingx_private_stream_message(
        &exchange_id(),
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        Some(perp_symbol_scope()),
        MarketType::Perpetual,
        &json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 1700000000000i64,
            "o": {
                "s": "BTC-USDT",
                "c": "client-1",
                "i": 1001,
                "S": "BUY",
                "o": "LIMIT",
                "q": "0.01",
                "p": "65000",
                "x": "NEW",
                "X": "NEW",
                "z": "0",
                "ps": "LONG"
            }
        }),
    )
    .expect("private stream");
    match private {
        BingxPrivateStreamMessage::Order(order) => {
            assert_eq!(order.exchange_order_id.as_deref(), Some("1001"));
            assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
        }
        other => panic!("unexpected private message {other:?}"),
    }
}

#[test]
fn bingx_private_stream_payload_should_map_listen_key_channels() {
    let spot_payload = bingx_private_subscribe_payload(&PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-spot"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    })
    .expect("spot private payload");
    assert_eq!(spot_payload["dataType"], "spot.executionReport");

    let perp_payload = bingx_private_subscribe_payload(&PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private-perp"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Positions,
    })
    .expect("perp private payload");
    assert_eq!(perp_payload["dataType"], "ACCOUNT_UPDATE");
}

#[tokio::test]
async fn bingx_adapter_should_return_stream_subscription_ids() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": {"listenKey": "listen-key-1"}
    })])
    .await;
    let adapter = BingxGatewayAdapter::new(BingxGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        recv_window_ms: 5_000,
        ..BingxGatewayConfig::default()
    })
    .expect("adapter");

    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::Trades,
        })
        .await
        .expect("public subscription");
    assert!(public_id.contains("BTC-USDT@trade"));
    assert!(public_id.contains("open-api-swap.bingx.com"));

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Fills,
        })
        .await
        .expect("private subscription");
    assert!(private_id.contains("listenKey=listen-key-1"));
    assert!(private_id.contains("ORDER_TRADE_UPDATE"));

    let requests = seen.lock().unwrap().clone();
    assert_request_matches_spec(&requests[0], "bingx.generate_listen_key");
}

#[test]
fn bingx_stream_runtime_policy_should_be_declared_in_toolchain_files() {
    let endpoint_mapping = include_str!("endpoint_mapping.yaml");
    assert!(endpoint_mapping.contains("websocket_runtime:"));
    assert!(endpoint_mapping.contains("public:"));
    assert!(endpoint_mapping.contains("private:"));
    assert!(endpoint_mapping.contains("inbound_text: [Ping, ping]"));
    assert!(endpoint_mapping.contains("outbound_text: Pong"));
    assert!(endpoint_mapping.contains("auth_renewal:"));
    assert!(endpoint_mapping.contains("fallback: rest_reconciliation"));
    assert!(endpoint_mapping.contains("snapshot_on_connect_or_gap"));
    assert!(endpoint_mapping.contains("resubscribe_then_refresh_snapshot"));
}
