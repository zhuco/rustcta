use rustcta_exchange_api::{
    ExchangeClient, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};

use super::streams::{
    coinex_custom_depth_subscription_spec, coinex_depth_checksum_string,
    coinex_depth_subscription_spec, coinex_ping_payload, coinex_private_login_payload,
    coinex_private_subscribe_payload, coinex_public_orderbook_ws_policy,
    coinex_public_subscribe_payload, coinex_reconnect_policy_ms,
    parse_coinex_public_orderbook_update, parse_coinex_stream_control, CoinExStreamControlMessage,
    COINEX_PRIVATE_WS_URL, COINEX_PUBLIC_WS_URL,
};
use super::test_support::{context, exchange_id, symbol_scope};
use super::{CoinExGatewayAdapter, CoinExGatewayConfig};

fn ws_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "public_depth_subscribe.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinex/ws/public_depth_subscribe.json"
        ),
        "public_depth_full.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/coinex/ws/public_depth_full.json")
        }
        "public_depth_incremental.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinex/ws/public_depth_incremental.json"
        ),
        _ => panic!("unknown CoinEx WS fixture {name}"),
    };
    serde_json::from_str(text).expect("CoinEx WS fixture")
}

#[test]
fn coinex_public_stream_spec_should_normalize_symbol_and_heartbeat() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("coinex-public-ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };

    let payload = coinex_public_subscribe_payload(&subscription, 1).expect("payload");

    assert_eq!(payload["method"], "depth.subscribe");
    assert_eq!(payload["params"]["market_list"][0][0], "BTCUSDT");
    assert_eq!(payload["params"]["market_list"][0][1], 50);
    assert_eq!(payload["params"]["market_list"][0][2], "0");
    assert_eq!(payload["params"]["market_list"][0][3], true);
    assert_eq!(coinex_ping_payload(2)["method"], "server.ping");
    assert_eq!(coinex_reconnect_policy_ms(), (30_000, 10_000, 45_000));
}

#[test]
fn coinex_public_orderbook_policy_should_capture_depth_structured_details() {
    let policy = coinex_public_orderbook_ws_policy();

    assert_eq!(policy.subscribe_method, "depth.subscribe");
    assert_eq!(policy.update_method, "depth.update");
    assert_eq!(policy.push_delay_ms, 200);
    assert_eq!(policy.full_refresh_interval_ms, 60_000);
    assert_eq!(policy.supported_limits, &[5, 10, 20, 50]);
    assert!(policy.supported_merge_intervals.contains(&"0.0001"));
    assert_eq!(policy.default_merge_interval, "0");
    assert_eq!(policy.checksum_algorithm, "crc32");
    assert_eq!(policy.checksum_type, "signed_i32");

    let json = policy.as_json();
    assert_eq!(json["params_shape"]["field"], "market_list");
    assert_eq!(json["checksum"]["resync_on_mismatch"], true);
}

#[test]
fn coinex_depth_subscription_spec_should_validate_limit_interval_and_full_mode() {
    let delta =
        coinex_depth_subscription_spec("btc/usdt", &PublicStreamKind::OrderBookDelta).unwrap();
    assert_eq!(
        delta.params_entry(),
        ws_fixture("public_depth_subscribe.json")["params"]["market_list"][0]
    );
    assert!(!delta.is_full);

    let snapshot =
        coinex_depth_subscription_spec("BTCUSDT", &PublicStreamKind::OrderBookSnapshot).unwrap();
    assert!(snapshot.is_full);

    let custom = coinex_custom_depth_subscription_spec("BTCUSDT", 10, "0.01", false).unwrap();
    assert_eq!(
        custom.params_entry(),
        serde_json::json!(["BTCUSDT", 10, "0.01", false])
    );

    assert!(coinex_custom_depth_subscription_spec("BTCUSDT", 7, "0", false).is_err());
    assert!(coinex_custom_depth_subscription_spec("BTCUSDT", 10, "0.0002", false).is_err());
}

#[test]
fn coinex_depth_update_parser_should_capture_full_incremental_and_crc32() {
    let full = parse_coinex_public_orderbook_update(
        &exchange_id(),
        symbol_scope(),
        &ws_fixture("public_depth_full.json"),
    )
    .expect("full update");
    assert_eq!(full.market, "BTCUSDT");
    assert!(full.is_full);
    assert_eq!(full.order_book.bids[0].price, 100.0);
    assert_eq!(full.order_book.asks[0].price, 100.5);
    assert_eq!(full.checksum, Some(1_213_826_333));
    assert_eq!(full.checksum_matches_payload, Some(true));

    let incremental = parse_coinex_public_orderbook_update(
        &exchange_id(),
        symbol_scope(),
        &ws_fixture("public_depth_incremental.json"),
    )
    .expect("incremental update");
    assert!(!incremental.is_full);
    assert_eq!(incremental.deleted_bid_prices, vec!["100.0"]);
    assert_eq!(incremental.order_book.bids[0].price, 99.5);
    assert_eq!(incremental.order_book.bids[0].quantity, 0.5);
    assert_eq!(incremental.checksum, Some(43_041_355));
    assert_eq!(incremental.checksum_matches_payload, Some(true));
}

#[test]
fn coinex_depth_checksum_should_preserve_wire_string_format() {
    let full = ws_fixture("public_depth_full.json");
    let depth = &full["data"]["depth"];

    assert_eq!(
        coinex_depth_checksum_string(depth).expect("checksum string"),
        "100.0:1.0:99.5:0.5:100.5:2.0:101.0:3.0"
    );
}

#[test]
fn coinex_private_stream_spec_should_sign_login_and_subscribe() {
    let subscription = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("coinex-private-ws"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Spot),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Orders,
    };

    let login =
        coinex_private_login_payload("key", "secret", 1_700_000_000_000, 1).expect("login payload");
    let subscribe = coinex_private_subscribe_payload(&subscription, 2).expect("subscribe payload");

    assert_eq!(login["method"], "server.sign");
    assert_eq!(login["params"]["access_id"], "key");
    assert!(login["params"]["signed_str"]
        .as_str()
        .is_some_and(|signature| !signature.is_empty()));
    assert_eq!(subscribe["method"], "order.subscribe");
    assert_eq!(subscribe["params"][0], "SPOT");
    assert_eq!(
        parse_coinex_stream_control(&serde_json::json!({"id": 1, "method": "server.pong"})),
        CoinExStreamControlMessage::Pong
    );
}

#[tokio::test]
async fn coinex_adapter_should_ack_ws_specs_and_expose_v2_policy() {
    let adapter = CoinExGatewayAdapter::new(CoinExGatewayConfig {
        api_key: "key".to_string(),
        api_secret: "secret".to_string(),
        enabled_private_rest: true,
        ..CoinExGatewayConfig::default()
    })
    .expect("adapter");

    let public_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("coinex-public-id"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::Trades,
        })
        .await
        .expect("public stream id");
    assert_eq!(
        public_id,
        format!("coinex:{COINEX_PUBLIC_WS_URL}:deals.subscribe")
    );

    let private_id = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("coinex-private-id"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Balances,
        })
        .await
        .expect("private stream id");
    assert_eq!(
        private_id,
        format!("coinex:{COINEX_PRIVATE_WS_URL}:balance.subscribe:account")
    );

    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_streams);
    assert!(capabilities.supports_private_streams);
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .resync
            .order_book
    );
    assert_eq!(
        capabilities.capabilities_v2.fills_history.max_limit,
        Some(super::capabilities::COINEX_SPOT_MAX_PAGE_LIMIT)
    );
    assert!(
        capabilities
            .private_stream_capabilities
            .as_ref()
            .is_some_and(
                |capabilities| capabilities.supports_orders && capabilities.supports_balances
            )
    );
}
