use rustcta_exchange_api::{
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::streams::{
    bitmart_check_version_continuity, bitmart_spot_book_ticker_channel, bitmart_spot_depth_channel,
    bitmart_spot_order_book_ws_policy, parse_control_message, parse_public_order_book_event,
    private_subscription_spec, public_subscription_spec, BitmartVersionContinuity,
};
use super::test_support::{context, exchange_id, perp_symbol_scope, spot_symbol_scope};
use super::BitmartGatewayConfig;

#[test]
fn bitmart_stream_specs_should_build_public_and_private_payloads() {
    let config = BitmartGatewayConfig {
        api_key: Some("bitmart-key".to_string()),
        api_secret: Some("bitmart-secret".to_string()),
        memo: Some("bitmart-memo".to_string()),
        ..BitmartGatewayConfig::default()
    };
    let public = public_subscription_spec(
        &config,
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        },
    )
    .expect("public spec");
    assert_eq!(public.payload["op"], "subscribe");
    assert_eq!(public.heartbeat_payload, json!("ping"));

    let private = private_subscription_spec(
        &config,
        &PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            account_id: rustcta_types::AccountId::new("account_a").expect("account"),
            kind: PrivateStreamKind::Orders,
        },
    )
    .expect("private spec");
    assert_eq!(private.payload["op"], "login");
    assert!(private.payload["subscribe"]["args"][0]
        .as_str()
        .expect("channel")
        .contains("order"));
}

#[test]
fn bitmart_spot_orderbook_policy_should_cover_depth_and_bbo_channels() {
    let policy = bitmart_spot_order_book_ws_policy();
    assert_eq!(policy.depth_increase_channel, "spot/depth/increase100");
    assert_eq!(policy.depth_increase_interval_ms, 100);
    assert_eq!(policy.depth_increase_levels, 100);
    assert_eq!(
        policy.full_depth_channels,
        &[
            ("spot/depth5", 5),
            ("spot/depth20", 20),
            ("spot/depth50", 50)
        ]
    );
    assert_eq!(policy.full_depth_interval_ms, 500);
    assert_eq!(policy.book_ticker_channel, "spot/bookTicker");
    assert_eq!(policy.book_ticker_interval_ms, None);
    assert_eq!(policy.sequence_field, "version");
    assert_eq!(policy.checksum, None);
    assert!(policy.resync.contains("version > local version + 1"));

    assert_eq!(bitmart_spot_depth_channel(5).unwrap(), "spot/depth5");
    assert_eq!(bitmart_spot_depth_channel(20).unwrap(), "spot/depth20");
    assert_eq!(bitmart_spot_depth_channel(50).unwrap(), "spot/depth50");
    assert!(bitmart_spot_depth_channel(100).is_err());
    assert_eq!(
        bitmart_spot_book_ticker_channel("BTC_USDT"),
        "spot/bookTicker:BTC_USDT"
    );
}

#[test]
fn bitmart_spot_orderbook_delta_should_use_increase100_channel() {
    let config = BitmartGatewayConfig::default();
    let delta = public_subscription_spec(
        &config,
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-delta"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        },
    )
    .expect("delta spec");
    assert_eq!(delta.payload["args"][0], "spot/depth/increase100:BTC_USDT");

    let snapshot = public_subscription_spec(
        &config,
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-snapshot"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        },
    )
    .expect("snapshot spec");
    assert_eq!(snapshot.payload["args"][0], "spot/depth50:BTC_USDT");
}

#[test]
fn bitmart_version_continuity_should_trigger_snapshot_rebuild_on_gap() {
    assert_eq!(
        bitmart_check_version_continuity(None, 4),
        BitmartVersionContinuity::First
    );
    assert_eq!(
        bitmart_check_version_continuity(Some(4), 5),
        BitmartVersionContinuity::Continuous
    );
    assert_eq!(
        bitmart_check_version_continuity(Some(5), 5),
        BitmartVersionContinuity::DuplicateOrStale
    );
    let gap = bitmart_check_version_continuity(Some(5), 7);
    assert_eq!(
        gap,
        BitmartVersionContinuity::Gap {
            expected: 6,
            actual: 7
        }
    );
    assert!(gap.requires_resync());
}

#[test]
fn bitmart_stream_parser_should_parse_orderbook_and_control_messages() {
    let fixture = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitmart/ws_public_depth.json"
    ))
    .expect("fixture");
    let snapshot = parse_public_order_book_event(&exchange_id(), MarketType::Perpetual, &fixture)
        .expect("parse")
        .expect("snapshot");
    assert_eq!(snapshot.best_bid().expect("bid").price, 50000.0);
    assert_eq!(parse_control_message(&json!("pong")), Some("pong"));
}

#[test]
fn bitmart_spot_orderbook_fixtures_should_parse_version_and_bbo() {
    let snapshot = parse_public_order_book_event(
        &exchange_id(),
        MarketType::Spot,
        &serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitmart/ws_spot_depth_increase_snapshot.json"
        ))
        .expect("snapshot fixture"),
    )
    .expect("parse")
    .expect("snapshot");
    assert_eq!(snapshot.sequence, Some(4));
    assert_eq!(snapshot.best_bid().expect("bid").price, 23105.0);

    let update = parse_public_order_book_event(
        &exchange_id(),
        MarketType::Spot,
        &serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitmart/ws_spot_depth_increase_update.json"
        ))
        .expect("update fixture"),
    )
    .expect("parse")
    .expect("update");
    assert_eq!(update.sequence, Some(5));
    assert_eq!(update.best_ask().expect("ask").quantity, 0.59959);

    let bbo = parse_public_order_book_event(
        &exchange_id(),
        MarketType::Spot,
        &serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/bitmart/ws_spot_book_ticker.json"
        ))
        .expect("bbo fixture"),
    )
    .expect("parse")
    .expect("bbo");
    assert_eq!(bbo.best_bid().expect("bid").price, 71291.2);
    assert_eq!(bbo.best_ask().expect("ask").quantity, 2.46310);
}
