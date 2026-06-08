use rustcta_exchange_api::HeartbeatDirection;
use serde_json::json;

use super::streams::{
    heartbeat_policy, parse_public_orderbook_notification, public_orderbook_subscribe_payload,
    public_orderbook_unsubscribe_payload, trading_login_hs256_payload,
};
use super::test_support::exchange_id;

fn load_ws_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "orderbook_partial.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/hitbtc/ws/orderbook_partial.json")
        }
        _ => panic!("unknown hitbtc ws fixture {name}"),
    };
    serde_json::from_str(text).expect("ws fixture")
}

#[test]
fn hitbtc_ws_payloads_should_cover_subscribe_unsubscribe_and_auth() {
    let symbols = vec!["ETHBTC".to_string()];
    assert_eq!(
        public_orderbook_subscribe_payload(&symbols, 5, "1000ms", 123),
        json!({
            "method": "subscribe",
            "ch": "orderbook/D5/1000ms",
            "params": {"symbols": ["ETHBTC"]},
            "id": 123
        })
    );
    assert_eq!(
        public_orderbook_unsubscribe_payload(&symbols, 5, "1000ms", 124),
        json!({
            "method": "unsubscribe",
            "ch": "orderbook/D5/1000ms",
            "params": {"symbols": ["ETHBTC"]},
            "id": 124
        })
    );

    let login = trading_login_hs256_payload("test-key", "test-secret", 1_710_000_000_000, 10_000)
        .expect("login payload");
    assert_eq!(login["method"], "login");
    assert_eq!(login["params"]["type"], "HS256");
    assert_eq!(
        login["params"]["signature"],
        "47caebaef064195539b50b7c53dcd103ff3ee8face5b2a4bbccb1234b6a54a7e"
    );
    assert!(!login.to_string().contains("test-secret"));
}

#[test]
fn hitbtc_ws_orderbook_fixture_should_parse_snapshot_for_rest_resync() {
    let parsed = parse_public_orderbook_notification(
        &exchange_id(),
        &load_ws_fixture("orderbook_partial.json"),
    )
    .expect("orderbook notification");
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].order_book.bids[0].price, 0.061867);
    assert_eq!(parsed[0].order_book.asks[0].quantity, 4.8257);
}

#[test]
fn hitbtc_ws_heartbeat_policy_should_track_server_ping() {
    let policy = heartbeat_policy();
    assert_eq!(policy.direction, HeartbeatDirection::ServerPing);
    assert_eq!(policy.ping_interval_ms, 30_000);
    assert_eq!(policy.stale_message_ms, 90_000);
}
