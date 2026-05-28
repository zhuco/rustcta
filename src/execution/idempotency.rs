use crate::execution::BundleLeg;
use crate::market::RuntimeMode;
use std::hash::{Hash, Hasher};

pub fn deterministic_client_order_id(
    mode: RuntimeMode,
    bundle_id: &str,
    leg: BundleLeg,
    attempt: u32,
) -> String {
    format!(
        "crossarb-{}-{}-{}-{}",
        runtime_mode_slug(mode),
        leg_slug_short(leg),
        attempt,
        bundle_hash_suffix(bundle_id)
    )
}

pub fn is_crossarb_client_order_id(value: &str) -> bool {
    normalize_client_order_id(value).starts_with("crossarb-")
}

pub fn crossarb_order_mode(value: &str) -> Option<&'static str> {
    let normalized = normalize_client_order_id(value);
    let mut parts = normalized.split('-');
    match (parts.next(), parts.next()) {
        (Some("crossarb"), Some("live")) => match parts.next() {
            Some("small") => Some("live-small"),
            Some("scaled") => Some("live-scaled"),
            _ => None,
        },
        (Some("crossarb"), Some("ls")) => Some("live-small"),
        (Some("crossarb"), Some("lx")) => Some("live-scaled"),
        (Some("crossarb"), Some("obs")) => Some("observe"),
        (Some("crossarb"), Some("sim")) => Some("simulation"),
        (Some("crossarb"), Some("shd")) => Some("shadow"),
        (Some("crossarb"), Some("observe")) => Some("observe"),
        (Some("crossarb"), Some("simulation")) => Some("simulation"),
        (Some("crossarb"), Some("shadow")) => Some("shadow"),
        _ => None,
    }
}

pub fn normalize_client_order_id(value: &str) -> String {
    value
        .strip_prefix("t-")
        .unwrap_or(value)
        .trim()
        .to_ascii_lowercase()
}

pub fn runtime_mode_slug(mode: RuntimeMode) -> &'static str {
    match mode {
        RuntimeMode::Observe => "obs",
        RuntimeMode::Simulation => "sim",
        RuntimeMode::Shadow => "shd",
        RuntimeMode::LiveSmall => "ls",
        RuntimeMode::LiveScaled => "lx",
    }
}

pub fn bundle_short_id(bundle_id: &str) -> String {
    let short = bundle_id
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(|ch| ch.to_lowercase())
        .take(12)
        .collect::<String>();

    if short.is_empty() {
        "unknown".to_string()
    } else {
        short
    }
}

fn bundle_hash_suffix(bundle_id: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bundle_id.hash(&mut hasher);
    let hash = hasher.finish();
    format!("{:08x}", hash as u32)
}

fn leg_slug_short(leg: BundleLeg) -> &'static str {
    match leg {
        BundleLeg::Long => "lo",
        BundleLeg::Short => "sh",
        BundleLeg::Maker => "mk",
        BundleLeg::Taker => "tk",
        BundleLeg::Hedge => "hg",
        BundleLeg::CloseLong => "cl",
        BundleLeg::CloseShort => "cs",
        BundleLeg::EmergencyCloseLong => "el",
        BundleLeg::EmergencyCloseShort => "es",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_should_generate_stable_client_order_id() {
        let first = deterministic_client_order_id(
            RuntimeMode::Simulation,
            "bundle-ABCDEF-1234567890",
            BundleLeg::Hedge,
            2,
        );
        let second = deterministic_client_order_id(
            RuntimeMode::Simulation,
            "bundle-ABCDEF-1234567890",
            BundleLeg::Hedge,
            2,
        );

        assert_eq!(first, second);
        assert!(first.starts_with("crossarb-sim-hg-2-"));
        assert!(first.len() <= 32);
    }

    #[test]
    fn idempotency_should_classify_strategy_client_order_ids() {
        assert!(is_crossarb_client_order_id(
            "crossarb-live-small-mk-1-deadbeef"
        ));
        assert!(is_crossarb_client_order_id("crossarb-ls-mk-1-deadbeef"));
        assert!(is_crossarb_client_order_id(
            "t-crossarb-live-small-mk-1-deadbeef"
        ));
        assert_eq!(
            crossarb_order_mode("crossarb-live-small-mk-1-deadbeef"),
            Some("live-small")
        );
        assert_eq!(
            crossarb_order_mode("crossarb-live-scaled-mk-1-deadbeef"),
            Some("live-scaled")
        );
        assert_eq!(
            crossarb_order_mode("crossarb-ls-mk-1-deadbeef"),
            Some("live-small")
        );
        assert_eq!(
            crossarb_order_mode("crossarb-lx-mk-1-deadbeef"),
            Some("live-scaled")
        );
        assert!(!is_crossarb_client_order_id("other-strategy-order"));
    }
}
