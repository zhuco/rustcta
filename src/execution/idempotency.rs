use crate::execution::BundleLeg;
use crate::market::RuntimeMode;

pub fn deterministic_client_order_id(
    mode: RuntimeMode,
    bundle_id: &str,
    leg: BundleLeg,
    attempt: u32,
) -> String {
    format!(
        "crossarb-{}-{}-{}-{}",
        runtime_mode_slug(mode),
        bundle_short_id(bundle_id),
        leg.as_slug(),
        attempt
    )
}

pub fn runtime_mode_slug(mode: RuntimeMode) -> &'static str {
    match mode {
        RuntimeMode::Observe => "observe",
        RuntimeMode::Simulation => "simulation",
        RuntimeMode::Shadow => "shadow",
        RuntimeMode::LiveSmall => "live-small",
        RuntimeMode::LiveScaled => "live-scaled",
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
        assert_eq!(first, "crossarb-simulation-bundleabcdef-hedge-2");
    }
}
