#![allow(clippy::all)]
use rustcta_tools_ops::{migrations_by_target, LegacyBinRetirement, LegacyBinTarget};

#[test]
fn cross_arb_observe_should_have_root_free_migration_plan() {
    let migrations = migrations_by_target(LegacyBinTarget::StrategyRuntime);
    let migration = migrations
        .into_iter()
        .find(|migration| migration.source == "cross_arb_observe.rs")
        .expect("cross_arb_observe should have a migration plan");

    assert_eq!(migration.compatibility, LegacyBinRetirement::KeepWrapper);
    assert!(!migration
        .new_command
        .contains("cargo run --bin cross_arb_observe"));
    assert!(migration
        .new_command
        .contains("rustcta-industrial -- cross-arb observe"));
    assert!(migration.retirement_milestone.contains("observe output"));
}
