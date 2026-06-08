# Backtesting Retirement Record

Status date: 2026-06-08

The offline research runner is retired from the active industrial workspace.
The previous migration plan for a dedicated app and library crate is no longer
an implementation target.

Retirement outcome:

- The dedicated app package and extracted library crate are removed from active
  workspace membership.
- The legacy root command wrappers for the research runner and short-ladder grid
  workflow are removed from active targets.
- Rust integration tests that exercised only the retired research surface are
  removed.
- CI should no longer check or smoke-test retired research commands.

This file remains only as a historical marker so older migration notes that
reference this path do not imply that the research runner is still a blocker for
control API, supervisor, gateway, tools, or live strategy migration.
