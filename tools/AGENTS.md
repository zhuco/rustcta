# Tool Boundary Rules

Tools are operator, migration, audit, and analysis executables that are not
long-running platform services.

Rules:

- Prefer moving ad-hoc or operational `src/bin/*.rs` programs here before
  moving production services.
- Tools may call platform crates and compatibility APIs, but they must not own
  strategy runtime loops, gateway servers, supervisor services, or web servers.
- Tools must not become a second credential-owning exchange gateway. Any live
  exchange mutation tool must be treated as a temporary migration candidate and
  documented in `rustcta-tools-ops`.
- Keep reusable domain logic in `crates/`; keep tools as composition and
  reporting layers.
- This directory is for directory/runtime boundary migration, not for expanding
  exchange adapter API coverage.

Required checks:

```bash
cargo check -p rustcta-tools-ops
cargo test -p rustcta-tools-ops
```
