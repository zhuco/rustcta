# Cross-Arb Local-To-Server Runbook

Use the local machine as the heavy build and validation environment. The server
should only run smoke checks, preflight, and the final release binary build.

## Local Gate

Fast validation while developing:

```bash
scripts/cross_arb_local_gate.sh quick
```

Full validation before pushing or copying to the server:

```bash
scripts/cross_arb_local_gate.sh full
```

Network validation from the current machine and VPN/proxy route:

```bash
scripts/cross_arb_local_gate.sh network
```

Private preflight after public network checks pass and API env vars are loaded:

```bash
scripts/cross_arb_local_gate.sh private
```

`private` reads account endpoints but does not submit orders.

## Server Gate

On the server, avoid repeating the full local test matrix unless dependencies or
toolchain changed on the server. Run:

```bash
scripts/cross_arb_local_gate.sh server-smoke
```

Then build only the binaries needed for the cross-exchange arbitrage workflow:

```bash
cargo build --release --bin cross_arb_observe --bin cross_arb_preflight --bin cross_arb_server
```

If the server also runs existing grid strategies, build those binaries
separately instead of `cargo build --release` for all targets.

## Build-Pressure Controls

- Set a persistent target directory on the server:

```bash
export CARGO_TARGET_DIR=/data/rustcta-target
```

- Warm dependencies once after pulling code:

```bash
cargo fetch
```

- Prefer binary-scoped commands:

```bash
cargo check --bin cross_arb_preflight --bin cross_arb_observe
cargo build --release --bin cross_arb_observe
```

- Keep `cargo test --all-features` local unless the server has a toolchain or
  environment-specific problem.
- Do not run private preflight until public connectivity is clean.
- Do not run live/small-order tests until `cross_arb_preflight --private`
  reports usable account, position, open-order, and fee reads.

## Current Tool Map

- Public network probe:
  `python3 scripts/public_connectivity_probe.py --timeout-ms 5000 --json`
- Cross-arb public/private preflight:
  `cargo run --bin cross_arb_preflight -- --timeout-ms 5000`
- Safe public market observation:
  `cargo run --bin cross_arb_observe -- --config config/cross_exchange_arbitrage_usdt.yml`
- Dashboard/API skeleton:
  `cargo run --bin cross_arb_server -- --config config/cross_exchange_arbitrage_usdt.yml`
- Local validation wrapper:
  `scripts/cross_arb_local_gate.sh quick|full|network|private|server-smoke`

## Server Readiness Rule

Do not start online testing on the server until all of these are true:

- local `full` passes;
- server `server-smoke` passes;
- public `network` passes for the exchanges enabled in config;
- private preflight passes for Binance/OKX before live use;
- Bitget/Gate remain private-trading disabled until verified core private
  exchange implementations are added.
