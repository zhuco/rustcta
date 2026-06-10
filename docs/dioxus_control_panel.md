# Dioxus Control Panel Architecture

Status date: 2026-06-07

This document covers the local Dioxus control panel path after the workspace
control API migration. The default entrypoint is `apps/control-api` binary
`rustcta-control-api`.

## Active Entry

`web-ui/dioxus/` is the single frontend source tree. In this checkout it is
served by the workspace control API app:

- `apps/control-api` owns local process wiring, static asset serving, local
  side-effect routes, and the local exchange credential env-store bridge.
- `crates/rustcta-control-api` owns the public workspace/read-model contract.
- Removed root `src/` entrypoints are historical migration context only and are
  not valid run commands.

The exchange configuration page uses `/api/exchange-api-keys` from
`apps/control-api`. That route reads `config/accounts.yml`, writes the local
env-store configured by `RUSTCTA_CONTROL_API_EXCHANGE_API_KEY_STORE`, and returns
only redacted field status.

## Process Boundary

```text
browser
  |
  | static Dioxus assets + /api/*
  v
rustcta-control-api process
  | serves static Dioxus assets
  | exposes secret-free read models
  | appends audited operator/local-agent command records
  v
supervisor registry, runtime snapshots, audit ledger, local-agent queue
  ^
  | sanitized MonitoringState snapshot writer
  |
strategy runtime process
  - spot_spot_taker_arbitrage
  - market data, scanning, risk gates, RuntimePublisher
  - LivePreflight, SmallLiveGate, live_dry_run plans
  - no browser UI binding by default
```

The Dioxus frontend has no exchange credentials, no exchange API clients, no
local shell execution, and no trading execution code. It calls
`rustcta-control-api` only.

## Migrated Features

| Old embedded dashboard feature | New control_api endpoint family | Dioxus panel |
| --- | --- | --- |
| Status and overview | `/api/status`, `/api/health` | Overview |
| Exchange health | `/api/exchanges` | Exchanges |
| Symbol status | `/api/symbols`, `/api/control/symbols` | Symbols |
| Order books | `/api/books`, `/api/control/symbols/:symbol/book` | Symbols |
| Opportunities and scanner | `/api/opportunities`, `/api/opportunities/recent`, `/api/scanner/*` | Opportunities, Scanner |
| Dry-run order plans | `/api/dry_run_plans`, `/api/live_dry_run/orders` | Dry-run plans |
| Risk gates and risk events | `/api/risk`, `/api/risk/events` | Risk |
| LivePreflight and SmallLiveGate | `/api/live_preflight*`, `/api/small_live_gate` | Risk |
| RuntimePublisher state | `/api/control/runtime-publisher/*`, `/api/events` | RuntimePublisher |
| Logs | `/api/logs` | Logs |
| Config summary | `/api/config`, `/api/config/summary` | Config |
| Inventory, fees, disabled symbols | `/api/inventory`, `/api/fees`, `/api/disabled` | Config |
| Control actions | `/api/local-agent/commands` | Controls |

Write-like UI actions either call the audited strategy command route or append a
local-agent command. The browser never executes shell scripts and never calls
exchange APIs.

## Single Entrypoint

The operational entrypoint for this panel is `scripts/rustcta_server.sh`.
Use it to build, upload, and restart the server copy of the Web panel:

```bash
scripts/rustcta_server.sh deploy-control-panel
```

For local static rebuilds without server upload:

```bash
scripts/rustcta_server.sh build-web
```

The script builds `web-ui/dioxus`, copies the Dioxus release output into
`web-ui/dioxus/dist`, uploads that exact `dist` directory to
`/home/cta/rustcta/web-ui/dioxus/dist`, and restarts `control-api`.

For the live cross-arb strategy plus the Web panel, use one stack entrypoint:

```bash
scripts/rustcta_server.sh deploy-cross-arb-live-stack
```

That command rebuilds/uploads the Dioxus panel, the control API binary, the
cross-arb live runner binary, and the live strategy config before restarting
both systemd user services.

## Build

```bash
scripts/rustcta_server.sh build-web
```

For direct debugging only, the equivalent manual build is:

```bash
cd web-ui/dioxus
dx build --release --debug-symbols false
```

If `dx` is missing:

```bash
rustup target add wasm32-unknown-unknown
cargo install dioxus-cli --version 0.7.9 --locked
```

Dioxus 0.7 writes release web assets under the workspace target directory at
`dx/rustcta-control-panel/release/web/public`. The server script discovers the
workspace target directory with `cargo metadata`. If that path changes, set
`RUSTCTA_DIOXUS_BUILD_PUBLIC` before running
`scripts/rustcta_server.sh build-web`.

## Local Run

```bash
export RUSTCTA_CONTROL_API_BIND=127.0.0.1:8091
export RUSTCTA_CONTROL_API_AGENT_ID=local-agent
export RUSTCTA_CONTROL_API_TENANT_ID=local
export RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH=run/supervisor/registry.json
export RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH=data/control_api/audit.jsonl
export RUSTCTA_CONTROL_API_EXCHANGE_API_KEY_STORE=data/control_api/exchange_api_keys.env
export RUSTCTA_CONTROL_API_ACCOUNTS_CONFIG=config/accounts.yml
export RUSTCTA_CONTROL_API_STATIC_DIR=web-ui/dioxus/dist
cargo run -p rustcta-control-api-app --bin rustcta-control-api
```

Open:

```text
http://127.0.0.1:8091
```

Port `8091` is the canonical local control panel entrypoint. The
`rustcta-control-api` process serves both the Dioxus static assets and every
`/api/*` route on `127.0.0.1:8091`.

Do not run any RustCTA control-panel Web service on another port. Do not use
`dx serve`, port `8080`, or an alternate `RUSTCTA_CONTROL_API_BIND` for this
panel. Build the Dioxus static assets and serve them through
`rustcta-control-api` on `127.0.0.1:8091`; that one process owns both static
files and `/api/*` routes.

Static assets are public on the local listener. `/api/*` routes return
secret-free read models and audited command responses.

Safe shutdown:

```bash
pkill -f 'rustcta-control-api' || true
```

Pid and log files:

| Process | PID | Log pointer |
| --- | --- | --- |
| Strategy runtime | `run/strategy.pid` | `run/strategy.log_path` |
| Control API | process manager specific | process manager specific |

Static assets are served from `web-ui/dioxus/dist` by default.

For the cross-exchange arbitrage page, the control API must read the live runner
dashboard snapshot:

```bash
RUSTCTA_CONTROL_API_LEGACY_SNAPSHOT_PATH=logs/cross_exchange_arbitrage/cross_arb_live_dashboard.json
```

Do not point the active control panel at
`logs/cross_exchange_arbitrage/cross_arb_dashboard_snapshot.json`; that is not
the current live runner output.

## Direct Commands

Strategy runtime:

```bash
target/release/rustcta \
  --strategy spot_spot_taker_arbitrage \
  --config config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml
```

Workspace control API on loopback:

```bash
RUSTCTA_CONTROL_API_BIND=127.0.0.1:8091 \
RUSTCTA_CONTROL_API_AGENT_ID=local-agent \
RUSTCTA_CONTROL_API_TENANT_ID=local \
RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH=run/supervisor/registry.json \
RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH=data/control_api/audit.jsonl \
RUSTCTA_CONTROL_API_STATIC_DIR=web-ui/dioxus/dist \
cargo run -p rustcta-control-api-app --bin rustcta-control-api
```

## External Access

Default production bind stays `127.0.0.1:8091`. Prefer one of these options.

### Option A: SSH Tunnel

From your workstation:

```bash
ssh -L 8091:127.0.0.1:8091 cta@45.77.253.180
```

Then open:

```text
http://127.0.0.1:8091
```

This keeps the control API off the public interface.

### Option B: Nginx HTTPS Reverse Proxy

Keep the active control API process bound to loopback, terminate TLS in Nginx,
and proxy to loopback:

```nginx
server {
    listen 443 ssl http2;
    server_name control.example.com;

    ssl_certificate /etc/letsencrypt/live/control.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/control.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8091;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

Firewall notes:

- Allow `443/tcp` to Nginx.
- Do not expose `8091/tcp` publicly.
- Keep exchange credential environment variables only on the strategy runtime process.

### Direct Public Bind

Direct `0.0.0.0` binding is not the default control panel mode. Prefer a
loopback listener behind SSH tunneling or an HTTPS reverse proxy.

## API Contract

Workspace control API endpoints:

- `GET /api/status`
- `GET /api/config`
- `GET /api/exchanges`
- `GET /api/symbols`
- `GET /api/books`
- `GET /api/opportunities`
- `GET /api/dry_run_plans`
- `GET /api/risk`
- `GET /api/logs`
- `GET /api/health`
- `GET /api/events` via SSE
- `GET /api/workspace`
- `GET /api/agents`
- `GET /api/agents/:agent_id`
- `GET /api/processes`
- `GET /api/processes/:process_id`
- `GET /api/processes/:id/logs`
- `GET /api/strategies`
- `GET /api/strategies/:strategy_id`
- `GET /api/strategies/:id/logs`
- `GET /api/gateway/status`
- `GET /api/events`
- `GET /api/audit`
- `GET /api/risk`
- `GET /api/fees`
- `GET /api/logs`
- `GET /api/strategy-logs`
- `GET /api/credentials/status`
- `GET /api/strategy-snapshots`
- `GET /api/strategies/:strategy_id/snapshot`
- `POST /api/commands`
- `GET /api/commands/:command_id`
- `GET /api/local-agent/status`
- `POST /api/local-agent/commands`
- `POST /api/local-agent/strategy-config`

Dioxus workspace route usage:

- The primary shell refreshes `workspace`, `agents`, `processes`,
  `strategies`, `gateway/status`, and `credentials/status` first.
- Status, config, risk, logs, fees, inventory, books, exchanges, symbols,
  opportunities, and strategy logs prefer workspace control API routes when
  present.
- The credential panel is read-only. It uses `/api/credentials/status` for
  public slots. It does not post, delete, persist, or locally store raw
  credential fields.
- Strategy create/lifecycle controls are queued through `/api/local-agent/commands`;
  the public control API router remains read-only.

## Safety Model

- `rustcta-control-api` returns only sanitized snapshot/read model data.
- The frontend contains no secrets and no exchange credential fields.
- API response bodies are screened for common secret markers before being returned.
- Control actions are queued to JSONL only.
- Runtime consumption of queued commands is not enabled.
- Dry-run plans must retain `would_submit=false`.
- Do not set `live_dry_run.submit_orders=true`, `dry_run=false`, or `live_trading_enabled=true` for this deployment path.

## Smoke Tests

Run the new app smoke test:

```bash
scripts/control_api_smoke_test.sh
```

The smoke test checks:

- status and health respond with sanitized JSON
- Dioxus static index loads
- SPA fallback loads
- secret markers are absent from checked API responses
- supervisor registry, strategy log, and runtime snapshot read models load

## Troubleshooting

- Wrong port or blank UI: use `http://127.0.0.1:8091`. Do not run an ad-hoc frontend server, `dx serve`, or any RustCTA control-panel Web service on another port.
- Empty/default workspace: the supervisor registry or typed runtime snapshots are not configured yet.
- Missing static UI: build Dioxus and confirm `web-ui/dioxus/dist/index.html` exists, then set `RUSTCTA_CONTROL_API_STATIC_DIR`.
- Public link unreachable over SSH tunnel: confirm `rustcta-control-api` is listening on `127.0.0.1:8091` on the server and the SSH session is still open.
- Public HTTPS unreachable: check DNS, Nginx TLS config, firewall `443/tcp`, and that Nginx can reach `127.0.0.1:8091`.
- Commands appear not to pause the runtime: this is expected. Commands are queued unless an approved runtime command consumer is added later.
