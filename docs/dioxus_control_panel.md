# Dioxus Control Panel Architecture

Status date: 2026-06-07

This document covers the current local Dioxus control panel path and the
workspace control API migration. During `industrial-workspace-migration`, there
are two control API entrypoints:

- Legacy local console: root binary `control_api`, launched by
  `scripts/separated_control_panel.sh`. It owns the current token-protected
  local UI workflow on `127.0.0.1:8080`.
- Workspace control API: `apps/control-api` binary `rustcta-control-api`. It is
  the migrating generic multi-strategy API surface and should be the target for
  new control-plane route/model work.

## Process Boundary

```text
browser
  |
  | static Dioxus assets + /api/* with Bearer token
  v
legacy control_api process
  | reads sanitized runtime snapshots
  | appends authenticated operator command records
  v
data/control_api/dashboard_snapshot.json
data/control_api/control_commands.jsonl
  ^
  | sanitized MonitoringState snapshot writer
  |
strategy runtime process
  - spot_spot_taker_arbitrage
  - market data, scanning, risk gates, RuntimePublisher
  - LivePreflight, SmallLiveGate, live_dry_run plans
  - no browser UI binding by default
```

The Dioxus frontend has no exchange credentials, no exchange API clients, and
no trading execution code. In the legacy local console flow it calls only
`control_api`. In the workspace flow it should call `rustcta-control-api`.

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
| Control actions | `/api/control/pause`, `/api/control/resume`, `/api/control/kill_switch` | Controls |

Write-like legacy control routes now append queued JSONL commands only. They include `would_submit_order=false`, `applied_to_runtime=false`, and a warning that runtime command consumption is not enabled.

## Build

```bash
cargo build --release --bin rustcta --bin control_api
cargo build --release -p rustcta-control-api-app --bin rustcta-control-api
cd web-ui/dioxus
dx build --release
```

The helper script builds the current legacy local-console binaries, builds the
Dioxus app, and syncs the Dioxus release output into `web-ui/dioxus/dist`:

```bash
scripts/separated_control_panel.sh build
```

If `dx` is missing:

```bash
rustup target add wasm32-unknown-unknown
cargo install dioxus-cli --version 0.7.9 --locked
```

Dioxus 0.7 writes release web assets under `web-ui/dioxus/target/dx/rustcta-control-panel/release/web/public`. If that path changes, set `DIOXUS_BUILD_PUBLIC` before running the helper script.

## Local Run

```bash
export RUSTCTA_MONITOR_TOKEN="$(openssl rand -hex 32)"
scripts/separated_control_panel.sh build
scripts/separated_control_panel.sh start
scripts/separated_control_panel.sh status
```

Open:

```text
http://127.0.0.1:8080
```

Paste the token into the Dioxus auth token input. Port `8080` is the canonical
local entrypoint for the legacy console. The `control_api` process serves both
the Dioxus static assets and every `/api/*` route on `127.0.0.1:8080`.

Do not use `dx serve` to run the operator control panel. `dx serve` starts a
separate frontend-only development server on another port and is useful only for
isolated UI work; it is not the live control API endpoint. For normal local
operation use `scripts/separated_control_panel.sh start` or
`scripts/separated_control_panel.sh start-control-api`.

Static assets are public on the local listener. Every `/api/*` route requires
`Authorization: Bearer $RUSTCTA_MONITOR_TOKEN`.

Safe shutdown:

```bash
scripts/separated_control_panel.sh stop-control-api
scripts/separated_control_panel.sh stop-strategy
```

Pid and log files:

| Process | PID | Log pointer |
| --- | --- | --- |
| Strategy runtime | `run/strategy.pid` | `run/strategy.log_path` |
| Control API | `run/control_api.pid` | `run/control_api.log_path` |

Static assets are served from `web-ui/dioxus/dist` by default.

## Direct Commands

Strategy runtime:

```bash
target/release/rustcta \
  --strategy spot_spot_taker_arbitrage \
  --config config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml
```

Legacy control API on loopback:

```bash
export RUSTCTA_MONITOR_TOKEN="<strong random token>"
target/release/control_api \
  --bind-addr 127.0.0.1:8080 \
  --snapshot-path data/control_api/dashboard_snapshot.json \
  --command-path data/control_api/control_commands.jsonl \
  --token-env RUSTCTA_MONITOR_TOKEN \
  --static-dir web-ui/dioxus/dist
```

Workspace control API on loopback:

```bash
RUSTCTA_CONTROL_API_BIND=127.0.0.1:18080 \
RUSTCTA_CONTROL_API_AGENT_ID=local-agent \
RUSTCTA_CONTROL_API_TENANT_ID=local \
RUSTCTA_CONTROL_API_LEGACY_SNAPSHOT_PATH=data/control_api/dashboard_snapshot.json \
RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH=run/supervisor/registry.json \
RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH=data/control_api/audit.jsonl \
RUSTCTA_CONTROL_API_STATIC_DIR=web-ui/dioxus/dist \
cargo run -p rustcta-control-api-app --bin rustcta-control-api
```

## External Access

Default production bind stays `127.0.0.1:8080`. Prefer one of these options.

### Option A: SSH Tunnel

From your workstation:

```bash
ssh -L 8080:127.0.0.1:8080 cta@45.77.253.180
```

Then open:

```text
http://127.0.0.1:8080
```

This keeps the control API off the public interface. API calls still require the monitor token.

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
        proxy_pass http://127.0.0.1:8080;
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
- Do not expose `8080/tcp` publicly.
- Keep exchange credential environment variables only on the strategy runtime process.

### Direct Public Bind

Direct `0.0.0.0` binding is intentionally guarded for the legacy local console.
It requires both:

```bash
CONTROL_API_EXTERNAL_ACCESS_ENABLED=true
RUSTCTA_MONITOR_TOKEN=<at least 24 characters>
```

and the explicit runtime flag:

```bash
target/release/control_api \
  --bind-addr 0.0.0.0:8080 \
  --external-access-enabled \
  --snapshot-path data/control_api/dashboard_snapshot.json \
  --command-path data/control_api/control_commands.jsonl \
  --token-env RUSTCTA_MONITOR_TOKEN \
  --static-dir web-ui/dioxus/dist
```

Use this only behind a firewall or reverse proxy. The service refuses non-loopback binding without the explicit flag and token.

## API Contract

Legacy local-console endpoints:

Core endpoints:

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
- `POST /api/control/pause`
- `POST /api/control/resume`
- `POST /api/control/kill_switch`

Compatibility read endpoints from the old dashboard are preserved under
`/api/*`, including `live_preflight`, `small_live_gate`, `scanner`,
`hedge-policy`, `runtime-publisher`, order reconciliation, balance
reconciliation, and spot-control snapshot routes.

Workspace control API endpoints:

- `GET /api/health`
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
- `POST /api/commands`
- `GET /api/commands/:command_id`

## Safety Model

- The old embedded HTTP server remains opt-in through `monitoring.http_enabled=false` defaults.
- `control_api` and `rustcta-control-api` return only sanitized snapshot/read
  model data.
- The frontend contains no secrets and no exchange credential fields.
- API response bodies are screened for common secret markers before being returned.
- Control actions are queued to JSONL only.
- Runtime consumption of queued commands is not enabled.
- Dry-run plans must retain `would_submit=false`.
- Do not set `live_dry_run.submit_orders=true`, `dry_run=false`, or `live_trading_enabled=true` for this deployment path.

## Smoke Tests

Start the legacy `control_api` on loopback, then run:

```bash
export RUSTCTA_MONITOR_TOKEN="<same token used by control_api>"
CONTROL_API_BASE_URL=http://127.0.0.1:8080 \
COMMAND_PATH=data/control_api/control_commands.jsonl \
scripts/control_api_smoke_test.sh
```

The smoke test checks:

- unauthenticated `/api/status` returns `401`
- authenticated status and health respond with sanitized JSON
- Dioxus static index loads
- SPA fallback loads
- secret markers are absent from checked API responses
- control action POST appends JSONL with `would_submit_order=false`
- the live-dry-run config does not enable unsafe settings

## Troubleshooting

- `401 Unauthorized`: verify `RUSTCTA_MONITOR_TOKEN` in the control API process and browser token input.
- Wrong port or blank UI: use `http://127.0.0.1:8080`. Do not use an ad-hoc `dx serve` port for operator access.
- Empty/default dashboard: the runtime has not written `data/control_api/dashboard_snapshot.json` yet, or `monitoring.enabled` is false.
- Missing static UI: run `scripts/separated_control_panel.sh build` and confirm `web-ui/dioxus/dist/index.html` exists.
- Public link unreachable over SSH tunnel: confirm `control_api` is listening on `127.0.0.1:8080` on the server and the SSH session is still open.
- Public HTTPS unreachable: check DNS, Nginx TLS config, firewall `443/tcp`, and that Nginx can reach `127.0.0.1:8080`.
- Direct public bind refused: set `--external-access-enabled` only after setting a strong `RUSTCTA_MONITOR_TOKEN`; prefer SSH tunnel or Nginx instead.
- Commands appear not to pause the runtime: this is expected. Commands are queued unless an approved runtime command consumer is added later.
