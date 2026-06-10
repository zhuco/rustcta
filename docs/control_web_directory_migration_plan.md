# Control API + Web Panel Current Directory Boundary

Status date: 2026-06-08

The root `src/` tree has been removed from this checkout. Control-panel docs and
runbooks must use the current workspace entrypoints only.

## Active Layout

- `apps/control-api/` is the runnable local control API app. Its binary is
  `rustcta-control-api`.
- `crates/rustcta-control-api/` owns the public workspace/read-model API
  contract.
- `web-ui/dioxus/` is the only Web panel frontend source tree.
- `config/accounts.yml` is the account-manager source for exchange account ids,
  enabled flags, and `env_prefix` values.
- `data/control_api/exchange_api_keys.env` is the default local env-store for
  exchange credentials. Override it with
  `RUSTCTA_CONTROL_API_EXCHANGE_API_KEY_STORE`.

There is no valid root `src/bin/control_api.rs`, `src/web/`, or root-package
control-panel binary in this checkout.

## Current Run Command

The server-side control panel must be deployed through the local server helper:

```bash
scripts/rustcta_server.sh deploy-control-panel
```

That is the only normal entrypoint for building the Dioxus panel, syncing
`web-ui/dioxus/dist` to the server, deploying `rustcta-control-api`, and
restarting the `control-api` user service.

For a full cross-arb live runner plus Web control panel deploy, use:

```bash
scripts/rustcta_server.sh deploy-cross-arb-live-stack
```

Direct `cargo run` is for local debugging only:

```bash
RUSTCTA_CONTROL_API_BIND=127.0.0.1:8091 \
RUSTCTA_CONTROL_API_AGENT_ID=local-agent \
RUSTCTA_CONTROL_API_TENANT_ID=local \
RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH=run/supervisor/registry.json \
RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH=data/control_api/audit.jsonl \
RUSTCTA_CONTROL_API_EXCHANGE_API_KEY_STORE=data/control_api/exchange_api_keys.env \
RUSTCTA_CONTROL_API_ACCOUNTS_CONFIG=config/accounts.yml \
RUSTCTA_CONTROL_API_STATIC_DIR=web-ui/dioxus/dist \
RUSTCTA_CONTROL_API_LEGACY_SNAPSHOT_PATH=logs/cross_exchange_arbitrage/cross_arb_live_dashboard.json \
cargo run -p rustcta-control-api-app --bin rustcta-control-api
```

Open `http://127.0.0.1:8091`.

`127.0.0.1:8091` is the fixed control-panel Web service bind. Do not run the
control panel through `8080`, `dx serve`, or any other long-running Web service
port.

The served static directory is always `web-ui/dioxus/dist`. The source tree is
`web-ui/dioxus`; Dioxus build output under `target/dx/.../public` is an
intermediate artifact and must be copied into `dist` before deployment.

For the cross-arb page, `control-api` must read
`logs/cross_exchange_arbitrage/cross_arb_live_dashboard.json`. Do not use the
old `cross_arb_dashboard_snapshot.json` path for the active panel.

## Exchange Configuration Contract

The Web exchange configuration page uses `/api/exchange-api-keys` from
`apps/control-api`.

- `GET /api/exchange-api-keys` reads `config/accounts.yml`, reads the local
  env-store, and returns supported exchanges, account-manager rows, account
  credential rows, and masked field status.
- `POST /api/exchange-api-keys` saves or clears credentials for an account that
  exists in `config/accounts.yml`.
- `DELETE /api/exchange-api-keys/:exchange` remains available for direct local
  cleanup, but the Web UI uses the POST `clear: true` path so it can include the
  selected account and credential namespace.

The browser never receives raw stored secrets. Existing secret values are shown
only as masked hints. Secret writes stay in the local env-store.

## Boundary Rules

- Do not add new control-panel runbooks that reference removed root `src/`
  entrypoints.
- Do not put local credential file IO into `crates/rustcta-control-api`; keep it
  in `apps/control-api` because it is a local deployment concern.
- Do not expose raw `api_key`, `api_secret`, `secret`, `passphrase`, or
  authorization values from read-model routes.
- If an old capability is needed again, migrate the behavior into
  `apps/control-api` or a dedicated workspace crate first, then update this
  document and `docs/dioxus_control_panel.md`.
