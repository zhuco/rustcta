# Web Control Panel

The existing dashboard read APIs remain available. Control-plane write APIs are added under `/api/control`.

Write APIs require Bearer token authentication and are unavailable when token auth is disabled. Every write requires `Idempotency-Key` and an expected lifecycle version in the payload. Handlers validate and enqueue/audit commands; long-running live actions are not executed synchronously.

Key endpoints:

- `POST /api/control/symbols/enable`
- `POST /api/control/symbols/{symbol}/pause`
- `POST /api/control/symbols/{symbol}/resume`
- `POST /api/control/symbols/{symbol}/disable`
- `POST /api/control/symbols/{symbol}/cancel-orders`
- `POST /api/control/symbols/{symbol}/confirm-liquidation`
- `POST /api/control/symbols/{symbol}/stop-liquidation`
- `POST /api/control/symbols/{symbol}/mark-dust-unmanaged`
- `GET /api/control/symbols`
- `GET /api/control/symbols/{symbol}`
- `GET /api/control/symbols/{symbol}/commands`
- `GET /api/control/symbols/{symbol}/liquidation`
- `GET /api/control/commands/{command_id}`
- `GET /api/control/audit`

Responses never include secrets. Current liquidation confirmation reports `live_submission_blocked: true`; real control-panel-triggered live submission remains blocked.
