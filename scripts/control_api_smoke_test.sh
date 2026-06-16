#!/usr/bin/env bash
set -Eeuo pipefail

MODE="${CONTROL_API_SMOKE_MODE:-local_minimal}"

if [[ "$MODE" == "local_minimal" ]]; then
  tmp_dir="$(mktemp -d)"
  server_log="$tmp_dir/control-api.log"
  server_pid=""
  cleanup() {
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
      kill "$server_pid" 2>/dev/null || true
      wait "$server_pid" 2>/dev/null || true
    fi
    rm -rf "$tmp_dir"
  }
  trap cleanup EXIT

  cat > "$tmp_dir/strategy.log" <<'LOG'
2026-06-07T12:00:00Z INFO strategy booted
2026-06-07T12:00:01Z WARN smoke event
LOG
  mkdir -p "$tmp_dir/dist"
  cat > "$tmp_dir/dist/index.html" <<'HTML'
<!doctype html>
<html><head><title>RustCTA Control</title></head><body>RustCTA Control</body></html>
HTML
  cat > "$tmp_dir/registry.json" <<JSON
{
  "schema_version": 1,
  "captured_at": "2026-06-07T12:00:02Z",
  "processes": [
    {
      "schema_version": 1,
      "strategy_id": "ci-unified-arb",
      "strategy_kind": "unified_arbitrage",
      "run_id": "ci-run",
      "tenant_id": "ci-tenant",
      "config_path": "config/unified_arbitrage_usdt.yml",
      "status": "Running",
      "process_id": 123,
      "started_at": "2026-06-07T12:00:00Z",
      "last_heartbeat_at": "2026-06-07T12:00:01Z",
      "last_snapshot_at": null,
      "restart_count": 0,
      "last_exit_code": null,
      "last_error": null,
      "log_path": "$tmp_dir/strategy.log"
    }
  ]
}
JSON

  bind="${RUSTCTA_CONTROL_API_BIND:-$(python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(f"127.0.0.1:{sock.getsockname()[1]}")
PY
)}"
  RUSTCTA_CONTROL_API_BIND="$bind" \
  RUSTCTA_CONTROL_API_ALLOW_NON_CANONICAL_BIND=true \
  RUSTCTA_CONTROL_API_AGENT_ID=ci-agent \
  RUSTCTA_CONTROL_API_TENANT_ID=ci-tenant \
  RUSTCTA_CONTROL_API_AGENT_CAPABILITIES=control-api,supervisor-reader \
  RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH="$tmp_dir/registry.json" \
  RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH="$tmp_dir/audit.jsonl" \
  RUSTCTA_CONTROL_API_STRATEGY_LOG_PATH="$tmp_dir/strategy.log" \
  RUSTCTA_CONTROL_API_STRATEGY_LOG_TAIL_LINES=200 \
  RUSTCTA_CONTROL_API_STRATEGY_LOG_TAIL_BYTES=65536 \
  RUSTCTA_CONTROL_API_STATIC_DIR="$tmp_dir/dist" \
  cargo run -q -p rustcta-control-api-app --bin rustcta-control-api >"$server_log" 2>&1 &
  server_pid="$!"

  if ! python3 - "$bind" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request

bind = sys.argv[1]
base_url = f"http://{bind}"
forbidden = ["API_KEY", "API_SECRET", "PASSPHRASE", "GATEIO_API", "BITGET_API"]

def get_json(path):
    req = urllib.request.Request(base_url + path, method="GET")
    with urllib.request.urlopen(req, timeout=5) as response:
        body = response.read().decode("utf-8", errors="replace")
        if response.status != 200:
            raise SystemExit(f"GET {path} returned HTTP {response.status}: {body[:200]}")
        upper = body.upper()
        for marker in forbidden:
            if marker in upper:
                raise SystemExit(f"GET {path} response contains forbidden marker {marker}")
        return json.loads(body) if body else None

def get_text(path):
    req = urllib.request.Request(base_url + path, method="GET")
    with urllib.request.urlopen(req, timeout=5) as response:
        body = response.read().decode("utf-8", errors="replace")
        if response.status != 200:
            raise SystemExit(f"GET {path} returned HTTP {response.status}: {body[:200]}")
        return body

deadline = time.time() + 90
last_error = None
while time.time() < deadline:
    try:
        health = get_json("/api/health")
        if health.get("status") == "ok":
            break
    except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
        last_error = exc
        time.sleep(0.25)
else:
    raise SystemExit(f"control api did not become healthy: {last_error}")

workspace = get_json("/api/workspace")
if workspace.get("process_count") != 1 or workspace.get("strategy_count") != 1:
    raise SystemExit(f"/api/workspace did not reflect supervisor registry: {workspace}")

strategies = get_json("/api/strategies")
if len(strategies) != 1 or strategies[0].get("strategy_id") != "ci-unified-arb":
    raise SystemExit(f"/api/strategies did not return registry process: {strategies}")
if strategies[0].get("strategy_kind") != "unified_arbitrage":
    raise SystemExit(f"/api/strategies returned wrong strategy kind: {strategies[0]}")
if strategies[0].get("log_configured") is not True:
    raise SystemExit(f"/api/strategies did not sanitize log path as log_configured: {strategies[0]}")
if "log_path" in strategies[0]:
    raise SystemExit("/api/strategies exposed log_path")

processes = get_json("/api/processes")
if processes != strategies:
    raise SystemExit("/api/processes and /api/strategies diverged for registry-backed view")

process = get_json("/api/processes/ci-unified-arb")
if process.get("strategy_id") != "ci-unified-arb" or process.get("status") != "Running":
    raise SystemExit(f"/api/processes/:id did not return running process: {process}")

logs = get_json("/api/strategy-logs")
if logs.get("configured") is not True or not logs.get("events"):
    raise SystemExit(f"/api/strategy-logs did not read configured strategy log: {logs}")

index = get_text("/")
if "<html" not in index.lower() or "RustCTA Control" not in index:
    raise SystemExit(f"/ did not return configured static index: {index[:120]}")

print(json.dumps({
    "ok": True,
    "base_url": base_url,
    "routes": [
        "/api/workspace",
        "/api/strategies",
        "/api/processes",
        "/api/processes/ci-unified-arb",
        "/api/strategy-logs"
    ],
    "strategy_id": strategies[0]["strategy_id"]
}, indent=2, sort_keys=True))
PY
  then
    printf '%s\n' 'control api server log:' >&2
    sed -n '1,160p' "$server_log" >&2 || true
    exit 1
  fi

  exit 0
fi

BASE_URL="${CONTROL_API_BASE_URL:-http://127.0.0.1:8091}"
TOKEN_ENV="${CONTROL_API_TOKEN_ENV:-RUSTCTA_MONITOR_TOKEN}"
COMMAND_PATH="${COMMAND_PATH:-data/control_api/control_commands.jsonl}"
SAFETY_CONFIG="${SAFETY_CONFIG:-config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml}"

if [[ -z "${!TOKEN_ENV:-}" ]]; then
  printf 'missing token env %s\n' "$TOKEN_ENV" >&2
  exit 2
fi

python3 - "$BASE_URL" "${!TOKEN_ENV}" "$COMMAND_PATH" "$SAFETY_CONFIG" <<'PY'
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

base_url, token, command_path, safety_config = sys.argv[1:5]
headers = {"Authorization": f"Bearer {token}"}
forbidden = ["API_KEY", "API_SECRET", "PASSPHRASE", "GATEIO_API", "BITGET_API"]

def request(path, *, token_required=True, method="GET"):
    req_headers = headers if token_required else {}
    req = urllib.request.Request(base_url + path, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            body = response.read().decode("utf-8", errors="replace")
            return response.status, body
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, body

def json_request(path, *, method="GET"):
    status, body = request(path, method=method)
    if status != 200:
        raise SystemExit(f"{method} {path} returned HTTP {status}: {body[:200]}")
    for marker in forbidden:
        if marker in body.upper():
            raise SystemExit(f"{method} {path} response contains forbidden marker {marker}")
    return json.loads(body) if body else None

status, _ = request("/api/status", token_required=False)
if status != 401:
    raise SystemExit(f"unauthenticated /api/status expected 401, got {status}")

status_json = json_request("/api/status")
if "dry_run" not in status_json or "live_trading_enabled" not in status_json:
    raise SystemExit("authenticated /api/status missing safety fields")

json_request("/api/health")
json_request("/api/config")
json_request("/api/exchanges")
json_request("/api/books")
json_request("/api/dry_run_plans")
json_request("/api/risk")
json_request("/api/control/runtime-publisher/status")

static_status, static_body = request("/", token_required=False)
if static_status != 200 or "<html" not in static_body.lower():
    raise SystemExit(f"static index did not load, status={static_status}")

spa_status, spa_body = request("/runtime", token_required=False)
if spa_status != 200 or "<html" not in spa_body.lower():
    raise SystemExit(f"SPA fallback did not load, status={spa_status}")

before = Path(command_path).read_text() if Path(command_path).exists() else ""
command = json_request("/api/control/kill_switch", method="POST")
if command.get("command", {}).get("would_submit_order") is not False:
    raise SystemExit("control command response did not preserve would_submit_order=false")
after = Path(command_path).read_text() if Path(command_path).exists() else ""
if len(after) <= len(before):
    raise SystemExit("control command was not appended to JSONL queue")
last = json.loads(after.strip().splitlines()[-1])
if last.get("would_submit_order") is not False:
    raise SystemExit("queued command did not preserve would_submit_order=false")

config_text = Path(safety_config).read_text()
unsafe_patterns = [
    (r"(?im)^\s*submit_orders\s*:\s*true\s*$", "submit_orders=true"),
    (r"(?im)^\s*dry_run\s*:\s*false\s*$", "dry_run=false"),
    (r"(?im)^\s*live_trading_enabled\s*:\s*true\s*$", "live_trading_enabled=true"),
    (r"(?im)^\s*http_enabled\s*:\s*true\s*$", "embedded http enabled"),
]
for pattern, label in unsafe_patterns:
    if re.search(pattern, config_text):
        raise SystemExit(f"safety config contains unsafe setting: {label}")

print(json.dumps({
    "ok": True,
    "base_url": base_url,
    "status": status_json,
    "command_id": command.get("command", {}).get("command_id"),
}, indent=2, sort_keys=True))
PY
