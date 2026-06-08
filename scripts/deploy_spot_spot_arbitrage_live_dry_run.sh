#!/usr/bin/env bash
set -Eeuo pipefail

# Deploy Spot <-> Spot cross-exchange arbitrage for monitoring and live_dry_run
# validation only. This script refuses every known live-order path.

REMOTE="${REMOTE:-cta@45.77.253.180}"
REMOTE_BASE="${REMOTE_BASE:-/home/cta/rustcta_spot_arb}"
SPOT_ARB_CONFIG="${SPOT_ARB_CONFIG:-config/spot_spot_taker_arbitrage_gateio_bitget.live-dry-run.example.yml}"
BIN_PATH="${BIN_PATH:-target/release/rustcta}"
RELEASE_ID="${RELEASE_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
REMOTE_RELEASE="${REMOTE_BASE}/releases/${RELEASE_ID}"
RUNTIME_CONFIG_REL="config/spot_spot_taker_arbitrage.runtime.yml"
RUNTIME_BIN_REL="bin/rustcta"
MIGRATION_SQL="${MIGRATION_SQL:-data/sql/cross_exchange_arbitrage.sql}"
SSH_OPTS="${SSH_OPTS:-}"
READINESS_TIMEOUT_SECONDS="${READINESS_TIMEOUT_SECONDS:-180}"
STARTUP_SETTLE_SECONDS="${STARTUP_SETTLE_SECONDS:-20}"
VALIDATE_ONLY="${VALIDATE_ONLY:-0}"
VALIDATOR_FILE=""

cleanup() {
  if [[ -n "${VALIDATOR_FILE:-}" ]]; then
    rm -f "$VALIDATOR_FILE"
  fi
}
trap cleanup EXIT

log() {
  printf '[deploy] %s\n' "$*"
}

die() {
  printf '[deploy][fatal] %s\n' "$*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

shell_quote() {
  printf '%q' "$1"
}

write_validator() {
  local validator_file="$1"
  cat >"$validator_file" <<'PY'
import json
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text()

errors = []

def fail(msg):
    errors.append(msg)

def strip_comment(line):
    in_single = False
    in_double = False
    out = []
    for ch in line:
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            break
        out.append(ch)
    return "".join(out)

values = {}
value_lines = {}
lists = {}
list_lines = {}
stack = []
for lineno, raw in enumerate(text.splitlines(), 1):
    line = strip_comment(raw).rstrip()
    if not line.strip():
        continue
    stripped = line.lstrip(" ")
    indent = len(line) - len(stripped)
    if stripped.startswith("- "):
        while stack and stack[-1][0] >= indent:
            stack.pop()
        key_path = ".".join(item[1] for item in stack)
        if key_path:
            item = stripped[2:].strip().strip('"').strip("'")
            lists.setdefault(key_path, []).append(item)
            list_lines.setdefault(key_path, []).append(lineno)
        continue
    if ":" not in stripped:
        continue
    key, value = stripped.split(":", 1)
    key = key.strip()
    if not key or key.startswith("{"):
        continue
    while stack and stack[-1][0] >= indent:
        stack.pop()
    key_path = ".".join([item[1] for item in stack] + [key])
    value = value.strip()
    if not value or value in {"|", ">"}:
        stack.append((indent, key))
        continue
    value = value.strip('"').strip("'")
    values[key_path] = value
    value_lines[key_path] = lineno

def scalar(name, default=None):
    return values.get(name, default)

def location(name):
    if name in value_lines:
        return f"{path}:{value_lines[name]}"
    if name in list_lines and list_lines[name]:
        return f"{path}:{list_lines[name][0]}"
    return f"{path}:?"

def parse_inline_list(value):
    if value is None:
        return []
    value = str(value).strip()
    if not (value.startswith("[") and value.endswith("]")):
        return []
    return [
        item.strip().strip('"').strip("'")
        for item in value[1:-1].split(",")
        if item.strip()
    ]

def list_value(name):
    if name in lists:
        return lists[name]
    return parse_inline_list(values.get(name))

def normalized_bool(value):
    if value is None:
        return None
    value = str(value).strip().strip('"').strip("'").lower()
    if value in {"true", "yes", "on", "1"}:
        return True
    if value in {"false", "no", "off", "0"}:
        return False
    return None

def require_scalar(name, expected):
    value = scalar(name)
    if value is None:
        fail(f"{location(name)}: missing required config key: {name}")
        return
    if str(value).strip().lower() != expected:
        fail(f"{location(name)}: {name} must be {expected}, got {value!r}")

def require_bool(name, expected):
    value = scalar(name)
    parsed = normalized_bool(value)
    if parsed is None:
        fail(f"{location(name)}: {name} must be boolean {str(expected).lower()}, got {value!r}")
    elif parsed != expected:
        fail(f"{location(name)}: {name} must be {str(expected).lower()}, got {value!r}")

def require_list(name, *, exact=None, min_len=None, max_len=None):
    items = [item for item in list_value(name) if item]
    if not items:
        fail(f"{location(name)}: {name} must list explicit entries")
        return items
    if exact is not None and len(items) != exact:
        fail(f"{location(name)}: {name} must contain exactly {exact} entries, got {len(items)}")
    if min_len is not None and len(items) < min_len:
        fail(f"{location(name)}: {name} must contain at least {min_len} entries, got {len(items)}")
    if max_len is not None and len(items) > max_len:
        fail(f"{location(name)}: {name} must contain at most {max_len} entries, got {len(items)}")
    normalized = [item.strip().lower() for item in items]
    if len(normalized) != len(set(normalized)):
        fail(f"{location(name)}: {name} must not contain duplicates")
    return items

def require_same_set(name, expected_items):
    items = list_value(name)
    if not items:
        return
    expected = {item.strip().lower() for item in expected_items}
    actual = {item.strip().lower() for item in items}
    if actual != expected:
        fail(f"{location(name)}: {name} must match {sorted(expected)}, got {sorted(actual)}")

for pattern, message in [
    (r"(?im)^\s*trading_mode\s*:\s*live\s*$", "trading_mode=live is forbidden"),
    (r"(?im)^\s*live_trading_enabled\s*:\s*true\s*$", "live_trading_enabled=true is forbidden"),
    (r"(?im)^\s*submit_orders\s*:\s*true\s*$", "live_dry_run.submit_orders=true is forbidden"),
    (r"(?im)^\s*dry_run\s*:\s*false\s*$", "dry_run=false is forbidden"),
    (r"(?im)^\s*allow_live_orders\s*:\s*true\s*$", "kill_switch.allow_live_orders=true is forbidden"),
    (r"(?im)^\s*maker_taker_execution\s*:\s*true\s*$", "maker_taker_execution=true is forbidden"),
    (r"(?im)^\s*explicit_live_confirmation\s*:\s*true\s*$", "explicit live confirmation is forbidden for validation-only deployment"),
    (r"(?im)^\s*require_api_key_trade_permission\s*:\s*true\s*$", "trade API permission requirement is forbidden"),
    (r"(?im)^\s*allow_unbounded_market_order\s*:\s*true\s*$", "unbounded market orders are forbidden"),
    (r"(?im)^\s*(transfer|withdraw|withdrawal)\s*:\s*true\s*$", "fund transfer/withdraw config is forbidden"),
]:
    compiled = re.compile(pattern)
    for lineno, raw in enumerate(text.splitlines(), 1):
        if compiled.search(strip_comment(raw)):
            fail(f"{path}:{lineno}: {message}")

if "ONE_EXPLICIT_SYMBOL" in text:
    for lineno, raw in enumerate(text.splitlines(), 1):
        if "ONE_EXPLICIT_SYMBOL" in raw:
            fail(f"{path}:{lineno}: replace ONE_EXPLICIT_SYMBOL with explicit production symbols before deployment")

require_scalar("trading_mode", "live_dry_run")
require_bool("dry_run", True)
require_bool("live_trading_enabled", False)
require_scalar("market_data_mode", "websocket_cache")
require_bool("websocket.enabled", True)
require_bool("rest_polling.enabled", False)
require_bool("monitoring.enabled", True)
require_bool("monitoring.require_token", True)
require_bool("live_preflight.enabled", True)
require_bool("live_preflight.require_api_key_trade_permission", False)
require_bool("live_preflight.require_withdraw_permission_absent", True)
require_bool("live_dry_run.enabled", True)
require_bool("live_dry_run.build_order_requests", True)
require_bool("live_dry_run.submit_orders", False)
require_bool("kill_switch.enabled", True)
require_bool("kill_switch.allow_live_orders", False)
require_bool("small_live_gate.enabled", True)
require_bool("small_live_gate.explicit_live_confirmation", False)
require_bool("arbitrage_scanner.enabled", True)
require_bool("arbitrage_scanner.relationships.spot_spot", True)
require_bool("arbitrage_scanner.relationships.spot_perp", False)
require_bool("arbitrage_scanner.relationships.perp_perp", False)
require_bool("arbitrage_scanner.execution_modes.taker_taker", True)
require_bool("arbitrage_scanner.execution_modes.maker_taker_execution", False)
require_bool("spot_symbol_control.enabled", True)
require_bool("spot_symbol_control.require_write_auth", True)
require_bool("spot_symbol_control.allow_future_small_live", False)
require_bool("spot_symbol_control.disable_defaults.cancel_active_orders", False)
require_bool("spot_symbol_control.market_liquidation.allow_unbounded_market_order", False)
require_bool("spot_symbol_control.passive_liquidation.enabled", False)
require_bool("spot_symbol_control.runtime_publisher.enabled", True)

bind_addr = scalar("monitoring.bind_addr", "127.0.0.1:8091")
expose_publicly = normalized_bool(scalar("monitoring.expose_publicly"))
require_token = normalized_bool(scalar("monitoring.require_token"))
if str(bind_addr).startswith("0.0.0.0:") or str(bind_addr).startswith("[::]:"):
    if expose_publicly is not True:
        fail(f"{location('monitoring.expose_publicly')}: public dashboard bind requires monitoring.expose_publicly=true")
    if require_token is not True:
        fail(f"{location('monitoring.require_token')}: public dashboard bind requires monitoring.require_token=true")
elif not str(bind_addr).startswith("127.0.0.1:") and not str(bind_addr).startswith("localhost:"):
    fail(f"{location('monitoring.bind_addr')}: monitoring.bind_addr must be loopback or explicit public bind, got {bind_addr!r}")

exchanges = require_list("exchanges", exact=2)
symbols = require_list("symbols", min_len=1, max_len=5)
websocket_exchanges = require_list("websocket.exchanges", exact=2)
websocket_symbols = require_list("websocket.symbols", min_len=1, max_len=5)
preflight_exchanges = require_list("live_preflight.exchanges", exact=2)
preflight_symbols = require_list("live_preflight.symbols", min_len=1, max_len=5)
small_gate_exchanges = require_list("small_live_gate.enabled_exchanges", exact=2)
small_gate_symbols = require_list("small_live_gate.enabled_symbols", min_len=1, max_len=5)

require_same_set("websocket.exchanges", exchanges)
require_same_set("live_preflight.exchanges", exchanges)
require_same_set("small_live_gate.enabled_exchanges", exchanges)
require_same_set("websocket.symbols", symbols)
require_same_set("live_preflight.symbols", symbols)
require_same_set("small_live_gate.enabled_symbols", symbols)

enable_database_recording = normalized_bool(scalar("enable_database_recording", "false"))

if errors:
    for item in errors:
        print(f"CONFIG SAFETY ERROR: {item}", file=sys.stderr)
    sys.exit(2)

print(json.dumps({
    "config_path": str(path),
    "monitoring_bind_addr": bind_addr,
    "dashboard_probe_addr": "127.0.0.1:" + str(bind_addr).rsplit(":", 1)[-1] if str(bind_addr).startswith("0.0.0.0:") else bind_addr,
    "monitoring_token_env": scalar("monitoring.token_env", "RUSTCTA_MONITOR_TOKEN"),
    "enable_database_recording": bool(enable_database_recording),
    "fee_config_path": scalar("fee_config_path", "config/fees.yml"),
    "disabled_registry_path": scalar("disabled_registry_path", "config/disabled_symbols.yml"),
    "exchanges": exchanges,
    "symbols": symbols,
}, sort_keys=True))
PY
}

json_get() {
  python3 -c 'import json,sys; print(json.load(sys.stdin).get(sys.argv[1], ""))' "$1"
}

check_prerequisites() {
  need_cmd cargo
  need_cmd python3
  need_cmd ssh
  need_cmd rsync
  [[ -f "$SPOT_ARB_CONFIG" ]] || die "config not found: $SPOT_ARB_CONFIG"
  [[ -f "$MIGRATION_SQL" ]] || die "migration SQL not found: $MIGRATION_SQL"
}

build_binary() {
  log "building release binary"
  cargo build --release --bin rustcta
  [[ -x "$BIN_PATH" ]] || die "release binary not found or not executable: $BIN_PATH"
}

config_file_list() {
  local files=(
    "$SPOT_ARB_CONFIG"
    "config/config.yaml"
    "config/global.yml"
    "config/fees.yml"
    "config/disabled_symbols.yml"
    "config/symbol_mappings.yml"
    "config/spot_symbol_control.yml"
    "config/spot_control_runtime_snapshot.yml"
    "config/spot_exchanges_example.yml"
    "config/binance_spot_example.yml"
    "config/okx_spot_example.yml"
    "config/mexc_spot_example.yml"
    "config/coinex_spot_example.yml"
    "config/paper_trading.yml"
  )
  local file
  for file in "${files[@]}"; do
    [[ -f "$file" ]] && printf '%s\n' "$file"
  done
}

upload_release() {
  log "creating remote release directory: ${REMOTE_RELEASE}"
  ssh ${SSH_OPTS} "$REMOTE" "mkdir -p $(shell_quote "$REMOTE_RELEASE")/bin $(shell_quote "$REMOTE_RELEASE")/config $(shell_quote "$REMOTE_BASE")/shared/data $(shell_quote "$REMOTE_BASE")/shared/logs"

  local rsync_ssh=()
  if [[ -n "$SSH_OPTS" ]]; then
    rsync_ssh=(-e "ssh $SSH_OPTS")
  fi

  log "uploading arbitrage binary"
  rsync -az "${rsync_ssh[@]}" "$BIN_PATH" "${REMOTE}:${REMOTE_RELEASE}/${RUNTIME_BIN_REL}"

  log "uploading Spot arbitrage configuration files"
  mapfile -t CONFIG_FILES < <(config_file_list)
  rsync -azR "${rsync_ssh[@]}" "${CONFIG_FILES[@]}" "${REMOTE}:${REMOTE_RELEASE}/"

  for asset_dir in web public static assets; do
    if [[ -d "$asset_dir" ]]; then
      log "uploading web control panel assets from ${asset_dir}/"
      rsync -azR "${rsync_ssh[@]}" "$asset_dir" "${REMOTE}:${REMOTE_RELEASE}/"
    fi
  done
}

render_remote_config() {
  log "rendering server-side runtime config from environment variables"
  ssh ${SSH_OPTS} "$REMOTE" \
    "REMOTE_BASE=$(shell_quote "$REMOTE_BASE") REMOTE_RELEASE=$(shell_quote "$REMOTE_RELEASE") TEMPLATE_REL=$(shell_quote "$SPOT_ARB_CONFIG") RUNTIME_CONFIG_REL=$(shell_quote "$RUNTIME_CONFIG_REL") bash -s" <<'REMOTE'
set -Eeuo pipefail
load_dotenv() {
  local env_file="$1" line key value
  [[ -f "$env_file" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "${line//[[:space:]]/}" || "${line#"${line%%[![:space:]]*}"}" == \#* || "$line" != *=* ]] && continue
    key="${line%%=*}"
    value="${line#*=}"
    key="${key#"${key%%[![:space:]]*}"}"
    key="${key%"${key##*[![:space:]]}"}"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    if [[ "${value:0:1}" == '"' && "${value: -1}" == '"' ]] || [[ "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
      value="${value:1:${#value}-2}"
    fi
    export "$key=$value"
  done < "$env_file"
}
set -a
for env_file in "$HOME/.profile" "$HOME/.bash_profile" "$HOME/.bashrc" "$HOME/.rustcta_env"; do
  if [[ -f "$env_file" ]]; then
    # shellcheck disable=SC1090
    . "$env_file"
  fi
done
set +a
load_dotenv "$HOME/rustcta/.env"
load_dotenv "$REMOTE_BASE/shared/.monitor_token_env"
[[ -z "${GATEIO_API_KEY:-}" && -n "${GATE_API_KEY:-}" ]] && export GATEIO_API_KEY="$GATE_API_KEY"
[[ -z "${GATEIO_API_SECRET:-}" && -n "${GATE_API_SECRET:-}" ]] && export GATEIO_API_SECRET="$GATE_API_SECRET"
[[ -z "${BITGET_API_PASSPHRASE:-}" && -n "${BITGET_PASSPHRASE:-}" ]] && export BITGET_API_PASSPHRASE="$BITGET_PASSPHRASE"
if [[ -z "${RUSTCTA_MONITOR_TOKEN:-}" ]]; then
  mkdir -p "$REMOTE_BASE/shared"
  token_file="$REMOTE_BASE/shared/.monitor_token_env"
  if [[ -f "$token_file" ]]; then
    set -a
    # shellcheck disable=SC1090
    . "$token_file"
    set +a
  fi
  if [[ -z "${RUSTCTA_MONITOR_TOKEN:-}" ]]; then
    token="$(python3 - <<'PY_TOKEN'
import secrets
print(secrets.token_hex(32))
PY_TOKEN
)"
    umask 077
    printf 'RUSTCTA_MONITOR_TOKEN=%s\n' "$token" > "$token_file"
    export RUSTCTA_MONITOR_TOKEN="$token"
  fi
fi
cd "$REMOTE_RELEASE"
mkdir -p "$(dirname "$RUNTIME_CONFIG_REL")" run
rm -rf data logs
ln -sfnT "$REMOTE_BASE/shared/data" data
ln -sfnT "$REMOTE_BASE/shared/logs" logs
python3 - "$TEMPLATE_REL" "$RUNTIME_CONFIG_REL" <<'PY_RENDER'
import os
import re
import sys
from pathlib import Path

template = Path(sys.argv[1])
output = Path(sys.argv[2])
pattern = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
missing = []
rendered = []

for lineno, line in enumerate(template.read_text().splitlines(True), 1):
    def replace(match):
        name = match.group(1)
        value = os.environ.get(name)
        if value is None or value == "":
            missing.append(f"{template}:{lineno}: missing required environment variable {name}")
            return match.group(0)
        return value

    rendered.append(pattern.sub(replace, line))

if missing:
    for item in missing:
        print(item, file=sys.stderr)
    sys.exit(12)

output.write_text("".join(rendered))
PY_RENDER
chmod 600 "$RUNTIME_CONFIG_REL"
REMOTE
}

validate_remote_config() {
  local validator_file="$1"
  log "validating rendered remote config"
  local remote_runtime_config="${REMOTE_RELEASE}/${RUNTIME_CONFIG_REL}"
  ssh ${SSH_OPTS} "$REMOTE" \
    "python3 - $(shell_quote "$remote_runtime_config")" \
    <"$validator_file" >/dev/null
}

run_migrations_if_required() {
  local db_required="$1"
  if [[ "$db_required" != "True" && "$db_required" != "true" ]]; then
    log "database migrations not required: enable_database_recording=false"
    return
  fi
  log "database recording enabled; applying ClickHouse migrations over ssh stdin"
  ssh ${SSH_OPTS} "$REMOTE" "command -v clickhouse-client >/dev/null && clickhouse-client --multiquery" <"$MIGRATION_SQL"
}

start_remote_runtime() {
  log "starting monitoring/live_dry_run runtime"
  ssh ${SSH_OPTS} "$REMOTE" \
    "REMOTE_BASE=$(shell_quote "$REMOTE_BASE") REMOTE_RELEASE=$(shell_quote "$REMOTE_RELEASE") RUNTIME_CONFIG_REL=$(shell_quote "$RUNTIME_CONFIG_REL") RUNTIME_BIN_REL=$(shell_quote "$RUNTIME_BIN_REL") bash -s" <<'REMOTE'
set -Eeuo pipefail
load_dotenv() {
  local env_file="$1" line key value
  [[ -f "$env_file" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "${line//[[:space:]]/}" || "${line#"${line%%[![:space:]]*}"}" == \#* || "$line" != *=* ]] && continue
    key="${line%%=*}"
    value="${line#*=}"
    key="${key#"${key%%[![:space:]]*}"}"
    key="${key%"${key##*[![:space:]]}"}"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    if [[ "${value:0:1}" == '"' && "${value: -1}" == '"' ]] || [[ "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
      value="${value:1:${#value}-2}"
    fi
    export "$key=$value"
  done < "$env_file"
}
set -a
for env_file in "$HOME/.profile" "$HOME/.bash_profile" "$HOME/.bashrc" "$HOME/.rustcta_env"; do
  if [[ -f "$env_file" ]]; then
    # shellcheck disable=SC1090
    . "$env_file"
  fi
done
set +a
load_dotenv "$HOME/rustcta/.env"
load_dotenv "$REMOTE_BASE/shared/.monitor_token_env"
[[ -z "${GATEIO_API_KEY:-}" && -n "${GATE_API_KEY:-}" ]] && export GATEIO_API_KEY="$GATE_API_KEY"
[[ -z "${GATEIO_API_SECRET:-}" && -n "${GATE_API_SECRET:-}" ]] && export GATEIO_API_SECRET="$GATE_API_SECRET"
[[ -z "${BITGET_API_PASSPHRASE:-}" && -n "${BITGET_PASSPHRASE:-}" ]] && export BITGET_API_PASSPHRASE="$BITGET_PASSPHRASE"
if [[ -z "${RUSTCTA_MONITOR_TOKEN:-}" ]]; then
  mkdir -p "$REMOTE_BASE/shared"
  token_file="$REMOTE_BASE/shared/.monitor_token_env"
  if [[ -f "$token_file" ]]; then
    set -a
    # shellcheck disable=SC1090
    . "$token_file"
    set +a
  fi
  if [[ -z "${RUSTCTA_MONITOR_TOKEN:-}" ]]; then
    token="$(python3 - <<'PY_TOKEN'
import secrets
print(secrets.token_hex(32))
PY_TOKEN
)"
    umask 077
    printf 'RUSTCTA_MONITOR_TOKEN=%s\n' "$token" > "$token_file"
    export RUSTCTA_MONITOR_TOKEN="$token"
  fi
fi
if [[ -f "$REMOTE_BASE/current/run/spot_arb.pid" ]]; then
  old_pid="$(cat "$REMOTE_BASE/current/run/spot_arb.pid" || true)"
  if [[ -n "$old_pid" ]] && kill -0 "$old_pid" 2>/dev/null; then
    echo "Stopping previous spot arbitrage runtime pid=$old_pid"
    kill "$old_pid"
    for _ in {1..20}; do
      kill -0 "$old_pid" 2>/dev/null || break
      sleep 0.5
    done
    if kill -0 "$old_pid" 2>/dev/null; then
      echo "Previous process did not stop after TERM; leaving it untouched" >&2
      exit 10
    fi
  fi
fi
ln -sfnT "$REMOTE_RELEASE" "$REMOTE_BASE/current"
cd "$REMOTE_BASE/current"
mkdir -p run
log_file="logs/spot_spot_arbitrage_${HOSTNAME}_$(date -u +%Y%m%dT%H%M%SZ).log"
RUST_LOG="${RUST_LOG:-info}" nohup "./$RUNTIME_BIN_REL" \
  --strategy spot_spot_taker_arbitrage \
  --config "$RUNTIME_CONFIG_REL" \
  >"$log_file" 2>&1 &
pid="$!"
echo "$pid" > run/spot_arb.pid
echo "$log_file" > run/spot_arb.log_path
sleep 2
if ! kill -0 "$pid" 2>/dev/null; then
  echo "runtime exited during startup; recent log:" >&2
  tail -n 120 "$log_file" >&2 || true
  exit 11
fi
echo "started pid=$pid log=$log_file"
REMOTE
}

stop_remote_runtime() {
  log "stopping remote runtime after failed readiness"
  ssh ${SSH_OPTS} "$REMOTE" \
    "REMOTE_BASE=$(shell_quote "$REMOTE_BASE") bash -s" <<'REMOTE'
set -Eeuo pipefail
pid_file="$REMOTE_BASE/current/run/spot_arb.pid"
if [[ -f "$pid_file" ]]; then
  pid="$(cat "$pid_file" || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    for _ in {1..20}; do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.5
    done
    if kill -0 "$pid" 2>/dev/null; then
      echo "runtime still running after TERM pid=$pid" >&2
      exit 1
    fi
    echo "stopped pid=$pid"
  fi
fi
REMOTE
}

verify_remote_readiness() {
  local bind_addr="$1"
  local token_env="$2"
  log "verifying dashboard, preflight, runtime publisher, books, exchanges, and dry-run order plans"
  ssh ${SSH_OPTS} "$REMOTE" \
    "REMOTE_BASE=$(shell_quote "$REMOTE_BASE") BIND_ADDR=$(shell_quote "$bind_addr") TOKEN_ENV=$(shell_quote "$token_env") READINESS_TIMEOUT_SECONDS=$(shell_quote "$READINESS_TIMEOUT_SECONDS") bash -s" <<'REMOTE'
set -Eeuo pipefail
load_dotenv() {
  local env_file="$1" line key value
  [[ -f "$env_file" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "${line//[[:space:]]/}" || "${line#"${line%%[![:space:]]*}"}" == \#* || "$line" != *=* ]] && continue
    key="${line%%=*}"
    value="${line#*=}"
    key="${key#"${key%%[![:space:]]*}"}"
    key="${key%"${key##*[![:space:]]}"}"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    if [[ "${value:0:1}" == '"' && "${value: -1}" == '"' ]] || [[ "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
      value="${value:1:${#value}-2}"
    fi
    export "$key=$value"
  done < "$env_file"
}
set -a
for env_file in "$HOME/.profile" "$HOME/.bash_profile" "$HOME/.bashrc" "$HOME/.rustcta_env"; do
  if [[ -f "$env_file" ]]; then
    # shellcheck disable=SC1090
    . "$env_file"
  fi
done
set +a
load_dotenv "$HOME/rustcta/.env"
load_dotenv "$REMOTE_BASE/shared/.monitor_token_env"
[[ -z "${GATEIO_API_KEY:-}" && -n "${GATE_API_KEY:-}" ]] && export GATEIO_API_KEY="$GATE_API_KEY"
[[ -z "${GATEIO_API_SECRET:-}" && -n "${GATE_API_SECRET:-}" ]] && export GATEIO_API_SECRET="$GATE_API_SECRET"
[[ -z "${BITGET_API_PASSPHRASE:-}" && -n "${BITGET_PASSPHRASE:-}" ]] && export BITGET_API_PASSPHRASE="$BITGET_PASSPHRASE"
if [[ -z "${RUSTCTA_MONITOR_TOKEN:-}" ]]; then
  token_file="$REMOTE_BASE/shared/.monitor_token_env"
  if [[ -f "$token_file" ]]; then
    set -a
    # shellcheck disable=SC1090
    . "$token_file"
    set +a
  fi
fi

cd "$REMOTE_BASE/current"
python3 - <<'PY'
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

bind_addr = os.environ["BIND_ADDR"]
token_env = os.environ["TOKEN_ENV"]
timeout = int(os.environ.get("READINESS_TIMEOUT_SECONDS", "180"))
base_url = f"http://{bind_addr}"
token = os.environ.get(token_env, "")
headers = {}
if token:
    headers["Authorization"] = f"Bearer {token}"

def get(path, required=True):
    req = urllib.request.Request(base_url + path, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        if required:
            raise RuntimeError(f"GET {path} failed with HTTP {exc.code}") from exc
        return None
    return json.loads(body) if body else None

last_error = None
status = None
for _ in range(timeout):
    try:
        status = get("/api/status")
        if (
            status.get("strategy_status") == "running"
            and status.get("trading_mode") == "live_dry_run"
            and status.get("dry_run") is True
            and status.get("live_trading_enabled") is False
        ):
            break
    except Exception as exc:
        last_error = exc
    time.sleep(1)
else:
    raise SystemExit(f"dashboard did not become ready before timeout; last_error={last_error}; status={status}")

config = get("/api/config/summary")
books = get("/api/books")
exchanges = get("/api/exchanges")
preflight = get("/api/live_preflight/summary")
small_gate = get("/api/small_live_gate")
orders = get("/api/live_dry_run/orders")
runtime_publisher = get("/api/control/runtime-publisher/status")
runtime_components = get("/api/control/runtime-publisher/components", required=False)
runtime_errors = get("/api/control/runtime-publisher/errors", required=False)

errors = []
if status.get("trading_mode") != "live_dry_run":
    errors.append(f"status.trading_mode={status.get('trading_mode')}")
if status.get("live_trading_enabled") is not False:
    errors.append("live_trading_enabled is not false")
if status.get("dry_run") is not True:
    errors.append("dry_run is not true")
if status.get("kill_switch_active") is True:
    errors.append("kill switch is active")

if not preflight:
    errors.append("missing LivePreflight summary")
else:
    if preflight.get("fail_count") != 0:
        errors.append(f"LivePreflight fail_count={preflight.get('fail_count')}")
    if preflight.get("decision") != "ready_for_live_dry_run":
        errors.append(f"LivePreflight decision={preflight.get('decision')}")

if not small_gate:
    errors.append("missing SmallLiveGate summary")
else:
    if small_gate.get("does_not_start_live_trading") is not True:
        errors.append("SmallLiveGate does_not_start_live_trading is not true")
    if small_gate.get("status") != "blocked":
        errors.append(f"SmallLiveGate must remain blocked for no-live deployment, got {small_gate.get('status')}")

if runtime_publisher.get("running") is not True:
    errors.append("RuntimePublisher is not running")
if runtime_publisher.get("last_critical_error"):
    errors.append(f"RuntimePublisher critical error: {runtime_publisher.get('last_critical_error')}")

if not books:
    errors.append("BookCache has no books")
else:
    stale_books = [book for book in books if book.get("is_stale")]
    if stale_books:
        errors.append(f"BookCache stale books={len(stale_books)}")
    non_ws_books = [book for book in books if book.get("source") != "websocket"]
    if non_ws_books:
        errors.append(f"non-websocket book sources observed={len(non_ws_books)}")

if not exchanges:
    errors.append("exchange status is empty")

unsafe_orders = [order for order in orders if order.get("would_submit")]
if unsafe_orders:
    errors.append(f"live_dry_run contains would_submit=true plans: {len(unsafe_orders)}")

print("\n=== process status ===")
pid_path = "run/spot_arb.pid"
try:
    pid = open(pid_path).read().strip()
    print(subprocess.check_output(["ps", "-o", "pid,ppid,stat,etime,cmd", "-p", pid], text=True).strip())
except Exception as exc:
    print(f"process status unavailable: {exc}")

print("\n=== listening ports ===")
try:
    print(subprocess.check_output(["ss", "-ltnp"], text=True).strip())
except Exception as exc:
    print(f"listening ports unavailable: {exc}")

print("\n=== dashboard URL ===")
print(base_url)

print("\n=== LivePreflight summary ===")
print(json.dumps(preflight, indent=2, sort_keys=True))

print("\n=== SmallLiveGate summary ===")
print(json.dumps(small_gate, indent=2, sort_keys=True))

print("\n=== RuntimePublisher health ===")
print(json.dumps(runtime_publisher, indent=2, sort_keys=True))

print("\n=== RuntimePublisher component snapshot ===")
print(json.dumps(runtime_components, indent=2, sort_keys=True))

print("\n=== RuntimePublisher errors ===")
print(json.dumps(runtime_errors, indent=2, sort_keys=True))

print("\n=== BookCache health ===")
print(json.dumps({
    "book_count": len(books),
    "stale_count": sum(1 for book in books if book.get("is_stale")),
    "sample": books[:10],
}, indent=2, sort_keys=True))

print("\n=== enabled symbols ===")
print(json.dumps(config.get("enabled_symbols", []), indent=2, sort_keys=True))

print("\n=== exchange status ===")
print(json.dumps(exchanges, indent=2, sort_keys=True))

print("\n=== live_dry_run order planning ===")
print(json.dumps({
    "plan_count": len(orders),
    "would_submit_count": len(unsafe_orders),
    "sample": orders[-5:],
}, indent=2, sort_keys=True))

if errors:
    print("\nREADINESS ERRORS:", file=sys.stderr)
    for error in errors:
        print(f"- {error}", file=sys.stderr)
    sys.exit(20)
PY
REMOTE
}

main() {
  check_prerequisites

  VALIDATOR_FILE="$(mktemp)"
  write_validator "$VALIDATOR_FILE"

  log "validating local deployment config: ${SPOT_ARB_CONFIG}"
  local summary_json
  summary_json="$(python3 "$VALIDATOR_FILE" "$SPOT_ARB_CONFIG")"
  local db_required bind_addr probe_addr token_env
  db_required="$(json_get enable_database_recording <<<"$summary_json")"
  bind_addr="$(json_get monitoring_bind_addr <<<"$summary_json")"
  probe_addr="$(json_get dashboard_probe_addr <<<"$summary_json")"
  token_env="$(json_get monitoring_token_env <<<"$summary_json")"

  if [[ "$VALIDATE_ONLY" == "1" || "$VALIDATE_ONLY" == "true" ]]; then
    log "validate-only mode passed; no build, upload, migration, or remote startup performed"
    printf '%s\n' "$summary_json"
    return
  fi

  build_binary
  upload_release
  render_remote_config
  validate_remote_config "$VALIDATOR_FILE"
  run_migrations_if_required "$db_required"
  start_remote_runtime
  if [[ "$STARTUP_SETTLE_SECONDS" != "0" ]]; then
    log "waiting ${STARTUP_SETTLE_SECONDS}s for websocket books and runtime publisher to settle"
    sleep "$STARTUP_SETTLE_SECONDS"
  fi
  if ! verify_remote_readiness "$probe_addr" "$token_env"; then
    stop_remote_runtime || true
    return 20
  fi

  local port
  port="${bind_addr##*:}"
  log "deployment complete"
  log "dashboard is bound on the server at http://${bind_addr}"
  if [[ "$bind_addr" == 0.0.0.0:* ]]; then
    log "external dashboard URL: http://${REMOTE#*@}:${port}"
  fi
  log "use an SSH tunnel if needed: ssh -L ${port}:127.0.0.1:${port} ${REMOTE}"
}

main "$@"
