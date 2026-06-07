#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

STRATEGY_CONFIG="${STRATEGY_CONFIG:-config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml}"
STRATEGY_BIN="${STRATEGY_BIN:-target/release/rustcta}"
CONTROL_API_BIN="${CONTROL_API_BIN:-target/release/control_api}"
CONTROL_API_BIND_ADDR="${CONTROL_API_BIND_ADDR:-127.0.0.1:8091}"
CONTROL_API_EXTERNAL_ACCESS_ENABLED="${CONTROL_API_EXTERNAL_ACCESS_ENABLED:-false}"
CONTROL_API_TOKEN_ENV="${CONTROL_API_TOKEN_ENV:-RUSTCTA_MONITOR_TOKEN}"
CROSS_ARB_EXECUTE="${CROSS_ARB_EXECUTE:-false}"
CROSS_ARB_SKIP_PRIVATE_AUDIT="${CROSS_ARB_SKIP_PRIVATE_AUDIT:-true}"
STRATEGY_ENV_FILE="${STRATEGY_ENV_FILE:-.env}"
EXCHANGE_API_KEY_STORE="${EXCHANGE_API_KEY_STORE:-data/control_api/exchange_api_keys.env}"
SNAPSHOT_PATH="${SNAPSHOT_PATH:-data/control_api/dashboard_snapshot.json}"
COMMAND_PATH="${COMMAND_PATH:-data/control_api/control_commands.jsonl}"
DIOXUS_DIR="${DIOXUS_DIR:-web-ui/dioxus}"
DIOXUS_STATIC_DIR="${DIOXUS_STATIC_DIR:-web-ui/dioxus/dist}"
DIOXUS_BUILD_PUBLIC="${DIOXUS_BUILD_PUBLIC:-$DIOXUS_DIR/target/dx/rustcta-control-panel/release/web/public}"
RUN_DIR="${RUN_DIR:-run}"
LOG_DIR="${LOG_DIR:-logs/control_panel}"
LOG_MAX_BYTES="${LOG_MAX_BYTES:-104857600}"
LOG_MAX_FILES="${LOG_MAX_FILES:-10}"

if [[ -z "${CROSS_ARB_BIN:-}" ]]; then
  if [[ -x target/debug/cross_arb_live && ( ! -x target/release/cross_arb_live || target/debug/cross_arb_live -nt target/release/cross_arb_live ) ]]; then
    CROSS_ARB_BIN="target/debug/cross_arb_live"
  else
    CROSS_ARB_BIN="target/release/cross_arb_live"
  fi
fi

usage() {
  cat <<'USAGE'
Usage:
  scripts/separated_control_panel.sh build
  scripts/separated_control_panel.sh start-strategy
  scripts/separated_control_panel.sh start-control-api
  scripts/separated_control_panel.sh start
  scripts/separated_control_panel.sh stop-strategy
  scripts/separated_control_panel.sh stop-control-api
  scripts/separated_control_panel.sh stop
  scripts/separated_control_panel.sh restart-strategy
  scripts/separated_control_panel.sh restart-control-api
  scripts/separated_control_panel.sh restart
  scripts/separated_control_panel.sh status
  scripts/separated_control_panel.sh logs-strategy [lines]
  scripts/separated_control_panel.sh logs-control-api [lines]

Environment:
  STRATEGY_CONFIG=config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml
  CROSS_ARB_BIN=target/release/cross_arb_live
  CROSS_ARB_EXECUTE=false
  CROSS_ARB_SKIP_PRIVATE_AUDIT=true
  CONTROL_API_BIND_ADDR=127.0.0.1:8091
  CONTROL_API_EXTERNAL_ACCESS_ENABLED=false
  RUSTCTA_MONITOR_TOKEN=<required for API access>
  STRATEGY_ENV_FILE=.env
  EXCHANGE_API_KEY_STORE=data/control_api/exchange_api_keys.env
  STRATEGY_CONFIG=config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml
  SNAPSHOT_PATH=data/control_api/dashboard_snapshot.json
  COMMAND_PATH=data/control_api/control_commands.jsonl
  DIOXUS_STATIC_DIR=web-ui/dioxus/dist
  DIOXUS_BUILD_PUBLIC=web-ui/dioxus/target/dx/rustcta-control-panel/release/web/public
USAGE
}

log() {
  printf '[separated-control] %s\n' "$*"
}

rotate_log_file() {
  local log_file="$1"
  local max_files="$2"
  [[ -e "$log_file" ]] || return 0
  if (( max_files <= 1 )); then
    rm -f "$log_file"
    return 0
  fi

  local last=$((max_files - 1))
  rm -f "$log_file.$last"
  local index
  for ((index = last - 1; index >= 1; index--)); do
    if [[ -e "$log_file.$index" ]]; then
      mv -f "$log_file.$index" "$log_file.$((index + 1))"
    fi
  done
  mv -f "$log_file" "$log_file.1"
}

rotating_log_writer() {
  local log_file="$1"
  local max_bytes="$2"
  local max_files="$3"
  mkdir -p "$(dirname "$log_file")"

  local line rendered current_size
  while IFS= read -r line || [[ -n "$line" ]]; do
    rendered="${line}"$'\n'
    current_size="$(stat -c%s "$log_file" 2>/dev/null || printf '0')"
    if (( max_bytes > 0 && current_size > 0 && current_size + ${#rendered} > max_bytes )); then
      rotate_log_file "$log_file" "$max_files"
    fi
    printf '%s' "$rendered" >> "$log_file"
  done
}

run_detached() {
  local log_file="$1"
  shift
  export -f rotate_log_file rotating_log_writer
  if command -v setsid >/dev/null 2>&1; then
    setsid nohup bash -c '
      log_file="$1"
      max_bytes="$2"
      max_files="$3"
      shift 3
      "$@" 2>&1 | LC_ALL=C rotating_log_writer "$log_file" "$max_bytes" "$max_files"
      exit "${PIPESTATUS[0]}"
    ' bash "$log_file" "$LOG_MAX_BYTES" "$LOG_MAX_FILES" "$@" >/dev/null 2>&1 &
  else
    nohup bash -c '
      log_file="$1"
      max_bytes="$2"
      max_files="$3"
      shift 3
      "$@" 2>&1 | LC_ALL=C rotating_log_writer "$log_file" "$max_bytes" "$max_files"
      exit "${PIPESTATUS[0]}"
    ' bash "$log_file" "$LOG_MAX_BYTES" "$LOG_MAX_FILES" "$@" >/dev/null 2>&1 &
  fi
  echo "$!"
}

process_pattern_for_name() {
  local name="$1"
  case "$name" in
    strategy)
      if is_cross_arb_config; then
        printf '%s --config %s' "$CROSS_ARB_BIN" "$STRATEGY_CONFIG"
      else
        printf '%s --strategy spot_spot_taker_arbitrage --config %s' "$STRATEGY_BIN" "$STRATEGY_CONFIG"
      fi
      ;;
    control_api)
      printf '%s --bind-addr %s' "$CONTROL_API_BIN" "$CONTROL_API_BIND_ADDR"
      ;;
  esac
}

process_patterns_for_name() {
  local name="$1"
  local primary
  primary="$(process_pattern_for_name "$name" || true)"
  [[ -n "$primary" ]] && printf '%s\n' "$primary"
}

is_cross_arb_config() {
  [[ -f "$STRATEGY_CONFIG" ]] || return 1
  grep -Eq 'cross_exchange_arbitrage|cross_exchange_arbitrage_usdt_perp|^[[:space:]]*execution:[[:space:]]*$' "$STRATEGY_CONFIG"
}

cross_arb_config_mode() {
  awk -F: '
    /^[[:space:]]*mode:[[:space:]]*/ {
      value=$2
      sub(/#.*/, "", value)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      print value
      exit
    }
  ' "$STRATEGY_CONFIG"
}

cross_arb_config_dry_run() {
  awk -F: '
    /^[[:space:]]*execution:[[:space:]]*$/ { in_execution=1; next }
    in_execution && /^[^[:space:]]/ { in_execution=0 }
    in_execution && /^[[:space:]]*dry_run:[[:space:]]*/ {
      value=$2
      sub(/#.*/, "", value)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      print value
      exit
    }
  ' "$STRATEGY_CONFIG"
}

strategy_launch_args() {
  if ! is_cross_arb_config; then
    printf '%s\0' "$STRATEGY_BIN" --strategy spot_spot_taker_arbitrage --config "$STRATEGY_CONFIG"
    return 0
  fi

  printf '%s\0' "$CROSS_ARB_BIN" --config "$STRATEGY_CONFIG" --run
  if [[ "$CROSS_ARB_SKIP_PRIVATE_AUDIT" == "true" ]]; then
    printf '%s\0' --skip-private-audit
  fi
  if [[ "$CROSS_ARB_EXECUTE" == "true" ]]; then
    printf '%s\0' --execute
  fi
}

find_pid_by_pattern() {
  local pattern="$1"
  [[ -n "$pattern" ]] || return 1
  pgrep -f -- "$pattern" 2>/dev/null | while read -r pid; do
    [[ "$pid" == "$$" || "$pid" == "$BASHPID" ]] && continue
    printf '%s\n' "$pid"
  done | tail -n 1
}

find_pids_by_pattern() {
  local pattern="$1"
  [[ -n "$pattern" ]] || return 1
  pgrep -f -- "$pattern" 2>/dev/null | while read -r pid; do
    [[ "$pid" == "$$" || "$pid" == "$BASHPID" ]] && continue
    printf '%s\n' "$pid"
  done
}

wait_for_process() {
  local pattern="$1"
  local pid
  for _ in {1..20}; do
    pid="$(find_pid_by_pattern "$pattern" || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      printf '%s' "$pid"
      return 0
    fi
    sleep 0.25
  done
  return 1
}

read_saved_token() {
  local token_file="$RUN_DIR/local_control_api_token.txt"
  [[ -f "$token_file" ]] || return 0
  local token
  token="$(sed -n "s/^${CONTROL_API_TOKEN_ENV}=//p" "$token_file" | tail -n 1)"
  if [[ -z "$token" ]]; then
    token="$(head -n 1 "$token_file" || true)"
  fi
  printf '%s' "$token"
}

ensure_token() {
  if [[ -n "${!CONTROL_API_TOKEN_ENV:-}" ]]; then
    return 0
  fi
  mkdir -p "$RUN_DIR"
  local token
  token="$(read_saved_token)"
  if [[ -z "$token" ]]; then
    token="$(openssl rand -hex 32)"
  fi
  export "$CONTROL_API_TOKEN_ENV=$token"
}

build() {
  log "building strategy runtime and control-api binaries"
  cargo build --release --bin rustcta --bin cross_arb_live --bin control_api
  log "building Dioxus web UI"
  if ! command -v dx >/dev/null 2>&1; then
    printf 'missing dx; install dioxus-cli or build %s manually\n' "$DIOXUS_DIR" >&2
    exit 3
  fi
  (cd "$DIOXUS_DIR" && dx build --release)
  if [[ ! -f "$DIOXUS_BUILD_PUBLIC/index.html" ]]; then
    printf 'Dioxus build output not found at %s\n' "$DIOXUS_BUILD_PUBLIC" >&2
    printf 'set DIOXUS_BUILD_PUBLIC to the dx web/public output directory if it changed\n' >&2
    exit 6
  fi
  log "syncing Dioxus static assets to $DIOXUS_STATIC_DIR"
  rm -rf "$DIOXUS_STATIC_DIR"
  mkdir -p "$DIOXUS_STATIC_DIR"
  cp -a "$DIOXUS_BUILD_PUBLIC"/. "$DIOXUS_STATIC_DIR"/
  mkdir -p "$DIOXUS_STATIC_DIR/assets"
  cp "$DIOXUS_DIR/assets/main.css" "$DIOXUS_STATIC_DIR/assets/main.css"
  perl -0pi -e 's#<title>.*?</title>#<title>RustCTA Control Panel</title>#s' "$DIOXUS_STATIC_DIR/index.html"
}

write_local_token_hint() {
  local token="${!CONTROL_API_TOKEN_ENV:-}"
  [[ -n "$token" ]] || return 0
  printf '%s\n' "$token" > "$RUN_DIR/local_control_api_token.txt"
  chmod 600 "$RUN_DIR/local_control_api_token.txt"
}

stop_pid() {
  local name="$1"
  local pid_file="$RUN_DIR/$name.pid"
  local pid
  local pids=()
  local pidfile_pids=()
  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file" || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      pids+=("$pid")
      pidfile_pids+=("$pid")
    fi
  else
    pid=""
  fi
  if [[ "$name" != "strategy" || -f "$pid_file" ]]; then
    while IFS= read -r pattern; do
      [[ -n "$pattern" ]] || continue
      while IFS= read -r pid; do
        [[ -n "$pid" ]] || continue
        pids+=("$pid")
      done < <(find_pids_by_pattern "$pattern" || true)
    done < <(process_patterns_for_name "$name" || true)
  fi
  if ((${#pids[@]} == 0)); then
    rm -f "$pid_file"
    return 0
  fi
  mapfile -t pids < <(printf '%s\n' "${pids[@]}" | awk '!seen[$0]++')
  for pid in "${pids[@]}"; do
    [[ -n "$pid" ]] || continue
    kill -0 "$pid" 2>/dev/null || continue
    log "stopping $name pid=$pid"
    if printf '%s\n' "${pidfile_pids[@]}" | grep -Fxq "$pid"; then
      local pgid
      pgid="$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d '[:space:]' || true)"
      if [[ -n "$pgid" && "$pgid" != "$$" ]]; then
        kill -- "-$pgid" 2>/dev/null || true
      fi
    fi
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${pids[@]}"; do
    [[ -n "$pid" ]] || continue
    for _ in {1..30}; do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.5
    done
    if kill -0 "$pid" 2>/dev/null; then
      printf '%s still running after TERM pid=%s\n' "$name" "$pid" >&2
      exit 4
    fi
  done
  rm -f "$pid_file"
}

start_strategy() {
  ensure_token
  mkdir -p "$RUN_DIR" "$LOG_DIR" "$(dirname "$SNAPSHOT_PATH")"
  write_local_token_hint
  stop_pid strategy
  if is_cross_arb_config; then
    local mode dry_run
    mode="$(cross_arb_config_mode)"
    dry_run="$(cross_arb_config_dry_run)"
    if [[ "$mode" != "live_small" && "$mode" != "livesmall" ]]; then
      log "cross-arb config mode=$mode is not live_small; strategy runtime will remain stopped after config save"
      rm -f "$RUN_DIR/strategy.pid" "$RUN_DIR/strategy.log_path"
      return 0
    fi
    if [[ "$dry_run" == "false" && "$CROSS_ARB_EXECUTE" != "true" ]]; then
      printf 'cross-arb execution.dry_run=false requires CROSS_ARB_EXECUTE=true\n' >&2
      return 2
    fi
  fi
  local launch_args=()
  mapfile -d '' -t launch_args < <(strategy_launch_args)
  if ((${#launch_args[@]} == 0)); then
    printf 'strategy launch command is empty\n' >&2
    return 2
  fi
  local log_file="$LOG_DIR/strategy_$(date -u +%Y%m%dT%H%M%SZ).log"
  log "starting strategy runtime; log=$log_file command=${launch_args[*]}"
  local pid
  pid="$(
    set -a
    if [[ -f "$STRATEGY_ENV_FILE" ]]; then
      # shellcheck disable=SC1090
      . "$STRATEGY_ENV_FILE"
    fi
    if [[ -f "$EXCHANGE_API_KEY_STORE" ]]; then
      # shellcheck disable=SC1090
      . "$EXCHANGE_API_KEY_STORE"
    fi
    export RUSTCTA_EXCHANGE_API_KEY_STORE="$EXCHANGE_API_KEY_STORE"
    set +a
    if [[ -z "${GATEIO_API_KEY:-}" && -n "${GATE_API_KEY:-}" ]]; then
      export GATEIO_API_KEY="$GATE_API_KEY"
    fi
    if [[ -z "${GATEIO_API_SECRET:-}" && -n "${GATE_API_SECRET:-}" ]]; then
      export GATEIO_API_SECRET="$GATE_API_SECRET"
    fi
    if [[ -z "${GATE_API_KEY:-}" && -n "${GATEIO_API_KEY:-}" ]]; then
      export GATE_API_KEY="$GATEIO_API_KEY"
    fi
    if [[ -z "${GATE_API_SECRET:-}" && -n "${GATEIO_API_SECRET:-}" ]]; then
      export GATE_API_SECRET="$GATEIO_API_SECRET"
    fi
    if [[ -z "${BITGET_API_PASSPHRASE:-}" && -n "${BITGET_PASSPHRASE:-}" ]]; then
      export BITGET_API_PASSPHRASE="$BITGET_PASSPHRASE"
    fi
    if [[ -z "${BITGET_PASSPHRASE:-}" && -n "${BITGET_API_PASSPHRASE:-}" ]]; then
      export BITGET_PASSPHRASE="$BITGET_API_PASSPHRASE"
    fi
    export "$CONTROL_API_TOKEN_ENV=${!CONTROL_API_TOKEN_ENV}"
    run_detached "$log_file" env \
      RUST_LOG="${RUST_LOG:-info}" \
      "${launch_args[@]}"
  )"
  [[ -n "$pid" ]] && printf '%s\n' "$pid" > "$RUN_DIR/strategy.pid"
  echo "$log_file" > "$RUN_DIR/strategy.log_path"
}

start_control_api() {
  ensure_token
  mkdir -p "$RUN_DIR" "$LOG_DIR" "$(dirname "$COMMAND_PATH")"
  write_local_token_hint
  stop_pid control_api
  local log_file="$LOG_DIR/control_api_$(date -u +%Y%m%dT%H%M%SZ).log"
  log "starting control-api; log=$log_file"
  local args=(
    --bind-addr "$CONTROL_API_BIND_ADDR"
    --snapshot-path "$SNAPSHOT_PATH"
    --command-path "$COMMAND_PATH"
    --token-env "$CONTROL_API_TOKEN_ENV"
    --static-dir "$DIOXUS_STATIC_DIR"
    --exchange-api-key-store "$EXCHANGE_API_KEY_STORE"
    --strategy-config "$STRATEGY_CONFIG"
    --restart-script "$ROOT_DIR/scripts/separated_control_panel.sh"
  )
  if [[ "$CONTROL_API_EXTERNAL_ACCESS_ENABLED" == "true" ]]; then
    args+=(--external-access-enabled)
  fi
  run_detached "$log_file" env -i \
    PATH="$PATH" \
    RUST_LOG="${RUST_LOG:-info}" \
    "$CONTROL_API_TOKEN_ENV=${!CONTROL_API_TOKEN_ENV}" \
    "$CONTROL_API_BIN" "${args[@]}" >/dev/null
  local pid
  pid="$(wait_for_process "$(process_pattern_for_name control_api)" || true)"
  [[ -n "$pid" ]] && printf '%s\n' "$pid" > "$RUN_DIR/control_api.pid"
  echo "$log_file" > "$RUN_DIR/control_api.log_path"
}

status_one() {
  local name="$1"
  local pid_file="$RUN_DIR/$name.pid"
  local pattern
  pattern="$(process_pattern_for_name "$name" || true)"
  if [[ ! -f "$pid_file" ]]; then
    if [[ "$name" == "strategy" ]]; then
      printf '%s: stopped\n' "$name"
      return
    fi
    local detected_pid
    detected_pid="$(find_pid_by_pattern "$pattern" || true)"
    if [[ -n "$detected_pid" ]] && kill -0 "$detected_pid" 2>/dev/null; then
      printf '%s\n' "$detected_pid" > "$pid_file"
      ps -o pid,ppid,stat,etime,cmd -p "$detected_pid"
      return
    fi
    printf '%s: stopped\n' "$name"
    return
  fi
  local pid
  pid="$(cat "$pid_file" || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    ps -o pid,ppid,stat,etime,cmd -p "$pid"
  else
    if [[ "$name" == "strategy" ]]; then
      printf '%s: stale pid file (%s)\n' "$name" "$pid"
      return
    fi
    local detected_pid
    detected_pid="$(find_pid_by_pattern "$pattern" || true)"
    if [[ -n "$detected_pid" ]] && kill -0 "$detected_pid" 2>/dev/null; then
      printf '%s\n' "$detected_pid" > "$pid_file"
      ps -o pid,ppid,stat,etime,cmd -p "$detected_pid"
    else
      printf '%s: stale pid file (%s)\n' "$name" "$pid"
    fi
  fi
}

show_log() {
  local name="$1"
  local lines="${2:-120}"
  local path_file="$RUN_DIR/$name.log_path"
  if [[ ! -f "$path_file" ]]; then
    printf '%s log path is missing\n' "$name" >&2
    exit 5
  fi
  tail -n "$lines" "$(cat "$path_file")"
}

case "${1:-}" in
  build)
    build
    ;;
  start-strategy)
    start_strategy
    ;;
  start-control-api)
    start_control_api
    ;;
  start)
    start_strategy
    start_control_api
    log "control API: http://$CONTROL_API_BIND_ADDR"
    ;;
  stop-strategy)
    stop_pid strategy
    ;;
  stop-control-api)
    stop_pid control_api
    ;;
  stop)
    stop_pid control_api
    stop_pid strategy
    ;;
  restart-strategy)
    stop_pid strategy
    start_strategy
    status_one strategy
    ;;
  restart-control-api)
    stop_pid control_api
    start_control_api
    ;;
  restart)
    stop_pid control_api
    stop_pid strategy
    start_strategy
    start_control_api
    ;;
  status)
    status_one strategy
    status_one control_api
    ;;
  logs-strategy)
    show_log strategy "${2:-120}"
    ;;
  logs-control-api)
    show_log control_api "${2:-120}"
    ;;
  ""|-h|--help|help)
    usage
    ;;
  *)
    printf 'unknown command: %s\n' "$1" >&2
    usage >&2
    exit 2
    ;;
esac
