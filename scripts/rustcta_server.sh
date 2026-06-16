#!/usr/bin/env bash
set -euo pipefail

SSH_TARGET="${RUSTCTA_SSH_TARGET:-cta@45.77.253.180}"
REMOTE_ROOT="${RUSTCTA_REMOTE_ROOT:-/home/cta/rustcta}"
REMOTE_BIN_DIR="${RUSTCTA_REMOTE_BIN_DIR:-$REMOTE_ROOT/target/release}"
REMOTE_LOG_DIR="${RUSTCTA_REMOTE_LOG_DIR:-$REMOTE_ROOT/logs}"
LOCAL_LOG_DIR="${RUSTCTA_LOCAL_LOG_DIR:-logs/server}"
SERVICE_PREFIX="${RUSTCTA_SERVICE_PREFIX:-}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=8)
DEFAULT_LIVE_SERVICE="${RUSTCTA_LIVE_SERVICE:-unified-arb-live}"
DEFAULT_LIVE_BIN="${RUSTCTA_LIVE_BIN:-unified-arbitrage-runtime}"
DEFAULT_LIVE_CONFIG="${RUSTCTA_LIVE_CONFIG:-config/unified_arbitrage_usdt.yml}"
DEFAULT_MONITOR_SERVICE="${RUSTCTA_MONITOR_SERVICE:-control-api}"
DEFAULT_MONITOR_BIN="${RUSTCTA_MONITOR_BIN:-rustcta-control-api}"
DEFAULT_MONITOR_REMOTE_BIN_DIR="${RUSTCTA_MONITOR_REMOTE_BIN_DIR:-$REMOTE_BIN_DIR}"
DEFAULT_GATEWAY_SERVICE="${RUSTCTA_GATEWAY_SERVICE:-rustcta-gateway}"
DEFAULT_GATEWAY_BIN="${RUSTCTA_GATEWAY_BIN:-rustcta-gateway}"
DEFAULT_GATEWAY_REMOTE_BIN_DIR="${RUSTCTA_GATEWAY_REMOTE_BIN_DIR:-$REMOTE_BIN_DIR}"
DEFAULT_WEB_DIR="${RUSTCTA_WEB_DIR:-web-ui/dioxus}"
DEFAULT_WEB_STATIC_DIR="${RUSTCTA_WEB_STATIC_DIR:-web-ui/dioxus/dist}"
DEFAULT_WEB_REMOTE_STATIC_DIR="${RUSTCTA_WEB_REMOTE_STATIC_DIR:-$REMOTE_ROOT/web-ui/dioxus/dist}"
DEFAULT_SYSTEMD_USER_DIR="${RUSTCTA_SYSTEMD_USER_DIR:-config/systemd/user}"
DEFAULT_REMOTE_SYSTEMD_USER_DIR="${RUSTCTA_REMOTE_SYSTEMD_USER_DIR:-/home/cta/.config/systemd/user}"
OBSOLETE_SERVICES=(${RUSTCTA_OBSOLETE_SERVICES:-control-api-8091})

usage() {
  cat <<'USAGE'
Usage:
  scripts/rustcta_server.sh check
  scripts/rustcta_server.sh status <service>
  scripts/rustcta_server.sh logs <service> [lines]
  scripts/rustcta_server.sh follow <service>
  scripts/rustcta_server.sh start <service>
  scripts/rustcta_server.sh stop <service>
  scripts/rustcta_server.sh restart <service>
  scripts/rustcta_server.sh pull-logs
  scripts/rustcta_server.sh build <bin> [cargo args...]
  scripts/rustcta_server.sh build-web
  scripts/rustcta_server.sh deploy-web
  scripts/rustcta_server.sh deploy-bin <bin> [remote-name]
  scripts/rustcta_server.sh deploy-service <service> <bin> [cargo args...]
  scripts/rustcta_server.sh deploy-live [cargo args...]
  scripts/rustcta_server.sh deploy-live-config [local-config]
  scripts/rustcta_server.sh deploy-systemd-units
  scripts/rustcta_server.sh clean-stack
  scripts/rustcta_server.sh deploy-monitor [cargo args...]
  scripts/rustcta_server.sh deploy-control-panel [cargo args...]
  scripts/rustcta_server.sh deploy-unified-arb-live-stack [cargo args...]

Defaults:
  RUSTCTA_SSH_TARGET=cta@45.77.253.180
  RUSTCTA_REMOTE_ROOT=/home/cta/rustcta
  RUSTCTA_REMOTE_BIN_DIR=/home/cta/rustcta/target/release
  RUSTCTA_LIVE_SERVICE=unified-arb-live
  RUSTCTA_LIVE_BIN=unified-arbitrage-runtime
  RUSTCTA_LIVE_CONFIG=config/unified_arbitrage_usdt.yml
  RUSTCTA_MONITOR_SERVICE=control-api
  RUSTCTA_MONITOR_BIN=rustcta-control-api
  RUSTCTA_MONITOR_REMOTE_BIN_DIR=/home/cta/rustcta/target/release
  RUSTCTA_GATEWAY_SERVICE=rustcta-gateway
  RUSTCTA_GATEWAY_BIN=rustcta-gateway
  RUSTCTA_GATEWAY_REMOTE_BIN_DIR=/home/cta/rustcta/target/release
  RUSTCTA_WEB_DIR=web-ui/dioxus
  RUSTCTA_WEB_STATIC_DIR=web-ui/dioxus/dist
  RUSTCTA_WEB_REMOTE_STATIC_DIR=/home/cta/rustcta/web-ui/dioxus/dist
  RUSTCTA_SYSTEMD_USER_DIR=config/systemd/user
  RUSTCTA_REMOTE_SYSTEMD_USER_DIR=/home/cta/.config/systemd/user
  RUSTCTA_OBSOLETE_SERVICES="control-api-8091"

Examples:
  scripts/rustcta_server.sh check
  scripts/rustcta_server.sh logs unified-arb-live 200
  scripts/rustcta_server.sh follow unified-arb-live
  scripts/rustcta_server.sh build unified-arbitrage-runtime
  scripts/rustcta_server.sh deploy-bin unified-arbitrage-runtime
  scripts/rustcta_server.sh deploy-live
  scripts/rustcta_server.sh deploy-live-config
  scripts/rustcta_server.sh deploy-systemd-units
  scripts/rustcta_server.sh clean-stack
  scripts/rustcta_server.sh deploy-monitor
  scripts/rustcta_server.sh deploy-control-panel
  scripts/rustcta_server.sh deploy-unified-arb-live-stack
USAGE
}

service_name() {
  local service="$1"
  if [[ "$service" == *.service ]]; then
    printf '%s\n' "$service"
  elif [[ -n "$SERVICE_PREFIX" && "$service" == "$SERVICE_PREFIX-"* ]]; then
    printf '%s.service\n' "$service"
  elif [[ -n "$SERVICE_PREFIX" ]]; then
    printf '%s-%s.service\n' "$SERVICE_PREFIX" "$service"
  else
    printf '%s.service\n' "$service"
  fi
}

remote() {
  ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "$@"
}

local_cargo_target_dir() {
  cargo metadata --no-deps --format-version 1 \
    | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p'
}

local_release_bin_path() {
  local bin="$1"
  local target_dir
  target_dir="$(local_cargo_target_dir)"
  test -n "$target_dir"
  printf '%s/release/%s\n' "$target_dir" "$bin"
}

local_dioxus_build_public() {
  if [[ -n "${RUSTCTA_DIOXUS_BUILD_PUBLIC:-}" ]]; then
    printf '%s\n' "$RUSTCTA_DIOXUS_BUILD_PUBLIC"
    return 0
  fi

  local target_dir
  target_dir="$(local_cargo_target_dir)"
  test -n "$target_dir"
  printf '%s/dx/rustcta-control-panel/release/web/public\n' "$target_dir"
}

build_web_static() {
  local build_public
  build_public="$(local_dioxus_build_public)"

  rm -rf "$build_public" "$DEFAULT_WEB_STATIC_DIR"
  mkdir -p "$DEFAULT_WEB_STATIC_DIR"
  (cd "$DEFAULT_WEB_DIR" && dx build --release --debug-symbols false)
  test -f "$build_public/index.html"
  rsync -a --delete "$build_public/" "$DEFAULT_WEB_STATIC_DIR/"
  test -f "$DEFAULT_WEB_STATIC_DIR/index.html"
  echo "web_static=$DEFAULT_WEB_STATIC_DIR"
  find "$DEFAULT_WEB_STATIC_DIR" -maxdepth 2 -type f -printf '%p %s bytes\n' | sort
}

deploy_web_static() {
  build_web_static
  remote "mkdir -p '$DEFAULT_WEB_REMOTE_STATIC_DIR'"
  rsync -av --delete "$DEFAULT_WEB_STATIC_DIR/" "$SSH_TARGET:$DEFAULT_WEB_REMOTE_STATIC_DIR/"
  remote "find '$DEFAULT_WEB_REMOTE_STATIC_DIR' -maxdepth 2 -type f -printf '%p %s bytes\\n' | sort"
}

deploy_systemd_units() {
  test -f "$DEFAULT_SYSTEMD_USER_DIR/$(service_name "$DEFAULT_LIVE_SERVICE")"
  test -f "$DEFAULT_SYSTEMD_USER_DIR/$(service_name "$DEFAULT_MONITOR_SERVICE")"
  remote "mkdir -p '$DEFAULT_REMOTE_SYSTEMD_USER_DIR'"
  local units=(
    "$DEFAULT_SYSTEMD_USER_DIR/$(service_name "$DEFAULT_LIVE_SERVICE")"
    "$DEFAULT_SYSTEMD_USER_DIR/$(service_name "$DEFAULT_MONITOR_SERVICE")"
  )
  if [[ -f "$DEFAULT_SYSTEMD_USER_DIR/$(service_name "$DEFAULT_GATEWAY_SERVICE")" ]]; then
    units+=("$DEFAULT_SYSTEMD_USER_DIR/$(service_name "$DEFAULT_GATEWAY_SERVICE")")
  fi
  rsync -av "${units[@]}" "$SSH_TARGET:$DEFAULT_REMOTE_SYSTEMD_USER_DIR/"
  local enable_units=(
    "$(service_name "$DEFAULT_LIVE_SERVICE")"
    "$(service_name "$DEFAULT_MONITOR_SERVICE")"
  )
  if [[ -f "$DEFAULT_SYSTEMD_USER_DIR/$(service_name "$DEFAULT_GATEWAY_SERVICE")" ]]; then
    enable_units+=("$(service_name "$DEFAULT_GATEWAY_SERVICE")")
  fi
  remote "systemctl --user daemon-reload && systemctl --user enable ${enable_units[*]}"
}

remote_unit_exists() {
  local service="$1"
  remote "systemctl --user list-unit-files '$(service_name "$service")' --no-legend | grep -q '$(service_name "$service")'"
}

stop_remote_service() {
  local service="$1"
  remote "systemctl --user stop $(service_name "$service") 2>/dev/null || true"
}

disable_remote_service() {
  local service="$1"
  remote "systemctl --user disable $(service_name "$service") 2>/dev/null || true"
}

clean_live_stack() {
  for service in "$DEFAULT_MONITOR_SERVICE" "$DEFAULT_LIVE_SERVICE" "$DEFAULT_GATEWAY_SERVICE"; do
    stop_remote_service "$service"
  done
  for service in "${OBSOLETE_SERVICES[@]}"; do
    stop_remote_service "$service"
    disable_remote_service "$service"
  done
  remote "pkill -TERM -f '$REMOTE_BIN_DIR/($DEFAULT_LIVE_BIN|$DEFAULT_MONITOR_BIN|$DEFAULT_GATEWAY_BIN)( |$)' 2>/dev/null || true; sleep 1; pkill -KILL -f '$REMOTE_BIN_DIR/($DEFAULT_LIVE_BIN|$DEFAULT_MONITOR_BIN|$DEFAULT_GATEWAY_BIN)( |$)' 2>/dev/null || true"
}

deploy_binary_to_dir() {
  local bin="$1"
  local remote_name="${2:-$bin}"
  local remote_dir="${3:-$REMOTE_BIN_DIR}"
  local remote_path="$remote_dir/$remote_name"
  local upload_path="$remote_path.uploading"
  local local_path
  local_path="$(local_release_bin_path "$bin")"

  test -x "$local_path"
  remote "mkdir -p '$remote_dir'"
  rsync -av "$local_path" "$SSH_TARGET:$upload_path"
  remote "chmod 755 '$upload_path' && mv '$upload_path' '$remote_path' && ls -lh '$remote_path'"
}

deploy_binary() {
  local bin="$1"
  local remote_name="${2:-$bin}"

  deploy_binary_to_dir "$bin" "$remote_name" "$REMOTE_BIN_DIR"
}

deploy_service() {
  local service="$1"
  local bin="$2"
  shift 2

  cargo build --release --bin "$bin" "$@"
  deploy_binary "$bin" "$bin"
  remote "systemctl --user restart $(service_name "$service")"
  sleep 2
  remote "systemctl --user status $(service_name "$service") --no-pager -l"
  remote "journalctl --user -u $(service_name "$service") -n 80 --no-pager"
}

deploy_monitor() {
  deploy_systemd_units
  deploy_web_static
  cargo build --release --bin "$DEFAULT_MONITOR_BIN" "$@"
  deploy_binary_to_dir "$DEFAULT_MONITOR_BIN" "$DEFAULT_MONITOR_BIN" "$DEFAULT_MONITOR_REMOTE_BIN_DIR"
  remote "systemctl --user restart $(service_name "$DEFAULT_MONITOR_SERVICE")"
  sleep 2
  remote "systemctl --user status $(service_name "$DEFAULT_MONITOR_SERVICE") --no-pager -l"
  remote "journalctl --user -u $(service_name "$DEFAULT_MONITOR_SERVICE") -n 80 --no-pager"
}

deploy_control_panel() {
  deploy_monitor "$@"
}

deploy_live() {
  deploy_systemd_units
  cargo build --release --bin "$DEFAULT_LIVE_BIN" "$@"
  deploy_binary "$DEFAULT_LIVE_BIN" "$DEFAULT_LIVE_BIN"
  remote "systemctl --user restart $(service_name "$DEFAULT_LIVE_SERVICE")"
  sleep 2
  remote "systemctl --user status $(service_name "$DEFAULT_LIVE_SERVICE") --no-pager -l"
  remote "journalctl --user -u $(service_name "$DEFAULT_LIVE_SERVICE") -n 80 --no-pager"
}

deploy_live_config() {
  local local_config="${1:-$DEFAULT_LIVE_CONFIG}"
  local filename
  filename="$(basename "$local_config")"
  local remote_dir="$REMOTE_ROOT/config"
  local remote_path="$remote_dir/$filename"
  local upload_path="$remote_path.uploading"

  test -f "$local_config"
  remote "mkdir -p '$remote_dir'"
  rsync -av "$local_config" "$SSH_TARGET:$upload_path"
  remote "chmod 640 '$upload_path' && mv '$upload_path' '$remote_path' && ls -lh '$remote_path'"
  remote "systemctl --user stop $(service_name "$DEFAULT_LIVE_SERVICE") || true; systemctl --user disable $(service_name "$DEFAULT_LIVE_SERVICE") || true"
  remote "systemctl --user status $(service_name "$DEFAULT_LIVE_SERVICE") --no-pager -l || true"
}

deploy_live_stack() {
  clean_live_stack
  deploy_systemd_units
  deploy_web_static
  cargo build --release --bin "$DEFAULT_LIVE_BIN" --bin "$DEFAULT_MONITOR_BIN" --bin "$DEFAULT_GATEWAY_BIN" "$@"
  deploy_binary "$DEFAULT_LIVE_BIN" "$DEFAULT_LIVE_BIN"
  deploy_binary_to_dir "$DEFAULT_MONITOR_BIN" "$DEFAULT_MONITOR_BIN" "$DEFAULT_MONITOR_REMOTE_BIN_DIR"
  deploy_binary_to_dir "$DEFAULT_GATEWAY_BIN" "$DEFAULT_GATEWAY_BIN" "$DEFAULT_GATEWAY_REMOTE_BIN_DIR"

  local filename remote_dir remote_path upload_path
  filename="$(basename "$DEFAULT_LIVE_CONFIG")"
  remote_dir="$REMOTE_ROOT/config"
  remote_path="$remote_dir/$filename"
  upload_path="$remote_path.uploading"
  test -f "$DEFAULT_LIVE_CONFIG"
  remote "mkdir -p '$remote_dir'"
  rsync -av "$DEFAULT_LIVE_CONFIG" "$SSH_TARGET:$upload_path"
  remote "chmod 640 '$upload_path' && mv '$upload_path' '$remote_path' && ls -lh '$remote_path'"

  if remote_unit_exists "$DEFAULT_GATEWAY_SERVICE"; then
    remote "systemctl --user restart $(service_name "$DEFAULT_GATEWAY_SERVICE")"
  else
    echo "gateway_service=absent synced_binary_only=$DEFAULT_GATEWAY_BIN"
  fi
  remote "systemctl --user restart $(service_name "$DEFAULT_LIVE_SERVICE")"
  remote "systemctl --user restart $(service_name "$DEFAULT_MONITOR_SERVICE")"
  sleep 2
  if remote_unit_exists "$DEFAULT_GATEWAY_SERVICE"; then
    remote "systemctl --user status $(service_name "$DEFAULT_GATEWAY_SERVICE") --no-pager -l"
  fi
  remote "systemctl --user status $(service_name "$DEFAULT_LIVE_SERVICE") --no-pager -l"
  remote "systemctl --user status $(service_name "$DEFAULT_MONITOR_SERVICE") --no-pager -l"
  if remote_unit_exists "$DEFAULT_GATEWAY_SERVICE"; then
    remote "journalctl --user -u $(service_name "$DEFAULT_GATEWAY_SERVICE") -n 80 --no-pager"
  fi
  remote "journalctl --user -u $(service_name "$DEFAULT_LIVE_SERVICE") -n 80 --no-pager"
  remote "journalctl --user -u $(service_name "$DEFAULT_MONITOR_SERVICE") -n 80 --no-pager"
}

cmd="${1:-}"
shift || true

case "$cmd" in
  check)
    remote 'echo SSH_OK && whoami && hostname && pwd'
    ;;
  status)
    service="${1:?missing service}"
    remote "systemctl --user status $(service_name "$service") --no-pager"
    ;;
  logs)
    service="${1:?missing service}"
    lines="${2:-200}"
    remote "journalctl --user -u $(service_name "$service") -n $lines --no-pager"
    ;;
  follow)
    service="${1:?missing service}"
    ssh "$SSH_TARGET" "journalctl --user -u $(service_name "$service") -f"
    ;;
  start|stop|restart)
    service="${1:?missing service}"
    remote "systemctl --user $cmd $(service_name "$service")"
    ;;
  disable-live)
    remote "systemctl --user stop $(service_name "$DEFAULT_LIVE_SERVICE") || true; systemctl --user disable $(service_name "$DEFAULT_LIVE_SERVICE") || true; systemctl --user status $(service_name "$DEFAULT_LIVE_SERVICE") --no-pager -l || true"
    ;;
  pull-logs)
    mkdir -p "$LOCAL_LOG_DIR"
    rsync -av --mkpath "$SSH_TARGET:$REMOTE_LOG_DIR/" "$LOCAL_LOG_DIR/"
    ;;
  build)
    bin="${1:?missing bin}"
    shift
    cargo build --release --bin "$bin" "$@"
    ;;
  build-web)
    build_web_static
    ;;
  deploy-web)
    deploy_web_static
    ;;
  deploy-bin)
    bin="${1:?missing bin}"
    remote_name="${2:-$bin}"
    cargo build --release --bin "$bin"
    deploy_binary "$bin" "$remote_name"
    ;;
  deploy-service)
    service="${1:?missing service}"
    bin="${2:?missing bin}"
    shift 2
    deploy_service "$service" "$bin" "$@"
    ;;
  deploy-live)
    deploy_live "$@"
    ;;
  deploy-live-config)
    deploy_live_config "$@"
    ;;
  deploy-systemd-units)
    deploy_systemd_units
    ;;
  clean-stack)
    clean_live_stack
    ;;
  deploy-monitor)
    deploy_monitor "$@"
    ;;
  deploy-control-panel)
    deploy_control_panel "$@"
    ;;
  deploy-unified-arb-live-stack)
    deploy_live_stack "$@"
    ;;
  ""|-h|--help|help)
    usage
    ;;
  *)
    echo "unknown command: $cmd" >&2
    usage >&2
    exit 2
    ;;
esac
