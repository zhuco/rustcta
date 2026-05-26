#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST="${URL_MANAGER_HOST:-127.0.0.1}"
PORT="${URL_MANAGER_PORT:-8090}"
DATA_FILE="${URL_MANAGER_DATA:-${ROOT_DIR}/config/url_manager.json}"
LOG_FILE="${URL_MANAGER_LOG:-${ROOT_DIR}/logs/url_manager.log}"
PID_FILE="${URL_MANAGER_PID:-${ROOT_DIR}/logs/url_manager.pid}"
RUST_LOG="${RUST_LOG:-info}"

mkdir -p "$(dirname "${LOG_FILE}")" "$(dirname "${PID_FILE}")"

cd "${ROOT_DIR}"
cargo build --bin url_manager

if [[ -f "${PID_FILE}" ]]; then
  old_pid="$(cat "${PID_FILE}")"
  if [[ -n "${old_pid}" ]] && kill -0 "${old_pid}" 2>/dev/null; then
    echo "url_manager is already running: ${old_pid}"
    exit 0
  fi
fi

RUST_LOG="${RUST_LOG}" setsid "${ROOT_DIR}/target/debug/url_manager" \
  --host "${HOST}" \
  --port "${PORT}" \
  --data "${DATA_FILE}" \
  > "${LOG_FILE}" 2>&1 &

pid="$!"
echo "${pid}" > "${PID_FILE}"
echo "url_manager started: pid=${pid}, url=http://${HOST}:${PORT}/"
