#!/usr/bin/env bash
set -euo pipefail

timestamp() {
  date +"%Y-%m-%dT%H:%M:%S%z"
}

log_info() {
  printf '%s [INFO] %s\n' "$(timestamp)" "$*" >&2
}

log_warn() {
  printf '%s [WARN] %s\n' "$(timestamp)" "$*" >&2
}

log_error() {
  printf '%s [ERROR] %s\n' "$(timestamp)" "$*" >&2
}

now_ms() {
  local candidate
  candidate="$(date +%s%3N 2>/dev/null || true)"
  if [[ "${candidate}" =~ ^[0-9]+$ ]]; then
    printf '%s\n' "${candidate}"
    return 0
  fi

  if command -v python3 >/dev/null 2>&1; then
    python3 -c 'import time; print(int(time.time() * 1000))'
    return 0
  fi

  printf '%s000\n' "$(date +%s)"
}

repo_root() {
  local source_dir
  source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  printf '%s\n' "${source_dir}"
}
