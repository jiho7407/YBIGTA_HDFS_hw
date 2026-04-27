#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"
# shellcheck source=lib/compose.sh
source "${SCRIPT_DIR}/lib/compose.sh"

SCENARIO_ID="manual"
LABEL="probe"
LINES="1024"

usage() {
  cat <<'USAGE'
Usage: scripts/inject_data.sh [--scenario ID] [--label LABEL] [--lines N]

Writes deterministic probe data to HDFS, reads it back, and emits JSON metrics.
Logs go to stderr. JSON goes to stdout.
USAGE
}

sha256_file() {
  local file
  file="$1"

  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${file}" | awk '{print $1}'
  else
    shasum -a 256 "${file}" | awk '{print $1}'
  fi
}

json_escape() {
  local value
  value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  printf '%s' "${value}"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --scenario)
        SCENARIO_ID="$2"
        shift 2
        ;;
      --label)
        LABEL="$2"
        shift 2
        ;;
      --lines)
        LINES="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        log_error "unknown argument: $1"
        usage
        exit 2
        ;;
    esac
  done
}

generate_payload() {
  local output
  output="$1"

  {
    printf 'scenario=%s\n' "${SCENARIO_ID}"
    printf 'label=%s\n' "${LABEL}"
    printf 'format_version=1\n'
    for i in $(seq 1 "${LINES}"); do
      printf '%s,%s,%06d,hdfs-ha-homework\n' "${SCENARIO_ID}" "${LABEL}" "${i}"
    done
  } > "${output}"
}

main() {
  parse_args "$@"

  local tmp_in tmp_out path expected actual bytes started_ms ended_ms duration_ms
  tmp_in="$(mktemp)"
  tmp_out="$(mktemp)"
  trap 'rm -f "${tmp_in:-}" "${tmp_out:-}"' EXIT

  started_ms="$(now_ms)"
  path="/homework/probes/${SCENARIO_ID}/${LABEL}-$(date +%Y%m%dT%H%M%S)-$$.txt"

  generate_payload "${tmp_in}"
  expected="$(sha256_file "${tmp_in}")"
  bytes="$(wc -c < "${tmp_in}" | tr -d ' ')"

  log_info "writing HDFS probe data to ${path}"
  hdfs_client dfs -mkdir -p "$(dirname "${path}")"
  hdfs_client dfs -put -f - "${path}" < "${tmp_in}"

  log_info "reading HDFS probe data from ${path}"
  hdfs_client dfs -cat "${path}" > "${tmp_out}"
  actual="$(sha256_file "${tmp_out}")"

  ended_ms="$(now_ms)"
  duration_ms="$((ended_ms - started_ms))"

  if [[ "${expected}" != "${actual}" ]]; then
    log_error "hash mismatch for ${path}: expected=${expected} actual=${actual}"
    printf '{'
    printf '"passed":false,'
    printf '"path":"%s",' "$(json_escape "${path}")"
    printf '"expected_sha256":"%s",' "${expected}"
    printf '"actual_sha256":"%s",' "${actual}"
    printf '"bytes":%s,' "${bytes}"
    printf '"duration_ms":%s' "${duration_ms}"
    printf '}\n'
    exit 1
  fi

  printf '{'
  printf '"passed":true,'
  printf '"path":"%s",' "$(json_escape "${path}")"
  printf '"expected_sha256":"%s",' "${expected}"
  printf '"actual_sha256":"%s",' "${actual}"
  printf '"bytes":%s,' "${bytes}"
  printf '"duration_ms":%s' "${duration_ms}"
  printf '}\n'
}

main "$@"
