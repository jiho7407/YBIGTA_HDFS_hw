#!/usr/bin/env bash
set -euo pipefail

# Learning objective:
# Verify that a larger HDFS write/read operation eventually succeeds while the
# client is forced through an Active NameNode failover window.

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCENARIO_DIR}/../.." && pwd)"

# shellcheck source=../../scripts/lib/common.sh
source "${ROOT_DIR}/scripts/lib/common.sh"
# shellcheck source=../../scripts/lib/compose.sh
source "${ROOT_DIR}/scripts/lib/compose.sh"
# shellcheck source=../../validators/common.sh
source "${ROOT_DIR}/validators/common.sh"

SCENARIO_ID="08-large-write-failover"
STOPPED_NN=""
COMMON_VALIDATION_FILE=""
LARGE_PROBE_FILE=""
CLEANED_UP=false
LARGE_WRITE_LINES="${LARGE_WRITE_LINES:-32768}"

usage() {
  cat <<'USAGE'
Usage: scenarios/08-large-write-failover/run.sh

Stops the current Active NameNode, then retries a larger deterministic HDFS
write/read probe until the client succeeds through failover. The stopped
NameNode is restarted before the common validator runs. Logs go to stderr.
JSON goes to stdout.
USAGE
}

active_namenode() {
  local nn state found
  found=""

  for nn in nn1 nn2; do
    state="$(ha_state "${nn}" || true)"
    if [[ "${state}" == "active" ]]; then
      if [[ -n "${found}" ]]; then
        return 1
      fi
      found="${nn}"
    fi
  done

  if [[ -z "${found}" ]]; then
    return 1
  fi

  printf '%s\n' "${found}"
}

json_number_field() {
  local file field
  file="$1"
  field="$2"

  grep -o "\"${field}\":[0-9]*" "${file}" | head -1 | cut -d: -f2
}

json_string_field() {
  local file field
  file="$1"
  field="$2"

  grep -o "\"${field}\":\"[^\"]*\"" "${file}" | head -1 | cut -d: -f2- | sed 's/^"//; s/"$//'
}

wait_for_large_write() {
  local retries label
  retries="${1:-45}"

  for attempt in $(seq 1 "${retries}"); do
    label="large-failover-attempt-${attempt}"
    if "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label "${label}" --lines "${LARGE_WRITE_LINES}" > "${LARGE_PROBE_FILE}"; then
      return 0
    fi
    sleep 1
  done

  return 1
}

cleanup() {
  if [[ "${CLEANED_UP}" == "true" ]]; then
    return 0
  fi
  CLEANED_UP=true

  if [[ -n "${STOPPED_NN}" ]]; then
    log_info "cleanup: starting ${STOPPED_NN}"
    compose up -d "${STOPPED_NN}" >/dev/null
    for _ in $(seq 1 30); do
      if ha_state "${STOPPED_NN}" >/dev/null 2>&1; then
        return 0
      fi
      sleep 2
    done
    log_error "cleanup: ${STOPPED_NN} did not rejoin HA state checks"
    return 1
  fi
}

emit_json() {
  local passed active_before active_after stopped_nn failure_ms recovered_ms recovery_time_ms
  local large_write_duration_ms large_write_bytes expected_hash actual_hash common_passed common_json probe_json
  passed="$1"
  active_before="$2"
  active_after="$3"
  stopped_nn="$4"
  failure_ms="$5"
  recovered_ms="$6"
  recovery_time_ms="$7"
  large_write_duration_ms="$8"
  large_write_bytes="$9"
  expected_hash="${10}"
  actual_hash="${11}"
  common_passed="${12}"

  common_json="$(cat "${COMMON_VALIDATION_FILE}" 2>/dev/null || printf 'null')"
  probe_json="$(cat "${LARGE_PROBE_FILE}" 2>/dev/null || printf 'null')"

  printf '{'
  printf '"schema_version":1,'
  printf '"id":"%s",' "${SCENARIO_ID}"
  printf '"name":"Large write/read during failover",'
  printf '"passed":%s,' "${passed}"
  printf '"active_before":"%s",' "$(json_escape "${active_before}")"
  printf '"active_after":"%s",' "$(json_escape "${active_after}")"
  printf '"stopped_namenode":"%s",' "$(json_escape "${stopped_nn}")"
  printf '"failure_injected_ms":%s,' "${failure_ms}"
  printf '"recovered_ms":%s,' "${recovered_ms}"
  printf '"recovery_time_ms":%s,' "${recovery_time_ms}"
  printf '"large_write_lines":%s,' "${LARGE_WRITE_LINES}"
  printf '"large_write_duration_ms":%s,' "${large_write_duration_ms}"
  printf '"large_write_bytes":%s,' "${large_write_bytes}"
  printf '"write_latency_after_recovery_ms":%s,' "${large_write_duration_ms}"
  printf '"expected_sha256":"%s",' "$(json_escape "${expected_hash}")"
  printf '"actual_sha256":"%s",' "$(json_escape "${actual_hash}")"
  printf '"hash_matched":%s,' "$([[ -n "${expected_hash}" && "${expected_hash}" == "${actual_hash}" ]] && printf 'true' || printf 'false')"
  printf '"false_positive_failover_count":0,'
  printf '"common_validator_passed":%s,' "${common_passed}"
  printf '"large_probe":%s,' "${probe_json}"
  printf '"common_validator":%s' "${common_json}"
  printf '}\n'
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  COMMON_VALIDATION_FILE="$(mktemp)"
  LARGE_PROBE_FILE="$(mktemp)"
  trap 'cleanup; rm -f "${COMMON_VALIDATION_FILE:-}" "${LARGE_PROBE_FILE:-}"' EXIT

  local active_before active_after active_count_before active_count_after failure_ms recovered_ms
  local recovery_time_ms large_write_duration_ms large_write_bytes expected_hash actual_hash
  local scenario_recovered cleanup_passed common_passed passed

  log_info "scenario ${SCENARIO_ID}: running pre-flight HA sanity check"
  active_count_before="$(active_namenode_count || printf '0')"
  if [[ "${active_count_before}" != "1" ]]; then
    log_error "expected exactly one Active NameNode before scenario, got ${active_count_before}"
    exit 1
  fi
  active_before="$(active_namenode)"

  log_info "scenario ${SCENARIO_ID}: verifying baseline write path"
  "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label preflight --lines 128 >/dev/null

  STOPPED_NN="${active_before}"
  log_info "scenario ${SCENARIO_ID}: stopping Active NameNode ${STOPPED_NN}"
  compose stop -t 10 "${STOPPED_NN}" >/dev/null
  failure_ms="$(now_ms)"

  scenario_recovered=false
  recovered_ms=0
  recovery_time_ms=0
  large_write_duration_ms=null
  large_write_bytes=null
  expected_hash=""
  actual_hash=""

  if wait_for_large_write; then
    scenario_recovered=true
    recovered_ms="$(now_ms)"
    recovery_time_ms="$((recovered_ms - failure_ms))"
    large_write_duration_ms="$(json_number_field "${LARGE_PROBE_FILE}" duration_ms || printf 'null')"
    large_write_bytes="$(json_number_field "${LARGE_PROBE_FILE}" bytes || printf 'null')"
    expected_hash="$(json_string_field "${LARGE_PROBE_FILE}" expected_sha256 || printf '')"
    actual_hash="$(json_string_field "${LARGE_PROBE_FILE}" actual_sha256 || printf '')"
  else
    log_error "scenario ${SCENARIO_ID}: large HDFS write/read did not recover"
  fi

  active_count_after="$(active_namenode_count || printf '0')"
  active_after="$(active_namenode || printf '')"
  if [[ "${active_count_after}" != "1" ]]; then
    scenario_recovered=false
    log_error "expected exactly one Active NameNode after failover, got ${active_count_after}"
  fi

  cleanup_passed=true
  if ! cleanup; then
    cleanup_passed=false
    scenario_recovered=false
  fi

  common_passed=false
  if "${ROOT_DIR}/validators/validate_common.sh" --scenario "${SCENARIO_ID}" > "${COMMON_VALIDATION_FILE}"; then
    common_passed=true
  fi

  passed=false
  if [[ "${scenario_recovered}" == "true" && "${cleanup_passed}" == "true" && "${common_passed}" == "true" && -n "${expected_hash}" && "${expected_hash}" == "${actual_hash}" ]]; then
    passed=true
  fi

  emit_json "${passed}" "${active_before}" "${active_after}" "${STOPPED_NN}" "${failure_ms}" "${recovered_ms}" "${recovery_time_ms}" "${large_write_duration_ms}" "${large_write_bytes}" "${expected_hash}" "${actual_hash}" "${common_passed}"

  if [[ "${passed}" != "true" ]]; then
    exit 1
  fi
}

main "$@"
