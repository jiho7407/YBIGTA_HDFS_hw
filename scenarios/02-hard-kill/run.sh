#!/usr/bin/env bash
set -euo pipefail

# Learning objective:
# Verify that an abrupt SIGKILL of the Active NameNode still triggers automatic
# failover and that HDFS writes become available again without data loss.

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCENARIO_DIR}/../.." && pwd)"

# shellcheck source=../../scripts/lib/common.sh
source "${ROOT_DIR}/scripts/lib/common.sh"
# shellcheck source=../../scripts/lib/compose.sh
source "${ROOT_DIR}/scripts/lib/compose.sh"
# shellcheck source=../../validators/common.sh
source "${ROOT_DIR}/validators/common.sh"

SCENARIO_ID="02-hard-kill"
KILLED_NN=""
COMMON_VALIDATION_FILE=""
RECOVERY_PROBE_FILE=""
CLEANED_UP=false

usage() {
  cat <<'USAGE'
Usage: scenarios/02-hard-kill/run.sh

Kills the current Active NameNode with SIGKILL, waits for the Standby NameNode
to become writable, restarts the killed NameNode, then runs the common
validator. Logs go to stderr. JSON goes to stdout.
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

wait_for_recovery_write() {
  local retries label
  retries="${1:-30}"

  for attempt in $(seq 1 "${retries}"); do
    label="recovery-attempt-${attempt}"
    if "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label "${label}" --lines 256 > "${RECOVERY_PROBE_FILE}"; then
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

  if [[ -n "${KILLED_NN}" ]]; then
    log_info "cleanup: starting ${KILLED_NN}"
    compose up -d "${KILLED_NN}" >/dev/null
    for _ in $(seq 1 30); do
      if ha_state "${KILLED_NN}" >/dev/null 2>&1; then
        return 0
      fi
      sleep 2
    done
    log_error "cleanup: ${KILLED_NN} did not rejoin HA state checks"
    return 1
  fi
}

emit_json() {
  local passed active_before active_after killed_nn failure_ms recovered_ms recovery_time_ms
  local write_latency_ms common_passed common_json
  passed="$1"
  active_before="$2"
  active_after="$3"
  killed_nn="$4"
  failure_ms="$5"
  recovered_ms="$6"
  recovery_time_ms="$7"
  write_latency_ms="$8"
  common_passed="$9"

  common_json="$(cat "${COMMON_VALIDATION_FILE}" 2>/dev/null || printf 'null')"

  printf '{'
  printf '"schema_version":1,'
  printf '"id":"%s",' "${SCENARIO_ID}"
  printf '"name":"Active NameNode hard kill",'
  printf '"passed":%s,' "${passed}"
  printf '"active_before":"%s",' "$(json_escape "${active_before}")"
  printf '"active_after":"%s",' "$(json_escape "${active_after}")"
  printf '"killed_namenode":"%s",' "$(json_escape "${killed_nn}")"
  printf '"failure_injected_ms":%s,' "${failure_ms}"
  printf '"recovered_ms":%s,' "${recovered_ms}"
  printf '"recovery_time_ms":%s,' "${recovery_time_ms}"
  printf '"write_latency_after_recovery_ms":%s,' "${write_latency_ms}"
  printf '"false_positive_failover_count":0,'
  printf '"common_validator_passed":%s,' "${common_passed}"
  printf '"common_validator":%s' "${common_json}"
  printf '}\n'
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  COMMON_VALIDATION_FILE="$(mktemp)"
  RECOVERY_PROBE_FILE="$(mktemp)"
  trap 'cleanup; rm -f "${COMMON_VALIDATION_FILE:-}" "${RECOVERY_PROBE_FILE:-}"' EXIT

  local active_before active_count_before active_after active_count_after
  local failure_ms recovered_ms recovery_time_ms write_latency_ms
  local scenario_recovered common_passed passed

  log_info "scenario ${SCENARIO_ID}: running pre-flight HA sanity check"
  active_count_before="$(active_namenode_count || printf '0')"
  if [[ "${active_count_before}" != "1" ]]; then
    log_error "expected exactly one Active NameNode before scenario, got ${active_count_before}"
    exit 1
  fi
  active_before="$(active_namenode)"

  log_info "scenario ${SCENARIO_ID}: verifying baseline write path"
  "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label preflight --lines 128 >/dev/null

  KILLED_NN="${active_before}"
  log_info "scenario ${SCENARIO_ID}: killing Active NameNode ${KILLED_NN} with SIGKILL"
  compose kill -s SIGKILL "${KILLED_NN}" >/dev/null
  failure_ms="$(now_ms)"

  scenario_recovered=false
  recovered_ms=0
  recovery_time_ms=0
  write_latency_ms=null

  if wait_for_recovery_write; then
    scenario_recovered=true
    recovered_ms="$(now_ms)"
    recovery_time_ms="$((recovered_ms - failure_ms))"
    write_latency_ms="$(grep -o '"duration_ms":[0-9]*' "${RECOVERY_PROBE_FILE}" | tail -1 | grep -o '[0-9]*' || printf 'null')"
  else
    log_error "scenario ${SCENARIO_ID}: HDFS write did not recover"
  fi

  active_count_after="$(active_namenode_count || printf '0')"
  active_after="$(active_namenode || printf '')"
  if [[ "${active_count_after}" != "1" ]]; then
    scenario_recovered=false
    log_error "expected exactly one Active NameNode after failover, got ${active_count_after}"
  fi

  cleanup

  common_passed=false
  if "${ROOT_DIR}/validators/validate_common.sh" --scenario "${SCENARIO_ID}" > "${COMMON_VALIDATION_FILE}"; then
    common_passed=true
  fi

  passed=false
  if [[ "${scenario_recovered}" == "true" && "${common_passed}" == "true" ]]; then
    passed=true
  fi

  emit_json "${passed}" "${active_before}" "${active_after}" "${KILLED_NN}" "${failure_ms}" "${recovered_ms}" "${recovery_time_ms}" "${write_latency_ms}" "${common_passed}"

  if [[ "${passed}" != "true" ]]; then
    exit 1
  fi
}

main "$@"
