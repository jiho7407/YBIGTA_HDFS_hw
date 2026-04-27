#!/usr/bin/env bash
set -euo pipefail

# Learning objective:
# Verify that HDFS writes continue while the Standby NameNode is unavailable
# and that the Active NameNode does not fail over unnecessarily.

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCENARIO_DIR}/../.." && pwd)"

# shellcheck source=../../scripts/lib/common.sh
source "${ROOT_DIR}/scripts/lib/common.sh"
# shellcheck source=../../scripts/lib/compose.sh
source "${ROOT_DIR}/scripts/lib/compose.sh"
# shellcheck source=../../validators/common.sh
source "${ROOT_DIR}/validators/common.sh"

SCENARIO_ID="03-standby-failure"
STOPPED_NN=""
COMMON_VALIDATION_FILE=""
WRITE_PROBE_FILE=""
CLEANED_UP=false

usage() {
  cat <<'USAGE'
Usage: scenarios/03-standby-failure/run.sh

Stops the current Standby NameNode, verifies that the Active NameNode keeps
serving HDFS writes without an unnecessary failover, restarts the stopped
NameNode, then runs the common validator. Logs go to stderr. JSON goes to stdout.
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

standby_namenode() {
  local nn state found
  found=""

  for nn in nn1 nn2; do
    state="$(ha_state "${nn}" || true)"
    if [[ "${state}" == "standby" ]]; then
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

run_writes_during_failure() {
  local writes label
  writes="${1:-3}"

  for attempt in $(seq 1 "${writes}"); do
    label="during-failure-${attempt}"
    if ! "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label "${label}" --lines 256 > "${WRITE_PROBE_FILE}"; then
      return 1
    fi
  done

  return 0
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
  local passed active_before active_during active_after stopped_nn failure_ms write_success_ms
  local write_continuity_time_ms write_latency_ms false_positive_count common_passed common_json
  passed="$1"
  active_before="$2"
  active_during="$3"
  active_after="$4"
  stopped_nn="$5"
  failure_ms="$6"
  write_success_ms="$7"
  write_continuity_time_ms="$8"
  write_latency_ms="$9"
  false_positive_count="${10}"
  common_passed="${11}"

  common_json="$(cat "${COMMON_VALIDATION_FILE}" 2>/dev/null || printf 'null')"

  printf '{'
  printf '"schema_version":1,'
  printf '"id":"%s",' "${SCENARIO_ID}"
  printf '"name":"Standby NameNode failure",'
  printf '"passed":%s,' "${passed}"
  printf '"active_before":"%s",' "$(json_escape "${active_before}")"
  printf '"active_during_failure":"%s",' "$(json_escape "${active_during}")"
  printf '"active_after_cleanup":"%s",' "$(json_escape "${active_after}")"
  printf '"stopped_namenode":"%s",' "$(json_escape "${stopped_nn}")"
  printf '"failure_injected_ms":%s,' "${failure_ms}"
  printf '"write_success_ms":%s,' "${write_success_ms}"
  printf '"recovery_time_ms":%s,' "${write_continuity_time_ms}"
  printf '"write_continuity_time_ms":%s,' "${write_continuity_time_ms}"
  printf '"write_latency_during_failure_ms":%s,' "${write_latency_ms}"
  printf '"false_positive_failover_count":%s,' "${false_positive_count}"
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
  WRITE_PROBE_FILE="$(mktemp)"
  trap 'cleanup; rm -f "${COMMON_VALIDATION_FILE:-}" "${WRITE_PROBE_FILE:-}"' EXIT

  local active_before standby_before active_count_before active_count_during active_during active_after
  local failure_ms write_success_ms write_continuity_time_ms write_latency_ms
  local writes_passed false_positive_count common_passed passed

  log_info "scenario ${SCENARIO_ID}: running pre-flight HA sanity check"
  active_count_before="$(active_namenode_count || printf '0')"
  if [[ "${active_count_before}" != "1" ]]; then
    log_error "expected exactly one Active NameNode before scenario, got ${active_count_before}"
    exit 1
  fi
  active_before="$(active_namenode)"
  standby_before="$(standby_namenode)"

  log_info "scenario ${SCENARIO_ID}: verifying baseline write path"
  "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label preflight --lines 128 >/dev/null

  STOPPED_NN="${standby_before}"
  log_info "scenario ${SCENARIO_ID}: stopping Standby NameNode ${STOPPED_NN}"
  compose stop -t 10 "${STOPPED_NN}" >/dev/null
  failure_ms="$(now_ms)"

  writes_passed=false
  write_success_ms=0
  write_continuity_time_ms=0
  write_latency_ms=null
  false_positive_count=0

  if run_writes_during_failure; then
    writes_passed=true
    write_success_ms="$(now_ms)"
    write_continuity_time_ms="$((write_success_ms - failure_ms))"
    write_latency_ms="$(grep -o '"duration_ms":[0-9]*' "${WRITE_PROBE_FILE}" | tail -1 | grep -o '[0-9]*' || printf 'null')"
  else
    log_error "scenario ${SCENARIO_ID}: HDFS write failed while Standby NameNode was unavailable"
  fi

  active_count_during="$(active_namenode_count || printf '0')"
  active_during="$(active_namenode || printf '')"
  if [[ "${active_count_during}" != "1" ]]; then
    writes_passed=false
    log_error "expected exactly one Active NameNode during Standby failure, got ${active_count_during}"
  fi

  if [[ "${active_during}" != "${active_before}" ]]; then
    false_positive_count=1
    writes_passed=false
    log_error "unexpected failover during Standby failure: before=${active_before} during=${active_during}"
  fi

  cleanup
  active_after="$(active_namenode || printf '')"

  common_passed=false
  if "${ROOT_DIR}/validators/validate_common.sh" --scenario "${SCENARIO_ID}" > "${COMMON_VALIDATION_FILE}"; then
    common_passed=true
  fi

  passed=false
  if [[ "${writes_passed}" == "true" && "${common_passed}" == "true" ]]; then
    passed=true
  fi

  emit_json "${passed}" "${active_before}" "${active_during}" "${active_after}" "${STOPPED_NN}" "${failure_ms}" "${write_success_ms}" "${write_continuity_time_ms}" "${write_latency_ms}" "${false_positive_count}" "${common_passed}"

  if [[ "${passed}" != "true" ]]; then
    exit 1
  fi
}

main "$@"
