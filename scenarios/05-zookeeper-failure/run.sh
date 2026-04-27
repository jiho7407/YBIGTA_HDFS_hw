#!/usr/bin/env bash
set -euo pipefail

# Learning objective:
# Verify that HDFS HA remains stable while one ZooKeeper server is unavailable
# and the ZooKeeper ensemble still has quorum.

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCENARIO_DIR}/../.." && pwd)"

# shellcheck source=../../scripts/lib/common.sh
source "${ROOT_DIR}/scripts/lib/common.sh"
# shellcheck source=../../scripts/lib/compose.sh
source "${ROOT_DIR}/scripts/lib/compose.sh"
# shellcheck source=../../validators/common.sh
source "${ROOT_DIR}/validators/common.sh"

SCENARIO_ID="05-zookeeper-failure"
STOPPED_ZK=""
COMMON_VALIDATION_FILE=""
WRITE_PROBE_FILE=""
CLEANED_UP=false

usage() {
  cat <<'USAGE'
Usage: scenarios/05-zookeeper-failure/run.sh

Stops one ZooKeeper server, verifies that HA state remains stable and HDFS
writes continue through the remaining ZooKeeper quorum, restarts ZooKeeper, then
runs the common validator. Logs go to stderr. JSON goes to stdout.
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

service_running() {
  local service
  service="$1"

  [[ -n "$(compose ps --status running -q "${service}" 2>/dev/null)" ]]
}

running_count() {
  local count service
  count=0

  for service in "$@"; do
    if service_running "${service}"; then
      count="$((count + 1))"
    fi
  done

  printf '%s\n' "${count}"
}

choose_zookeeper() {
  local zk

  for zk in zk1 zk2 zk3; do
    if service_running "${zk}"; then
      printf '%s\n' "${zk}"
      return 0
    fi
  done

  return 1
}

json_number_field() {
  local file field
  file="$1"
  field="$2"

  grep -o "\"${field}\":[0-9]*" "${file}" | head -1 | cut -d: -f2
}

run_writes_during_failure() {
  local writes label
  writes="${1:-3}"

  for attempt in $(seq 1 "${writes}"); do
    label="during-zk-failure-${attempt}"
    if ! "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label "${label}" --lines 256 > "${WRITE_PROBE_FILE}"; then
      return 1
    fi
  done

  return 0
}

wait_for_service_running() {
  local service retries
  service="$1"
  retries="${2:-30}"

  for _ in $(seq 1 "${retries}"); do
    if service_running "${service}"; then
      return 0
    fi
    sleep 2
  done

  return 1
}

cleanup() {
  if [[ "${CLEANED_UP}" == "true" ]]; then
    return 0
  fi
  CLEANED_UP=true

  if [[ -n "${STOPPED_ZK}" ]]; then
    log_info "cleanup: starting ${STOPPED_ZK}"
    compose up -d "${STOPPED_ZK}" >/dev/null
    if wait_for_service_running "${STOPPED_ZK}"; then
      return 0
    fi
    log_error "cleanup: ${STOPPED_ZK} did not return to running state"
    return 1
  fi
}

emit_json() {
  local passed active_before active_during active_after stopped_zk failure_ms write_success_ms
  local recovery_time_ms write_latency_ms zk_count_before zk_count_during zk_count_after
  local false_positive_count common_passed common_json
  passed="$1"
  active_before="$2"
  active_during="$3"
  active_after="$4"
  stopped_zk="$5"
  failure_ms="$6"
  write_success_ms="$7"
  recovery_time_ms="$8"
  write_latency_ms="$9"
  zk_count_before="${10}"
  zk_count_during="${11}"
  zk_count_after="${12}"
  false_positive_count="${13}"
  common_passed="${14}"

  common_json="$(cat "${COMMON_VALIDATION_FILE}" 2>/dev/null || printf 'null')"

  printf '{'
  printf '"schema_version":1,'
  printf '"id":"%s",' "${SCENARIO_ID}"
  printf '"name":"ZooKeeper one-node failure",'
  printf '"passed":%s,' "${passed}"
  printf '"active_before":"%s",' "$(json_escape "${active_before}")"
  printf '"active_during_failure":"%s",' "$(json_escape "${active_during}")"
  printf '"active_after_cleanup":"%s",' "$(json_escape "${active_after}")"
  printf '"stopped_zookeeper":"%s",' "$(json_escape "${stopped_zk}")"
  printf '"failure_injected_ms":%s,' "${failure_ms}"
  printf '"write_success_ms":%s,' "${write_success_ms}"
  printf '"recovery_time_ms":%s,' "${recovery_time_ms}"
  printf '"write_continuity_time_ms":%s,' "${recovery_time_ms}"
  printf '"write_latency_during_failure_ms":%s,' "${write_latency_ms}"
  printf '"zookeepers_running_before":%s,' "${zk_count_before}"
  printf '"zookeepers_running_during_failure":%s,' "${zk_count_during}"
  printf '"zookeepers_running_after_cleanup":%s,' "${zk_count_after}"
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

  local active_before active_during active_after active_count_before active_count_during
  local zk_count_before zk_count_during zk_count_after failure_ms write_success_ms
  local recovery_time_ms write_latency_ms writes_passed cleanup_passed
  local false_positive_count common_passed passed

  log_info "scenario ${SCENARIO_ID}: running pre-flight HA sanity check"
  active_count_before="$(active_namenode_count || printf '0')"
  if [[ "${active_count_before}" != "1" ]]; then
    log_error "expected exactly one Active NameNode before scenario, got ${active_count_before}"
    exit 1
  fi
  active_before="$(active_namenode)"

  zk_count_before="$(running_count zk1 zk2 zk3)"
  if [[ "${zk_count_before}" != "3" ]]; then
    log_error "expected three running ZooKeeper servers before scenario, got ${zk_count_before}"
    exit 1
  fi

  log_info "scenario ${SCENARIO_ID}: verifying baseline write path"
  "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label preflight --lines 128 >/dev/null

  STOPPED_ZK="$(choose_zookeeper)"
  log_info "scenario ${SCENARIO_ID}: stopping ZooKeeper server ${STOPPED_ZK}"
  compose stop -t 10 "${STOPPED_ZK}" >/dev/null
  failure_ms="$(now_ms)"

  writes_passed=false
  write_success_ms=0
  recovery_time_ms=0
  write_latency_ms=null
  false_positive_count=0

  zk_count_during="$(running_count zk1 zk2 zk3)"
  if [[ "${zk_count_during}" != "2" ]]; then
    log_error "expected two running ZooKeeper servers during failure, got ${zk_count_during}"
  fi

  if run_writes_during_failure; then
    writes_passed=true
    write_success_ms="$(now_ms)"
    recovery_time_ms="$((write_success_ms - failure_ms))"
    write_latency_ms="$(json_number_field "${WRITE_PROBE_FILE}" duration_ms || printf 'null')"
  else
    log_error "scenario ${SCENARIO_ID}: HDFS write failed while one ZooKeeper server was unavailable"
  fi

  if [[ "${zk_count_during}" != "2" ]]; then
    writes_passed=false
  fi

  active_count_during="$(active_namenode_count || printf '0')"
  active_during="$(active_namenode || printf '')"
  if [[ "${active_count_during}" != "1" ]]; then
    writes_passed=false
    log_error "expected exactly one Active NameNode during ZooKeeper failure, got ${active_count_during}"
  fi

  if [[ "${active_during}" != "${active_before}" ]]; then
    false_positive_count=1
    writes_passed=false
    log_error "unexpected failover during ZooKeeper one-node failure: before=${active_before} during=${active_during}"
  fi

  cleanup_passed=true
  if ! cleanup; then
    cleanup_passed=false
    writes_passed=false
  fi
  zk_count_after="$(running_count zk1 zk2 zk3)"
  active_after="$(active_namenode || printf '')"

  common_passed=false
  if "${ROOT_DIR}/validators/validate_common.sh" --scenario "${SCENARIO_ID}" > "${COMMON_VALIDATION_FILE}"; then
    common_passed=true
  fi

  passed=false
  if [[ "${writes_passed}" == "true" && "${cleanup_passed}" == "true" && "${common_passed}" == "true" && "${zk_count_after}" == "3" ]]; then
    passed=true
  fi

  emit_json "${passed}" "${active_before}" "${active_during}" "${active_after}" "${STOPPED_ZK}" "${failure_ms}" "${write_success_ms}" "${recovery_time_ms}" "${write_latency_ms}" "${zk_count_before}" "${zk_count_during}" "${zk_count_after}" "${false_positive_count}" "${common_passed}"

  if [[ "${passed}" != "true" ]]; then
    exit 1
  fi
}

main "$@"
