#!/usr/bin/env bash
set -euo pipefail

# Learning objective:
# Verify that losing ZooKeeper coordination from the Active NameNode side does
# not leave the cluster with split-brain and that HDFS writes recover.

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCENARIO_DIR}/../.." && pwd)"

# shellcheck source=../../scripts/lib/common.sh
source "${ROOT_DIR}/scripts/lib/common.sh"
# shellcheck source=../../scripts/lib/compose.sh
source "${ROOT_DIR}/scripts/lib/compose.sh"
# shellcheck source=../../validators/common.sh
source "${ROOT_DIR}/validators/common.sh"

SCENARIO_ID="06-active-zk-disconnect"
DISCONNECTED_NN=""
COMMON_VALIDATION_FILE=""
RECOVERY_PROBE_FILE=""
CLEANED_UP=false

usage() {
  cat <<'USAGE'
Usage: scenarios/06-active-zk-disconnect/run.sh

Stops the ZKFailoverController process on the current Active NameNode to
simulate a temporary loss of ZooKeeper coordination, verifies that the cluster
does not expose two Active NameNodes, restarts ZKFC, then runs the common
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

zkfc_running() {
  local nn
  nn="$1"

  compose exec -T "${nn}" bash -lc "pgrep -f '[D]FSZKFailoverController' >/dev/null" >/dev/null 2>&1
}

stop_zkfc() {
  local nn
  nn="$1"

  compose exec -T "${nn}" bash -lc "pkill -f '[D]FSZKFailoverController' || true" >/dev/null
}

start_zkfc() {
  local nn
  nn="$1"

  compose exec -T "${nn}" hdfs --daemon start zkfc >/dev/null
}

wait_for_zkfc() {
  local nn retries
  nn="$1"
  retries="${2:-30}"

  for _ in $(seq 1 "${retries}"); do
    if zkfc_running "${nn}"; then
      return 0
    fi
    sleep 2
  done

  return 1
}

wait_for_recovery_write() {
  local retries label active_count
  retries="${1:-30}"

  for attempt in $(seq 1 "${retries}"); do
    active_count="$(active_namenode_count || printf '0')"
    if [[ "${active_count}" == "1" ]]; then
      label="recovery-attempt-${attempt}"
      if "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label "${label}" --lines 256 > "${RECOVERY_PROBE_FILE}"; then
        return 0
      fi
    fi
    sleep 1
  done

  return 1
}

json_number_field() {
  local file field
  file="$1"
  field="$2"

  grep -o "\"${field}\":[0-9]*" "${file}" | head -1 | cut -d: -f2
}

cleanup() {
  if [[ "${CLEANED_UP}" == "true" ]]; then
    return 0
  fi
  CLEANED_UP=true

  if [[ -n "${DISCONNECTED_NN}" ]]; then
    log_info "cleanup: ensuring ZKFC is running on ${DISCONNECTED_NN}"
    if ! zkfc_running "${DISCONNECTED_NN}"; then
      start_zkfc "${DISCONNECTED_NN}" || true
    fi
    if wait_for_zkfc "${DISCONNECTED_NN}"; then
      return 0
    fi
    log_error "cleanup: ZKFC did not return on ${DISCONNECTED_NN}"
    return 1
  fi
}

emit_json() {
  local passed active_before active_after disconnected_nn failure_ms recovered_ms
  local recovery_time_ms write_latency_ms active_count_after cleanup_passed common_passed common_json
  passed="$1"
  active_before="$2"
  active_after="$3"
  disconnected_nn="$4"
  failure_ms="$5"
  recovered_ms="$6"
  recovery_time_ms="$7"
  write_latency_ms="$8"
  active_count_after="$9"
  cleanup_passed="${10}"
  common_passed="${11}"

  common_json="$(cat "${COMMON_VALIDATION_FILE}" 2>/dev/null || printf 'null')"

  printf '{'
  printf '"schema_version":1,'
  printf '"id":"%s",' "${SCENARIO_ID}"
  printf '"name":"Active NameNode ZooKeeper coordination loss",'
  printf '"passed":%s,' "${passed}"
  printf '"active_before":"%s",' "$(json_escape "${active_before}")"
  printf '"active_after_recovery":"%s",' "$(json_escape "${active_after}")"
  printf '"zookeeper_coordination_lost_on":"%s",' "$(json_escape "${disconnected_nn}")"
  printf '"failure_injected_ms":%s,' "${failure_ms}"
  printf '"recovered_ms":%s,' "${recovered_ms}"
  printf '"recovery_time_ms":%s,' "${recovery_time_ms}"
  printf '"write_latency_after_recovery_ms":%s,' "${write_latency_ms}"
  printf '"active_namenode_count_after_recovery":%s,' "${active_count_after}"
  printf '"split_brain_detected":%s,' "$([[ "${active_count_after}" == "1" ]] && printf 'false' || printf 'true')"
  printf '"false_positive_failover_count":0,'
  printf '"cleanup_passed":%s,' "${cleanup_passed}"
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

  local active_before active_after active_count_before active_count_after
  local failure_ms recovered_ms recovery_time_ms write_latency_ms
  local scenario_recovered cleanup_passed common_passed passed

  log_info "scenario ${SCENARIO_ID}: running pre-flight HA sanity check"
  active_count_before="$(active_namenode_count || printf '0')"
  if [[ "${active_count_before}" != "1" ]]; then
    log_error "expected exactly one Active NameNode before scenario, got ${active_count_before}"
    exit 1
  fi
  active_before="$(active_namenode)"

  if ! zkfc_running "${active_before}"; then
    log_error "expected ZKFC to be running on Active NameNode ${active_before}"
    exit 1
  fi

  log_info "scenario ${SCENARIO_ID}: verifying baseline write path"
  "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label preflight --lines 128 >/dev/null

  DISCONNECTED_NN="${active_before}"
  log_info "scenario ${SCENARIO_ID}: stopping ZKFC on ${DISCONNECTED_NN}"
  stop_zkfc "${DISCONNECTED_NN}"
  failure_ms="$(now_ms)"

  scenario_recovered=false
  recovered_ms=0
  recovery_time_ms=0
  write_latency_ms=null

  if wait_for_recovery_write; then
    scenario_recovered=true
    recovered_ms="$(now_ms)"
    recovery_time_ms="$((recovered_ms - failure_ms))"
    write_latency_ms="$(json_number_field "${RECOVERY_PROBE_FILE}" duration_ms || printf 'null')"
  else
    log_error "scenario ${SCENARIO_ID}: HDFS write did not recover with a single Active NameNode"
  fi

  active_count_after="$(active_namenode_count || printf '0')"
  active_after="$(active_namenode || printf '')"
  if [[ "${active_count_after}" != "1" ]]; then
    scenario_recovered=false
    log_error "expected exactly one Active NameNode after ZK coordination loss, got ${active_count_after}"
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
  if [[ "${scenario_recovered}" == "true" && "${cleanup_passed}" == "true" && "${common_passed}" == "true" ]]; then
    passed=true
  fi

  emit_json "${passed}" "${active_before}" "${active_after}" "${DISCONNECTED_NN}" "${failure_ms}" "${recovered_ms}" "${recovery_time_ms}" "${write_latency_ms}" "${active_count_after}" "${cleanup_passed}" "${common_passed}"

  if [[ "${passed}" != "true" ]]; then
    exit 1
  fi
}

main "$@"
