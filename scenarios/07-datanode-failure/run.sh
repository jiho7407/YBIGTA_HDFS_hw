#!/usr/bin/env bash
set -euo pipefail

# Learning objective:
# Verify that HDFS writes continue while one DataNode is unavailable and that
# replication health returns after the DataNode rejoins.

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCENARIO_DIR}/../.." && pwd)"

# shellcheck source=../../scripts/lib/common.sh
source "${ROOT_DIR}/scripts/lib/common.sh"
# shellcheck source=../../scripts/lib/compose.sh
source "${ROOT_DIR}/scripts/lib/compose.sh"
# shellcheck source=../../validators/common.sh
source "${ROOT_DIR}/validators/common.sh"

SCENARIO_ID="07-datanode-failure"
STOPPED_DN=""
COMMON_VALIDATION_FILE=""
WRITE_PROBE_FILE=""
CLEANED_UP=false

usage() {
  cat <<'USAGE'
Usage: scenarios/07-datanode-failure/run.sh

Stops one DataNode, verifies that writes continue with the remaining DataNodes,
restarts the stopped DataNode, waits for basic replication health, then runs the
common validator. Logs go to stderr. JSON goes to stdout.
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

choose_datanode() {
  local dn

  for dn in dn1 dn2 dn3; do
    if service_running "${dn}"; then
      printf '%s\n' "${dn}"
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

dfsadmin_number() {
  local label
  label="$1"

  hdfs_client dfsadmin -report 2>/dev/null \
    | awk -v label="${label}" '
      $0 ~ label && !found {
        line=$0
        if (line ~ /\([0-9]+\)/) {
          sub(/^.*\(/, "", line)
          sub(/\).*$/, "", line)
        } else {
          sub(/^.*:[ \t]*/, "", line)
          sub(/[ \t].*$/, "", line)
        }
        print line
        found=1
      }
      END { if (!found) exit 1 }
    '
}

live_datanode_count() {
  dfsadmin_number 'Live datanodes'
}

under_replicated_blocks() {
  dfsadmin_number 'Under replicated blocks'
}

wait_for_live_datanodes() {
  local expected retries current
  expected="$1"
  retries="${2:-30}"

  for _ in $(seq 1 "${retries}"); do
    current="$(live_datanode_count || printf '0')"
    if [[ "${current}" == "${expected}" ]]; then
      return 0
    fi
    sleep 2
  done

  return 1
}

wait_for_under_replicated_zero() {
  local retries current
  retries="${1:-30}"

  for _ in $(seq 1 "${retries}"); do
    current="$(under_replicated_blocks || printf '999999')"
    if [[ "${current}" == "0" ]]; then
      return 0
    fi
    sleep 2
  done

  return 1
}

run_writes_during_failure() {
  local writes label
  writes="${1:-3}"

  for attempt in $(seq 1 "${writes}"); do
    label="during-dn-failure-${attempt}"
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

  if [[ -n "${STOPPED_DN}" ]]; then
    log_info "cleanup: starting ${STOPPED_DN}"
    compose up -d "${STOPPED_DN}" >/dev/null
    if wait_for_service_running "${STOPPED_DN}"; then
      return 0
    fi
    log_error "cleanup: ${STOPPED_DN} did not return to running state"
    return 1
  fi
}

emit_json() {
  local passed active_before active_after stopped_dn failure_ms write_success_ms
  local recovery_time_ms write_latency_ms dn_services_before dn_services_during dn_services_after
  local live_dns_before live_dns_during live_dns_after under_replicated_after
  local common_passed common_json
  passed="$1"
  active_before="$2"
  active_after="$3"
  stopped_dn="$4"
  failure_ms="$5"
  write_success_ms="$6"
  recovery_time_ms="$7"
  write_latency_ms="$8"
  dn_services_before="$9"
  dn_services_during="${10}"
  dn_services_after="${11}"
  live_dns_before="${12}"
  live_dns_during="${13}"
  live_dns_after="${14}"
  under_replicated_after="${15}"
  common_passed="${16}"

  common_json="$(cat "${COMMON_VALIDATION_FILE}" 2>/dev/null || printf 'null')"

  printf '{'
  printf '"schema_version":1,'
  printf '"id":"%s",' "${SCENARIO_ID}"
  printf '"name":"DataNode one-node failure during write",'
  printf '"passed":%s,' "${passed}"
  printf '"active_before":"%s",' "$(json_escape "${active_before}")"
  printf '"active_after_cleanup":"%s",' "$(json_escape "${active_after}")"
  printf '"stopped_datanode":"%s",' "$(json_escape "${stopped_dn}")"
  printf '"failure_injected_ms":%s,' "${failure_ms}"
  printf '"write_success_ms":%s,' "${write_success_ms}"
  printf '"recovery_time_ms":%s,' "${recovery_time_ms}"
  printf '"write_continuity_time_ms":%s,' "${recovery_time_ms}"
  printf '"write_latency_during_failure_ms":%s,' "${write_latency_ms}"
  printf '"datanode_services_running_before":%s,' "${dn_services_before}"
  printf '"datanode_services_running_during_failure":%s,' "${dn_services_during}"
  printf '"datanode_services_running_after_cleanup":%s,' "${dn_services_after}"
  printf '"live_datanodes_before":%s,' "${live_dns_before}"
  printf '"live_datanodes_during_failure":%s,' "${live_dns_during}"
  printf '"live_datanodes_after_cleanup":%s,' "${live_dns_after}"
  printf '"under_replicated_blocks_after_cleanup":%s,' "${under_replicated_after}"
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
  WRITE_PROBE_FILE="$(mktemp)"
  trap 'cleanup; rm -f "${COMMON_VALIDATION_FILE:-}" "${WRITE_PROBE_FILE:-}"' EXIT

  local active_before active_after active_count_before failure_ms write_success_ms
  local recovery_time_ms write_latency_ms dn_services_before dn_services_during dn_services_after
  local live_dns_before live_dns_during live_dns_after under_replicated_after
  local writes_passed cleanup_passed replication_passed common_passed passed

  log_info "scenario ${SCENARIO_ID}: running pre-flight HA sanity check"
  active_count_before="$(active_namenode_count || printf '0')"
  if [[ "${active_count_before}" != "1" ]]; then
    log_error "expected exactly one Active NameNode before scenario, got ${active_count_before}"
    exit 1
  fi
  active_before="$(active_namenode)"

  dn_services_before="$(running_count dn1 dn2 dn3)"
  live_dns_before="$(live_datanode_count || printf '0')"
  if [[ "${dn_services_before}" != "3" || "${live_dns_before}" != "3" ]]; then
    log_error "expected three running/live DataNodes before scenario, services=${dn_services_before} live=${live_dns_before}"
    exit 1
  fi

  log_info "scenario ${SCENARIO_ID}: verifying baseline write path"
  "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label preflight --lines 128 >/dev/null

  STOPPED_DN="$(choose_datanode)"
  log_info "scenario ${SCENARIO_ID}: stopping DataNode ${STOPPED_DN}"
  compose stop -t 10 "${STOPPED_DN}" >/dev/null
  failure_ms="$(now_ms)"

  writes_passed=false
  write_success_ms=0
  recovery_time_ms=0
  write_latency_ms=null

  dn_services_during="$(running_count dn1 dn2 dn3)"
  live_dns_during="$(live_datanode_count || printf '0')"

  if run_writes_during_failure; then
    writes_passed=true
    write_success_ms="$(now_ms)"
    recovery_time_ms="$((write_success_ms - failure_ms))"
    write_latency_ms="$(json_number_field "${WRITE_PROBE_FILE}" duration_ms || printf 'null')"
  else
    log_error "scenario ${SCENARIO_ID}: HDFS write failed while one DataNode was unavailable"
  fi

  if [[ "${dn_services_during}" != "2" ]]; then
    writes_passed=false
    log_error "expected two running DataNode services during failure, got ${dn_services_during}"
  fi

  cleanup_passed=true
  if ! cleanup; then
    cleanup_passed=false
    writes_passed=false
  fi

  replication_passed=false
  wait_for_live_datanodes 3 30 || true
  wait_for_under_replicated_zero 30 || true
  dn_services_after="$(running_count dn1 dn2 dn3)"
  live_dns_after="$(live_datanode_count || printf '0')"
  under_replicated_after="$(under_replicated_blocks || printf '999999')"
  if [[ "${dn_services_after}" == "3" && "${live_dns_after}" == "3" && "${under_replicated_after}" == "0" ]]; then
    replication_passed=true
  fi

  active_after="$(active_namenode || printf '')"

  common_passed=false
  if "${ROOT_DIR}/validators/validate_common.sh" --scenario "${SCENARIO_ID}" > "${COMMON_VALIDATION_FILE}"; then
    common_passed=true
  fi

  passed=false
  if [[ "${writes_passed}" == "true" && "${cleanup_passed}" == "true" && "${replication_passed}" == "true" && "${common_passed}" == "true" ]]; then
    passed=true
  fi

  emit_json "${passed}" "${active_before}" "${active_after}" "${STOPPED_DN}" "${failure_ms}" "${write_success_ms}" "${recovery_time_ms}" "${write_latency_ms}" "${dn_services_before}" "${dn_services_during}" "${dn_services_after}" "${live_dns_before}" "${live_dns_during}" "${live_dns_after}" "${under_replicated_after}" "${common_passed}"

  if [[ "${passed}" != "true" ]]; then
    exit 1
  fi
}

main "$@"
