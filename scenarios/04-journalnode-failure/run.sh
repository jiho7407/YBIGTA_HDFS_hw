#!/usr/bin/env bash
set -euo pipefail

# Learning objective:
# Verify that HDFS edit log writes continue while one JournalNode is
# unavailable, because the remaining JournalNodes still form a QJM majority.

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCENARIO_DIR}/../.." && pwd)"

# shellcheck source=../../scripts/lib/common.sh
source "${ROOT_DIR}/scripts/lib/common.sh"
# shellcheck source=../../scripts/lib/compose.sh
source "${ROOT_DIR}/scripts/lib/compose.sh"
# shellcheck source=../../validators/common.sh
source "${ROOT_DIR}/validators/common.sh"

SCENARIO_ID="04-journalnode-failure"
STOPPED_JN=""
COMMON_VALIDATION_FILE=""
WRITE_PROBE_DIR=""
CLEANED_UP=false
WRITE_COUNT=0
WRITE_LATENCY_SUM_MS=0
WRITE_LATENCY_MAX_MS=0

usage() {
  cat <<'USAGE'
Usage: scenarios/04-journalnode-failure/run.sh

Stops one JournalNode, verifies that HDFS writes continue through the remaining
QJM quorum without an unnecessary NameNode failover, restarts the JournalNode,
then runs the common validator. Logs go to stderr. JSON goes to stdout.
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

journalnode_running() {
  local jn
  jn="$1"

  [[ -n "$(compose ps --status running -q "${jn}" 2>/dev/null)" ]]
}

journalnode_running_count() {
  local count jn
  count=0

  for jn in jn1 jn2 jn3; do
    if journalnode_running "${jn}"; then
      count="$((count + 1))"
    fi
  done

  printf '%s\n' "${count}"
}

choose_journalnode() {
  local jn

  for jn in jn1 jn2 jn3; do
    if journalnode_running "${jn}"; then
      printf '%s\n' "${jn}"
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

run_probe() {
  local label lines output_file
  label="$1"
  lines="$2"
  output_file="$3"

  "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label "${label}" --lines "${lines}" > "${output_file}"
}

run_writes_during_failure() {
  local writes label probe_file duration
  writes="${1:-3}"

  WRITE_COUNT=0
  WRITE_LATENCY_SUM_MS=0
  WRITE_LATENCY_MAX_MS=0

  for attempt in $(seq 1 "${writes}"); do
    label="during-jn-failure-${attempt}"
    probe_file="${WRITE_PROBE_DIR}/${label}.json"
    if ! run_probe "${label}" 256 "${probe_file}"; then
      return 1
    fi

    duration="$(json_number_field "${probe_file}" duration_ms || printf '0')"
    WRITE_COUNT="$((WRITE_COUNT + 1))"
    WRITE_LATENCY_SUM_MS="$((WRITE_LATENCY_SUM_MS + duration))"
    if [[ "${duration}" -gt "${WRITE_LATENCY_MAX_MS}" ]]; then
      WRITE_LATENCY_MAX_MS="${duration}"
    fi
  done

  return 0
}

wait_for_journalnode_running() {
  local jn retries
  jn="$1"
  retries="${2:-30}"

  for _ in $(seq 1 "${retries}"); do
    if journalnode_running "${jn}"; then
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

  if [[ -n "${STOPPED_JN}" ]]; then
    log_info "cleanup: starting ${STOPPED_JN}"
    compose up -d "${STOPPED_JN}" >/dev/null
    if wait_for_journalnode_running "${STOPPED_JN}"; then
      return 0
    fi
    log_error "cleanup: ${STOPPED_JN} did not return to running state"
    return 1
  fi
}

emit_json() {
  local passed active_before active_during active_after stopped_jn failure_ms write_success_ms
  local write_continuity_time_ms baseline_latency_ms avg_latency_ms max_latency_ms
  local latency_delta_ms write_count jn_count_before jn_count_during jn_count_after
  local false_positive_count common_passed common_json
  passed="$1"
  active_before="$2"
  active_during="$3"
  active_after="$4"
  stopped_jn="$5"
  failure_ms="$6"
  write_success_ms="$7"
  write_continuity_time_ms="$8"
  baseline_latency_ms="$9"
  avg_latency_ms="${10}"
  max_latency_ms="${11}"
  latency_delta_ms="${12}"
  write_count="${13}"
  jn_count_before="${14}"
  jn_count_during="${15}"
  jn_count_after="${16}"
  false_positive_count="${17}"
  common_passed="${18}"

  common_json="$(cat "${COMMON_VALIDATION_FILE}" 2>/dev/null || printf 'null')"

  printf '{'
  printf '"schema_version":1,'
  printf '"id":"%s",' "${SCENARIO_ID}"
  printf '"name":"JournalNode one-node failure",'
  printf '"passed":%s,' "${passed}"
  printf '"active_before":"%s",' "$(json_escape "${active_before}")"
  printf '"active_during_failure":"%s",' "$(json_escape "${active_during}")"
  printf '"active_after_cleanup":"%s",' "$(json_escape "${active_after}")"
  printf '"stopped_journalnode":"%s",' "$(json_escape "${stopped_jn}")"
  printf '"failure_injected_ms":%s,' "${failure_ms}"
  printf '"write_success_ms":%s,' "${write_success_ms}"
  printf '"recovery_time_ms":%s,' "${write_continuity_time_ms}"
  printf '"write_continuity_time_ms":%s,' "${write_continuity_time_ms}"
  printf '"baseline_write_latency_ms":%s,' "${baseline_latency_ms}"
  printf '"write_latency_during_failure_avg_ms":%s,' "${avg_latency_ms}"
  printf '"write_latency_during_failure_max_ms":%s,' "${max_latency_ms}"
  printf '"write_latency_delta_ms":%s,' "${latency_delta_ms}"
  printf '"write_count_during_failure":%s,' "${write_count}"
  printf '"journalnodes_running_before":%s,' "${jn_count_before}"
  printf '"journalnodes_running_during_failure":%s,' "${jn_count_during}"
  printf '"journalnodes_running_after_cleanup":%s,' "${jn_count_after}"
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
  WRITE_PROBE_DIR="$(mktemp -d)"
  trap 'cleanup; rm -f "${COMMON_VALIDATION_FILE:-}"; rm -rf "${WRITE_PROBE_DIR:-}"' EXIT

  local active_before active_count_before active_count_during active_during active_after
  local failure_ms write_success_ms write_continuity_time_ms baseline_latency_ms
  local avg_latency_ms max_latency_ms latency_delta_ms
  local jn_count_before jn_count_during jn_count_after
  local writes_passed cleanup_passed false_positive_count common_passed passed preflight_probe

  log_info "scenario ${SCENARIO_ID}: running pre-flight HA sanity check"
  active_count_before="$(active_namenode_count || printf '0')"
  if [[ "${active_count_before}" != "1" ]]; then
    log_error "expected exactly one Active NameNode before scenario, got ${active_count_before}"
    exit 1
  fi
  active_before="$(active_namenode)"

  jn_count_before="$(journalnode_running_count)"
  if [[ "${jn_count_before}" != "3" ]]; then
    log_error "expected three running JournalNodes before scenario, got ${jn_count_before}"
    exit 1
  fi

  log_info "scenario ${SCENARIO_ID}: verifying baseline write path"
  preflight_probe="${WRITE_PROBE_DIR}/preflight.json"
  run_probe preflight 128 "${preflight_probe}"
  baseline_latency_ms="$(json_number_field "${preflight_probe}" duration_ms || printf 'null')"

  STOPPED_JN="$(choose_journalnode)"
  log_info "scenario ${SCENARIO_ID}: stopping JournalNode ${STOPPED_JN}"
  compose stop -t 10 "${STOPPED_JN}" >/dev/null
  failure_ms="$(now_ms)"

  writes_passed=false
  write_success_ms=0
  write_continuity_time_ms=0
  avg_latency_ms=null
  max_latency_ms=null
  latency_delta_ms=null
  false_positive_count=0
  WRITE_COUNT=0
  WRITE_LATENCY_SUM_MS=0
  WRITE_LATENCY_MAX_MS=0

  jn_count_during="$(journalnode_running_count)"
  if [[ "${jn_count_during}" != "2" ]]; then
    log_error "expected two running JournalNodes during failure, got ${jn_count_during}"
  fi

  if run_writes_during_failure; then
    writes_passed=true
    write_success_ms="$(now_ms)"
    write_continuity_time_ms="$((write_success_ms - failure_ms))"
    avg_latency_ms="$((WRITE_LATENCY_SUM_MS / WRITE_COUNT))"
    max_latency_ms="${WRITE_LATENCY_MAX_MS}"
    if [[ "${baseline_latency_ms}" =~ ^[0-9]+$ ]]; then
      latency_delta_ms="$((avg_latency_ms - baseline_latency_ms))"
    fi
  else
    log_error "scenario ${SCENARIO_ID}: HDFS write failed while one JournalNode was unavailable"
  fi

  if [[ "${jn_count_during}" != "2" ]]; then
    writes_passed=false
  fi

  active_count_during="$(active_namenode_count || printf '0')"
  active_during="$(active_namenode || printf '')"
  if [[ "${active_count_during}" != "1" ]]; then
    writes_passed=false
    log_error "expected exactly one Active NameNode during JournalNode failure, got ${active_count_during}"
  fi

  if [[ "${active_during}" != "${active_before}" ]]; then
    false_positive_count=1
    writes_passed=false
    log_error "unexpected failover during JournalNode failure: before=${active_before} during=${active_during}"
  fi

  cleanup_passed=true
  if ! cleanup; then
    cleanup_passed=false
    writes_passed=false
  fi
  jn_count_after="$(journalnode_running_count)"
  active_after="$(active_namenode || printf '')"

  common_passed=false
  if "${ROOT_DIR}/validators/validate_common.sh" --scenario "${SCENARIO_ID}" > "${COMMON_VALIDATION_FILE}"; then
    common_passed=true
  fi

  passed=false
  if [[ "${writes_passed}" == "true" && "${cleanup_passed}" == "true" && "${common_passed}" == "true" && "${jn_count_after}" == "3" ]]; then
    passed=true
  fi

  emit_json "${passed}" "${active_before}" "${active_during}" "${active_after}" "${STOPPED_JN}" "${failure_ms}" "${write_success_ms}" "${write_continuity_time_ms}" "${baseline_latency_ms}" "${avg_latency_ms}" "${max_latency_ms}" "${latency_delta_ms}" "${WRITE_COUNT}" "${jn_count_before}" "${jn_count_during}" "${jn_count_after}" "${false_positive_count}" "${common_passed}"

  if [[ "${passed}" != "true" ]]; then
    exit 1
  fi
}

main "$@"
