#!/usr/bin/env bash
set -euo pipefail

# Learning objective:
# Verify that the HA cluster stays stable across a short sequence of mild
# failures and that repeated recovery times remain bounded.

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCENARIO_DIR}/../.." && pwd)"

# shellcheck source=../../scripts/lib/common.sh
source "${ROOT_DIR}/scripts/lib/common.sh"
# shellcheck source=../../scripts/lib/compose.sh
source "${ROOT_DIR}/scripts/lib/compose.sh"
# shellcheck source=../../validators/common.sh
source "${ROOT_DIR}/validators/common.sh"

SCENARIO_ID="09-chaos-test"
COMMON_VALIDATION_FILE=""
WRITE_PROBE_FILE=""
STOPPED_SERVICE=""
STOPPED_KIND=""
CLEANED_UP=false
ITERATION_JSON=""
RECOVERY_TIMES=()

usage() {
  cat <<'USAGE'
Usage: scenarios/09-chaos-test/run.sh

Runs a short chaos sequence by stopping and restarting one Standby NameNode, one
ZooKeeper, one JournalNode, and one DataNode. Each injection must still allow a
deterministic HDFS write/read probe, and the Active NameNode should not change
for these non-Active failures. Logs go to stderr. JSON goes to stdout.
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

service_running() {
  local service
  service="$1"

  [[ -n "$(compose ps --status running -q "${service}" 2>/dev/null)" ]]
}

json_number_field() {
  local file field
  file="$1"
  field="$2"

  grep -o "\"${field}\":[0-9]*" "${file}" | head -1 | cut -d: -f2
}

choose_running_service() {
  local service

  for service in "$@"; do
    if service_running "${service}"; then
      printf '%s\n' "${service}"
      return 0
    fi
  done

  return 1
}

wait_for_service_ready() {
  local kind service retries
  kind="$1"
  service="$2"
  retries="${3:-30}"

  for _ in $(seq 1 "${retries}"); do
    case "${kind}" in
      namenode)
        if ha_state "${service}" >/dev/null 2>&1; then
          return 0
        fi
        ;;
      service)
        if service_running "${service}"; then
          return 0
        fi
        ;;
      *)
        return 1
        ;;
    esac
    sleep 2
  done

  return 1
}

cleanup_current() {
  local service kind
  service="${STOPPED_SERVICE}"
  kind="${STOPPED_KIND}"

  if [[ -z "${service}" ]]; then
    return 0
  fi

  log_info "cleanup: starting ${service}"
  compose up -d "${service}" >/dev/null
  if wait_for_service_ready "${kind}" "${service}"; then
    STOPPED_SERVICE=""
    STOPPED_KIND=""
    return 0
  fi

  log_error "cleanup: ${service} did not become ready"
  return 1
}

cleanup() {
  if [[ "${CLEANED_UP}" == "true" ]]; then
    return 0
  fi
  CLEANED_UP=true
  cleanup_current
}

wait_for_probe_write() {
  local label retries
  label="$1"
  retries="${2:-20}"

  for attempt in $(seq 1 "${retries}"); do
    if "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label "${label}-attempt-${attempt}" --lines 256 > "${WRITE_PROBE_FILE}"; then
      return 0
    fi
    sleep 1
  done

  return 1
}

append_iteration_json() {
  local label service passed failure_ms recovered_ms recovery_time_ms write_latency_ms active_before active_after
  label="$1"
  service="$2"
  passed="$3"
  failure_ms="$4"
  recovered_ms="$5"
  recovery_time_ms="$6"
  write_latency_ms="$7"
  active_before="$8"
  active_after="$9"

  if [[ -n "${ITERATION_JSON}" ]]; then
    ITERATION_JSON+=","
  fi

  ITERATION_JSON+="{"
  ITERATION_JSON+="\"label\":\"$(json_escape "${label}")\","
  ITERATION_JSON+="\"service\":\"$(json_escape "${service}")\","
  ITERATION_JSON+="\"passed\":${passed},"
  ITERATION_JSON+="\"failure_injected_ms\":${failure_ms},"
  ITERATION_JSON+="\"recovered_ms\":${recovered_ms},"
  ITERATION_JSON+="\"recovery_time_ms\":${recovery_time_ms},"
  ITERATION_JSON+="\"write_latency_ms\":${write_latency_ms},"
  ITERATION_JSON+="\"active_before\":\"$(json_escape "${active_before}")\","
  ITERATION_JSON+="\"active_after\":\"$(json_escape "${active_after}")\""
  ITERATION_JSON+="}"
}

run_iteration() {
  local label kind service active_before active_after active_count_after
  local failure_ms recovered_ms recovery_time_ms write_latency_ms iteration_passed
  label="$1"
  kind="$2"
  service="$3"

  active_before="$(active_namenode || printf '')"
  log_info "scenario ${SCENARIO_ID}: ${label}: stopping ${service}"
  STOPPED_SERVICE="${service}"
  STOPPED_KIND="${kind}"
  compose stop -t 5 "${service}" >/dev/null
  failure_ms="$(now_ms)"

  iteration_passed=false
  recovered_ms=0
  recovery_time_ms=0
  write_latency_ms=null

  if wait_for_probe_write "${label}"; then
    recovered_ms="$(now_ms)"
    recovery_time_ms="$((recovered_ms - failure_ms))"
    write_latency_ms="$(json_number_field "${WRITE_PROBE_FILE}" duration_ms || printf 'null')"
    iteration_passed=true
    RECOVERY_TIMES+=("${recovery_time_ms}")
  else
    log_error "scenario ${SCENARIO_ID}: ${label}: HDFS write did not succeed"
  fi

  active_count_after="$(active_namenode_count || printf '0')"
  active_after="$(active_namenode || printf '')"
  if [[ "${active_count_after}" != "1" ]]; then
    iteration_passed=false
    log_error "scenario ${SCENARIO_ID}: ${label}: expected exactly one Active NameNode, got ${active_count_after}"
  fi

  if [[ "${active_before}" != "${active_after}" ]]; then
    FALSE_POSITIVE_FAILOVERS="$((FALSE_POSITIVE_FAILOVERS + 1))"
    iteration_passed=false
    log_error "scenario ${SCENARIO_ID}: ${label}: unexpected Active NameNode change from ${active_before} to ${active_after}"
  fi

  if ! cleanup_current; then
    iteration_passed=false
  fi

  append_iteration_json "${label}" "${service}" "${iteration_passed}" "${failure_ms}" "${recovered_ms}" "${recovery_time_ms}" "${write_latency_ms}" "${active_before}" "${active_after}"

  if [[ "${iteration_passed}" == "true" ]]; then
    return 0
  fi
  return 1
}

average_recovery_time() {
  local sum value
  if [[ "${#RECOVERY_TIMES[@]}" -eq 0 ]]; then
    printf 'null\n'
    return 0
  fi

  sum=0
  for value in "${RECOVERY_TIMES[@]}"; do
    sum="$((sum + value))"
  done

  printf '%s\n' "$((sum / ${#RECOVERY_TIMES[@]}))"
}

p95_recovery_time() {
  local sorted count rank
  if [[ "${#RECOVERY_TIMES[@]}" -eq 0 ]]; then
    printf 'null\n'
    return 0
  fi

  sorted="$(printf '%s\n' "${RECOVERY_TIMES[@]}" | sort -n)"
  count="${#RECOVERY_TIMES[@]}"
  rank="$(((count * 95 + 99) / 100))"
  printf '%s\n' "${sorted}" | sed -n "${rank}p"
}

emit_json() {
  local passed iterations_passed iterations_total avg_recovery p95_recovery common_passed common_json
  passed="$1"
  iterations_passed="$2"
  iterations_total="$3"
  avg_recovery="$4"
  p95_recovery="$5"
  common_passed="$6"

  common_json="$(cat "${COMMON_VALIDATION_FILE}" 2>/dev/null || printf 'null')"

  printf '{'
  printf '"schema_version":1,'
  printf '"id":"%s",' "${SCENARIO_ID}"
  printf '"name":"Repeated mild chaos test",'
  printf '"passed":%s,' "${passed}"
  printf '"recovery_time_ms":%s,' "${avg_recovery}"
  printf '"avg_recovery_time_ms":%s,' "${avg_recovery}"
  printf '"p95_recovery_time_ms":%s,' "${p95_recovery}"
  printf '"false_positive_failover_count":%s,' "${FALSE_POSITIVE_FAILOVERS}"
  printf '"iterations_passed":%s,' "${iterations_passed}"
  printf '"iterations_total":%s,' "${iterations_total}"
  printf '"iterations":[%s],' "${ITERATION_JSON}"
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

  local active_count_before standby_nn zk_service jn_service dn_service
  local iterations_total iterations_passed common_passed passed avg_recovery p95_recovery

  log_info "scenario ${SCENARIO_ID}: running pre-flight HA sanity check"
  active_count_before="$(active_namenode_count || printf '0')"
  if [[ "${active_count_before}" != "1" ]]; then
    log_error "expected exactly one Active NameNode before scenario, got ${active_count_before}"
    exit 1
  fi

  log_info "scenario ${SCENARIO_ID}: verifying baseline write path"
  "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label preflight --lines 128 >/dev/null

  FALSE_POSITIVE_FAILOVERS=0
  iterations_total=0
  iterations_passed=0

  standby_nn="$(standby_namenode)"
  zk_service="$(choose_running_service zk1 zk2 zk3)"
  jn_service="$(choose_running_service jn1 jn2 jn3)"
  dn_service="$(choose_running_service dn1 dn2 dn3)"

  for spec in \
    "standby-namenode namenode ${standby_nn}" \
    "zookeeper service ${zk_service}" \
    "journalnode service ${jn_service}" \
    "datanode service ${dn_service}"; do
    iterations_total="$((iterations_total + 1))"
    # shellcheck disable=SC2086
    if run_iteration ${spec}; then
      iterations_passed="$((iterations_passed + 1))"
    fi
  done

  avg_recovery="$(average_recovery_time)"
  p95_recovery="$(p95_recovery_time)"

  common_passed=false
  if "${ROOT_DIR}/validators/validate_common.sh" --scenario "${SCENARIO_ID}" > "${COMMON_VALIDATION_FILE}"; then
    common_passed=true
  fi

  passed=false
  if [[ "${iterations_passed}" == "${iterations_total}" && "${FALSE_POSITIVE_FAILOVERS}" == "0" && "${common_passed}" == "true" ]]; then
    passed=true
  fi

  emit_json "${passed}" "${iterations_passed}" "${iterations_total}" "${avg_recovery}" "${p95_recovery}" "${common_passed}"

  if [[ "${passed}" != "true" ]]; then
    exit 1
  fi
}

main "$@"
