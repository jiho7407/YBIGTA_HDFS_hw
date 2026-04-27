#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=scripts/lib/common.sh
source "${SCRIPT_DIR}/scripts/lib/common.sh"

RESULT_DIR="${SCRIPT_DIR}/results"
RESULT_FILE="${RESULT_DIR}/result.json"
SCENARIOS_TOTAL="${SCENARIOS_TOTAL:-9}"

mkdir -p "${RESULT_DIR}"

json_field_bool() {
  local file field
  file="$1"
  field="$2"
  grep -o "\"${field}\":\\(true\\|false\\)" "${file}" | head -1 | cut -d: -f2
}

json_field_number() {
  local file field
  file="$1"
  field="$2"
  grep -o "\"${field}\":[0-9]*" "${file}" | head -1 | cut -d: -f2
}

json_array_from_files() {
  local first file
  first=true

  printf '['
  for file in "$@"; do
    if [[ "${first}" == "true" ]]; then
      first=false
    else
      printf ','
    fi
    cat "${file}"
  done
  printf ']'
}

json_add_number() {
  local file field value tmp
  file="$1"
  field="$2"
  value="$3"
  tmp="${file}.tmp"

  if python3 - "${file}" "${field}" "${value}" "${tmp}" <<'PY'
import json
import sys

src, field, value, dst = sys.argv[1:5]

try:
    number = int(value)
except ValueError:
    sys.exit(1)

try:
    with open(src) as f:
        data = json.load(f)
except Exception:
    sys.exit(1)

data[field] = number
with open(dst, "w") as f:
    json.dump(data, f, separators=(",", ":"))
    f.write("\n")
PY
  then
    mv "${tmp}" "${file}"
  else
    rm -f "${tmp}"
  fi
}

main() {
  local tmp_dir scenario script output_file
  local scenarios_implemented scenarios_passed false_positive_failover_count
  local recovery_sum recovery_count max_recovery avg_recovery penalty_elapsed_sum
  local total_started_ms total_ended_ms total_runtime_ms
  local scenario_started_ms scenario_ended_ms scenario_elapsed_ms
  local scenario_outputs=()

  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "${tmp_dir:-}"' EXIT

  scenarios_implemented=0
  scenarios_passed=0
  false_positive_failover_count=0
  recovery_sum=0
  recovery_count=0
  max_recovery=0
  penalty_elapsed_sum=0
  total_started_ms="$(now_ms)"

  shopt -s nullglob
  for script in "${SCRIPT_DIR}"/scenarios/[0-9][0-9]-*/run.sh; do
    scenario="$(basename "$(dirname "${script}")")"
    output_file="${tmp_dir}/${scenario}.json"
    scenario_outputs+=("${output_file}")
    scenarios_implemented="$((scenarios_implemented + 1))"

    log_info "running scenario ${scenario}"
    scenario_started_ms="$(now_ms)"
    if "${script}" > "${output_file}"; then
      log_info "scenario ${scenario} completed"
    else
      log_warn "scenario ${scenario} failed; preserving its JSON result when available"
      if [[ ! -s "${output_file}" ]]; then
        printf '{"schema_version":1,"id":"%s","passed":false,"error":"scenario produced no JSON"}\n' "${scenario}" > "${output_file}"
      fi
    fi
    scenario_ended_ms="$(now_ms)"
    scenario_elapsed_ms="$((scenario_ended_ms - scenario_started_ms))"
    json_add_number "${output_file}" elapsed_ms "${scenario_elapsed_ms}"

    if [[ "$(json_field_bool "${output_file}" passed || printf 'false')" == "true" ]]; then
      local recovery_time
      scenarios_passed="$((scenarios_passed + 1))"
      penalty_elapsed_sum="$((penalty_elapsed_sum + scenario_elapsed_ms))"
      recovery_time="$(json_field_number "${output_file}" recovery_time_ms || printf '0')"
      recovery_sum="$((recovery_sum + recovery_time))"
      recovery_count="$((recovery_count + 1))"
      if [[ "${recovery_time}" -gt "${max_recovery}" ]]; then
        max_recovery="${recovery_time}"
      fi
    fi

    local false_positive
    false_positive="$(json_field_number "${output_file}" false_positive_failover_count || printf '0')"
    false_positive_failover_count="$((false_positive_failover_count + false_positive))"
  done
  shopt -u nullglob

  total_ended_ms="$(now_ms)"
  total_runtime_ms="$((total_ended_ms - total_started_ms))"

  if [[ "${recovery_count}" -gt 0 ]]; then
    avg_recovery="$((recovery_sum / recovery_count))"
  else
    avg_recovery=null
    max_recovery=null
  fi

  local student_conf_sha256 git_commit
  student_conf_sha256="$(find "${SCRIPT_DIR}/student/conf" -type f | sort | xargs sha256sum 2>/dev/null | sha256sum | awk '{print $1}')"
  git_commit="$(git -C "${SCRIPT_DIR}" rev-parse --short HEAD 2>/dev/null || printf 'unknown')"

  {
    printf '{'
    printf '"schema_version":1,'
    printf '"team_id":"local",'
    printf '"timestamp":"%s",' "$(timestamp)"
    printf '"git_commit":"%s",' "${git_commit}"
    printf '"student_conf_sha256":"%s",' "${student_conf_sha256}"
    printf '"environment":{'
    printf '"os":"%s",' "$(uname -s)"
    printf '"cpu_arch":"%s"' "$(uname -m)"
    printf '},'
    printf '"summary":{'
    printf '"scenarios_total":%s,' "${SCENARIOS_TOTAL}"
    printf '"scenarios_implemented":%s,' "${scenarios_implemented}"
    printf '"scenarios_passed":%s,' "${scenarios_passed}"
    printf '"solved_scenarios":%s,' "${scenarios_passed}"
    printf '"penalty_ms":%s,' "${penalty_elapsed_sum}"
    printf '"total_runtime_ms":%s,' "${total_runtime_ms}"
    printf '"recovery_penalty_ms":%s,' "${recovery_sum}"
    printf '"avg_recovery_time_ms":%s,' "${avg_recovery}"
    printf '"max_recovery_time_ms":%s,' "${max_recovery}"
    printf '"false_positive_failover_count":%s' "${false_positive_failover_count}"
    printf '},'
    printf '"scenarios":'
    json_array_from_files "${scenario_outputs[@]}"
    printf '}\n'
  } > "${RESULT_FILE}"

  log_info "wrote ${RESULT_FILE}"
  printf '%s\n' "${RESULT_FILE}"
}

main "$@"
