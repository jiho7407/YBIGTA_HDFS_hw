#!/usr/bin/env bash
set -euo pipefail

# Learning objective:
# Verify the shared HDFS HA invariants used by every failure scenario.

VALIDATOR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${VALIDATOR_DIR}/.." && pwd)"

# shellcheck source=common.sh
source "${VALIDATOR_DIR}/common.sh"

SCENARIO_ID="common"

usage() {
  cat <<'USAGE'
Usage: validators/validate_common.sh [--scenario ID]

Runs common HDFS HA validation and emits one JSON object to stdout.
USAGE
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --scenario)
        SCENARIO_ID="$2"
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

main() {
  parse_args "$@"

  local tmp_fsck tmp_probe started_ms ended_ms duration_ms active_count
  local data_passed fsck_passed active_passed nn_health_passed edit_log_passed edit_log_consistency

  tmp_fsck="$(mktemp)"
  tmp_probe="$(mktemp)"
  trap 'rm -f "${tmp_fsck:-}" "${tmp_probe:-}"' EXIT

  started_ms="$(now_ms)"

  data_passed=false
  fsck_passed=false
  active_passed=false
  nn_health_passed=false
  edit_log_passed=false
  edit_log_consistency="txid-mismatch"

  log_info "validating deterministic HDFS write/read probe"
  if "${ROOT_DIR}/scripts/inject_data.sh" --scenario "${SCENARIO_ID}" --label validator > "${tmp_probe}"; then
    data_passed=true
  fi

  log_info "validating Active NameNode count"
  active_count="$(active_namenode_count || printf '0')"
  if [[ "${active_count}" == "1" ]]; then
    active_passed=true
  fi

  log_info "validating NameNode health"
  if check_namenode_health; then
    nn_health_passed=true
  fi

  log_info "validating fsck health"
  if run_fsck "${tmp_fsck}" && fsck_has_zero_corrupt_blocks "${tmp_fsck}"; then
    fsck_passed=true
  fi

  log_info "validating edit log consistency"
  if wait_for_edit_log_consistency; then
    edit_log_passed=true
    edit_log_consistency="txid-match"
  fi

  ended_ms="$(now_ms)"
  duration_ms="$((ended_ms - started_ms))"

  local passed
  passed=false
  if [[ "${data_passed}" == "true" && "${active_passed}" == "true" && "${nn_health_passed}" == "true" && "${fsck_passed}" == "true" && "${edit_log_passed}" == "true" ]]; then
    passed=true
  fi

  printf '{'
  printf '"schema_version":1,'
  printf '"validator":"common",'
  printf '"scenario_id":"%s",' "$(json_escape "${SCENARIO_ID}")"
  printf '"passed":%s,' "${passed}"
  printf '"duration_ms":%s,' "${duration_ms}"
  printf '"checks":{'
  printf '"hdfs_probe":%s,' "${data_passed}"
  printf '"active_namenode_count":%s,' "${active_count}"
  printf '"active_namenode_count_passed":%s,' "${active_passed}"
  printf '"namenode_health":%s,' "${nn_health_passed}"
  printf '"fsck_zero_corrupt_blocks":%s,' "${fsck_passed}"
  printf '"edit_log_consistency":%s,' "${edit_log_passed}"
  printf '"edit_log_consistency_method":"%s",' "${edit_log_consistency}"
  printf '"nn1_last_applied_or_written_txid":"%s",' "$(json_escape "${NN1_EDIT_LOG_TXID}")"
  printf '"nn2_last_applied_or_written_txid":"%s"' "$(json_escape "${NN2_EDIT_LOG_TXID}")"
  printf '},'
  printf '"probe":%s' "$(cat "${tmp_probe}" 2>/dev/null || printf 'null')"
  printf '}\n'

  if [[ "${passed}" != "true" ]]; then
    exit 1
  fi
}

main "$@"
