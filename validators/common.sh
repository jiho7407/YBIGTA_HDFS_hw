#!/usr/bin/env bash
set -euo pipefail

VALIDATOR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${VALIDATOR_DIR}/.." && pwd)"

# shellcheck source=../scripts/lib/common.sh
source "${ROOT_DIR}/scripts/lib/common.sh"
# shellcheck source=../scripts/lib/compose.sh
source "${ROOT_DIR}/scripts/lib/compose.sh"

json_escape() {
  local value
  value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  printf '%s' "${value}"
}

ha_state() {
  local nn_id
  nn_id="$1"
  hdfs_client haadmin -getServiceState "${nn_id}" 2>/dev/null \
    | tr -d '\r' \
    | awk '/^(active|standby|observer)$/ { state=$0 } END { if (state != "") print state; else exit 1 }'
}

active_namenode_count() {
  local active_count state nn
  active_count=0

  for nn in nn1 nn2; do
    state="$(ha_state "${nn}" || true)"
    if [[ "${state}" == "active" ]]; then
      active_count="$((active_count + 1))"
    fi
  done

  printf '%s\n' "${active_count}"
}

check_namenode_health() {
  local nn
  for nn in nn1 nn2; do
    hdfs_client haadmin -checkHealth "${nn}" >/dev/null
  done
}

run_fsck() {
  local output_file
  output_file="$1"
  hdfs_client fsck / -files -blocks > "${output_file}" 2>&1
}

NN1_EDIT_LOG_TXID=""
NN2_EDIT_LOG_TXID=""

namenode_last_txid() {
  local nn
  nn="$1"

  client_bash "curl -fsS 'http://${nn}:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo' | grep -o 'LastAppliedOrWrittenTxId[^}]*' | grep -o '[0-9][0-9]*' | tail -1"
}

wait_for_edit_log_consistency() {
  local retries
  retries="${1:-30}"

  hdfs_client dfsadmin -rollEdits >/dev/null 2>&1 || return 1

  for _ in $(seq 1 "${retries}"); do
    NN1_EDIT_LOG_TXID="$(namenode_last_txid nn1 2>/dev/null || true)"
    NN2_EDIT_LOG_TXID="$(namenode_last_txid nn2 2>/dev/null || true)"

    if [[ -n "${NN1_EDIT_LOG_TXID}" && "${NN1_EDIT_LOG_TXID}" == "${NN2_EDIT_LOG_TXID}" ]]; then
      return 0
    fi
    sleep 1
  done

  return 1
}

fsck_has_zero_corrupt_blocks() {
  local output_file
  output_file="$1"

  if grep -Eiq 'CORRUPT[[:space:]]+blocks:[[:space:]]+0|Corrupt blocks:[[:space:]]+0' "${output_file}"; then
    return 0
  fi

  if grep -Eiq 'The filesystem under path .+ is HEALTHY' "${output_file}"; then
    return 0
  fi

  return 1
}
