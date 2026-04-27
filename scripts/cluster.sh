#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"
# shellcheck source=lib/compose.sh
source "${SCRIPT_DIR}/lib/compose.sh"

wait_for_service() {
  local service cmd retries
  service="$1"
  cmd="$2"
  retries="${3:-30}"

  for _ in $(seq 1 "${retries}"); do
    if compose exec -T "${service}" bash -lc "${cmd}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done

  log_error "service ${service} did not become ready"
  return 1
}

is_nn1_formatted() {
  compose run --rm --entrypoint bash nn1 -lc 'test -d /hadoop/dfs/name/current' >/dev/null 2>&1
}

is_nn2_formatted() {
  compose run --rm --entrypoint bash nn2 -lc 'test -d /hadoop/dfs/name/current' >/dev/null 2>&1
}

zk_failover_state_exists() {
  compose exec -T zk1 zkCli.sh stat /hadoop-ha/hacluster >/dev/null 2>&1
}

wait_for_hdfs_ready() {
  local retries safemode
  retries="${1:-30}"

  for _ in $(seq 1 "${retries}"); do
    safemode="$(hdfs_client dfsadmin -safemode get 2>/dev/null || true)"
    if [[ "${safemode}" == *"Safe mode is OFF"* ]]; then
      if hdfs_client dfs -mkdir -p /homework/.ready >/dev/null 2>&1; then
        return 0
      fi
    fi
    sleep 2
  done

  log_error "HDFS did not become writable"
  return 1
}

build() {
  log_info "building Hadoop image"
  compose build
}

init() {
  local formatted_nn1
  formatted_nn1=0

  log_info "starting ZooKeeper and JournalNode quorum"
  compose up -d zk1 zk2 zk3 jn1 jn2 jn3

  log_info "waiting for JournalNodes"
  wait_for_service jn1 'nc -z localhost 8485'
  wait_for_service jn2 'nc -z localhost 8485'
  wait_for_service jn3 'nc -z localhost 8485'

  if is_nn1_formatted; then
    log_info "nn1 metadata already formatted"
  else
    log_info "formatting nn1 metadata"
    compose run --rm nn1 hdfs namenode -format -force -nonInteractive hacluster
    formatted_nn1=1
  fi

  if [[ "${formatted_nn1}" -eq 1 ]] || ! zk_failover_state_exists; then
    log_info "formatting ZooKeeper failover state"
    compose run --rm nn1 hdfs zkfc -formatZK -force -nonInteractive
  else
    log_info "ZooKeeper failover state already exists"
  fi

  log_info "starting nn1 before standby bootstrap"
  compose up -d nn1
  wait_for_service nn1 'nc -z nn1 8020'

  if is_nn2_formatted; then
    log_info "nn2 metadata already bootstrapped"
  else
    log_info "bootstrapping nn2 standby metadata"
    compose run --rm nn2 hdfs namenode -bootstrapStandby -force -nonInteractive
  fi

  log_info "starting full HDFS HA cluster"
  compose up -d nn1 nn2 dn1 dn2 dn3
  log_info "waiting for HDFS to become writable"
  wait_for_hdfs_ready
}

up() {
  log_info "starting full compose project"
  compose up -d
}

down() {
  log_info "stopping compose project"
  compose down
}

clean() {
  log_warn "removing containers and named volumes"
  compose down -v
}

status() {
  compose ps
  log_info "NameNode HA state"
  compose exec -T nn1 hdfs haadmin -getAllServiceState || true
}

usage() {
  cat <<'USAGE'
Usage: scripts/cluster.sh <command>

Commands:
  build     Build the local Hadoop image
  init      Start quorum services, format HA metadata, and start HDFS
  up        Start all existing containers
  down      Stop containers without deleting volumes
  clean     Stop containers and delete named volumes
  status    Show container status and NameNode HA state
USAGE
}

main() {
  local command
  command="${1:-}"

  case "${command}" in
    build) build ;;
    init) init ;;
    up) up ;;
    down) down ;;
    clean) clean ;;
    status) status ;;
    -h|--help|help|"") usage ;;
    *)
      log_error "unknown command: ${command}"
      usage
      exit 2
      ;;
  esac
}

main "$@"
