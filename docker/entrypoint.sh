#!/usr/bin/env bash
set -euo pipefail

timestamp() {
  date +"%Y-%m-%dT%H:%M:%S%z"
}

log() {
  printf '%s [entrypoint] %s\n' "$(timestamp)" "$*" >&2
}

tail_logs() {
  touch "${HADOOP_HOME}/logs/.keep"
  shopt -s nullglob
  local files=("${HADOOP_HOME}"/logs/* "${HADOOP_HOME}/logs/.keep")
  exec tail -n +1 -F "${files[@]}"
}

if [[ $# -eq 0 ]]; then
  exec bash
fi

case "$1" in
  journalnode)
    log "starting JournalNode"
    hdfs --daemon start journalnode
    tail_logs
    ;;
  namenode)
    log "starting NameNode"
    hdfs --daemon start namenode
    log "starting ZKFailoverController"
    hdfs --daemon start zkfc
    tail_logs
    ;;
  datanode)
    log "starting DataNode"
    hdfs --daemon start datanode
    tail_logs
    ;;
  bash|sh|hdfs|hadoop|java)
    exec "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
