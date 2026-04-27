#!/usr/bin/env bash
set -euo pipefail

SCRIPT_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHALLENGE_ROOT="$(cd "${SCRIPT_LIB_DIR}/../.." && pwd)"

compose_args() {
  printf '%s\n' --project-directory
  printf '%s\n' "${CHALLENGE_ROOT}"
  printf '%s\n' -f
  printf '%s\n' "${CHALLENGE_ROOT}/docker-compose.yml"

  if [[ -f "${CHALLENGE_ROOT}/student/docker-compose.student.yml" ]]; then
    printf '%s\n' -f
    printf '%s\n' "${CHALLENGE_ROOT}/student/docker-compose.student.yml"
  fi
}

compose() {
  local args=()
  while IFS= read -r arg; do
    args+=("${arg}")
  done < <(compose_args)
  COMPOSE_PROGRESS="${COMPOSE_PROGRESS:-quiet}" docker compose "${args[@]}" "$@"
}

hdfs_client() {
  compose run --rm -T --no-deps --entrypoint hdfs client "$@"
}

hadoop_client() {
  compose run --rm -T --no-deps --entrypoint hadoop client "$@"
}

client_bash() {
  compose run --rm -T --no-deps --entrypoint bash client -lc "$1"
}
