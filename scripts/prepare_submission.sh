#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage: scripts/prepare_submission.sh <name>

Copies results/result.json to submissions/<name>/result.json and stages only
that submission file.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi

NAME="$1"

if [[ "${NAME}" == *"/"* || "${NAME}" == "." || "${NAME}" == ".." || -z "${NAME}" ]]; then
  echo "Invalid name: ${NAME}" >&2
  echo "Use a folder-safe name without '/'." >&2
  exit 1
fi

RESULT_FILE="${ROOT_DIR}/results/result.json"
STUDENT_CONF_DIR="${ROOT_DIR}/student/conf"
DEST_DIR="${ROOT_DIR}/submissions/${NAME}"
DEST_RESULT="${DEST_DIR}/result.json"
DEST_CONF_DIR="${DEST_DIR}/conf"
REL_RESULT="submissions/${NAME}/result.json"
REL_CONF="submissions/${NAME}/conf"

if [[ ! -f "${RESULT_FILE}" ]]; then
  echo "Missing ${RESULT_FILE}" >&2
  echo "Run ./run_all.sh first." >&2
  exit 1
fi

if [[ ! -d "${STUDENT_CONF_DIR}" ]]; then
  echo "Missing ${STUDENT_CONF_DIR}" >&2
  exit 1
fi

mkdir -p "${DEST_DIR}"
cp "${RESULT_FILE}" "${DEST_RESULT}"

# Copy student/conf into submissions/<name>/conf so the grader can verify
# that result.json's student_conf_sha256 matches what was actually used.
rm -rf "${DEST_CONF_DIR}"
mkdir -p "${DEST_CONF_DIR}"
cp -R "${STUDENT_CONF_DIR}/." "${DEST_CONF_DIR}/"

python3 "${ROOT_DIR}/scripts/validate_submission_result.py" "${NAME}"

git -C "${ROOT_DIR}" add -- "${REL_RESULT}" "${REL_CONF}"

echo
echo "Prepared ${REL_RESULT} and ${REL_CONF}/"
echo
echo "Currently staged files:"
git -C "${ROOT_DIR}" diff --cached --name-only

UNEXPECTED_STAGED="$(
  git -C "${ROOT_DIR}" diff --cached --name-only \
    | grep -v -E "^submissions/[^/]+/(result\\.json|conf/[^/]+)$" || true
)"

if [[ -n "${UNEXPECTED_STAGED}" ]]; then
  echo
  echo "Warning: unrelated files are already staged. Unstage them before committing:"
  printf '%s\n' "${UNEXPECTED_STAGED}"
  echo
  echo "Suggested command:"
  echo "  git restore --staged ${UNEXPECTED_STAGED//$'\n'/ }"
fi

echo
echo "Commit command:"
echo "  git commit -m \"submit: ${NAME}\""
