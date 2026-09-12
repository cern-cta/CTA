#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

readonly GITLAB_URL="https://gitlab.cern.ch"
readonly PROJECT_ID="139306"

readonly CONFIG_DIR="${HOME}/.config/cta"
readonly TOKEN_FILE="${CONFIG_DIR}/gitlab-api-token"

script_dir="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
readonly script_dir
repository_dir="$(realpath "${script_dir}/..")"
readonly repository_dir

source "${script_dir}/utils/log_utils.sh"

job_id=""
output_dir=""
gitlab_token=""
download_file=""

usage() {
  cat <<EOF

Usage:
  $(basename "$0") <job-id|job-url> [--output DIRECTORY]

Download and extract the artifacts of one CTA GitLab pipeline job.

Positional arguments:
  job-id|job-url       GitLab job ID or job URL

Options:
  -o, --output DIR     Extract into DIR (default: <repository>/tmp/ci-job-artifacts/<job-id>)
  -h, --help           Show this help

Examples:
  $(basename "$0") 12345678
  $(basename "$0") https://gitlab.cern.ch/cta/CTA/-/jobs/12345678
  $(basename "$0") 12345678 --output ./artifacts

EOF
}

require_command() {
  command -v "$1" >/dev/null \
    || die "Required command '$1' is not installed."
}

gitlab_api() {
  local method="$1"
  local endpoint="$2"

  shift 2

  curl \
    --silent \
    --show-error \
    --fail \
    --request "${method}" \
    --header "PRIVATE-TOKEN: ${gitlab_token}" \
    "$@" \
    "${GITLAB_URL}/api/v4${endpoint}"
}

parse_arguments() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -h|--help)
        usage
        exit 0
        ;;

      -o|--output)
        [[ $# -ge 2 ]] || die "$1 requires an argument"
        output_dir="$2"
        shift
        ;;

      -*)
        die "Unknown option: $1"
        ;;

      *)
        [[ -z "${job_id}" ]] || die "Unexpected argument: $1"
        job_id="$1"
        ;;
    esac

    shift
  done

  [[ -n "${job_id}" ]] || {
    usage
    exit 1
  }

  if [[ "${job_id}" =~ ^https?:// ]]; then
    job_id="${job_id%%\?*}"
    job_id="${job_id%/}"
    job_id="${job_id##*/}"
  fi

  [[ "${job_id}" =~ ^[0-9]+$ ]] \
    || die "Invalid job ID."

  if [[ -z "${output_dir}" ]]; then
    output_dir="${repository_dir}/tmp/ci-job-artifacts/${job_id}"
  fi
}

load_gitlab_token() {
  mkdir -p "${CONFIG_DIR}"

  if [[ -f "${TOKEN_FILE}" ]]; then
    gitlab_token="$(<"${TOKEN_FILE}")"
    return
  fi

  echo
  echo "Authentication is required."
  echo "Provide a GitLab Personal Access Token or Project Access Token with the \`api\` scope."
  echo

  read -rsp "Token: " gitlab_token
  echo

  local answer
  read -rp "Store token in ${TOKEN_FILE}? [Y/n] " answer

  if [[ -z "${answer}" || "${answer}" =~ ^[Yy]$ ]]; then
    printf "%s" "${gitlab_token}" > "${TOKEN_FILE}"
    chmod 600 "${TOKEN_FILE}"
  fi
}

verify_gitlab_auth() {
  log_task "Checking GitLab authentication..."

  load_gitlab_token

  if ! gitlab_api GET "/user" >/dev/null; then
    die "GitLab authentication failed."
  fi
}

clean_up() {
  if [[ -n "${download_file}" && -f "${download_file}" ]]; then
    rm -f "${download_file}"
  fi
}

download_artifacts() {
  [[ ! -e "${output_dir}" ]] \
    || die "Output path already exists: ${output_dir}"

  local job
  local job_name

  log_task "Querying job ${job_id}..."

  job="$(gitlab_api GET "/projects/${PROJECT_ID}/jobs/${job_id}")"
  job_name="$(jq -r '.name' <<< "${job}")"

  [[ -n "${job_name}" && "${job_name}" != "null" ]] \
    || die "Could not determine the job name."

  download_file="$(mktemp "${TMPDIR:-/tmp}/cta-job-artifacts.XXXXXX.zip")"
  trap clean_up EXIT

  log_task "Downloading artifacts for ${job_name}..."

  gitlab_api \
    GET \
    "/projects/${PROJECT_ID}/jobs/${job_id}/artifacts" \
    --output "${download_file}"

  mkdir -p "${output_dir}"
  unzip -q "${download_file}" -d "${output_dir}"

  clean_up
  trap - EXIT

  log_success "Artifacts extracted to ${output_dir}"
}

main() {
  parse_arguments "$@"

  require_command curl
  require_command jq
  require_command unzip

  verify_gitlab_auth
  download_artifacts
}

main "$@"
