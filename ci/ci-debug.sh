#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

# =========================================================================
#  Configuration
# =========================================================================

readonly GITLAB_URL="https://gitlab.cern.ch"
readonly PROJECT_ID="139306"
readonly REGISTRY="gitlab-registry.cern.ch"
readonly IMAGE_REPOSITORY="${REGISTRY}/cta/ctageneric/cta-debug"
readonly DEBUG_IMAGE_JOB_NAME="build-cta-images: [cta-debug]"

readonly CONFIG_DIR="${HOME}/.config/cta-ci-debug"
readonly TOKEN_FILE="${CONFIG_DIR}/token"
readonly CACHE_DIR="${HOME}/.cache/cta-ci-debug"
script_dir="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
readonly script_dir

source "${script_dir}/utils/log_utils.sh"

# =========================================================================
#  Globals
# =========================================================================

pipeline=""
job_name=""

gitlab_token=""

debug_image_name=""
debug_image_job_id=""
debug_image_job_status=""
pipeline_web_url=""

artifact_dir=""

# =========================================================================
#  Usage
# =========================================================================

usage() {
  cat <<EOF

Usage:
  $(basename "$0") <pipeline-id|pipeline-url> [options]

Open an interactive debug container for investigating core dumps from a CI
pipeline.

The script will:
  - Verify GitLab and container registry authentication
  - Ensure the pipeline's debug image exists (building it if necessary)
  - Download artifacts from failed system-test jobs
  - Extract the artifacts
  - Launch an interactive debug container with the extracted artifacts
    mounted under /artifacts

Positional arguments:
  pipeline-id|pipeline-url   Pipeline ID or GitLab pipeline URL

Options:
  --job NAME                 Download artifacts only from the specified job
  -h, --help                 Show this help

Examples:
  $(basename "$0") 15382224
  $(basename "$0") 15382224 --job test-client-gfal2
  $(basename "$0") https://gitlab.cern.ch/.../pipelines/15382224

EOF
  exit 1
}

# =========================================================================
#  Helpers
# =========================================================================

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

# =========================================================================
#  Argument parsing
# =========================================================================

parse_arguments() {

  [[ $# -ge 1 ]] || usage

  while [[ $# -gt 0 ]]; do
    case "$1" in

      -h|--help)
        usage
        ;;

      --job)
        [[ $# -ge 2 ]] || die "--job requires an argument"
        job_name="$2"
        shift
        ;;

      -*)
        die "Unknown option: $1"
        ;;

      *)
        if [[ -z "${pipeline}" ]]; then
          pipeline="$1"
        else
          die "Unexpected argument: $1"
        fi
        ;;

    esac

    shift
  done

  [[ -n "${pipeline}" ]] || usage

  if [[ "${pipeline}" =~ ^https?:// ]]; then
    pipeline="${pipeline%/}"
    pipeline="${pipeline##*/}"
  fi

  [[ "${pipeline}" =~ ^[0-9]+$ ]] \
    || die "Invalid pipeline ID."
}

# =========================================================================
#  Setup and Authentication
# =========================================================================

check_prerequisites() {

  log_task "Checking prerequisites..."

  require_command curl
  require_command jq
  require_command unzip
  require_command podman

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
  echo "The token is used to query pipelines, download artifacts, and trigger the debug-image build if needed."
  echo

  read -rsp "Token: " gitlab_token
  echo

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
    rm -f "${TOKEN_FILE}"
    die "GitLab authentication failed. Token removed."
  fi

}

check_registry_login() {

  log_task "Checking registry login..."

  if podman login --get-login "${REGISTRY}" >/dev/null 2>&1; then
    return
  fi

  echo
  echo "Podman is not logged into ${REGISTRY}."
  echo "See https://docs.gitlab.com/user/packages/container_registry/authenticate_with_container_registry/"
  echo

  podman login "${REGISTRY}"

}

query_pipeline() {

  log_task "Querying pipeline ${pipeline}..."

  local response
  local pipeline_sha
  local pipeline_short_sha
  local image_tag

  response="$(
    gitlab_api GET "/projects/${PROJECT_ID}/pipelines/${pipeline}"
  )"

  pipeline_sha="$(
    jq -r '.sha' <<< "${response}"
  )"

  [[ "${pipeline_sha}" != "null" ]] \
    || die "Could not determine pipeline SHA."

  pipeline_short_sha="${pipeline_sha:0:8}"

  # This image tag construction must match whatever we do in CI
  image_tag="${pipeline}git${pipeline_short_sha}"

  debug_image_name="${IMAGE_REPOSITORY}:${image_tag}"

  pipeline_web_url="$(jq -r '.web_url' <<< "${response}")"

  echo "Pipeline SHA : ${pipeline_sha}"
  echo "Debug image  : ${debug_image_name}"

}

# =========================================================================
#  Debug Image
# =========================================================================

find_debug_image_job() {

  local jobs

  jobs="$(
    gitlab_api GET "/projects/${PROJECT_ID}/pipelines/${pipeline}/jobs?per_page=100"
  )"

  debug_image_job_id="$(
    jq -r --arg job_name "${DEBUG_IMAGE_JOB_NAME}" '
      .[]
      | select(.name == $job_name)
      | .id
    ' <<< "${jobs}"
  )"

  debug_image_job_status="$(
    jq -r --arg job_name "${DEBUG_IMAGE_JOB_NAME}" '
      .[]
      | select(.name == $job_name)
      | .status
    ' <<< "${jobs}"
  )"

  [[ -n "${debug_image_job_id}" && "${debug_image_job_id}" != "null" ]] \
    || die "Could not find ${DEBUG_IMAGE_JOB_NAME} job."

}

wait_for_job() {

  local status

  log_task "Waiting for debug image build..."

  while true; do

    sleep 5

    status="$(
      gitlab_api GET "/projects/${PROJECT_ID}/jobs/${debug_image_job_id}" \
      | jq -r '.status'
    )"

    echo "    Current job status: ${status}"

    case "${status}" in
      success)
        echo
        return
        ;;

      failed|canceled)
        echo
        log_error "Debug image build failed."
        echo
        echo "Investigate:"
        echo "${pipeline_web_url}"
        exit 1
        ;;

      *)
        ;;
    esac

  done

}

ensure_debug_image() {

  log_task "Checking debug image..."

  find_debug_image_job

  case "${debug_image_job_status}" in

    success)
      echo "Debug image already exists."
      ;;

    manual)

      echo "Triggering debug image build..."

      gitlab_api \
        POST \
        "/projects/${PROJECT_ID}/jobs/${debug_image_job_id}/play" \
        >/dev/null

      wait_for_job
      ;;

    pending|running)

      echo "Debug image is already building."

      wait_for_job
      ;;

    *)

      die "Unexpected debug image job status: ${debug_image_job_status}"

      ;;

  esac

  log_task "Pulling debug image..."

  podman pull "${debug_image_name}"

}

# =========================================================================
#  Job Artifacts
# =========================================================================

download_artifacts() {

  log_task "Finding failed system-test jobs..."

  local jobs
  local ids

  jobs="$(
    gitlab_api GET "/projects/${PROJECT_ID}/pipelines/${pipeline}/jobs?per_page=100"
  )"

  if [[ -n "${job_name}" ]]; then

    ids="$(
      jq -r \
        --arg job "${job_name}" '
          .[]
          | select(.stage=="system-test")
          | select(.name==$job)
          | .id
        ' <<< "${jobs}"
    )"

  else

    ids="$(
      jq -r '
        .[]
        | select(.stage=="system-test")
        | select(.status=="failed")
        | .id
      ' <<< "${jobs}"
    )"

  fi

  [[ -n "${ids}" ]] \
    || die "No matching jobs found."

  while read -r id; do

    [[ -n "${id}" ]] || continue

    local name
    local zip

    name="$(
      jq -r \
        --argjson id "${id}" '
          .[]
          | select(.id==$id)
          | .name
        ' <<< "${jobs}"
    )"

    log_task "Downloading artifacts for ${name}..."

    zip="${artifact_dir}/${name}.zip"

    gitlab_api \
      GET \
      "/projects/${PROJECT_ID}/jobs/${id}/artifacts" \
      --output "${zip}"

    mkdir -p "${artifact_dir}/${name}"

    unzip \
      -q \
      "${zip}" \
      -d "${artifact_dir}/${name}"

    rm -f "${zip}"

    while IFS= read -r -d '' archive; do

      log_task "Extracting $(basename "${archive}")..."

      tar \
        -xJf "${archive}" \
        -C "$(dirname "${archive}")"

      rm -f "${archive}"

    done < <(
      find "${artifact_dir}/${name}" \
        -type f \
        -name varlog.tar.xz \
        -print0
    )

  done <<< "${ids}"

}

# =========================================================================
#  Debug Container
# =========================================================================

launch_debug_container() {

  log_task "Starting debug container..."

  echo
  echo "Artifacts are mounted at:"
  echo
  echo "    /artifacts"
  echo
  echo "Core dumps found:"
  echo

  find "${artifact_dir}" \
      -type f \
      -name '*.core' \
      -printf '    /artifacts/%P\n'

  echo
  echo "To inspect a core dump with gdb:"
  echo "    gdb /usr/bin/<service> <core-dump>"
  echo

  podman run \
    --rm \
    -it \
    --hostname cta-debug \
    -v "${artifact_dir}:/artifacts:ro,Z" \
    "${debug_image_name}"

}

# =========================================================================
#  Main
# =========================================================================

main() {

  parse_arguments "$@"

  check_prerequisites
  verify_gitlab_auth
  check_registry_login
  query_pipeline

  artifact_dir="${CACHE_DIR}/${pipeline}"
  rm -rf "${artifact_dir}"
  mkdir -p "${artifact_dir}"

  ensure_debug_image
  download_artifacts
  launch_debug_container
}

main "$@"
