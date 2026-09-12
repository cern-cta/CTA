#!/bin/bash -e

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"

usage() {
  echo
  echo "Usage: $0 [options] -t|--tag <image_tag> -s|--package-src <package source>"
  echo
  echo "Builds CTA images from locally built packages."
  echo "  -t, --tag <image_tag>:          Container image tag. For example \"-t dev\""
  echo "  -s, --package-src <path>:       Path to the packages to install. Can be absolute or relative to the current directory."
  echo
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "      --dockerfile <path>:            Path to the Dockerfile (default: 'ci/docker/cta/{defaultplatform}/prod.Dockerfile')."
  echo "      --enable-internal-repos:        Use internal package repositories instead of public ones."
  echo "      --enable-oracle-support:        Build the images for use with the Oracle catalogue."
  echo
  exit 1
}

project_root=$(git rev-parse --show-toplevel)

# Default values
package_src=""
image_tag=""
default_platform=$(jq -r .dev.defaultPlatform "${project_root}/project.json")
dockerfile_path="ci/docker/cta/${default_platform}/prod.Dockerfile"
enable_debug_image=false
enable_internal_repos="0"
enable_oracle_support="0"

while [[ "$#" -gt 0 ]]; do
  case "$1" in
  -h | --help) usage ;;
  -s | --package-src | --rpm-src)
    if [[ $# -gt 1 ]]; then
      package_src=$(realpath "$2")
      shift
    else
      error_usage "-s|--package-src requires an argument"
    fi
    ;;
  -t | --tag)
    if [[ $# -gt 1 ]]; then
      image_tag="$2"
      shift
    else
      error_usage "-t|--tag requires an argument"
    fi
    ;;
  --enable-debug-image) enable_debug_image=true ;;
  --enable-internal-repos) enable_internal_repos="1" ;;
  --enable-oracle-support) enable_oracle_support="1" ;;
  --dockerfile)
    if [[ $# -gt 1 ]]; then
      dockerfile_path="$2"
      shift
    else
      error_usage "--dockerfile requires an argument"
    fi
    ;;
  *)
    die_usage "Unsupported argument: $1"
    ;;
  esac
  shift
done

if [[ -z "${image_tag}" ]]; then
  die_usage "Missing mandatory argument -t | --tag"
fi

if [[ -z "${package_src}" ]]; then
  die_usage "Missing mandatory argument -s | --package-src"
fi

# Use registries.conf to configure proxy for image pulling.
build_command=(env REGISTRIES_CONFIG_PATH="${project_root}/ci/docker/registries.conf" podman build)

cd "$(dirname ${dockerfile_path})"
dockerfile="$(basename ${dockerfile_path})"

colors=(
  $_log_blue
  $_log_yellow
  $_log_magenta
  $_log_cyan
  $_log_orange
  $_log_purple
  $_log_teal
  $_log_sky
)
targets=(
  "cta-taped"
  "cta-maintd"
  "cta-rmcd"
  "cta-frontend"
  "cta-tools"
)
if [[ "$enable_debug_image" == "true" ]]; then
  targets+=( "cta-debug" )
fi

declare -A previous_image_ids=()
for target in "${targets[@]}"; do
  previous_image_ids["$target"]="$(podman image inspect \
    --format '{{.Id}}' "cta/ctageneric/${target}:${image_tag}" 2>/dev/null || true)"
done

BUILD_ID=$(date +%Y%m%d-%H%M%S)
SECONDS=0

build_target() {
  local target="$1"
  local color="$2"
  local image_ref="cta/ctageneric/${target}:${image_tag}"

  (
    "${build_command[@]}" . -f "${dockerfile}" \
      -t "${image_ref}" \
      --build-context package_context="${package_src}" \
      --build-arg ENABLE_INTERNAL_REPOS=${enable_internal_repos} \
      --build-arg ENABLE_ORACLE_SUPPORT=${enable_oracle_support} \
      --build-arg SUPPRESS_BUILD_SERVICE_STDOUT=true \
      --network host \
      --label build.id="$BUILD_ID" \
      --target "$target"
  ) 2>&1 | # some magic to get color output
    awk -v prefix="[$target]:" -v color="$color" '
      {
        printf "%s%s\033[0m %s\n", color, prefix, $0
        fflush()
      }
    '
}

# Build only the common stages first. Starting every service target at once on a
# clean cache makes the independent builder processes duplicate repo-builder and
# base before any of them can reuse the resulting layers.
base_cache_ref="cta/ctageneric/cta-build-base-cache:${image_tag}"

log_task "Building base to populate the shared stage cache..."
if ! "${build_command[@]}" . -f "${dockerfile}" \
  -t "${base_cache_ref}" \
  --build-context package_context="${package_src}" \
  --build-arg SUPPRESS_BUILD_SERVICE_STDOUT=true \
  --network host \
  --target base; then
  log_error "Failed to build the shared base stage."
  exit 1
fi

echo

pids=()

# The common stages are now cached, so build the remaining targets in parallel.
i=0
for target in "${targets[@]}"; do
  color="${colors[$((i % ${#colors[@]}))]}"
  (( ++i ))
  build_target "$target" "$color" &
  pids+=($!)
done

status=0
for pid in "${pids[@]}"; do
  wait "$pid" || status=1
done

if [[ $status == 1 ]]; then
  log_error "Failed to build one or more container images."
  exit "$status"
fi

log_task "Cleaning up superseded CTA images..."
for target in "${targets[@]}"; do
  previous_image_id="${previous_image_ids[$target]}"
  [[ -z "$previous_image_id" ]] && continue
  new_image_id="$(podman image inspect \
    --format '{{.Id}}' "cta/ctageneric/${target}:${image_tag}" 2>/dev/null || true)"
  if [[ -n "$new_image_id" && "$previous_image_id" != "$new_image_id" ]]; then
    podman image rm "$previous_image_id" >/dev/null 2>&1 || true
  fi
done

echo
echo "Built images:"
podman images --filter "label=build.id=$BUILD_ID"
echo
log_success "Built container images in ${SECONDS} seconds."
