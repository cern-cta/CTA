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

list_build_containers() {
  podman ps --all --external --format json \
    | jq -r '.[] | select(.State == "storage" and .Command == ["buildah"]) | .Id'
}

# Preserve existing working containers. Unrelated builds must not run concurrently under this user.
initial_build_containers=$(list_build_containers)
declare -A existing_build_containers=()
while IFS= read -r container_id; do
  [[ -z "$container_id" ]] || existing_build_containers["$container_id"]=1
done <<< "$initial_build_containers"

# Use registries.conf to configure proxy for image pulling.
build_command=(setsid env REGISTRIES_CONFIG_PATH="${project_root}/ci/docker/registries.conf"
  podman build)

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
for target in "${targets[@]}" cta-build-base-cache; do
  previous_image_ids["$target"]="$(podman image inspect \
    --format '{{.Id}}' "cta/ctageneric/${target}:${image_tag}" 2>/dev/null || true)"
done

# Track Podman itself, rather than a shell wrapping an output pipeline.
# Separate sessions keep terminal signals from interrupting cleanup a second time.
declare -A active_build_pids=()

# Podman can exit on a signal before Buildah's deferred cleanup runs.
# Remove only working containers that appeared since the initial snapshot.
remove_build_containers() {
  local current_build_containers container_id
  local container_ids=()

  if ! current_build_containers=$(list_build_containers); then
    log_warn "Could not list leftover working containers for this image build."
    return
  fi

  while IFS= read -r container_id; do
    [[ -n "$container_id" ]] || continue
    if [[ -z "${existing_build_containers[$container_id]:-}" ]]; then
      container_ids+=("$container_id")
    fi
  done <<< "$current_build_containers"

  if (( ${#container_ids[@]} )); then
    log_task "Removing ${#container_ids[@]} leftover working containers from this build..."
    podman rm --force --ignore "${container_ids[@]}" >/dev/null \
      || log_warn "Could not remove all working containers for this image build."
  fi
}

cancel_builds() {
  local exit_status="$1"
  local pid

  trap '' INT TERM
  trap - EXIT
  if (( ${#active_build_pids[@]} )); then
    log_warn "Stopping image builds and waiting for Podman cleanup..."
    for pid in "${!active_build_pids[@]}"; do
      kill -TERM "$pid" 2>/dev/null || true
    done
    for pid in "${!active_build_pids[@]}"; do
      wait "$pid" || true
    done
  fi
  remove_build_containers
  exit "$exit_status"
}

wait_build() {
  local status=0
  wait "$1" || status=$?
  unset "active_build_pids[$1]"
  return "$status"
}

trap 'cancel_builds 130' INT
trap 'cancel_builds 143' TERM
trap 'cancel_builds "$?"' EXIT

BUILD_ID=$(date +%Y%m%d-%H%M%S)
SECONDS=0

build_target() {
  local target="$1"
  local color="$2"
  local image_ref="cta/ctageneric/${target}:${image_tag}"

  "${build_command[@]}" . -f "${dockerfile}" \
    -t "${image_ref}" \
    --build-context package_context="${package_src}" \
    --build-arg ENABLE_INTERNAL_REPOS=${enable_internal_repos} \
    --build-arg ENABLE_ORACLE_SUPPORT=${enable_oracle_support} \
    --build-arg SUPPRESS_BUILD_SERVICE_STDOUT=true \
    --network host \
    --label build.id="$BUILD_ID" \
    --target "$target" > >(
      # Keep draining output while Podman handles cancellation and removes build containers.
      trap '' INT TERM
      awk -v prefix="[$target]:" -v color="$color" '
        {
          printf "%s%s\033[0m %s\n", color, prefix, $0
          fflush()
        }
      '
    ) 2>&1 &
  pids+=("$!")
  active_build_pids[$!]=1
}

# Warm the base and its RPM repository dependency before parallel service builds.
base_cache_ref="cta/ctageneric/cta-build-base-cache:${image_tag}"

log_task "Building base to populate the shared stage cache..."
"${build_command[@]}" . -f "${dockerfile}" \
  -t "${base_cache_ref}" \
  --build-context package_context="${package_src}" \
  --build-arg SUPPRESS_BUILD_SERVICE_STDOUT=true \
  --network host \
  --target base &
base_pid=$!
active_build_pids[$base_pid]=1
if ! wait_build "$base_pid"; then
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
  build_target "$target" "$color"
done

status=0
successful_targets=()
for i in "${!pids[@]}"; do
  if wait_build "${pids[$i]}"; then
    successful_targets+=("${targets[$i]}")
  else
    status=1
  fi
done

# Clean up the base after the superseded service images that may depend on it.
successful_targets+=(cta-build-base-cache)

log_task "Cleaning up superseded CTA images..."
for target in "${successful_targets[@]}"; do
  previous_image_id="${previous_image_ids[$target]}"
  [[ -z "$previous_image_id" ]] && continue
  if ! new_image_id="$(podman image inspect \
    --format '{{.Id}}' "cta/ctageneric/${target}:${image_tag}" 2>/dev/null)"; then
    continue
  fi
  if [[ -n "$new_image_id" && "$previous_image_id" != "$new_image_id" ]]; then
    podman image rm "$previous_image_id" >/dev/null 2>&1 || true
  fi
done

if [[ $status == 1 ]]; then
  log_error "Failed to build one or more container images."
  exit "$status"
fi

echo
echo "Built images:"
podman images --filter "label=build.id=$BUILD_ID"
echo
log_success "Built container images in ${SECONDS} seconds."
