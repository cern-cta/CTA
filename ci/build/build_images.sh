#!/bin/bash -e

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"

usage() {
  echo
  echo "Usage: $0 [options] -t|--tag <image_tag> -s|--rpm-src <rpm source>"
  echo
  echo "Builds an image based on the CTA rpms"
  echo "  -t, --tag <image_tag>:          Docker image tag. For example \"-t dev\""
  echo "  -s, --rpm-src <rpm source>:     Path to the RPMs to be installed. Can be absolute or relative to where the script is executed from. For example \"-s build_rpm/RPM/RPMS/x86_64\""
  echo
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -l, --load-into-k8s:                Load images from the selected container runtime into the detected local Kubernetes setup."
  echo "  -c, --container-runtime <runtime>:  Container runtime to use. Docker requires the Buildx plugin. Defaults to podman."
  echo "      --dockerfile <path>:            Path to the Dockerfile (default: 'ci/docker/cta/{defaultplatform}/prod.Dockerfile')."
  echo "      --enable-internal-repos:        Use the internal yum repos instead of the public repos."
  echo "      --enable-oracle-support:        Build the images for use with the Oracle catalogue."
  echo "      --skip-image-cleanup:           Keep superseded images in the container runtime."
  echo
  exit 1
}

project_root=$(git rev-parse --show-toplevel)

# Default values
rpm_src=""
image_tag=""
container_runtime="podman"
default_platform=$(jq -r .dev.defaultPlatform "${project_root}/project.json")
dockerfile_path="ci/docker/cta/${default_platform}/prod.Dockerfile"
load_into_k8s=false
enable_debug_image=false
enable_internal_repos="0"
enable_oracle_support="0"
image_cleanup=true

while [[ "$#" -gt 0 ]]; do
  case "$1" in
  -h | --help) usage ;;
  -c | --container-runtime)
    if [[ $# -gt 1 ]]; then
      if [[ "$2" != "docker" ]] && [[ "$2" != "podman" ]]; then
        error_usage "-c | --container-runtime is \"$2\" but must be one of [docker, podman]."
      fi
      container_runtime="$2"
      shift
    else
      error_usage "-c | --container-runtime requires an argument"
    fi
    ;;
  -s | --rpm-src)
    if [[ $# -gt 1 ]]; then
      rpm_src=$(realpath "$2")
      shift
    else
      error_usage "-s|--rpm-src requires an argument"
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
  -l | --load-into-k8s) load_into_k8s=true ;;
  --enable-debug-image) enable_debug_image=true ;;
  --enable-internal-repos) enable_internal_repos="1" ;;
  --enable-oracle-support) enable_oracle_support="1" ;;
  --skip-image-cleanup) image_cleanup=false ;;
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

if [[ -z "${rpm_src}" ]]; then
  die_usage "Missing mandatory argument -s | --rpm-src"
fi

build_command=("$container_runtime" build)
if [[ $container_runtime == docker ]]; then
  if ! docker buildx version >/dev/null 2>&1; then
    die "Docker Buildx is required to build CTA images. Install the Docker Buildx plugin or use Podman."
  fi
  build_command=(docker buildx build --load)
fi

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
if [[ "$image_cleanup" == "true" ]]; then
  for target in "${targets[@]}"; do
    previous_image_ids["$target"]="$(${container_runtime} image inspect \
      --format '{{.Id}}' "cta/ctageneric/${target}:${image_tag}" 2>/dev/null || true)"
  done
fi

BUILD_ID=$(date +%Y%m%d-%H%M%S)
SECONDS=0

build_target() {
  local target="$1"
  local color="$2"
  local image_ref="cta/ctageneric/${target}:${image_tag}"

  (
    set -eo pipefail
    "${build_command[@]}" . -f "${dockerfile}" \
      -t "${image_ref}" \
      --build-context rpm_context="${rpm_src}" \
      --build-arg ENABLE_INTERNAL_REPOS=${enable_internal_repos} \
      --build-arg ENABLE_ORACLE_SUPPORT=${enable_oracle_support} \
      --network host \
      --label build.id="$BUILD_ID" \
      --target "$target"
    # Note that the below checks are rather crude (for speed)
    if [[ "$load_into_k8s" == "true" ]]; then
      # Load into minikube (use stdin to avoid a temp file)
      if command -v minikube >/dev/null 2>&1; then
        log_task "Loading ${image_ref} into minikube..."
        ${container_runtime} save "${image_ref}" | minikube image load --overwrite -
      fi

      # Load into k3s (stream into containerd)
      if command -v k3s >/dev/null 2>&1; then
        log_task "Loading ${image_ref} into k3s/containerd..."
        ${container_runtime} save "${image_ref}" | sudo /usr/local/bin/k3s ctr images import -
      fi
    fi
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
  --build-context rpm_context="${rpm_src}" \
  --network host \
  --target base; then
  log_error "Failed to build the shared base stage."
  exit 1
fi

echo

pids=()

# The common stages are now cached, so build and load the remaining targets in
# parallel.
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
  log_error "Failed to build or load one or more container images."
  exit "$status"
fi

if [[ "$image_cleanup" == "true" ]]; then
  log_task "Cleaning up superseded CTA images..."
  for target in "${targets[@]}"; do
    previous_image_id="${previous_image_ids[$target]}"
    [[ -z "$previous_image_id" ]] && continue
    new_image_id="$(${container_runtime} image inspect \
      --format '{{.Id}}' "cta/ctageneric/${target}:${image_tag}" 2>/dev/null || true)"
    if [[ -n "$new_image_id" && "$previous_image_id" != "$new_image_id" ]]; then
      ${container_runtime} image rm "$previous_image_id" >/dev/null 2>&1 || true
    fi
  done
fi

echo
echo "Built images:"
${container_runtime} images --filter "label=build.id=$BUILD_ID"
echo
log_success "Built and loaded container images in ${SECONDS} seconds."
