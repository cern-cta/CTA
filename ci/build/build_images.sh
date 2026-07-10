#!/bin/bash -e

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_wrapper.sh"

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
  echo "  -l, --load-into-k8s:                Takes the image from the podman registry and ensures that it is present in the image registry used by the local K8s setup."
  echo "  -c, --container-runtime <runtime>:  The container runtime to use for the build container. Defaults to podman."
  echo "      --dockerfile <path>:            Path to the Dockerfile (default: 'ci/docker/{defaultplatform}/prod.Dockerfile')."
  echo "      --use-internal-repos:           Use the internal yum repos instead of the public repos."
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
use_internal_repos="0"
use_oracle_catalogue="0"

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
  -l | --load-into-k8s)
    load_into_k8s=true
    ;;
  --use-internal-repos)
    use_internal_repos="1"
    ;;
  --use-oracle-catalogue)
    use_oracle_catalogue="1"
    ;;
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

cd "$(dirname ${dockerfile_path})"
dockerfile="$(basename ${dockerfile_path})"
colors=(
  $'\e[31m' # red
  $'\e[32m' # green
  $'\e[33m' # yellow
  $'\e[34m' # blue
  $'\e[35m' # magenta
  $'\e[36m' # cyan
)
targets=(
  "cta-taped"
  "cta-maintd"
  "cta-rmcd"
  "cta-frontend"
  "cta-tools"
)
BUILD_ID=$(date +%Y%m%d-%H%M%S)
SECONDS=0

pids=()

# Build and load all targets
i=0
for target in "${targets[@]}"; do
  color="${colors[$((i % ${#colors[@]}))]}"
  (( ++i ))
  image_ref="cta/ctageneric/${target}:${image_tag}"
  (
    set -eo pipefail
    build() {
      ${container_runtime} build . -f ${dockerfile} \
        -t ${image_ref} \
        --build-context rpm_context="${rpm_src}" \
        --build-arg USE_INTERNAL_REPOS=${use_internal_repos} \
        --build-arg USE_ORACLE_CATALOGUE=0 \
        --network host \
        --label build.id="$BUILD_ID" \
        --target $target
      # Note that the below checks are rather crude (for speed)
      if [[ "$load_into_k8s" == "true" ]]; then
        # Load into minikube (use stdin to avoid a temp file)
        if command -v minikube >/dev/null 2>&1; then
          echo "Minikube detected -> loading $image_ref into minikube"
          ${container_runtime} save "${image_ref}" | minikube image load --overwrite -
        fi

        # Load into k3s (stream into containerd)
        if command -v k3s >/dev/null 2>&1; then
          echo "k3s detected -> loading $image_ref into k3s/containerd"
          ${container_runtime} save "${image_ref}" | sudo /usr/local/bin/k3s ctr images import -
        fi
      fi
    }
    build
  ) 2>&1 | # some magic to get color output
    awk -v prefix="[$target]:" -v color="$color" '
      {
        printf "%s%s\033[0m %s\n", color, prefix, $0
        fflush()
      }
    ' & # execute as background process so that we can build in parallel
  pids+=($!)
done

status=0
for pid in "${pids[@]}"; do
  wait "$pid" || status=1
done

if [[ $status == 1 ]]; then
  echo "Image building/loading failed"
  exit "$status"
fi

echo "Built the following images in ${SECONDS}s:"
podman images --filter "label=build.id=$BUILD_ID"
exit "$status"
