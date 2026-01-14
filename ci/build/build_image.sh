#!/bin/bash -e

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_wrapper.sh"

usage() {
  echo
  echo "Usage: $0 [options] -t|--tag <image_tag> -s|--rpm-src <rpm source>"
  echo
  echo "Builds an image based on the CTA rpms"
  echo "  -t, --tag <image_tag>:          Docker image tag. For example \"-t dev\""
  echo "  -s, --rpm-src <rpm source>:     Path to the RPMs to be installed. Can be absolute or relative to where the script is executed from. For example \"-s build_rpm/RPM/RPMS/x86_64\""
  echo "      --rpm-version <version>:    Version of the RPMs to ensure the correct RPMs are copied from the RPM source. Only files of the structure \"*{version}*.rpm\" will be copied."
  echo
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -n, --name:                         The Docker image name. Defaults to ctageneric"
  echo "  -l, --load-into-k8s:                Takes the image from the podman registry and ensures that it is present in the image registry used by the local K8s setup."
  echo "  -c, --container-runtime <runtime>:  The container runtime to use for the build container. Defaults to podman."
  echo "      --dockerfile <path>:            Path to the Dockerfile (default: 'ci/docker/{defaultplatform}/dev-local-rpms.Dockerfile'). Should be relative to the repository root."
  echo "      --use-internal-repos:           Use the internal yum repos instead of the public repos."
  echo "      --target <target>:              Set the target build stage to build."
  echo
  exit 1
}

buildImage() {
  project_root=$(git rev-parse --show-toplevel)

  # Default values
  local rpm_src=""
  local image_tag=""
  local container_runtime="podman"
  local rpm_default_src="image_rpms"
  local defaultPlatform=$(jq -r .dev.defaultPlatform "${project_root}/project.json")
  local dockerfile="ci/docker/${defaultPlatform}/prod.Dockerfile"
  local load_into_k8s=false
  # Note that the capitalization here is intentional as this is passed directly as a build arg
  local use_internal_repos="FALSE"
  local rpm_version=""
  local target=""

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
    --rpm-version)
      if [[ $# -gt 1 ]]; then
        rpm_version="$2"
        shift
      else
        error_usage "--rpm-version requires an argument"
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
    --target)
      if [[ $# -gt 1 ]]; then
        target="$2"
        shift
      else
        error_usage "--target requires an argument"
      fi
      ;;
    -l | --load-into-k8s)
      load_into_k8s=true
      ;;
    --use-internal-repos)
      use_internal_repos="TRUE"
      ;;
    --dockerfile)
      if [[ $# -gt 1 ]]; then
        dockerfile="$2"
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

  if [[ -z "${rpm_version}" ]]; then
    die_usage "Missing mandatory argument --rpm-version"
  fi

  # navigate to root directory
  cd "${project_root}"

  # Ensure we clean up before we start to ensure we don't copy any existing and potentially unrelated things
  rm -rf "${rpm_default_src}"
  mkdir -p "${rpm_default_src}"

  # Copy the rpms into a predefined rpm directory
  # This is important to ensure that the RPMs are accessible from the Docker build context
  # (as the provided location might be outside of the project root)
  cp -r ${rpm_src}/*${rpm_version}*.rpm "${rpm_default_src}"

  # TODO: handle grpc
  targets=(
    "cta-taped"
    "cta-maintd"
    "cta-rmcd"
    "cta-frontend-xrd"
    "cta-tools-xrd"
  )

  for target in "${targets[@]}"; do
    image_ref="${target}:${image_tag}"

    echo "Building ${tag} from $dockerfile --target $target"

    (
      set -x
      ${container_runtime} build . -f ${dockerfile} \
        -t ${image_ref} \
        --network host \
        --build-arg USE_INTERNAL_REPOS=${use_internal_repos} \
        --target $target
    )

    if [[ "$load_into_k8s" == "true" ]]; then
      # Note that the below checks are rather crude (for speed)

      # Load into minikube (use stdin to avoid a temp file)
      if command -v minikube >/dev/null 2>&1; then
        echo "Minikube detected -> loading image into minikube"
        ${container_runtime} save "localhost/${image_ref}" | minikube image load --overwrite -
      fi

      # Load into k3s (stream into containerd, no temp file)
      if command -v k3s >/dev/null 2>&1; then
        echo "k3s detected -> loading image into k3s/containerd"
        ${container_runtime} save "localhost/${image_ref}" | sudo /usr/local/bin/k3s ctr images import -
      fi
    fi
  done

  # Clean up again
  rm -rf "${rpm_default_src}"

}

buildImage "$@"
